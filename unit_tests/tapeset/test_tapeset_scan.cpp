/*
 * Copyright 2008 Search Solution Corporation
 * Copyright 2016 CUBRID Corporation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

/*
 * test_tapeset_scan.cpp - Synthetic N-Tape split gate (redesign G005, issue #70).
 *
 * Drives the new offset-arithmetic scan state machine (qfile::tapeset_scan)
 * over genuinely multi-Tape in-memory Tapesets -- no server boot, no page
 * buffer, no next_vpid.  This exercises the actually-new code (tape_idx
 * threading, page_offset arithmetic, empty-skip, cross-Tape forward/reverse/
 * jump, terminal, S_END-on-last) directly, avoiding the "passthrough-tautology"
 * gate anti-pattern (evidence #76 I-2): a legacy 1-Tape adapter would verify
 * none of this.
 */

#include "qfile_tape.hpp"
#include "qfile_chunk.hpp"
#include "object_representation.h"
#include "error_code.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <string>
#include <thread>
#include <atomic>
#include <algorithm>

#include <unistd.h>
#include <sys/stat.h>

namespace
{
  /* A test tuple is [length(4) | prev_length(4) | id(4) | pad(4)] = 16 bytes.
   * The scan only reads tuple length / prev length / tuple count / last-tuple
   * offset / overflow id; the id payload lets the test verify tuple identity. */
  const int TEST_PAGE_BUF_SIZE = 1024;
  const int TEST_TUPLE_LEN = 16;
  const int TEST_ID_OFFSET = 8;

  /* Build one list page holding the given tuple ids.  An empty id list yields a
   * zero-tuple page (which the scan must skip).  Caller frees with free, or
   * hands ownership to a memory_tape created with owns == true. */
  char *
  make_page (const std::vector<int> &ids)
  {
    char *page = (char *) malloc (TEST_PAGE_BUF_SIZE);
    std::memset (page, 0, TEST_PAGE_BUF_SIZE);

    int n = (int) ids.size ();
    QFILE_PUT_TUPLE_COUNT (page, n);
    QFILE_PUT_PREV_VPID_NULL (page);
    QFILE_PUT_NEXT_VPID_NULL (page);
    QFILE_PUT_OVERFLOW_VPID_NULL (page);

    int off = QFILE_PAGE_HEADER_SIZE;
    int prev_len = 0;
    for (int i = 0; i < n; i++)
      {
	char *tpl = page + off;
	QFILE_PUT_TUPLE_LENGTH (tpl, TEST_TUPLE_LEN);
	QFILE_PUT_PREV_TUPLE_LENGTH (tpl, prev_len);
	OR_PUT_INT (tpl + TEST_ID_OFFSET, ids[i]);
	QFILE_PUT_LAST_TUPLE_OFFSET (page, off);
	prev_len = TEST_TUPLE_LEN;
	off += TEST_TUPLE_LEN;
      }
    return page;
  }

  int
  tuple_id (const QFILE_TUPLE tpl)
  {
    return OR_GET_INT (tpl + TEST_ID_OFFSET);
  }

  /* Build the standard multi-Tape fixture and the expected forward id sequence.
   *   Tape 0: page{0,1,2}, page{3,4}            (multi-page tape)
   *   Tape 1: (no pages)                        (empty-skip)
   *   Tape 2: page{5}, page{} , page{6,7}       (zero-tuple-page skip + terminal)
   * Expected: 0 1 2 3 4 5 6 7
   * The Tapeset owns its Tapes; each memory_tape owns its page buffers. */
  void
  build_fixture (qfile::tapeset &ts, std::vector<int> &expected)
  {
    ts.set_owns_tapes (true);

    qfile::memory_tape *t0 = new qfile::memory_tape (true);
    t0->append_page (make_page ({ 0, 1, 2 }));
    t0->append_page (make_page ({ 3, 4 }));
    ts.append_tape (t0);

    qfile::memory_tape *t1 = new qfile::memory_tape (true);	/* empty */
    ts.append_tape (t1);

    qfile::memory_tape *t2 = new qfile::memory_tape (true);
    t2->append_page (make_page ({ 5 }));
    t2->append_page (make_page (std::vector<int> ()));		/* zero-tuple page */
    t2->append_page (make_page ({ 6, 7 }));
    ts.append_tape (t2);

    expected = { 0, 1, 2, 3, 4, 5, 6, 7 };
  }

  /* ---- Phase1 1B (issue #71) file-backed helpers ------------------- */

  /* A DB_PAGESIZE list page (the BufFile reads/writes full 16KB pages, unlike
   * the 1A in-RAM tapes that only needed a small header buffer). */
  char *
  make_db_page (const std::vector<int> &ids)
  {
    char *page = (char *) malloc (DB_PAGESIZE);
    std::memset (page, 0, DB_PAGESIZE);

    int n = (int) ids.size ();
    QFILE_PUT_TUPLE_COUNT (page, n);
    QFILE_PUT_PREV_VPID_NULL (page);
    QFILE_PUT_NEXT_VPID_NULL (page);
    QFILE_PUT_OVERFLOW_VPID_NULL (page);

    int off = QFILE_PAGE_HEADER_SIZE;
    int prev_len = 0;
    for (int i = 0; i < n; i++)
      {
	char *tpl = page + off;
	QFILE_PUT_TUPLE_LENGTH (tpl, TEST_TUPLE_LEN);
	QFILE_PUT_PREV_TUPLE_LENGTH (tpl, prev_len);
	OR_PUT_INT (tpl + TEST_ID_OFFSET, ids[i]);
	QFILE_PUT_LAST_TUPLE_OFFSET (page, off);
	prev_len = TEST_TUPLE_LEN;
	off += TEST_TUPLE_LEN;
      }
    return page;
  }

  const char *
  test_scratch_dir ()
  {
    static std::string dir;
    if (dir.empty ())
      {
	char buf[256];
	std::snprintf (buf, sizeof (buf), "/tmp/cubrid_buffile_ut_%ld", (long) getpid ());
	dir = buf;
      }
    return dir.c_str ();
  }

  struct writer_result
  {
    qfile::tape *tape;		/* frozen tape (caller owns / hands to a tapeset) */
    bool spilled;
    long producer_pgbuf_fixes;	/* MUST be 0 (producer-side pgbuf-bypass) */
    long pages_appended_to_file;
    int file_pages;
  };

  /* Drive a tape_writer over `pages` with the given prefix budget; freeze.
   * Producer metrics are snapshotted before freeze (which transfers the
   * BufFile out of the writer). */
  writer_result
  build_writer_tape (int budget, const std::vector<std::vector<int> > &pages)
  {
    static std::uint64_t seq = 1000;
    writer_result r = { NULL, false, -1, 0, 0 };

    qfile::tape_writer w (budget, TDE_ALGORITHM_NONE, test_scratch_dir (), seq++, 0);
    for (const std::vector<int> &ids : pages)
      {
	char *p = make_db_page (ids);
	int rc = w.append_page (NULL, (PAGE_PTR) p);
	free (p);
	if (rc != NO_ERROR)
	  {
	    return r;		/* tape == NULL signals failure */
	  }
      }

    r.spilled = w.spilled ();
    r.file_pages = w.file_pages ();
    if (r.spilled && w.file_metrics () != NULL)
      {
	r.producer_pgbuf_fixes = w.file_metrics ()->pgbuf_fixes;
	r.pages_appended_to_file = w.file_metrics ()->pages_appended;
      }
    else
      {
	r.producer_pgbuf_fixes = 0;
      }
    r.tape = w.freeze (NULL);
    return r;
  }

  int
  scan_forward_ids (qfile::tapeset &ts, std::vector<int> &out, long *scan_pgbuf_fixes)
  {
    qfile::tapeset_scan scan (&ts);
    QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
    SCAN_CODE code;
    while ((code = scan.forward (NULL, &tplrec, PEEK)) == S_SUCCESS)
      {
	out.push_back (tuple_id (tplrec.tpl));
      }
    if (scan_pgbuf_fixes != NULL)
      {
	*scan_pgbuf_fixes = scan.metrics ().pgbuf_fixes;
      }
    scan.close (NULL);
    return (code == S_END) ? 0 : 1;
  }

  /* G8: a spilled file-backed Tape returns identical tuples forward (robust
   * parity) and reversed backward; producer + scan do 0 pgbuf fixes. */
  int
  run_file_parity ()
  {
    std::vector<std::vector<int> > pages = {
      { 0, 1, 2 }, { 3, 4 }, { 5, 6, 7, 8 }, { 9 }, { 10, 11 }, { 12 }
    };
    std::vector<int> expected;
    for (const std::vector<int> &p : pages)
      {
	for (int id : p)
	  {
	    expected.push_back (id);
	  }
      }

    writer_result wr = build_writer_tape (/*budget*/ 2, pages);
    if (wr.tape == NULL)
      {
	return 1;
      }
    if (!wr.spilled || wr.file_pages != 4 || wr.producer_pgbuf_fixes != 0)
      {
	delete wr.tape;
	return 2;
      }

    qfile::tapeset ts;
    ts.set_owns_tapes (true);
    ts.append_tape (wr.tape);

    /* forward parity */
    std::vector<int> got;
    long scan_fixes = -1;
    if (scan_forward_ids (ts, got, &scan_fixes) != 0 || got != expected || scan_fixes != 0)
      {
	return 3;
      }

    /* backward == reversed */
    {
      qfile::tapeset_scan scan (&ts);
      QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
      SCAN_CODE code;
      while ((code = scan.forward (NULL, &tplrec, PEEK)) == S_SUCCESS)
	{
	  ;
	}
      if (code != S_END)
	{
	  return 4;
	}
      std::vector<int> back;
      while ((code = scan.backward (NULL, &tplrec, PEEK)) == S_SUCCESS)
	{
	  back.push_back (tuple_id (tplrec.tpl));
	}
      std::vector<int> reversed (expected.rbegin (), expected.rend ());
      if (code != S_END || back != reversed || scan.metrics ().pgbuf_fixes != 0)
	{
	  return 5;
	}
      scan.close (NULL);
    }

    /* save + jump across the spill boundary (prefix page 1 -> file pages) */
    {
      qfile::tapeset_scan scan (&ts);
      QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
      std::vector<QFILE_TUPLE_POSITION> positions;
      SCAN_CODE code;
      while ((code = scan.forward (NULL, &tplrec, PEEK)) == S_SUCCESS)
	{
	  QFILE_TUPLE_POSITION pos;
	  std::memset (&pos, 0, sizeof (pos));
	  scan.save_position (&pos);
	  positions.push_back (pos);
	}
      if (code != S_END || (int) positions.size () != (int) expected.size ())
	{
	  return 6;
	}
      /* probe ids that span prefix(0..4) and file(5..12) */
      const int probes[] = { 1, 5, 8, 12 };
      for (int p = 0; p < 4; p++)
	{
	  int k = probes[p];
	  if (scan.jump (NULL, &positions[k], &tplrec, PEEK) != S_SUCCESS || tuple_id (tplrec.tpl) != expected[k])
	    {
	      return 10 + p;
	    }
	}
      scan.close (NULL);
    }
    return 0;
  }

  /* G9: tiny result fits the work buffer -> no spill, all-RAM Tape (no file). */
  int
  run_tiny_no_spill ()
  {
    std::vector<std::vector<int> > pages = { { 0, 1 }, { 2 }, { 3, 4 } };
    std::vector<int> expected = { 0, 1, 2, 3, 4 };

    writer_result wr = build_writer_tape (/*budget*/ 10, pages);
    if (wr.tape == NULL)
      {
	return 1;
      }
    if (wr.spilled || wr.file_pages != 0)	/* tiny-no-spill: disk untouched */
      {
	delete wr.tape;
	return 2;
      }

    qfile::tapeset ts;
    ts.set_owns_tapes (true);
    ts.append_tape (wr.tape);

    std::vector<int> got;
    long scan_fixes = -1;
    if (scan_forward_ids (ts, got, &scan_fixes) != 0 || got != expected || scan_fixes != 0)
      {
	return 3;
      }
    return 0;
  }

  /* G10: a multi-Tape Tapeset mixing spilled and tiny Tapes scans as one stream;
   * every spilled producer did 0 pgbuf fixes (producer-side bypass gate). */
  int
  run_file_multitape ()
  {
    writer_result t0 = build_writer_tape (1, { { 0, 1, 2 }, { 3, 4 } });		/* spill */
    writer_result t1 = build_writer_tape (10, { { 5 } });				/* tiny */
    writer_result t2 = build_writer_tape (1, { { 6, 7 }, { 8 }, { 9, 10 } });	/* spill */
    if (t0.tape == NULL || t1.tape == NULL || t2.tape == NULL)
      {
	return 1;
      }
    if (!t0.spilled || t1.spilled || !t2.spilled)
      {
	return 2;
      }
    if (t0.producer_pgbuf_fixes != 0 || t2.producer_pgbuf_fixes != 0)
      {
	return 3;
      }

    qfile::tapeset ts;
    ts.set_owns_tapes (true);
    ts.append_tape (t0.tape);
    ts.append_tape (t1.tape);
    ts.append_tape (t2.tape);

    std::vector<int> expected = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    std::vector<int> got;
    long scan_fixes = -1;
    if (scan_forward_ids (ts, got, &scan_fixes) != 0 || got != expected || scan_fixes != 0)
      {
	return 4;
      }
    return 0;
  }

  struct testcase
  {
    const char *name;
    int (*run) ();
  };

  /* forward + terminal + S_END-on-last + idempotent S_AFTER */
  int
  run_forward ()
  {
    qfile::tapeset ts;
    std::vector<int> expected;
    build_fixture (ts, expected);

    qfile::tapeset_scan scan (&ts);
    QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
    std::vector<int> got;
    SCAN_CODE code;
    while ((code = scan.forward (NULL, &tplrec, PEEK)) == S_SUCCESS)
      {
	got.push_back (tuple_id (tplrec.tpl));
      }
    if (code != S_END)
      {
	return 1;
      }
    if (got != expected)
      {
	return 2;
      }
    /* S_END must be sticky on S_AFTER (last Tape exhausted). */
    if (scan.forward (NULL, &tplrec, PEEK) != S_END)
      {
	return 3;
      }
    scan.close (NULL);
    return 0;
  }

  /* reverse from S_AFTER + S_BEFORE-on-first */
  int
  run_backward ()
  {
    qfile::tapeset ts;
    std::vector<int> expected;
    build_fixture (ts, expected);

    qfile::tapeset_scan scan (&ts);
    QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
    SCAN_CODE code;

    /* advance to S_AFTER */
    while ((code = scan.forward (NULL, &tplrec, PEEK)) == S_SUCCESS)
      {
	;
      }
    if (code != S_END)
      {
	return 1;
      }

    std::vector<int> got;
    while ((code = scan.backward (NULL, &tplrec, PEEK)) == S_SUCCESS)
      {
	got.push_back (tuple_id (tplrec.tpl));
      }
    if (code != S_END)
      {
	return 2;
      }
    std::vector<int> reversed (expected.rbegin (), expected.rend ());
    if (got != reversed)
      {
	return 3;
      }
    /* S_END sticky on S_BEFORE */
    if (scan.backward (NULL, &tplrec, PEEK) != S_END)
      {
	return 4;
      }
    scan.close (NULL);
    return 0;
  }

  /* save during forward, then jump (incl. cross-Tape and boundary) */
  int
  run_jump ()
  {
    qfile::tapeset ts;
    std::vector<int> expected;
    build_fixture (ts, expected);

    qfile::tapeset_scan scan (&ts);
    QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
    SCAN_CODE code;

    /* collect a saved position for every tuple */
    std::vector<QFILE_TUPLE_POSITION> positions;
    int idx = 0;
    while ((code = scan.forward (NULL, &tplrec, PEEK)) == S_SUCCESS)
      {
	if (tuple_id (tplrec.tpl) != expected[idx])
	  {
	    return 1;
	  }
	QFILE_TUPLE_POSITION pos;
	std::memset (&pos, 0, sizeof (pos));
	scan.save_position (&pos);
	if (!qfile_tuple_position_is_tape (&pos))
	  {
	    return 2;
	  }
	positions.push_back (pos);
	idx++;
      }
    if (code != S_END || (int) positions.size () != (int) expected.size ())
      {
	return 3;
      }

    /* jump to a few positions: first, mid same-tape, cross-tape, terminal */
    const int probes[] = { 0, 3, 6, 7 };
    for (int p = 0; p < (int) (sizeof (probes) / sizeof (probes[0])); p++)
      {
	int k = probes[p];
	if (scan.jump (NULL, &positions[k], &tplrec, PEEK) != S_SUCCESS)
	  {
	    return 10 + p;
	  }
	if (tuple_id (tplrec.tpl) != expected[k])
	  {
	    return 20 + p;
	  }
	/* a forward step after the jump must land on k+1 (or S_END at the end) */
	code = scan.forward (NULL, &tplrec, PEEK);
	if (k + 1 < (int) expected.size ())
	  {
	    if (code != S_SUCCESS || tuple_id (tplrec.tpl) != expected[k + 1])
	      {
		return 30 + p;
	      }
	  }
	else if (code != S_END)
	  {
	    return 40 + p;
	  }
      }

    /* jump to an S_BEFORE / S_AFTER position must yield S_END */
    QFILE_TUPLE_POSITION before_pos;
    std::memset (&before_pos, 0, sizeof (before_pos));
    qfile_tuple_position_set_tape (&before_pos, 0, 0, 0);
    before_pos.position = S_BEFORE;
    if (scan.jump (NULL, &before_pos, &tplrec, PEEK) != S_END)
      {
	return 50;
      }
    QFILE_TUPLE_POSITION after_pos = before_pos;
    after_pos.position = S_AFTER;
    if (scan.jump (NULL, &after_pos, &tplrec, PEEK) != S_END)
      {
	return 51;
      }
    scan.close (NULL);
    return 0;
  }

  /* copy-mode retrieve (pre-sized record so no allocation is needed) */
  int
  run_copy ()
  {
    qfile::tapeset ts;
    std::vector<int> expected;
    build_fixture (ts, expected);

    qfile::tapeset_scan scan (&ts);
    char buf[64];
    QFILE_TUPLE_RECORD tplrec = { buf, (int) sizeof (buf) };
    std::vector<int> got;
    SCAN_CODE code;
    while ((code = scan.forward (NULL, &tplrec, 0 /* copy */)) == S_SUCCESS)
      {
	/* copied into buf; pointer must be our buffer, not the page */
	if (tplrec.tpl != buf)
	  {
	    return 1;
	  }
	got.push_back (tuple_id (tplrec.tpl));
      }
    if (code != S_END || got != expected)
      {
	return 2;
      }
    scan.close (NULL);
    return 0;
  }

  /* drive through the QFILE_LIST_SCAN_ID bridge + verify the state mirror */
  int
  run_bridge ()
  {
    qfile::tapeset ts;
    std::vector<int> expected;
    build_fixture (ts, expected);

    QFILE_LIST_SCAN_ID sid;
    std::memset (&sid, 0, sizeof (sid));
    QFILE_CLEAR_LIST_ID (&sid.list_id);
    QFILE_LIST_ID_TAPESET (&sid.list_id) = &ts;	/* borrow */
    QFILE_LIST_ID_OWNS_TAPESET (&sid.list_id) = false;
    sid.tapeset_scan_ = NULL;

    if (qfile_tapeset_scan_open (&sid) != NO_ERROR || sid.tapeset_scan_ == NULL)
      {
	return 1;
      }

    QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
    std::vector<int> got;
    SCAN_CODE code;
    while ((code = qfile_tapeset_scan_forward (NULL, &sid, &tplrec, PEEK)) == S_SUCCESS)
      {
	/* mirror: the scan_id curr_tpl must equal the returned tuple */
	if (sid.curr_tpl != tplrec.tpl || sid.position != S_ON)
	  {
	    qfile_tapeset_scan_close (NULL, &sid);
	    return 2;
	  }
	got.push_back (tuple_id (tplrec.tpl));
      }
    if (code != S_END || got != expected || sid.position != S_AFTER)
      {
	qfile_tapeset_scan_close (NULL, &sid);
	return 3;
      }

    qfile_tapeset_scan_close (NULL, &sid);
    if (sid.tapeset_scan_ != NULL)
      {
	return 4;
      }
    return 0;
  }

  /* a Tapeset of only empty Tapes must immediately yield S_END */
  int
  run_empty_only ()
  {
    qfile::tapeset ts;
    ts.set_owns_tapes (true);
    ts.append_tape (new qfile::memory_tape (true));
    ts.append_tape (new qfile::memory_tape (true));

    qfile::tapeset_scan scan (&ts);
    QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
    if (scan.forward (NULL, &tplrec, PEEK) != S_END)
      {
	return 1;
      }
    /* backward from S_AFTER over all-empty must also be S_END */
    if (scan.backward (NULL, &tplrec, PEEK) != S_END)
      {
	return 2;
      }
    scan.close (NULL);
    return 0;
  }

  /* measurement hooks (issue #68, 1A-reachable slice): the scan exposes real
   * operation counters; assert the pgbuf-bypass invariant (scan does 0 pgbuf
   * fixes) and the forward/copy counts. */
  int
  run_metrics ()
  {
    qfile::tapeset ts;
    std::vector<int> expected;
    build_fixture (ts, expected);

    /* peek forward pass */
    qfile::tapeset_scan scan (&ts);
    QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
    while (scan.forward (NULL, &tplrec, PEEK) == S_SUCCESS)
      {
	;
      }
    const qfile::tapeset_scan_metrics &m = scan.metrics ();
    if (m.pgbuf_fixes != 0)		/* pgbuf-bypass hard gate (scan side) */
      {
	return 1;
      }
    if (m.tuple_reads != (long) expected.size () || m.peeks != (long) expected.size () || m.copies != 0)
      {
	return 2;
      }
    /* every tuple-bearing page (5) plus skipped empty/zero-tuple pages were
     * fetched only via tape::page_at -- at least one per tuple-bearing page. */
    if (m.page_reads < 5)
      {
	return 3;
      }
    if (m.tape_advances < 1)		/* tape0 -> tape2 (tape1 empty, skipped) */
      {
	return 4;
      }
    scan.close (NULL);

    /* copy forward pass on a fresh scan */
    qfile::tapeset_scan scan2 (&ts);
    char buf[64];
    QFILE_TUPLE_RECORD crec = { buf, (int) sizeof (buf) };
    while (scan2.forward (NULL, &crec, 0 /* copy */) == S_SUCCESS)
      {
	;
      }
    const qfile::tapeset_scan_metrics &m2 = scan2.metrics ();
    if (m2.pgbuf_fixes != 0 || m2.copies != (long) expected.size () || m2.peeks != 0)
      {
	return 5;
      }
    scan2.close (NULL);
    return 0;
  }

  /* ---- Phase1 1C (issue #72) holdable-reparent helpers ------------- */
  /*
   * Bootless mirrors of qfile_copy_list_id's tapeset-ownership branches and
   * qfile_clear_list_id's tapeset teardown.  The production functions call
   * thread_get_thread_entry_info() (to bump an unrelated qlist counter) which
   * asserts a thread-local entry a bootless test never sets up; the tapeset
   * transfer itself is exactly the 2-4 lines mirrored here.  The REAL
   * qfile_copy_list_id(MOVE)/qfile_clear_list_id wiring (and a real on-disk
   * file + TDE) is exercised in-server by qfile_heldtape_selftest.
   */
  void
  tapeset_reparent_move (QFILE_LIST_ID *dest, QFILE_LIST_ID *src)
  {
    /* mirrors qfile_copy_list_id memcpy + QFILE_MOVE_DEPENDENT branch */
    QFILE_LIST_ID_TAPESET (dest) = QFILE_LIST_ID_TAPESET (src);
    QFILE_LIST_ID_OWNS_TAPESET (dest) = QFILE_LIST_ID_OWNS_TAPESET (src);
    QFILE_LIST_ID_TAPESET (src) = NULL;
    QFILE_LIST_ID_OWNS_TAPESET (src) = false;
  }

  void
  tapeset_borrow_skip (QFILE_LIST_ID *dest, QFILE_LIST_ID *src)
  {
    /* mirrors qfile_copy_list_id memcpy + QFILE_SKIP_DEPENDENT branch */
    QFILE_LIST_ID_TAPESET (dest) = QFILE_LIST_ID_TAPESET (src);
    QFILE_LIST_ID_OWNS_TAPESET (dest) = false;	/* borrow: do not own */
  }

  void
  tapeset_teardown (QFILE_LIST_ID *lid)
  {
    /* mirrors qfile_clear_list_id's owned-tapeset destroy */
    if (QFILE_LIST_ID_TAPESET (lid) != NULL && QFILE_LIST_ID_OWNS_TAPESET (lid))
      {
	qfile_tapeset_destroy (QFILE_LIST_ID_TAPESET (lid));
      }
    QFILE_LIST_ID_TAPESET (lid) = NULL;
    QFILE_LIST_ID_OWNS_TAPESET (lid) = false;
  }

  /* G11: holdable reparent (MOVE) is a zero-copy ownership move tran->session,
   * parity holds across the commit boundary, and session teardown is
   * orphan-zero -- the private file handle AND the RAM prefix both return to
   * baseline (SSOT #75 §5.5 (1) / §6, ADR 0001). */
  int
  run_reparent_orphan_zero ()
  {
    const qfile::tape_backing_census_snapshot base = qfile::tape_backing_census ();

    std::vector<std::vector<int> > pages = {
      { 0, 1, 2 }, { 3, 4 }, { 5, 6, 7, 8 }, { 9 }, { 10, 11 }, { 12 }
    };
    std::vector<int> expected;
    for (const std::vector<int> &p : pages)
      {
	for (int id : p)
	  {
	    expected.push_back (id);
	  }
      }

    writer_result wr = build_writer_tape (/*budget*/ 2, pages);	/* 2 prefix + 4 file */
    if (wr.tape == NULL || !wr.spilled)
      {
	return 1;
      }

    qfile::tapeset *ts = new qfile::tapeset ();
    ts->set_owns_tapes (true);
    ts->append_tape (wr.tape);

    QFILE_LIST_ID producer;
    std::memset (&producer, 0, sizeof (producer));
    QFILE_CLEAR_LIST_ID (&producer);
    QFILE_LIST_ID_TAPESET (&producer) = ts;
    QFILE_LIST_ID_OWNS_TAPESET (&producer) = true;

    /* one live file + 2 RAM prefix pages */
    const qfile::tape_backing_census_snapshot produced = qfile::tape_backing_census ();
    if (produced.open_files != base.open_files + 1 || produced.held_prefix_pages != base.held_prefix_pages + 2)
      {
	tapeset_teardown (&producer);
	return 2;
      }

    /* read the first half (the "before commit" rows) */
    std::vector<int> got;
    qfile::tapeset_scan scan (ts);
    QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
    SCAN_CODE code = S_SUCCESS;
    const int half = (int) expected.size () / 2;
    int read = 0;
    while (read < half && (code = scan.forward (NULL, &tplrec, PEEK)) == S_SUCCESS)
      {
	got.push_back (tuple_id (tplrec.tpl));
	read++;
      }

    /* COMMIT: reparent ownership transaction -> session (MOVE) */
    QFILE_LIST_ID session;
    std::memset (&session, 0, sizeof (session));
    QFILE_CLEAR_LIST_ID (&session);
    tapeset_reparent_move (&session, &producer);

    /* zero copy: census unchanged, ownership transferred off the producer */
    const qfile::tape_backing_census_snapshot moved = qfile::tape_backing_census ();
    if (moved.open_files != produced.open_files || moved.held_prefix_pages != produced.held_prefix_pages
	|| QFILE_LIST_ID_TAPESET (&producer) != NULL || QFILE_LIST_ID_TAPESET (&session) != ts
	|| !QFILE_LIST_ID_OWNS_TAPESET (&session))
      {
	scan.close (NULL);
	tapeset_teardown (&session);
	tapeset_teardown (&producer);
	return 4;
      }

    /* remaining rows across the boundary -- same backing, no copy */
    while ((code = scan.forward (NULL, &tplrec, PEEK)) == S_SUCCESS)
      {
	got.push_back (tuple_id (tplrec.tpl));
      }
    scan.close (NULL);
    if (code != S_END || got != expected)
      {
	tapeset_teardown (&session);
	tapeset_teardown (&producer);
	return 5;
      }

    /* session teardown frees the reparented backing (file + RAM) */
    tapeset_teardown (&session);
    const qfile::tape_backing_census_snapshot torn = qfile::tape_backing_census ();
    if (torn.open_files != base.open_files || torn.held_prefix_pages != base.held_prefix_pages)
      {
	tapeset_teardown (&producer);
	return 6;
      }

    /* producer disowned at MOVE: tearing it down must not double-free */
    tapeset_teardown (&producer);
    const qfile::tape_backing_census_snapshot done = qfile::tape_backing_census ();
    if (done.open_files != base.open_files || done.held_prefix_pages != base.held_prefix_pages)
      {
	return 7;
      }
    return 0;
  }

  /* G12: a tiny (all-RAM, no-spill) holdable result reparents with zero disk
   * touch -- the RAM prefix moves by ownership and teardown frees it (RAM
   * orphan-zero); open_files never moves. */
  int
  run_reparent_tiny ()
  {
    const qfile::tape_backing_census_snapshot base = qfile::tape_backing_census ();

    std::vector<std::vector<int> > pages = { { 0, 1 }, { 2 }, { 3, 4 } };	/* 3 pages */
    std::vector<int> expected = { 0, 1, 2, 3, 4 };

    writer_result wr = build_writer_tape (/*budget*/ 10, pages);
    if (wr.tape == NULL || wr.spilled || wr.file_pages != 0)
      {
	if (wr.tape != NULL)
	  {
	    delete wr.tape;
	  }
	return 1;
      }

    qfile::tapeset *ts = new qfile::tapeset ();
    ts->set_owns_tapes (true);
    ts->append_tape (wr.tape);

    QFILE_LIST_ID producer;
    std::memset (&producer, 0, sizeof (producer));
    QFILE_CLEAR_LIST_ID (&producer);
    QFILE_LIST_ID_TAPESET (&producer) = ts;
    QFILE_LIST_ID_OWNS_TAPESET (&producer) = true;

    /* no file, 3 RAM prefix pages */
    const qfile::tape_backing_census_snapshot produced = qfile::tape_backing_census ();
    if (produced.open_files != base.open_files || produced.held_prefix_pages != base.held_prefix_pages + 3)
      {
	tapeset_teardown (&producer);
	return 2;
      }

    QFILE_LIST_ID session;
    std::memset (&session, 0, sizeof (session));
    QFILE_CLEAR_LIST_ID (&session);
    tapeset_reparent_move (&session, &producer);

    const qfile::tape_backing_census_snapshot moved = qfile::tape_backing_census ();
    if (moved.open_files != base.open_files || moved.held_prefix_pages != produced.held_prefix_pages
	|| QFILE_LIST_ID_TAPESET (&producer) != NULL || !QFILE_LIST_ID_OWNS_TAPESET (&session))
      {
	tapeset_teardown (&session);
	tapeset_teardown (&producer);
	return 4;
      }

    /* parity over the session-held Tapeset */
    std::vector<int> got;
    long fixes = -1;
    if (scan_forward_ids (*ts, got, &fixes) != 0 || got != expected || fixes != 0)
      {
	tapeset_teardown (&session);
	tapeset_teardown (&producer);
	return 5;
      }

    /* teardown -> RAM orphan-zero; open_files never moved */
    tapeset_teardown (&session);
    const qfile::tape_backing_census_snapshot torn = qfile::tape_backing_census ();
    if (torn.open_files != base.open_files || torn.held_prefix_pages != base.held_prefix_pages)
      {
	tapeset_teardown (&producer);
	return 6;
      }

    tapeset_teardown (&producer);
    return 0;
  }

  /* G13: a borrowing scan copy (SKIP) shares the producer's Tapes but owns
   * none; tearing it down must NOT free them (single-owner invariant) -- only
   * the owner's teardown is orphan-zero. */
  int
  run_borrow_no_free ()
  {
    const qfile::tape_backing_census_snapshot base = qfile::tape_backing_census ();

    writer_result wr = build_writer_tape (/*budget*/ 1, { { 0, 1, 2 }, { 3, 4 } });	/* spill */
    if (wr.tape == NULL || !wr.spilled)
      {
	return 1;
      }

    qfile::tapeset *ts = new qfile::tapeset ();
    ts->set_owns_tapes (true);
    ts->append_tape (wr.tape);

    QFILE_LIST_ID producer;
    std::memset (&producer, 0, sizeof (producer));
    QFILE_CLEAR_LIST_ID (&producer);
    QFILE_LIST_ID_TAPESET (&producer) = ts;
    QFILE_LIST_ID_OWNS_TAPESET (&producer) = true;

    const qfile::tape_backing_census_snapshot produced = qfile::tape_backing_census ();

    /* SKIP copy = borrow (the scan-open path) */
    QFILE_LIST_ID borrow;
    std::memset (&borrow, 0, sizeof (borrow));
    QFILE_CLEAR_LIST_ID (&borrow);
    tapeset_borrow_skip (&borrow, &producer);
    if (QFILE_LIST_ID_TAPESET (&borrow) != ts || QFILE_LIST_ID_OWNS_TAPESET (&borrow)
	|| QFILE_LIST_ID_TAPESET (&producer) != ts || !QFILE_LIST_ID_OWNS_TAPESET (&producer))
      {
	tapeset_teardown (&producer);
	return 3;
      }

    /* tearing down the borrow must not free the Tapes */
    tapeset_teardown (&borrow);
    const qfile::tape_backing_census_snapshot after_borrow = qfile::tape_backing_census ();
    if (after_borrow.open_files != produced.open_files || after_borrow.held_prefix_pages != produced.held_prefix_pages)
      {
	tapeset_teardown (&producer);
	return 4;
      }

    /* producer's Tapes are still alive and scannable (not double-freed) */
    std::vector<int> got;
    long fixes = -1;
    std::vector<int> expected = { 0, 1, 2, 3, 4 };
    if (scan_forward_ids (*ts, got, &fixes) != 0 || got != expected || fixes != 0)
      {
	tapeset_teardown (&producer);
	return 5;
      }

    /* only the owner's teardown frees -> orphan-zero */
    tapeset_teardown (&producer);
    const qfile::tape_backing_census_snapshot torn = qfile::tape_backing_census ();
    if (torn.open_files != base.open_files || torn.held_prefix_pages != base.held_prefix_pages)
      {
	return 6;
      }
    return 0;
  }

  /* ---- Phase2 MIGRATE (issue #73) R2 + no-mixed-backing gates --------- */

  /* G14: R2 offset-range work-stealing (qfile::chunk_distributor, ADR 0003).
   *  (a) coverage under REAL concurrency: over a skewed multi-Tape page space
   *      (huge + tiny + empty Tapes), N reader threads claim chunks via the
   *      shared atomic; every page is claimed exactly once -- no gap, no
   *      double-claim (scheduling-independent: fetch_add hands each chunk to
   *      exactly one reader).
   *  (b) balance: a huge single Tape split among equal-rate readers stays
   *      balanced -- chunk-skew CoV <= 15% and the per-reader spread is bounded
   *      by one chunk (the work-stealing balance property, modelled by the
   *      round-robin = equal-rate interleaving: deterministic, not flaky).
   *  (c) the same balance holds for a huge Tape mixed with many tiny Tapes. */
  int
  run_r2_distribution ()
  {
    /* (a) concurrent coverage + no-double-claim over a skewed fixture. */
    {
      const std::vector<int> counts = { 200, 0, 5, 64, 1, 130, 0, 33 };
      const int N = 6;
      qfile::chunk_distributor d (counts, N, 64);

      std::vector<std::vector<qfile::chunk_distributor::range> > claimed (N);
      auto drain = [&d, &claimed] (int rid)
      {
	qfile::chunk_distributor::range r;
	while (d.next_chunk (rid, r))
	  {
	    claimed[rid].push_back (r);
	  }
      };
      std::vector<std::thread> threads;
      for (int i = 0; i < N; i++)
	{
	  threads.emplace_back (drain, i);
	}
      for (std::thread &t : threads)
	{
	  t.join ();
	}

      std::vector<std::vector<char> > seen (counts.size ());
      for (size_t t = 0; t < counts.size (); t++)
	{
	  seen[t].assign (counts[t] > 0 ? counts[t] : 0, 0);
	}
      long claimed_pages = 0;
      for (int rid = 0; rid < N; rid++)
	{
	  for (const qfile::chunk_distributor::range &r : claimed[rid])
	    {
	      if (r.tape_idx < 0 || r.tape_idx >= (int) counts.size ())
		{
		  return 1;
		}
	      if (r.page_count <= 0 || r.page_count > 64)
		{
		  return 2;
		}
	      for (int p = r.start_page; p < r.start_page + r.page_count; p++)
		{
		  if (p < 0 || p >= counts[r.tape_idx])
		    {
		      return 3;
		    }
		  if (seen[r.tape_idx][p] != 0)
		    {
		      return 4;	/* double-claim */
		    }
		  seen[r.tape_idx][p] = 1;
		  claimed_pages++;
		}
	    }
	}
      long total = 0;
      for (int c : counts)
	{
	  total += (c > 0 ? c : 0);
	}
      if (claimed_pages != total || d.total_pages () != total)
	{
	  return 5;
	}
      for (size_t t = 0; t < counts.size (); t++)
	{
	  const int n = counts[t] > 0 ? counts[t] : 0;
	  for (int p = 0; p < n; p++)
	    {
	      if (seen[t][p] != 1)
		{
		  return 6;	/* gap */
		}
	    }
	}
    }

    /* (b) balance over a huge single Tape (equal-rate readers = round-robin). */
    {
      const int N = 8;
      const int huge = 64 * 100 + 17;	/* 6417 pages -> 101 chunks (1 partial) */
      qfile::chunk_distributor d (std::vector<int> { huge }, N, 64);
      qfile::chunk_distributor::range r;
      long covered = 0;
      int rid = 0;
      while (d.next_chunk (rid, r))
	{
	  covered += r.page_count;
	  rid = (rid + 1) % N;
	}
      const qfile::r2_metrics m = d.metrics ();
      if (covered != huge || m.total_pages != huge)
	{
	  return 10;
	}
      if (m.max_reader_pages - m.min_reader_pages > 64)	/* bounded by one chunk */
	{
	  return 11;
	}
      if (m.cov > 0.15)					/* chunk-skew CoV <= 15% */
	{
	  return 12;
	}
    }

    /* (c) balance over a huge Tape mixed with many tiny Tapes. */
    {
      const int N = 8;
      std::vector<int> counts;
      counts.push_back (64 * 200);		/* 12800-page huge Tape */
      for (int i = 0; i < 20; i++)
	{
	  counts.push_back (3);			/* tiny Tapes */
	}
      qfile::chunk_distributor d (counts, N, 64);
      qfile::chunk_distributor::range r;
      int rid = 0;
      while (d.next_chunk (rid, r))
	{
	  rid = (rid + 1) % N;
	}
      if (d.coefficient_of_variation () > 0.15)
	{
	  return 20;
	}
    }
    return 0;
  }

  /* G15: no-mixed-backing invariant (qfile_list_is_mixed_backing).  The check
   * must DISCRIMINATE: pass a clean OLD list and a clean NEW list, catch a
   * synthetic mixed list (the FAIL-03/06 shape).  We drive the predicate
   * directly; qfile_check_no_mixed_backing wraps this same predicate in a
   * debug assert, which a bootless run cannot trip without aborting. */
  int
  run_no_mixed_backing ()
  {
    QFILE_LIST_ID lst;
    std::memset (&lst, 0, sizeof (lst));
    QFILE_CLEAR_LIST_ID (&lst);

    /* cleared: no backing committed -> not mixed */
    if (qfile_list_has_old_backing (&lst) || qfile_list_has_new_backing (&lst)
	|| qfile_list_is_mixed_backing (&lst) || QFILE_LIST_ID_BACKING_KIND (&lst) != QFILE_BACKING_NONE)
      {
	return 1;
      }

    /* clean OLD: real first-page VPID, no Tapeset */
    QFILE_LIST_ID_FIRST_VPID (&lst).pageid = 42;
    QFILE_LIST_ID_FIRST_VPID (&lst).volid = 0;
    QFILE_LIST_ID_BACKING_KIND (&lst) = QFILE_BACKING_OLD;
    if (!qfile_list_has_old_backing (&lst) || qfile_list_has_new_backing (&lst) || qfile_list_is_mixed_backing (&lst))
      {
	return 2;
      }

    /* clean NEW: a Tapeset, no old identity */
    qfile::tapeset ts_new;
    QFILE_CLEAR_LIST_ID (&lst);
    QFILE_LIST_ID_TAPESET (&lst) = &ts_new;
    QFILE_LIST_ID_BACKING_KIND (&lst) = QFILE_BACKING_NEW;
    if (qfile_list_has_old_backing (&lst) || !qfile_list_has_new_backing (&lst) || qfile_list_is_mixed_backing (&lst))
      {
	return 3;
      }

    /* mixed (the violation): old VPID + Tapeset -> MUST be detected */
    QFILE_LIST_ID_FIRST_VPID (&lst).pageid = 42;
    QFILE_LIST_ID_FIRST_VPID (&lst).volid = 0;
    if (!qfile_list_is_mixed_backing (&lst))
      {
	return 4;
      }

    /* mixed via the other OLD signal (tfile_vfid) + Tapeset */
    QFILE_CLEAR_LIST_ID (&lst);
    QFILE_LIST_ID_TAPESET (&lst) = &ts_new;
    QFILE_LIST_ID_TFILE_VFID (&lst) = (struct qmgr_temp_file *) &lst;	/* non-null sentinel, never dereferenced */
    if (!qfile_list_is_mixed_backing (&lst))
      {
	return 5;
      }

    /* drop the Tapeset -> clean OLD again (not mixed) */
    QFILE_LIST_ID_TAPESET (&lst) = NULL;
    if (qfile_list_is_mixed_backing (&lst) || !qfile_list_has_old_backing (&lst))
      {
	return 6;
      }
    return 0;
  }

  /* ---- 2A-0 overflow + concurrent-read + backing-guard (ADR 0005/0006) ---- */

  static char
  overflow_byte (int id, int i)
  {
    return (char) ((id * 7 + i) & 0xff);
  }

  /* Build the contiguous page run of one overflow tuple: `tuple_len` bytes laid
   * out starting at logical offset `start`, carrying `id` at TEST_ID_OFFSET and
   * a body byte pattern so a reader can verify the full reassembly.  Caller frees
   * each page (or hands them to a memory_tape created owns==true). */
  std::vector<char *>
  make_overflow_run (int start, int tuple_len, int id)
  {
    const int per = QFILE_MAX_TUPLE_SIZE_IN_PAGE;
    const int run_pages = (tuple_len + per - 1) / per;
    const int run_end = start + run_pages - 1;

    std::vector<char> tup ((size_t) tuple_len, 0);
    QFILE_PUT_TUPLE_LENGTH (tup.data (), tuple_len);
    QFILE_PUT_PREV_TUPLE_LENGTH (tup.data (), 0);
    OR_PUT_INT (tup.data () + TEST_ID_OFFSET, id);
    for (int i = 16; i < tuple_len; i++)
      {
	tup[i] = overflow_byte (id, i);
      }

    std::vector<char *> pages;
    int copied = 0;
    for (int p = 0; p < run_pages; p++)
      {
	char *page = (char *) malloc (DB_PAGESIZE);
	std::memset (page, 0, DB_PAGESIZE);
	QFILE_PUT_PREV_VPID_NULL (page);
	QFILE_PUT_NEXT_VPID_NULL (page);
	if (p == 0)
	  {
	    qfile_overflow_set_start (page, start);
	  }
	else
	  {
	    qfile_overflow_set_continuation (page, start, run_end);
	  }
	int csz = tuple_len - copied;
	if (csz > per)
	  {
	    csz = per;
	  }
	std::memcpy (page + QFILE_PAGE_HEADER_SIZE, tup.data () + copied, csz);
	copied += csz;
	pages.push_back (page);
      }
    return pages;
  }

  /* G16: backing-kind ENTRY guard discriminates + A~E counter plumbing.
   * Uses the pure (er_set-free) predicate so it stays bootless-safe. */
  int
  run_backing_guard ()
  {
    QFILE_LIST_ID lst;
    std::memset (&lst, 0, sizeof (lst));
    QFILE_CLEAR_LIST_ID (&lst);
    qfile::tapeset ts_new;

    /* clean OLD: OK into an OLD mechanism, REJECTED by a NEW mechanism. */
    QFILE_LIST_ID_FIRST_VPID (&lst).pageid = 42;
    QFILE_LIST_ID_FIRST_VPID (&lst).volid = 0;
    if (qfile_backing_mechanism_violation (&lst, QFILE_BACKING_OLD))
      {
	return 1;
      }
    if (!qfile_backing_mechanism_violation (&lst, QFILE_BACKING_NEW))
      {
	return 2;
      }

    /* clean NEW: OK into a NEW mechanism, REJECTED by an OLD mechanism. */
    QFILE_CLEAR_LIST_ID (&lst);
    QFILE_LIST_ID_TAPESET (&lst) = &ts_new;
    if (qfile_backing_mechanism_violation (&lst, QFILE_BACKING_NEW))
      {
	return 3;
      }
    if (!qfile_backing_mechanism_violation (&lst, QFILE_BACKING_OLD))
      {
	return 4;
      }

    /* NULL list is never a violation. */
    if (qfile_backing_mechanism_violation (NULL, QFILE_BACKING_OLD)
	|| qfile_backing_mechanism_violation (NULL, QFILE_BACKING_NEW))
      {
	return 5;
      }

    /* A~E counter: starts/returns to 0, increments. */
    qfile_ae_reset_old_touch_count ();
    if (qfile_ae_old_touch_count () != 0)
      {
	return 6;
      }
    qfile_ae_record_old_touch ();
    qfile_ae_record_old_touch ();
    if (qfile_ae_old_touch_count () != 2)
      {
	return 7;
      }
    qfile_ae_reset_old_touch_count ();
    if (qfile_ae_old_touch_count () != 0)
      {
	return 8;
      }
    return 0;
  }

  /* G17: overflow-continuation run reassembled across a Chunk boundary (ADR
   * 0006).  Layout (memory tape): off0 {0,1} | off1..3 overflow(99) | off4 {2,3}.
   * R1 forward/backward/jump reassemble the run as one tuple; R2 readers
   * (chunk_pages=2 so the run crosses chunk[0,1]->chunk[2,3]) read it exactly
   * once -- the first-page owner reassembles, the others skip. */
  int
  run_overflow_crosschunk ()
  {
    const int big_id = 99;
    const int tuple_len = 2 * QFILE_MAX_TUPLE_SIZE_IN_PAGE + 100;	/* spans 3 pages */
    const int cap = tuple_len + 4096;
    char *buf = (char *) malloc (cap);

    qfile::tapeset ts;
    ts.set_owns_tapes (true);
    qfile::memory_tape *t = new qfile::memory_tape (true);
    t->append_page (make_db_page ({ 0, 1 }));
    std::vector<char *> run = make_overflow_run (1, tuple_len, big_id);
    for (char *pg : run)
      {
	t->append_page (pg);
      }
    t->append_page (make_db_page ({ 2, 3 }));
    ts.append_tape (t);

    int rc = 0;
#define BIG_OK(tpl) (QFILE_GET_TUPLE_LENGTH (tpl) == tuple_len && tuple_id (tpl) == big_id \
		     && (tpl)[16] == overflow_byte (big_id, 16) \
		     && (tpl)[tuple_len - 1] == overflow_byte (big_id, tuple_len - 1))

    /* R1 forward (copy). */
    {
      qfile::tapeset_scan scan (&ts);
      QFILE_TUPLE_RECORD tr = { buf, cap };
      std::vector<int> ids;
      bool big_ok = false;
      SCAN_CODE code;
      while ((code = scan.forward (NULL, &tr, 0)) == S_SUCCESS)
	{
	  ids.push_back (tuple_id (tr.tpl));
	  if (tuple_id (tr.tpl) == big_id)
	    {
	      big_ok = BIG_OK (tr.tpl);
	    }
	}
      scan.close (NULL);
      std::vector<int> exp = { 0, 1, big_id, 2, 3 };
      if (code != S_END || ids != exp || !big_ok)
	{
	  rc = 1;
	}
    }

    /* R1 backward from S_AFTER. */
    if (rc == 0)
      {
	qfile::tapeset_scan scan (&ts);
	QFILE_TUPLE_RECORD tr = { buf, cap };
	while (scan.forward (NULL, &tr, 0) == S_SUCCESS)	/* advance to S_AFTER first */
	  {
	    ;
	  }
	std::vector<int> ids;
	bool big_ok = false;
	SCAN_CODE code;
	while ((code = scan.backward (NULL, &tr, 0)) == S_SUCCESS)
	  {
	    ids.push_back (tuple_id (tr.tpl));
	    if (tuple_id (tr.tpl) == big_id)
	      {
		big_ok = BIG_OK (tr.tpl);
	      }
	  }
	scan.close (NULL);
	std::vector<int> exp = { 3, 2, big_id, 1, 0 };
	if (code != S_END || ids != exp || !big_ok)
	  {
	    rc = 2;
	  }
      }

    /* R1 jump: save at the overflow tuple, step away, jump back, resume. */
    if (rc == 0)
      {
	qfile::tapeset_scan scan (&ts);
	QFILE_TUPLE_RECORD tr = { buf, cap };
	QFILE_TUPLE_POSITION pos;
	std::memset (&pos, 0, sizeof (pos));
	if (scan.forward (NULL, &tr, 0) != S_SUCCESS	/* 0 */
	    || scan.forward (NULL, &tr, 0) != S_SUCCESS	/* 1 */
	    || scan.forward (NULL, &tr, 0) != S_SUCCESS || tuple_id (tr.tpl) != big_id)
	  {
	    rc = 3;
	  }
	if (rc == 0)
	  {
	    scan.save_position (&pos);
	    if (scan.forward (NULL, &tr, 0) != S_SUCCESS || tuple_id (tr.tpl) != 2
		|| scan.jump (NULL, &pos, &tr, 0) != S_SUCCESS || !BIG_OK (tr.tpl)
		|| scan.forward (NULL, &tr, 0) != S_SUCCESS || tuple_id (tr.tpl) != 2)
	      {
		rc = 4;
	      }
	  }
	scan.close (NULL);
      }

    /* R2: chunk_pages=2, N readers; run [1..3] crosses chunk[0,1]->chunk[2,3]. */
    if (rc == 0)
      {
	const int N = 4;
	qfile::chunk_distributor d (&ts, N, 2);
	std::vector<std::vector<int> > got (N);
	std::vector<int> big_count (N, 0);
	std::vector<int> ok (N, 1);
	std::vector<std::thread> th;
	for (int r = 0; r < N; r++)
	  {
	    th.emplace_back ([&, r] ()
	    {
	      int lcap = tuple_len + 4096;
	      char *lbuf = (char *) malloc (lcap);
	      qfile::tapeset_reader rd (&ts, &d, r);
	      QFILE_TUPLE_RECORD tr = { lbuf, lcap };
	      SCAN_CODE c;
	      while ((c = rd.next (NULL, &tr, 0)) == S_SUCCESS)
		{
		  int tid = tuple_id (tr.tpl);
		  got[r].push_back (tid);
		  if (tid == big_id)
		    {
		      big_count[r]++;
		      if (!BIG_OK (tr.tpl))
			{
			  ok[r] = 0;
			}
		    }
		}
	      free (lbuf);
	    });
	  }
	for (auto &x : th)
	  {
	    x.join ();
	  }
	std::vector<int> merged;
	int total_big = 0;
	for (int r = 0; r < N; r++)
	  {
	    for (int v : got[r])
	      {
		merged.push_back (v);
	      }
	    total_big += big_count[r];
	    if (!ok[r])
	      {
		rc = 5;
	      }
	  }
	std::sort (merged.begin (), merged.end ());
	std::vector<int> exp = { 0, 1, 2, 3, big_id };
	std::sort (exp.begin (), exp.end ());
	if (rc == 0 && merged != exp)
	  {
	    rc = 6;	/* every tuple exactly once */
	  }
	if (rc == 0 && total_big != 1)
	  {
	    rc = 7;	/* overflow read exactly once (first-page owner) */
	  }
      }
#undef BIG_OK
    free (buf);
    return rc;
  }

  /* G18: N reader threads read the SAME spilled (file-backed) Tapeset
   * concurrently via tapeset_reader.  A non-re-entrant read (shared scratch)
   * would corrupt pages -> wrong/missing/dup tuple ids; we assert exact
   * coverage and scan-side pgbuf-bypass (ADR 0005). */
  int
  run_concurrent_file_readers ()
  {
    const int NPAGES = 200;
    const int PER_PAGE = 5;
    std::vector<std::vector<int> > pages;
    std::vector<int> expected;
    int next = 0;
    for (int p = 0; p < NPAGES; p++)
      {
	std::vector<int> ids;
	for (int k = 0; k < PER_PAGE; k++)
	  {
	    ids.push_back (next);
	    expected.push_back (next);
	    next++;
	  }
	pages.push_back (ids);
      }
    writer_result wr = build_writer_tape (0, pages);	/* budget 0 -> all spilled */
    if (wr.tape == NULL || !wr.spilled || wr.producer_pgbuf_fixes != 0)
      {
	delete wr.tape;
	return 1;
      }

    qfile::tapeset ts;
    ts.set_owns_tapes (true);
    ts.append_tape (wr.tape);

    const int N = 8;
    qfile::chunk_distributor d (&ts, N, 4);
    std::vector<std::vector<int> > got (N);
    std::vector<long> pgbuf (N, -1);
    std::vector<std::thread> th;
    for (int r = 0; r < N; r++)
      {
	th.emplace_back ([&, r] ()
	{
	  char buf[64];
	  qfile::tapeset_reader rd (&ts, &d, r);
	  QFILE_TUPLE_RECORD tr = { buf, (int) sizeof (buf) };
	  SCAN_CODE c;
	  while ((c = rd.next (NULL, &tr, 0)) == S_SUCCESS)
	    {
	      got[r].push_back (tuple_id (tr.tpl));
	    }
	  pgbuf[r] = rd.metrics ().pgbuf_fixes;
	});
      }
    for (auto &x : th)
      {
	x.join ();
      }
    std::vector<int> merged;
    for (int r = 0; r < N; r++)
      {
	for (int v : got[r])
	  {
	    merged.push_back (v);
	  }
	if (pgbuf[r] != 0)
	  {
	    return 2;	/* scan-side pgbuf-bypass (ADR 0005/0003) */
	  }
      }
    std::sort (merged.begin (), merged.end ());
    std::sort (expected.begin (), expected.end ());
    if (merged != expected)
      {
	return 3;	/* every tuple exactly once, no race loss/dup/corruption */
      }
    return 0;
  }
}

int
main (int, char **)
{
  const testcase cases[] = {
    { "G1 forward / terminal / S_END-on-last", run_forward },
    { "G2 backward from S_AFTER", run_backward },
    { "G3 save + jump (same-tape / cross-tape / boundary)", run_jump },
    { "G4 copy-mode retrieve", run_copy },
    { "G5 QFILE_LIST_SCAN_ID bridge + mirror", run_bridge },
    { "G6 empty-only Tapeset", run_empty_only },
    { "G7 measurement hooks (pgbuf-bypass=0, op counters)", run_metrics },
    { "G8 file-backed Tape robust parity (forward/backward/jump)", run_file_parity },
    { "G9 tiny result stays in-memory (no spill)", run_tiny_no_spill },
    { "G10 multi-Tape spill+tiny mix, producer pgbuf-bypass", run_file_multitape },
    { "G11 holdable reparent (MOVE) zero-copy + parity + orphan-zero", run_reparent_orphan_zero },
    { "G12 tiny all-RAM reparent (zero disk touch, RAM orphan-zero)", run_reparent_tiny },
    { "G13 borrow (SKIP) does not free owner's Tapes", run_borrow_no_free },
    { "G14 R2 offset-range work-stealing (coverage + CoV <= 15%)", run_r2_distribution },
    { "G15 migration no-mixed-backing discriminates (old+new shape)", run_no_mixed_backing },
    { "G16 backing-kind entry guard discriminates + A~E counter", run_backing_guard },
    { "G17 overflow run reassembly across a Chunk boundary (R1+R2)", run_overflow_crosschunk },
    { "G18 N-reader concurrent file read (re-entrant, pgbuf-bypass)", run_concurrent_file_readers },
  };

  bool all_passed = true;
  for (const testcase &c : cases)
    {
      int error = c.run ();
      if (error == 0)
	{
	  std::printf ("%s PASS\n", c.name);
	}
      else
	{
	  std::printf ("%s FAIL (%d)\n", c.name, error);
	  all_passed = false;
	}
    }

  /* best-effort cleanup of the per-pid scratch dir (BufFiles already unlinked) */
  (void) rmdir (test_scratch_dir ());

  /* suite-wide orphan-zero: every gate must leave the backing census balanced
   * (no leaked private file handle, no leaked RAM prefix page). */
  const qfile::tape_backing_census_snapshot suite_census = qfile::tape_backing_census ();
  if (suite_census.open_files != 0 || suite_census.held_prefix_pages != 0)
    {
      std::printf ("SUITE orphan-zero FAIL (open_files=%ld held_prefix_pages=%ld)\n",
		   suite_census.open_files, suite_census.held_prefix_pages);
      all_passed = false;
    }

  if (all_passed)
    {
      std::printf ("ALL TESTS PASSED\n");
      return 0;
    }
  return 1;
}
