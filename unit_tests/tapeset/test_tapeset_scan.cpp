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
#include "object_representation.h"
#include "error_code.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

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

  if (all_passed)
    {
      std::printf ("ALL TESTS PASSED\n");
      return 0;
    }
  return 1;
}
