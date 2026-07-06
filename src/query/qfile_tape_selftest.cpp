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
 * qfile_tape_selftest.cpp - in-server self-tests for the tape / tapeset backing.
 * See qfile_tape_selftest.hpp.
 */

#include "qfile_tape_selftest.hpp"
#include "qfile_tape.hpp"

#include "error_manager.h"
#include "memory_alloc.h"
#include "object_representation.h"	/* OR_GET_INT used by the QFILE_GET_* page macros */
#include "list_file.h"		/* qfile_copy_list_id / qfile_clear_list_id / QFILE_MOVE_DEPENDENT */
#include "page_buffer.h"	/* pgbuf_get_fix_debug_count */
#include "system_parameter.h"	/* prm_get_integer_value / PRM_ID_TDE_DEFAULT_ALGORITHM */
#include "file_io.h"		/* PEEK */
#include "query_workmem.hpp"	/* work_mem accountant: reserve_held / release_held */

#include <cassert>
#include <cerrno>		/* ENOSPC/EDQUOT (ensure_buffile os_error mapping) */
#include <cstdlib>
#include <cstring>
#include <sys/stat.h>		/* stat (orphan-zero on-disk check) */
#include <thread>		/* N-reader concurrent selftest */
#include <algorithm>		/* std::sort (coverage check) */
#include <atomic>		/* process-unique producer BufFile sequence */

#include "memory_wrapper.hpp"

/* ------------------------------------------------------------------ */
/* In-server self-test: holdable reparent lifecycle. */
/* Gated by env CUBRID_WM_HELDTAPE_SELFTEST (debug-only invocation). */
/* ------------------------------------------------------------------ */

namespace
{
  /* One DB_PAGESIZE list page of 16-byte tuples [length|prev_length|id|pad],
   * matching the unit-test layout so the scan reads back known ids. */
  const int HELDTAPE_TUPLE_LEN = 16;
  const int HELDTAPE_ID_OFFSET = 8;

  char *
  heldtape_make_page (const std::vector<int> &ids)
  {
    char *page = (char *) malloc (DB_PAGESIZE);
    if (page == NULL)
      {
	return NULL;
      }
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
	QFILE_PUT_TUPLE_LENGTH (tpl, HELDTAPE_TUPLE_LEN);
	QFILE_PUT_PREV_TUPLE_LENGTH (tpl, prev_len);
	OR_PUT_INT (tpl + HELDTAPE_ID_OFFSET, ids[i]);
	QFILE_PUT_LAST_TUPLE_OFFSET (page, off);
	prev_len = HELDTAPE_TUPLE_LEN;
	off += HELDTAPE_TUPLE_LEN;
      }
    return page;
  }
}				/* anonymous namespace */

int
qfile_heldtape_selftest (THREAD_ENTRY *thread_p)
{
  std::string dir;
  if (!qfile::buffile::default_scratch_dir (dir))
    {
      return ER_FAILED;
    }

  /* Pick TDE iff a cipher is loaded so a TDE database exercises the reparent
   * + teardown of an ENCRYPTED holdable backing. */
  TDE_ALGORITHM algo = TDE_ALGORITHM_NONE;
  if (tde_is_loaded ())
    {
      const TDE_ALGORITHM def = (TDE_ALGORITHM) prm_get_integer_value (PRM_ID_TDE_DEFAULT_ALGORITHM);
      if (def != TDE_ALGORITHM_NONE)
	{
	  algo = def;
	}
    }

  const qfile::tape_backing_census_snapshot base = qfile::tape_backing_census ();

  /* 8 pages with prefix budget 2 -> 6 pages spill to a real on-disk file. */
  const std::vector<std::vector<int> > pages = {
    { 0, 1, 2 }, { 3, 4 }, { 5, 6 }, { 7, 8 }, { 9 }, { 10, 11 }, { 12, 13 }, { 14 }
  };
  std::vector<int> expected;
  for (const std::vector<int> &p : pages)
    {
      for (int id : p)
	{
	  expected.push_back (id);
	}
    }

  /* --- producer: build + freeze a spilled Tape (transaction-scoped) --- */
  qfile::tape *frozen = NULL;
  {
    static std::uint64_t seq = 70000;
    qfile::tape_writer w (2, algo, dir, seq++, 0);
    int prc = NO_ERROR;
    for (const std::vector<int> &ids : pages)
      {
	char *p = heldtape_make_page (ids);
	if (p == NULL)
	  {
	    prc = ER_FAILED;
	    break;
	  }
	prc = w.append_page (thread_p, (PAGE_PTR) p);
	free (p);
	if (prc != NO_ERROR)
	  {
	    break;
	  }
      }
    if (prc != NO_ERROR || !w.spilled ())
      {
	return ER_FAILED;	/* writer dtor frees the partial spill */
      }
    frozen = w.freeze (thread_p);	/* ownership leaves the writer */
    if (frozen == NULL)
      {
	return ER_FAILED;
      }
  }

  /* the spilled file path, captured for the post-teardown unlink check */
  std::string file_path;
  {
    qfile::buffile_tape *bt = static_cast<qfile::buffile_tape *> (frozen);
    if (bt->backing () != NULL)
      {
	file_path = bt->backing ()->path ();
      }
  }

  int rc = NO_ERROR;

  /* wrap in a Tapeset owned by the transaction-scoped (producer) list_id */
  qfile::tapeset *ts = new qfile::tapeset ();
  ts->set_owns_tapes (true);
  ts->append_tape (frozen);

  QFILE_LIST_ID producer;
  QFILE_CLEAR_LIST_ID (&producer);
  QFILE_LIST_ID_TAPESET (&producer) = ts;
  QFILE_LIST_ID_OWNS_TAPESET (&producer) = true;

  const qfile::tape_backing_census_snapshot produced = qfile::tape_backing_census ();
  if (produced.open_files != base.open_files + 1)
    {
      rc = ER_FAILED;
    }

  std::vector<int> got;
  {
    qfile::tapeset_scan scan (ts);
    QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
    SCAN_CODE code = S_SUCCESS;

    /* read the first half (the "before commit" rows) */
    const int half = (int) expected.size () / 2;
    int read = 0;
    while (read < half && (code = scan.forward (thread_p, &tplrec, PEEK)) == S_SUCCESS)
      {
	got.push_back (OR_GET_INT ((char *) tplrec.tpl + HELDTAPE_ID_OFFSET));
	read++;
      }

    /* --- COMMIT: reparent ownership transaction -> session (zero copy) --- */
    QFILE_LIST_ID session;
    QFILE_CLEAR_LIST_ID (&session);
    if (rc == NO_ERROR && qfile_copy_list_id (&session, &producer, false, QFILE_MOVE_DEPENDENT) != NO_ERROR)
      {
	rc = ER_FAILED;
      }

    const qfile::tape_backing_census_snapshot moved = qfile::tape_backing_census ();
    if (rc == NO_ERROR
	&& (moved.open_files != produced.open_files || moved.held_prefix_pages != produced.held_prefix_pages
	    || QFILE_LIST_ID_TAPESET (&producer) != NULL || QFILE_LIST_ID_TAPESET (&session) != ts
	    || !QFILE_LIST_ID_OWNS_TAPESET (&session)))
      {
	rc = ER_FAILED;		/* not a zero-copy move */
      }

    /* read the remaining rows across the reparent boundary (same backing) */
    if (rc == NO_ERROR)
      {
	while ((code = scan.forward (thread_p, &tplrec, PEEK)) == S_SUCCESS)
	  {
	    got.push_back (OR_GET_INT ((char *) tplrec.tpl + HELDTAPE_ID_OFFSET));
	  }
	if (code != S_END || got != expected)
	  {
	    rc = ER_FAILED;
	  }
      }
    scan.close (thread_p);

    /* the backing file must still exist before teardown */
    if (rc == NO_ERROR && !file_path.empty ())
      {
	struct stat st;
	if (stat (file_path.c_str (), &st) != 0)
	  {
	    rc = ER_FAILED;
	  }
      }

    /* --- session teardown (== session_free_sentry_data) frees the backing --- */
    qfile_clear_list_id (&session);
  }

  /* orphan-zero: census back to baseline (files + RAM) AND file unlinked */
  const qfile::tape_backing_census_snapshot torn = qfile::tape_backing_census ();
  if (rc == NO_ERROR && (torn.open_files != base.open_files || torn.held_prefix_pages != base.held_prefix_pages))
    {
      rc = ER_FAILED;
    }
  if (rc == NO_ERROR && !file_path.empty ())
    {
      struct stat st;
      if (stat (file_path.c_str (), &st) == 0)
	{
	  rc = ER_FAILED;	/* file still on disk -> orphan */
	}
    }

  /* producer disowned the Tapeset at MOVE; clearing it must not double-free
   * (on an error path where MOVE was skipped, this frees the backing). */
  qfile_clear_list_id (&producer);

  er_log_debug (ARG_FILE_LINE, "HELDTAPE_SELFTEST algo=%d result=%d (0=PASS)\n", (int) algo, rc);
  fprintf (stderr, "HELDTAPE_SELFTEST algo=%d result=%d (0=PASS)\n", (int) algo, rc);
  return rc;
}

/* ------------------------------------------------------------------ */
/* In-server self-test: N-reader CONCURRENT read of a frozen TDE Tape  */
/* Gated by env CUBRID_WM_TAPEREAD_SELFTEST.                            */
/* A non-re-entrant decrypt (shared member scratch) would corrupt pages */
/* under concurrent readers -> wrong tuples; this proves per-reader     */
/* scratch + shared fd + pread is safe on an ENCRYPTED backing.         */
/* ------------------------------------------------------------------ */

int
qfile_taperead_selftest (THREAD_ENTRY *thread_p)
{
  std::string dir;
  if (!qfile::buffile::default_scratch_dir (dir))
    {
      return ER_FAILED;
    }

  TDE_ALGORITHM algo = TDE_ALGORITHM_NONE;
  if (tde_is_loaded ())
    {
      const TDE_ALGORITHM def = (TDE_ALGORITHM) prm_get_integer_value (PRM_ID_TDE_DEFAULT_ALGORITHM);
      if (def != TDE_ALGORITHM_NONE)
	{
	  algo = def;
	}
    }

  const int NPAGES = 40;
  const int PER_PAGE = 6;
  std::vector<int> expected;
  std::vector<std::vector<int> > pages;
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

  /* produce + freeze a spilled (all-on-disk, budget 0) Tape */
  qfile::tape *frozen = NULL;
  {
    static std::uint64_t seq = 80000;
    qfile::tape_writer w (0, algo, dir, seq++, 0);
    int prc = NO_ERROR;
    for (const std::vector<int> &ids : pages)
      {
	char *pg = heldtape_make_page (ids);
	if (pg == NULL)
	  {
	    prc = ER_FAILED;
	    break;
	  }
	prc = w.append_page (thread_p, (PAGE_PTR) pg);
	free (pg);
	if (prc != NO_ERROR)
	  {
	    break;
	  }
      }
    if (prc != NO_ERROR || !w.spilled ())
      {
	return ER_FAILED;
      }
    frozen = w.freeze (thread_p);
    if (frozen == NULL)
      {
	return ER_FAILED;
      }
  }

  qfile::tapeset ts;
  ts.set_owns_tapes (true);
  ts.append_tape (frozen);

  const int N = 6;
  qfile::chunk_distributor d (&ts, N, 8);
  std::vector<std::vector<int> > got ((size_t) N);
  std::vector<int> ok ((size_t) N, 1);
  std::vector<long> pgbuf ((size_t) N, -1);
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
	    if (QFILE_GET_TUPLE_LENGTH (tr.tpl) != HELDTAPE_TUPLE_LEN)
	      {
		ok[r] = 0;
	      }
	    got[r].push_back (OR_GET_INT ((char *) tr.tpl + HELDTAPE_ID_OFFSET));
	  }
	pgbuf[r] = rd.metrics ().pgbuf_fixes;
      });
    }
  for (std::thread &x : th)
    {
      x.join ();
    }

  int rc = NO_ERROR;
  std::vector<int> merged;
  for (int r = 0; r < N; r++)
    {
      for (int v : got[r])
	{
	  merged.push_back (v);
	}
      if (!ok[r] || pgbuf[r] != 0)
	{
	  rc = ER_FAILED;
	}
    }
  std::sort (merged.begin (), merged.end ());
  std::sort (expected.begin (), expected.end ());
  if (rc == NO_ERROR && merged != expected)
    {
      rc = ER_FAILED;
    }

  er_log_debug (ARG_FILE_LINE, "TAPEREAD_SELFTEST algo=%d result=%d (0=PASS)\n", (int) algo, rc);
  fprintf (stderr, "TAPEREAD_SELFTEST algo=%d result=%d (0=PASS)\n", (int) algo, rc);
  return rc;
}

/* Production code builds writers via qfile_producer_create_for_list only; this
 * raw-args constructor is selftest-only (fixed test seqs below 0x100000000ULL). */
static void *
qfile_producer_create (int prefix_budget_pages, TDE_ALGORITHM tde_algo, unsigned long long seq, unsigned int worker_id)
{
  std::string dir;
  if (!qfile::buffile::default_scratch_dir (dir))
    {
      return NULL;
    }
  return new qfile::tape_writer (prefix_budget_pages, tde_algo, dir, (std::uint64_t) seq, worker_id);
}

/* In-server overflow round trip for the producer hook: produce a mix of small
 * and multi-page overflow tuples through qfile_add_tuple_to_list onto a
 * tapeset-backed list, freeze, then scan the frozen Tapeset back and verify
 * every tuple's length, id, and payload bytes -- the producer-side overflow
 * stamping matched against the scan-side reassembly (proves cross-page run
 * layout, no VPID chain). */
static int
qfile_producer_overflow_roundtrip (THREAD_ENTRY *thread_p, TDE_ALGORITHM algo)
{
  const int OV_N = 60;
  const int ID_OFFSET = 8;
  const int SMALL_LEN = 16;
  const int BIG_LEN = 2 * QFILE_MAX_TUPLE_SIZE_IN_PAGE + 100;	/* spans 3 pages */
  QFILE_LIST_ID ov;
  void *writer;
  char *scratch;
  char *big;
  int rc = NO_ERROR;
  int i;

  QFILE_CLEAR_LIST_ID (&ov);
  writer = qfile_producer_create (2, algo, 90001ULL, 0);
  scratch = (char *) malloc (DB_PAGESIZE);
  big = (char *) malloc (BIG_LEN);
  if (writer == NULL || scratch == NULL || big == NULL)
    {
      delete (qfile::tape_writer *) writer;
      free (scratch);
      free (big);
      return ER_FAILED;
    }
  QFILE_LIST_ID_PRODUCER_WRITER (&ov) = writer;
  QFILE_LIST_ID_PRODUCER_PAGE (&ov) = scratch;

  for (i = 0; i < OV_N && rc == NO_ERROR; i++)
    {
      bool is_big = ((i % 7) == 3);
      int len = is_big ? BIG_LEN : SMALL_LEN;
      char small[16];
      char *t = is_big ? big : small;
      int j;

      for (j = 0; j < len; j++)
	{
	  t[j] = (char) ((i * 31 + j) & 0xFF);
	}
      QFILE_PUT_TUPLE_LENGTH (t, len);
      QFILE_PUT_PREV_TUPLE_LENGTH (t, 0);
      OR_PUT_INT (t + ID_OFFSET, i);
      rc = qfile_add_tuple_to_list (thread_p, &ov, (QFILE_TUPLE) t);
    }

  if (rc == NO_ERROR)
    {
      qfile_close_list (thread_p, &ov);	/* tapeset branch: freeze -> tapeset, frees scratch */
      if (QFILE_LIST_ID_TAPESET (&ov) == NULL || QFILE_LIST_ID_BACKING_KIND (&ov) != QFILE_BACKING_TAPESET
	  || qfile_list_is_mixed_backing (&ov) || qfile_list_has_pgbuf_backing (&ov))
	{
	  rc = ER_FAILED;
	}
    }

  if (rc == NO_ERROR)
    {
      QFILE_LIST_SCAN_ID sid;
      QFILE_TUPLE_RECORD tr = { NULL, 0 };

      std::memset (&sid, 0, sizeof (sid));
      QFILE_CLEAR_LIST_ID (&sid.list_id);
      QFILE_LIST_ID_TAPESET (&sid.list_id) = QFILE_LIST_ID_TAPESET (&ov);	/* borrow */
      QFILE_LIST_ID_OWNS_TAPESET (&sid.list_id) = false;
      sid.tapeset_scan_ = NULL;

      if (qfile_tapeset_scan_open (&sid) != NO_ERROR)
	{
	  rc = ER_FAILED;
	}
      else
	{
	  int expected = 0;
	  SCAN_CODE code;

	  while (rc == NO_ERROR && (code = qfile_tapeset_scan_forward (thread_p, &sid, &tr, COPY)) == S_SUCCESS)
	    {
	      bool is_big = ((expected % 7) == 3);
	      int want_len = is_big ? BIG_LEN : SMALL_LEN;
	      int j;

	      if (QFILE_GET_TUPLE_LENGTH (tr.tpl) != want_len
		  || OR_GET_INT ((char *) tr.tpl + ID_OFFSET) != expected)
		{
		  rc = ER_FAILED;
		  break;
		}
	      /* payload past the tuple header (0-7) and id slot (8-11) must match
	       * the deterministic pattern, proving cross-page reassembly. */
	      for (j = ID_OFFSET + 4; j < want_len; j++)
		{
		  if (((char *) tr.tpl)[j] != (char) ((expected * 31 + j) & 0xFF))
		    {
		      rc = ER_FAILED;
		      break;
		    }
		}
	      expected++;
	    }
	  if (rc == NO_ERROR && (code != S_END || expected != OV_N))
	    {
	      rc = ER_FAILED;
	    }
	  qfile_tapeset_scan_close (thread_p, &sid);
	}
      if (tr.tpl != NULL)
	{
	  db_private_free_and_init (thread_p, tr.tpl);
	}
    }

  free (big);
  qfile_clear_list_id (&ov);	/* frees owned Tapeset + any producer residue */
  return rc;
}

/* ------------------------------------------------------------------ */
/* In-server self-test: tapeset-backing producer hook                  */
/* Gated by env CUBRID_WM_PRODUCER_SELFTEST (debug-only invocation).    */
/* ------------------------------------------------------------------ */

int
qfile_producer_selftest (THREAD_ENTRY *thread_p)
{
  TDE_ALGORITHM algo = TDE_ALGORITHM_NONE;
  if (tde_is_loaded ())
    {
      const TDE_ALGORITHM def = (TDE_ALGORITHM) prm_get_integer_value (PRM_ID_TDE_DEFAULT_ALGORITHM);
      if (def != TDE_ALGORITHM_NONE)
	{
	  algo = def;
	}
    }

  const int N = 5000;		/* 16B tuples -> several pages -> spills past budget */
  const int TUPLE_LEN = 16;
  const int ID_OFFSET = 8;

  /* A tapeset-backed list: no pgbuf temp-file (tfile_vfid NULL), producer_writer_ +
   * scratch attached.  The qfile producer hook routes its completed pages to
   * the tape_writer (no qmgr, no VPID). */
  QFILE_LIST_ID lst;
  QFILE_CLEAR_LIST_ID (&lst);
  void *writer = qfile_producer_create (2, algo, 90000ULL, 0);
  if (writer == NULL)
    {
      return ER_FAILED;
    }
  QFILE_LIST_ID_PRODUCER_WRITER (&lst) = writer;
  char *scratch = (char *) malloc (DB_PAGESIZE);
  if (scratch == NULL)
    {
      delete (qfile::tape_writer *) writer;
      return ER_FAILED;
    }
  QFILE_LIST_ID_PRODUCER_PAGE (&lst) = scratch;

  int rc = NO_ERROR;
  char tuple[TUPLE_LEN];
  for (int i = 0; i < N && rc == NO_ERROR; i++)
    {
      std::memset (tuple, 0, TUPLE_LEN);
      QFILE_PUT_TUPLE_LENGTH (tuple, TUPLE_LEN);
      QFILE_PUT_PREV_TUPLE_LENGTH (tuple, (i == 0) ? 0 : TUPLE_LEN);
      OR_PUT_INT (tuple + ID_OFFSET, i);
      rc = qfile_add_tuple_to_list (thread_p, &lst, (QFILE_TUPLE) tuple);
    }

  if (rc == NO_ERROR)
    {
      qfile_close_list (thread_p, &lst);	/* tapeset branch: freeze -> tapeset_, backing_kind=TAPESET */
      if (QFILE_LIST_ID_TAPESET (&lst) == NULL || QFILE_LIST_ID_BACKING_KIND (&lst) != QFILE_BACKING_TAPESET
	  || qfile_list_is_mixed_backing (&lst) || !qfile_list_has_tapeset (&lst)
	  || qfile_list_has_pgbuf_backing (&lst))
	{
	  rc = ER_FAILED;
	}
    }

  /* scan the frozen Tapeset back and verify ids 0..N-1 in order */
  if (rc == NO_ERROR)
    {
      QFILE_LIST_SCAN_ID sid;
      std::memset (&sid, 0, sizeof (sid));
      QFILE_CLEAR_LIST_ID (&sid.list_id);
      QFILE_LIST_ID_TAPESET (&sid.list_id) = QFILE_LIST_ID_TAPESET (&lst);	/* borrow */
      QFILE_LIST_ID_OWNS_TAPESET (&sid.list_id) = false;
      sid.tapeset_scan_ = NULL;

      if (qfile_tapeset_scan_open (&sid) != NO_ERROR)
	{
	  rc = ER_FAILED;
	}
      else
	{
	  QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
	  int expected = 0;
	  SCAN_CODE code;
	  while ((code = qfile_tapeset_scan_forward (thread_p, &sid, &tplrec, PEEK)) == S_SUCCESS)
	    {
	      if (OR_GET_INT ((char *) tplrec.tpl + ID_OFFSET) != expected)
		{
		  rc = ER_FAILED;
		  break;
		}
	      expected++;
	    }
	  if (rc == NO_ERROR && (code != S_END || expected != N))
	    {
	      rc = ER_FAILED;
	    }
	  qfile_tapeset_scan_close (thread_p, &sid);
	}
    }

  if (rc == NO_ERROR)
    {
      rc = qfile_producer_overflow_roundtrip (thread_p, algo);
    }

  qfile_clear_list_id (&lst);	/* frees the owned Tapeset + any producer residue */

  er_log_debug (ARG_FILE_LINE, "PRODUCER_SELFTEST algo=%d result=%d (0=PASS)\n", (int) algo, rc);
  fprintf (stderr, "PRODUCER_SELFTEST algo=%d result=%d (0=PASS)\n", (int) algo, rc);
  return rc;
}

/* ------------------------------------------------------------------ */
/* In-server self-test: #146 T3 S0 per-op work_mem limit accessor      */
/* Gated by env CUBRID_WM_OPLIMIT_SELFTEST (debug-only invocation).    */
/* ------------------------------------------------------------------ */

int
qfile_workmem_op_limit_selftest (THREAD_ENTRY *thread_p)
{
  (void) thread_p;
  int rc = NO_ERROR;

  const UINT64 saved_work_mem = prm_get_bigint_value (PRM_ID_WORK_MEM);
  const float saved_multiplier = prm_get_float_value (PRM_ID_HASH_MEM_MULTIPLIER);

  const UINT64 test_work_mem = 8ULL * 1024 * 1024;
  prm_set_bigint_value (PRM_ID_WORK_MEM, test_work_mem);
  prm_set_float_value (PRM_ID_HASH_MEM_MULTIPLIER, 2.0f);

  /* row-store-shaped state (sort/tuplestore/list membuf) reads pure work_mem. */
  if (temp_page_store::op_limit_bytes (temp_page_store::op_workmem_kind::row_store) != test_work_mem)
    {
      rc = ER_FAILED;
    }

  /* hash-shaped state (hash build/agg hash/memoize-like) reads work_mem * multiplier. */
  if (rc == NO_ERROR
      && temp_page_store::op_limit_bytes (temp_page_store::op_workmem_kind::hash) != test_work_mem * 2)
    {
      rc = ER_FAILED;
    }

  /* a session hash_mem_multiplier change must be reflected without a restart. */
  if (rc == NO_ERROR)
    {
      prm_set_float_value (PRM_ID_HASH_MEM_MULTIPLIER, 4.0f);
      if (temp_page_store::op_limit_bytes (temp_page_store::op_workmem_kind::hash) != test_work_mem * 4)
	{
	  rc = ER_FAILED;
	}
    }

  prm_set_bigint_value (PRM_ID_WORK_MEM, saved_work_mem);
  prm_set_float_value (PRM_ID_HASH_MEM_MULTIPLIER, saved_multiplier);

  er_log_debug (ARG_FILE_LINE, "WORKMEM_OPLIMIT_SELFTEST result=%d (0=PASS)\n", rc);
  fprintf (stderr, "WORKMEM_OPLIMIT_SELFTEST result=%d (0=PASS)\n", rc);
  return rc;
}

#if !defined (NDEBUG)
/* ------------------------------------------------------------------ */
/* In-server self-test: close/freeze ENOSPC failure propagation      */
/* Gated by env CUBRID_WM_CLOSE_FAULT_SELFTEST (debug-only invocation). */
/* ------------------------------------------------------------------ */
/*
 * Drives the buffile flush fault injector to prove the two silent-data-loss
 * holes this test guards:
 *   P1 (writer sticky error):  an append whose spill flush fails must make
 *       freeze () return NULL.  A writer must never hand back a "successful"
 *       -- but silently short -- Tape after it has lost a page.
 *   P2 (close contract):  a tapeset-backed list whose freeze flush fails must be
 *       marked so the next scan-open raises ER_QPROC_OUT_OF_TEMP_SPACE, never
 *       silently scans 0 rows while tuple_cnt still reads full.
 * Census must return to its pre-test baseline on both failure paths (the
 * partial spill is reclaimed, not orphaned).  Returns 0 (NO_ERROR) on PASS.
 *
 * Without the sticky flag / close-mark / scan-open guard, P1 sees a non-NULL
 * short Tape and P2 sees scan-open succeed with 0 rows -- both flip the result
 * to FAIL, so the test discriminates the fix.
 */
int
qfile_close_fault_selftest (THREAD_ENTRY *thread_p)
{
  int rc = NO_ERROR;
  const qfile::tape_backing_census_snapshot base = qfile::tape_backing_census ();

  /* ---- P1: sticky error -> freeze () refuses to fake success ---- */
  {
    std::string dir;
    if (!qfile::buffile::default_scratch_dir (dir))
      {
	rc = ER_FAILED;
      }
    else
      {
	static std::uint64_t seq = 86000;
	qfile::tape_writer w (0, TDE_ALGORITHM_NONE, dir, seq++, 0);	/* budget 0 -> all spill */
	int prc = NO_ERROR;

	qfile::buffile_fault_arm_flush_fail (1);	/* fail the first real (batch) flush */
	/* BufFile batches 8 pages; the 9th append triggers the flush that the
	 * injector fails, surfacing as an append error. */
	for (int p = 0; p < 16 && prc == NO_ERROR; p++)
	  {
	    char *pg = (char *) malloc (DB_PAGESIZE);
	    if (pg == NULL)
	      {
		prc = ER_FAILED;
		break;
	      }
	    std::memset (pg, 0, DB_PAGESIZE);
	    prc = w.append_page (thread_p, (PAGE_PTR) pg);
	    free (pg);
	  }
	qfile::buffile_fault_arm_flush_fail (0);	/* disarm: freeze must fail on its own memory of the loss */

	if (prc == NO_ERROR)
	  {
	    rc = ER_FAILED;		/* injection never fired -> test is not exercising the path */
	  }
	qfile::tape *t = w.freeze (thread_p);
	if (t != NULL)
	  {
	    delete t;			/* a silently truncated Tape */
	    rc = ER_FAILED;
	  }
	/* w destructs here; the partial spill it still owns is reclaimed. */
      }
    qfile::buffile_fault_arm_flush_fail (0);
  }

  /* ---- P2: failed close -> scan-open raises, never silent 0 rows ---- */
  if (rc == NO_ERROR)
    {
      const int N = 5000;		/* 16B tuples -> spills well past budget */
      const int TUPLE_LEN = 16;
      const int ID_OFFSET = 8;
      QFILE_LIST_ID lst;
      QFILE_CLEAR_LIST_ID (&lst);
      void *writer = qfile_producer_create (2, TDE_ALGORITHM_NONE, 86100ULL, 0);
      char *scratch = (writer != NULL) ? (char *) malloc (DB_PAGESIZE) : NULL;
      if (writer == NULL || scratch == NULL)
	{
	  delete (qfile::tape_writer *) writer;
	  free (scratch);
	  rc = ER_FAILED;
	}
      else
	{
	  int prc = NO_ERROR;
	  char tuple[16];

	  QFILE_LIST_ID_PRODUCER_WRITER (&lst) = writer;
	  QFILE_LIST_ID_PRODUCER_PAGE (&lst) = scratch;
	  for (int i = 0; i < N && prc == NO_ERROR; i++)
	    {
	      std::memset (tuple, 0, TUPLE_LEN);
	      QFILE_PUT_TUPLE_LENGTH (tuple, TUPLE_LEN);
	      QFILE_PUT_PREV_TUPLE_LENGTH (tuple, (i == 0) ? 0 : TUPLE_LEN);
	      OR_PUT_INT (tuple + ID_OFFSET, i);
	      prc = qfile_add_tuple_to_list (thread_p, &lst, (QFILE_TUPLE) tuple);
	    }
	  if (prc != NO_ERROR)
	    {
	      rc = ER_FAILED;		/* production must succeed before the injected close */
	    }

	  /* Arm so the freeze flush inside close fails; close must not swallow it. */
	  qfile::buffile_fault_arm_flush_fail (1);
	  qfile_close_list (thread_p, &lst);
	  qfile::buffile_fault_arm_flush_fail (0);

	  if (rc == NO_ERROR)
	    {
	      QFILE_LIST_SCAN_ID sid;
	      std::memset (&sid, 0, sizeof (sid));
	      er_clear ();
	      int open_rc = qfile_open_list_scan (&lst, &sid);
	      if (open_rc == NO_ERROR)
		{
		  /* A failed close left the list scannable
		   * while tuple_cnt still reads full but there is no backing
		   * (first_vpid + tapeset both NULL).  The next scan step would then
		   * hand back a silently short / empty result (top-level fetch) or
		   * fault on the NULL first page (intermediate scan) -- never a clean
		   * error.  A failed close must never open cleanly; do NOT walk the
		   * scan here (walking a backing-less list is itself the crash the
		   * fix prevents).  Log the symptom and fail. */
		  qfile_close_scan (thread_p, &sid);
		  er_log_debug (ARG_FILE_LINE,
				"CLOSE_FAULT_SELFTEST no-propagation: scan-open OK on failed close, tuple_cnt=%lld\n",
				(long long) lst.tuple_cnt);
		  fprintf (stderr,
			   "CLOSE_FAULT_SELFTEST no-propagation: scan-open OK on failed close, tuple_cnt=%lld\n",
			   (long long) lst.tuple_cnt);
		  rc = ER_FAILED;
		}
	      else if (er_errid () != ER_QPROC_OUT_OF_TEMP_SPACE)
		{
		  rc = ER_FAILED;	/* raised, but not the temp-space error */
		}
	    }

	  qfile_clear_list_id (&lst);	/* free producer residue / any backing */
	}
    }
  qfile::buffile_fault_arm_flush_fail (0);

  /* ---- leak check: census back to baseline on the failure path ---- */
  if (rc == NO_ERROR)
    {
      const qfile::tape_backing_census_snapshot torn = qfile::tape_backing_census ();
      if (torn.open_files != base.open_files || torn.held_prefix_pages != base.held_prefix_pages)
	{
	  rc = ER_FAILED;
	}
    }

  er_log_debug (ARG_FILE_LINE, "CLOSE_FAULT_SELFTEST result=%d (0=PASS)\n", rc);
  fprintf (stderr, "CLOSE_FAULT_SELFTEST result=%d (0=PASS)\n", rc);
  return rc;
}

/* ------------------------------------------------------------------ */
/* In-server self-test: BufFile fd-exhaustion error mapping          */
/* Gated by env CUBRID_WM_EMFILE_FAULT_SELFTEST (debug-only invocation).*/
/* ------------------------------------------------------------------ */
/*
 * The BufFile writer's ensure_buffile () must map an open () failure from fd
 * exhaustion (EMFILE/ENFILE, like ENOSPC/EDQUOT) to ER_QPROC_OUT_OF_TEMP_SPACE;
 * otherwise fd starvation surfaces as a generic ER_FAILED that no operator can
 * act on.
 *
 * Drives the create-fault injector to force buffile::create () to report EMFILE
 * (then ENFILE) on the first spill append (prefix budget 0 -> immediate spill)
 * and asserts each raises ER_QPROC_OUT_OF_TEMP_SPACE.  Returns 0 on PASS.
 *
 * Without the EMFILE/ENFILE arm in ensure_buffile's os_error switch, both
 * injections fall through to ER_FAILED, so the test discriminates the fix.
 */
int
qfile_emfile_fault_selftest (THREAD_ENTRY *thread_p)
{
  int rc = NO_ERROR;
  const qfile::tape_backing_census_snapshot base = qfile::tape_backing_census ();

  std::string dir;
  if (!qfile::buffile::default_scratch_dir (dir))
    {
      rc = ER_FAILED;
    }
  else
    {
      const int inject_errnos[2] = { EMFILE, ENFILE };
      static std::uint64_t seq = 125000;
      for (int k = 0; k < 2 && rc == NO_ERROR; k++)
	{
	  qfile::tape_writer w (0, TDE_ALGORITHM_NONE, dir, seq++, 0);	/* budget 0 -> first append spills */
	  char *pg = (char *) malloc (DB_PAGESIZE);
	  if (pg == NULL)
	    {
	      rc = ER_FAILED;
	      break;
	    }
	  std::memset (pg, 0, DB_PAGESIZE);

	  er_clear ();
	  qfile::buffile_fault_arm_create_fail (inject_errnos[k]);
	  const int prc = w.append_page (thread_p, (PAGE_PTR) pg);	/* -> ensure_buffile -> create fails */
	  qfile::buffile_fault_arm_create_fail (0);
	  free (pg);

	  if (prc == NO_ERROR)
	    {
	      rc = ER_FAILED;		/* injection never fired -> path not exercised */
	    }
	  else if (er_errid () != ER_QPROC_OUT_OF_TEMP_SPACE)
	    {
	      /* generic ER_FAILED instead of the temp-space
	       * diagnosis.  Log the symptom and fail. */
	      er_log_debug (ARG_FILE_LINE,
			    "EMFILE_FAULT_SELFTEST no-mapping: errno=%d gave er_errid=%d (want %d)\n",
			    inject_errnos[k], er_errid (), ER_QPROC_OUT_OF_TEMP_SPACE);
	      fprintf (stderr,
		       "EMFILE_FAULT_SELFTEST no-mapping: errno=%d gave er_errid=%d (want %d)\n",
		       inject_errnos[k], er_errid (), ER_QPROC_OUT_OF_TEMP_SPACE);
	      rc = ER_FAILED;
	    }
	  /* w destructs here; no buffile was ever opened, so nothing to reclaim. */
	}
    }
  qfile::buffile_fault_arm_create_fail (0);

  /* leak check: the failed creates leave no orphaned fd/prefix behind. */
  if (rc == NO_ERROR)
    {
      const qfile::tape_backing_census_snapshot torn = qfile::tape_backing_census ();
      if (torn.open_files != base.open_files || torn.held_prefix_pages != base.held_prefix_pages)
	{
	  rc = ER_FAILED;
	}
    }

  er_log_debug (ARG_FILE_LINE, "EMFILE_FAULT_SELFTEST result=%d (0=PASS)\n", rc);
  fprintf (stderr, "EMFILE_FAULT_SELFTEST result=%d (0=PASS)\n", rc);
  return rc;
}

/* ------------------------------------------------------------------ */
/* In-server self-test: freeze() OOM ownership recovery               */
/* Gated by env CUBRID_WM_FREEZE_OOM_SELFTEST (debug-only invocation).  */
/* ------------------------------------------------------------------ */
/*
 * SERVER_MODE new is noexcept (returns NULL on OOM), so freeze() must NULL-check
 * both tape allocations and, on failure, retain ownership of the prefix +
 * BufFile so the caller's teardown reclaims them.  Drives the alloc fault
 * injector to force each path's tape allocation to NULL and asserts:
 *   P1 (spill/buffile path): freeze()==NULL (no crash, no fake success),
 *       er=ER_OUT_OF_VIRTUAL_MEMORY, writer still owns the BufFile (spilled())
 *       and is latched failed(); after the writer is destroyed the census
 *       open_files returns to baseline (fd closed + spill file unlinked by
 *       ~buffile -- not orphaned).
 *   P2 (tiny/RAM path): freeze()==NULL (no NULL-deref crash), er=OOM, writer
 *       still owns its prefix pages (prefix_pages() unchanged) and is latched
 *       failed(); after destroy the census returns to baseline (prefix freed,
 *       not lost via a premature clear).
 * Returns 0 (NO_ERROR) on PASS.  Without the NULL checks the tiny path
 * NULL-derefs and the spill path orphans the fd/file.
 */
int
qfile_freeze_oom_selftest (THREAD_ENTRY *thread_p)
{
  int rc = NO_ERROR;
  std::string dir;
  if (!qfile::buffile::default_scratch_dir (dir))
    {
      rc = ER_FAILED;
    }

  /* ---- P1: spill/buffile path OOM -> fd + file reclaimed, no orphan ---- */
  if (rc == NO_ERROR)
    {
      const qfile::tape_backing_census_snapshot base = qfile::tape_backing_census ();
      {
	static std::uint64_t seq = 95000;
	qfile::tape_writer w (0, TDE_ALGORITHM_NONE, dir, seq++, 0);	/* budget 0 -> spill */
	int prc = NO_ERROR;
	for (int p = 0; p < 12 && prc == NO_ERROR; p++)
	  {
	    char *pg = (char *) malloc (DB_PAGESIZE);
	    if (pg == NULL)
	      {
		prc = ER_FAILED;
		break;
	      }
	    std::memset (pg, 0, DB_PAGESIZE);
	    prc = w.append_page (thread_p, (PAGE_PTR) pg);
	    free (pg);
	  }
	if (prc != NO_ERROR || !w.spilled ())
	  {
	    rc = ER_FAILED;		/* setup: must have spilled to a BufFile */
	  }
	else
	  {
	    er_clear ();
	    qfile::tape_fault_arm_alloc_fail (1);	/* fail the buffile_tape alloc */
	    qfile::tape *t = w.freeze (thread_p);
	    qfile::tape_fault_arm_alloc_fail (0);
	    if (t != NULL)
	      {
		delete t;		/* freeze must fail on OOM, not fake success */
		rc = ER_FAILED;
	      }
	    else if (!w.spilled () || !w.failed () || er_errid () != ER_OUT_OF_VIRTUAL_MEMORY)
	      {
		/* BufFile must still be the writer's, failure latched + OOM. */
		rc = ER_FAILED;
	      }
	  }
	/* w destructs here -> ~tape_writer closes + unlinks the BufFile. */
      }
      if (rc == NO_ERROR)
	{
	  const qfile::tape_backing_census_snapshot torn = qfile::tape_backing_census ();
	  if (torn.open_files != base.open_files || torn.held_prefix_pages != base.held_prefix_pages)
	    {
	      rc = ER_FAILED;		/* orphaned fd / prefix -> leak */
	    }
	}
    }

  /* ---- P2: tiny/RAM path OOM -> no crash, prefix retained then freed ---- */
  if (rc == NO_ERROR)
    {
      const int NP = 5;
      const qfile::tape_backing_census_snapshot base = qfile::tape_backing_census ();
      {
	static std::uint64_t seq = 95500;
	qfile::tape_writer w (64, TDE_ALGORITHM_NONE, dir, seq++, 0);	/* big budget -> all RAM */
	int prc = NO_ERROR;
	for (int p = 0; p < NP && prc == NO_ERROR; p++)
	  {
	    char *pg = (char *) malloc (DB_PAGESIZE);
	    if (pg == NULL)
	      {
		prc = ER_FAILED;
		break;
	      }
	    std::memset (pg, 0, DB_PAGESIZE);
	    prc = w.append_page (thread_p, (PAGE_PTR) pg);
	    free (pg);
	  }
	if (prc != NO_ERROR || w.spilled () || w.prefix_pages () != NP)
	  {
	    rc = ER_FAILED;		/* setup: must be all-RAM prefix, no spill */
	  }
	else
	  {
	    er_clear ();
	    qfile::tape_fault_arm_alloc_fail (1);	/* fail the memory_tape alloc */
	    qfile::tape *t = w.freeze (thread_p);
	    qfile::tape_fault_arm_alloc_fail (0);
	    if (t != NULL)
	      {
		delete t;
		rc = ER_FAILED;
	      }
	    else if (w.prefix_pages () != NP || !w.failed () || er_errid () != ER_OUT_OF_VIRTUAL_MEMORY)
	      {
		/* prefix must be retained (not cleared), failure latched + OOM. */
		rc = ER_FAILED;
	      }
	  }
	/* w destructs here -> ~tape_writer frees the retained prefix pages. */
      }
      if (rc == NO_ERROR)
	{
	  const qfile::tape_backing_census_snapshot torn = qfile::tape_backing_census ();
	  if (torn.open_files != base.open_files || torn.held_prefix_pages != base.held_prefix_pages)
	    {
	      rc = ER_FAILED;
	    }
	}
    }

  qfile::tape_fault_arm_alloc_fail (0);
  er_log_debug (ARG_FILE_LINE, "FREEZE_OOM_SELFTEST result=%d (0=PASS)\n", rc);
  fprintf (stderr, "FREEZE_OOM_SELFTEST result=%d (0=PASS)\n", rc);
  return rc;
}
#endif /* !NDEBUG */
