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
 * qfile_tape.cpp - Phase1 1A scan contract (redesign G005, issue #70).
 * See qfile_tape.hpp for the design rationale.
 */

#include "qfile_tape.hpp"

#include "error_manager.h"
#include "memory_alloc.h"
#include "object_representation.h"	/* OR_GET_INT used by the QFILE_GET_* page macros */
#include "list_file.h"		/* qfile_copy_list_id / qfile_clear_list_id / QFILE_MOVE_DEPENDENT */
#include "system_parameter.h"	/* prm_get_integer_value / PRM_ID_TDE_DEFAULT_ALGORITHM */
#include "file_io.h"		/* PEEK */

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <sys/stat.h>		/* stat (orphan-zero on-disk check) */

#include "memory_wrapper.hpp"

namespace qfile
{
  /* ------------------------------------------------------------------ */
  /* memory_tape                                                        */
  /* ------------------------------------------------------------------ */

  memory_tape::memory_tape ()
    : m_pages (), m_owns (false)
  {
  }

  memory_tape::memory_tape (bool owns)
    : m_pages (), m_owns (owns)
  {
  }

  memory_tape::~memory_tape ()
  {
    if (m_owns)
      {
	tape_backing_census_prefix_removed ((long) m_pages.size ());
	for (char *page : m_pages)
	  {
	    free (page);
	  }
      }
    m_pages.clear ();
  }

  void
  memory_tape::append_page (char *page)
  {
    m_pages.push_back (page);
    if (m_owns)
      {
	tape_backing_census_prefix_added (1);
      }
  }

  PAGE_PTR
  memory_tape::page_at (THREAD_ENTRY *thread_p, int page_offset)
  {
    (void) thread_p;
    if (page_offset < 0 || page_offset >= (int) m_pages.size ())
      {
	return NULL;
      }
    return (PAGE_PTR) m_pages[page_offset];
  }

  void
  memory_tape::release_page (THREAD_ENTRY *thread_p, PAGE_PTR page)
  {
    /* RAM-resident: nothing to unfix. */
    (void) thread_p;
    (void) page;
  }

  /* ------------------------------------------------------------------ */
  /* buffile_tape                                                       */
  /* ------------------------------------------------------------------ */

  buffile_tape::buffile_tape (std::vector<char *> &&prefix_pages, bool owns_prefix, buffile *bf, bool owns_buffile)
    : m_prefix (std::move (prefix_pages))
    , m_owns_prefix (owns_prefix)
    , m_buffile (bf)
    , m_owns_buffile (owns_buffile)
    , m_readbuf (NULL)
  {
    if (m_owns_prefix)
      {
	tape_backing_census_prefix_added ((long) m_prefix.size ());
      }
    if (m_buffile != NULL)
      {
	m_readbuf = (char *) malloc (DB_PAGESIZE);
      }
  }

  buffile_tape::~buffile_tape ()
  {
    if (m_owns_prefix)
      {
	tape_backing_census_prefix_removed ((long) m_prefix.size ());
	for (char *page : m_prefix)
	  {
	    free (page);
	  }
      }
    m_prefix.clear ();
    if (m_owns_buffile)
      {
	delete m_buffile;	/* closes + unlinks */
      }
    m_buffile = NULL;
    free (m_readbuf);
    m_readbuf = NULL;
  }

  PAGE_PTR
  buffile_tape::page_at (THREAD_ENTRY *thread_p, int page_offset)
  {
    const int prefix = (int) m_prefix.size ();
    if (page_offset < 0 || page_offset >= total_page_count ())
      {
	return NULL;
      }
    if (page_offset < prefix)
      {
	return (PAGE_PTR) m_prefix[page_offset];
      }
    if (m_buffile == NULL || m_readbuf == NULL)
      {
	return NULL;
      }
    if (m_buffile->read_page (thread_p, page_offset - prefix, (PAGE_PTR) m_readbuf) != NO_ERROR)
      {
	return NULL;
      }
    return (PAGE_PTR) m_readbuf;
  }

  void
  buffile_tape::release_page (THREAD_ENTRY *thread_p, PAGE_PTR page)
  {
    /* RAM prefix: nothing to free.  File page: it lives in the reused scratch
     * buffer, released implicitly by the next page_at.  No pgbuf unfix -- this
     * backing never enters a pgbuf BCB. */
    (void) thread_p;
    (void) page;
  }

  int
  buffile_tape::total_page_count () const
  {
    return (int) m_prefix.size () + (m_buffile != NULL ? m_buffile->page_count () : 0);
  }

  /* ------------------------------------------------------------------ */
  /* tape_writer (membuf Option-A producer)                             */
  /* ------------------------------------------------------------------ */

  tape_writer::tape_writer (int prefix_budget_pages, TDE_ALGORITHM tde_algo, const std::string &dir,
			    std::uint64_t seq, unsigned int worker_id)
    : m_prefix_budget (prefix_budget_pages < 0 ? 0 : prefix_budget_pages)
    , m_tde_algo (tde_algo)
    , m_dir (dir)
    , m_seq (seq)
    , m_worker_id (worker_id)
    , m_prefix ()
    , m_buffile (NULL)
    , m_frozen (false)
  {
  }

  tape_writer::~tape_writer ()
  {
    /* Not frozen (error path): the writer still owns prefix pages + BufFile. */
    for (char *page : m_prefix)
      {
	free (page);
      }
    m_prefix.clear ();
    if (m_buffile != NULL)
      {
	delete m_buffile;	/* closes + unlinks the partial spill */
	m_buffile = NULL;
      }
  }

  int
  tape_writer::ensure_buffile (THREAD_ENTRY *thread_p)
  {
    if (m_buffile != NULL)
      {
	return NO_ERROR;
      }
    int os_error = 0;
    m_buffile = buffile::create (thread_p, m_dir.c_str (), m_seq, m_worker_id, m_tde_algo, &os_error);
    if (m_buffile == NULL)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	return ER_FAILED;
      }
    return NO_ERROR;
  }

  int
  tape_writer::append_page (THREAD_ENTRY *thread_p, const PAGE_PTR list_page)
  {
    if (m_frozen || list_page == NULL)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	return ER_FAILED;
      }

    if ((int) m_prefix.size () < m_prefix_budget)
      {
	char *copy = (char *) malloc (DB_PAGESIZE);
	if (copy == NULL)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) DB_PAGESIZE);
	    return ER_OUT_OF_VIRTUAL_MEMORY;
	  }
	std::memcpy (copy, list_page, DB_PAGESIZE);
	m_prefix.push_back (copy);
	return NO_ERROR;
      }

    int rc = ensure_buffile (thread_p);
    if (rc != NO_ERROR)
      {
	return rc;
      }
    return m_buffile->append_page (thread_p, list_page);
  }

  tape *
  tape_writer::freeze (THREAD_ENTRY *thread_p)
  {
    if (m_frozen)
      {
	return NULL;
      }

    if (m_buffile == NULL)
      {
	/* tiny / no spill: an all-RAM Tape, no disk touch. */
	memory_tape *mt = new memory_tape (true);
	for (char *page : m_prefix)
	  {
	    mt->append_page (page);
	  }
	m_prefix.clear ();	/* ownership transferred to the memory_tape */
	m_frozen = true;
	return mt;
      }

    if (m_buffile->flush (thread_p) != NO_ERROR)
      {
	return NULL;
      }
    buffile *bf = m_buffile;
    m_buffile = NULL;		/* ownership transfers to the buffile_tape */
    buffile_tape *bt = new buffile_tape (std::move (m_prefix), true, bf, true);
    m_prefix.clear ();
    m_frozen = true;
    return bt;
  }

  int
  tape_writer::file_pages () const
  {
    return m_buffile != NULL ? m_buffile->page_count () : 0;
  }

  const buffile_metrics *
  tape_writer::file_metrics () const
  {
    return m_buffile != NULL ? &m_buffile->metrics () : NULL;
  }

  /* ------------------------------------------------------------------ */
  /* tapeset                                                            */
  /* ------------------------------------------------------------------ */

  tapeset::tapeset ()
    : m_tapes (), m_owns_tapes (false)
  {
  }

  tapeset::~tapeset ()
  {
    if (m_owns_tapes)
      {
	for (tape *tape_p : m_tapes)
	  {
	    delete tape_p;
	  }
      }
    m_tapes.clear ();
  }

  void
  tapeset::append_tape (tape *tape_p)
  {
    m_tapes.push_back (tape_p);
  }

  /* ------------------------------------------------------------------ */
  /* tapeset_scan                                                       */
  /* ------------------------------------------------------------------ */

  tapeset_scan::tapeset_scan (tapeset *ts)
    : m_tapeset (ts)
    , m_position (S_BEFORE)
    , m_status (S_OPENED)
    , m_tape_idx (-1)
    , m_page_offset (-1)
    , m_page (NULL)
    , m_offset (0)
    , m_tplno (0)
    , m_curr_tpl (NULL)
  {
  }

  tapeset_scan::~tapeset_scan ()
  {
    /* The held page (if any) is released by close() before destruction; the
     * destructor has no THREAD_ENTRY to unfix a file-backed page. */
  }

  void
  tapeset_scan::release_page (THREAD_ENTRY *thread_p)
  {
    if (m_page != NULL && m_tapeset != NULL)
      {
	tape *tape_p = m_tapeset->get_tape (m_tape_idx);
	if (tape_p != NULL)
	  {
	    tape_p->release_page (thread_p, m_page);
	  }
      }
    m_page = NULL;
    m_curr_tpl = NULL;
  }

  void
  tapeset_scan::set_on (int tape_idx, int page_offset, PAGE_PTR page, int byte_offset, int tplno)
  {
    if (m_tape_idx >= 0 && m_tape_idx != tape_idx)
      {
	m_metrics.tape_advances++;
      }
    m_tape_idx = tape_idx;
    m_page_offset = page_offset;
    m_page = page;
    m_offset = byte_offset;
    m_tplno = tplno;
    m_curr_tpl = page + byte_offset;
    m_position = S_ON;
    m_status = S_STARTED;
  }

  SCAN_CODE
  tapeset_scan::retrieve (THREAD_ENTRY *thread_p, QFILE_TUPLE_RECORD *tuple_record_p, int peek)
  {
    if (QFILE_GET_OVERFLOW_PAGE_ID (m_page) != NULL_PAGEID)
      {
	/* Overflow-tuple reassembly in the new consecutive-page model is defined
	 * by the producer (Phase1 1B / migration), not by the 1A scan contract.
	 * Never hand back a silently-wrong tuple. */
	assert (false);
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
	return S_ERROR;
      }

    if (peek)
      {
	tuple_record_p->tpl = m_curr_tpl;
	m_metrics.peeks++;
	m_metrics.tuple_reads++;
	return S_SUCCESS;
      }

    int tuple_size = QFILE_GET_TUPLE_LENGTH (m_curr_tpl);
    if (tuple_record_p->size < tuple_size)
      {
	char *area = (char *) db_private_realloc (thread_p, tuple_record_p->tpl, tuple_size);
	if (area == NULL)
	  {
	    return S_ERROR;
	  }
	tuple_record_p->tpl = area;
	tuple_record_p->size = tuple_size;
      }
    std::memcpy (tuple_record_p->tpl, m_curr_tpl, tuple_size);
    m_metrics.copies++;
    m_metrics.tuple_reads++;
    return S_SUCCESS;
  }

  /* Scan forward from (start_tape, start_page) for the next tuple-bearing page.
   * The start tape begins at start_page; every later tape begins at page 0.
   * Zero-tuple pages and empty/missing tapes are skipped.  S_END (with
   * m_position = S_AFTER) only when the last tape is exhausted. */
  SCAN_CODE
  tapeset_scan::forward (THREAD_ENTRY *thread_p, QFILE_TUPLE_RECORD *tuple_record_p, int peek)
  {
    if (m_tapeset == NULL)
      {
	m_position = S_AFTER;
	return S_END;
      }

    int start_tape;
    int start_page;

    if (m_position == S_BEFORE)
      {
	start_tape = 0;
	start_page = 0;
      }
    else if (m_position == S_ON)
      {
	int count = QFILE_GET_TUPLE_COUNT (m_page);
	if (m_tplno < count - 1)
	  {
	    int len = QFILE_GET_TUPLE_LENGTH (m_curr_tpl);
	    m_offset += len;
	    m_curr_tpl += len;
	    m_tplno++;
	    return retrieve (thread_p, tuple_record_p, peek);
	  }
	start_tape = m_tape_idx;
	start_page = m_page_offset + 1;
	release_page (thread_p);
      }
    else if (m_position == S_AFTER)
      {
	return S_END;
      }
    else
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
	return S_ERROR;
      }

    int ntapes = m_tapeset->tape_count ();
    int page_offset = start_page;
    for (int ti = start_tape; ti < ntapes; ti++, page_offset = 0)
      {
	tape *tape_p = m_tapeset->get_tape (ti);
	if (tape_p == NULL)
	  {
	    continue;
	  }
	int npages = tape_p->total_page_count ();
	for (; page_offset < npages; page_offset++)
	  {
	    PAGE_PTR page = tape_p->page_at (thread_p, page_offset);
	    if (page == NULL)
	      {
		return S_ERROR;
	      }
	    m_metrics.page_reads++;
	    if (QFILE_GET_TUPLE_COUNT (page) > 0)
	      {
		set_on (ti, page_offset, page, QFILE_PAGE_HEADER_SIZE, 0);
		return retrieve (thread_p, tuple_record_p, peek);
	      }
	    tape_p->release_page (thread_p, page);
	  }
      }

    m_position = S_AFTER;
    m_page = NULL;
    m_curr_tpl = NULL;
    return S_END;
  }

  /* Scan backward from (start_tape, start_page) for the previous tuple-bearing
   * page; position at that page's LAST tuple.  start_page < 0 means "no pages
   * left in start_tape" (move to the previous tape).  Each earlier tape starts
   * at its last page.  S_END (with m_position = S_BEFORE) when the first tape
   * is exhausted. */
  SCAN_CODE
  tapeset_scan::backward (THREAD_ENTRY *thread_p, QFILE_TUPLE_RECORD *tuple_record_p, int peek)
  {
    if (m_tapeset == NULL)
      {
	m_position = S_BEFORE;
	return S_END;
      }

    int start_tape;
    int start_page;

    if (m_position == S_BEFORE)
      {
	return S_END;
      }
    else if (m_position == S_ON)
      {
	if (m_tplno > 0)
	  {
	    int prev_len = QFILE_GET_PREV_TUPLE_LENGTH (m_curr_tpl);
	    m_offset -= prev_len;
	    m_curr_tpl -= prev_len;
	    m_tplno--;
	    return retrieve (thread_p, tuple_record_p, peek);
	  }
	start_tape = m_tape_idx;
	start_page = m_page_offset - 1;
	release_page (thread_p);
      }
    else if (m_position == S_AFTER)
      {
	start_tape = m_tapeset->tape_count () - 1;
	tape *last_tape = m_tapeset->get_tape (start_tape);
	start_page = (last_tape != NULL) ? last_tape->total_page_count () - 1 : -1;
      }
    else
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
	return S_ERROR;
      }

    for (int ti = start_tape; ti >= 0; ti--)
      {
	tape *tape_p = m_tapeset->get_tape (ti);
	if (tape_p == NULL)
	  {
	    continue;
	  }
	int page_offset = (ti == start_tape) ? start_page : tape_p->total_page_count () - 1;
	for (; page_offset >= 0; page_offset--)
	  {
	    PAGE_PTR page = tape_p->page_at (thread_p, page_offset);
	    if (page == NULL)
	      {
		return S_ERROR;
	      }
	    m_metrics.page_reads++;
	    int count = QFILE_GET_TUPLE_COUNT (page);
	    if (count > 0)
	      {
		set_on (ti, page_offset, page, QFILE_GET_LAST_TUPLE_OFFSET (page), count - 1);
		return retrieve (thread_p, tuple_record_p, peek);
	      }
	    tape_p->release_page (thread_p, page);
	  }
      }

    m_position = S_BEFORE;
    m_page = NULL;
    m_curr_tpl = NULL;
    return S_END;
  }

  void
  tapeset_scan::save_position (QFILE_TUPLE_POSITION *tuple_position_p) const
  {
    tuple_position_p->status = m_status;
    tuple_position_p->position = m_position;
    qfile_tuple_position_set_tape (tuple_position_p, m_tape_idx, m_page_offset, m_offset);
    tuple_position_p->tpl = m_curr_tpl;
    tuple_position_p->tplno = m_tplno;
  }

  SCAN_CODE
  tapeset_scan::jump (THREAD_ENTRY *thread_p, const QFILE_TUPLE_POSITION *tuple_position_p,
		      QFILE_TUPLE_RECORD *tuple_record_p, int peek)
  {
    if (m_tapeset == NULL)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
	return S_ERROR;
      }

    if (tuple_position_p->position == S_ON)
      {
	tape *tape_p = m_tapeset->get_tape (tuple_position_p->tape_idx);
	if (tape_p == NULL)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
	    return S_ERROR;
	  }
	PAGE_PTR page = tape_p->page_at (thread_p, tuple_position_p->tape_page_offset);
	if (page == NULL)
	  {
	    return S_ERROR;
	  }
	m_metrics.page_reads++;
	release_page (thread_p);
	set_on (tuple_position_p->tape_idx, tuple_position_p->tape_page_offset, page,
		tuple_position_p->tape_byte_offset, tuple_position_p->tplno);
	m_status = tuple_position_p->status;
	m_metrics.jumps++;
	return retrieve (thread_p, tuple_record_p, peek);
      }
    else if (tuple_position_p->position == S_BEFORE || tuple_position_p->position == S_AFTER)
      {
	release_page (thread_p);
	m_status = tuple_position_p->status;
	m_position = tuple_position_p->position;
	return S_END;
      }

    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
    return S_ERROR;
  }

  void
  tapeset_scan::close (THREAD_ENTRY *thread_p)
  {
    release_page (thread_p);
  }
}				/* namespace qfile */

/* ------------------------------------------------------------------ */
/* C++-linkage bridge (list_file.c + unit test)                       */
/* ------------------------------------------------------------------ */

static void
qfile_tapeset_mirror (qfile::tapeset_scan *scan, QFILE_LIST_SCAN_ID *scan_id_p)
{
  /* Mirror the multi-Tape scan state into the outer QFILE_LIST_SCAN_ID so that
   * code reading scan_id->curr_* / position / status after a scan step sees a
   * consistent view.  curr_vpid is synthetic (volid = NULL_VOLID, pageid =
   * logical page offset within the current Tape) -- there is no physical VPID. */
  scan_id_p->status = scan->status ();
  scan_id_p->position = scan->position ();
  scan_id_p->curr_pgptr = scan->curr_page ();
  scan_id_p->curr_tpl = scan->curr_tpl ();
  scan_id_p->curr_offset = scan->curr_offset ();
  scan_id_p->curr_tplno = scan->curr_tplno ();
  scan_id_p->curr_vpid.volid = NULL_VOLID;
  scan_id_p->curr_vpid.pageid = scan->page_offset ();
}

int
qfile_tapeset_scan_open (QFILE_LIST_SCAN_ID *scan_id_p)
{
  qfile::tapeset *ts = (qfile::tapeset *) QFILE_LIST_ID_TAPESET (&scan_id_p->list_id);
  if (ts == NULL)
    {
      scan_id_p->tapeset_scan_ = NULL;
      return NO_ERROR;
    }
  qfile::tapeset_scan *scan = new qfile::tapeset_scan (ts);
  scan_id_p->tapeset_scan_ = scan;
  return NO_ERROR;
}

void
qfile_tapeset_scan_close (THREAD_ENTRY *thread_p, QFILE_LIST_SCAN_ID *scan_id_p)
{
  qfile::tapeset_scan *scan = (qfile::tapeset_scan *) scan_id_p->tapeset_scan_;
  if (scan != NULL)
    {
      scan->close (thread_p);
      delete scan;
      scan_id_p->tapeset_scan_ = NULL;
    }
}

SCAN_CODE
qfile_tapeset_scan_forward (THREAD_ENTRY *thread_p, QFILE_LIST_SCAN_ID *scan_id_p, QFILE_TUPLE_RECORD *tuple_record_p,
			    int peek)
{
  qfile::tapeset_scan *scan = (qfile::tapeset_scan *) scan_id_p->tapeset_scan_;
  assert (scan != NULL);
  SCAN_CODE code = scan->forward (thread_p, tuple_record_p, peek);
  qfile_tapeset_mirror (scan, scan_id_p);
  return code;
}

SCAN_CODE
qfile_tapeset_scan_backward (THREAD_ENTRY *thread_p, QFILE_LIST_SCAN_ID *scan_id_p, QFILE_TUPLE_RECORD *tuple_record_p,
			     int peek)
{
  qfile::tapeset_scan *scan = (qfile::tapeset_scan *) scan_id_p->tapeset_scan_;
  assert (scan != NULL);
  SCAN_CODE code = scan->backward (thread_p, tuple_record_p, peek);
  qfile_tapeset_mirror (scan, scan_id_p);
  return code;
}

SCAN_CODE
qfile_tapeset_scan_jump (THREAD_ENTRY *thread_p, QFILE_LIST_SCAN_ID *scan_id_p,
			 const QFILE_TUPLE_POSITION *tuple_position_p, QFILE_TUPLE_RECORD *tuple_record_p, int peek)
{
  qfile::tapeset_scan *scan = (qfile::tapeset_scan *) scan_id_p->tapeset_scan_;
  assert (scan != NULL);
  SCAN_CODE code = scan->jump (thread_p, tuple_position_p, tuple_record_p, peek);
  qfile_tapeset_mirror (scan, scan_id_p);
  return code;
}

void
qfile_tapeset_scan_save_position (QFILE_LIST_SCAN_ID *scan_id_p, QFILE_TUPLE_POSITION *tuple_position_p)
{
  qfile::tapeset_scan *scan = (qfile::tapeset_scan *) scan_id_p->tapeset_scan_;
  assert (scan != NULL);
  scan->save_position (tuple_position_p);
}

void
qfile_tapeset_destroy (void *tapeset_ptr)
{
  delete (qfile::tapeset *) tapeset_ptr;
}

/* ------------------------------------------------------------------ */
/* In-server self-test: holdable reparent lifecycle (Phase1 1C, #72). */
/* Gated by env CUBRID_HELDTAPE_SELFTEST (debug-only invocation).      */
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
