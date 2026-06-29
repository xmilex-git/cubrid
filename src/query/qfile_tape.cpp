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

#include <cassert>
#include <cstdlib>
#include <cstring>

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
