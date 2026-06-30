/*
 *
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
 * px_scan_input_handler_list.cpp
 */

#include "px_scan_input_handler_list.hpp"
#include "error_code.h"
#include "error_manager.h"
#include "list_file.h"
#include "object_representation.h"	/* OR_GET_INT used by QFILE_GET_TUPLE_COUNT */
#include "query_list.h"
#include "query_manager.h"

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace parallel_scan
{
  thread_local UINT64 input_handler_list::m_tl_bitmap = 0;
  thread_local VSID input_handler_list::m_tl_vsid = {NULL_SECTID, NULL_VOLID};
  thread_local QMGR_TEMP_FILE *input_handler_list::m_tl_current_tfile = nullptr;
  thread_local bool input_handler_list::m_tl_is_membuf_worker = false;
  thread_local int input_handler_list::m_tl_membuf_pageid = 0;
  thread_local int input_handler_list::m_tl_new_reader_id = -1;
  thread_local qfile::chunk_distributor::range input_handler_list::m_tl_new_range = { 0, 0, 0 };
  thread_local bool input_handler_list::m_tl_new_have_chunk = false;
  thread_local int input_handler_list::m_tl_new_page_offset = 0;
  thread_local char *input_handler_list::m_tl_new_page_raw = nullptr;
  thread_local PAGE_PTR input_handler_list::m_tl_new_page_buf = nullptr;
  thread_local qfile::tde_read_scratch *input_handler_list::m_tl_new_tde = nullptr;


  int
  input_handler_list::init_on_main (THREAD_ENTRY *thread_p, QFILE_LIST_ID *list_id, int parallelism)
  {
    /* Idempotent close before re-open. */
    qfile_close_list_sector_scan (thread_p, &m_sector_scan);
    delete m_new_dist;
    m_new_dist = nullptr;
    m_new_tapeset = nullptr;
    m_new_parallelism = 0;
    m_next_new_reader_id.store (0, std::memory_order_relaxed);
    m_list_id = nullptr;

    if (parallelism <= 0 || list_id == nullptr)
      {
	return NO_ERROR;
      }

    if (qfile_list_has_new_backing (list_id))
      {
	qfile::tapeset *ts = (qfile::tapeset *) QFILE_LIST_ID_TAPESET (list_id);
	if (ts == nullptr || QFILE_LIST_ID_NEW_CONTAINS_OVERFLOW (list_id))
	  {
	    return ER_FAILED;
	  }

	m_new_dist = new qfile::chunk_distributor (ts, parallelism);
	if (m_new_dist == nullptr)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
		    sizeof (qfile::chunk_distributor));
	    return ER_FAILED;
	  }

	m_new_tapeset = ts;
	m_new_parallelism = parallelism;
	m_list_id = list_id;
	return NO_ERROR;
      }

    if (VPID_ISNULL (&QFILE_LIST_ID_FIRST_VPID(list_id)))
      {
	return NO_ERROR;
      }

    /* Defer publishing m_list_id until open succeeds. */
    int error_code = qfile_open_list_sector_scan (thread_p, list_id, &m_sector_scan);
    if (error_code != NO_ERROR)
      {
	return error_code;
      }
    m_list_id = list_id;
    return NO_ERROR;
  }

  int
  input_handler_list::initialize (THREAD_ENTRY *thread_p, HFID *hfid, SCAN_ID *scan_id)
  {
    m_tl_bitmap = 0;
    VSID_SET_NULL (&m_tl_vsid);
    m_tl_current_tfile = nullptr;

    /* first live worker wins membuf — cf. sector_page_iterator (px_hash_join_task_manager.cpp:225) */
    m_tl_is_membuf_worker = false;
    m_tl_membuf_pageid = 0;
    m_tl_new_reader_id = -1;
    m_tl_new_have_chunk = false;
    m_tl_new_page_offset = 0;

    if (m_new_tapeset != nullptr)
      {
	m_tl_new_reader_id = m_next_new_reader_id.fetch_add (1, std::memory_order_relaxed);
	if (m_tl_new_reader_id < 0 || m_tl_new_reader_id >= m_new_parallelism)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	    return ER_FAILED;
	  }
	if (m_tl_new_page_raw == nullptr)
	  {
	    m_tl_new_page_raw = (char *) malloc (DB_PAGESIZE);
	    if (m_tl_new_page_raw == nullptr)
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, DB_PAGESIZE);
		return ER_FAILED;
	      }
	    m_tl_new_page_buf = (PAGE_PTR) m_tl_new_page_raw;
	  }
	if (m_tl_new_tde == nullptr)
	  {
	    m_tl_new_tde = new qfile::tde_read_scratch ();
	    if (m_tl_new_tde == nullptr)
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (qfile::tde_read_scratch));
		free (m_tl_new_page_raw);
		m_tl_new_page_raw = nullptr;
		m_tl_new_page_buf = nullptr;
		return ER_FAILED;
	      }
	  }
      }

    if (m_sector_scan.sector_info.membuf_tfile != nullptr)
      {
	bool expected = false;
	if (m_sector_scan.membuf_claimed.compare_exchange_strong (expected, true, std::memory_order_acq_rel))
	  {
	    m_tl_is_membuf_worker = true;
	  }
      }

    (void) hfid;
    (void) scan_id;
    return NO_ERROR;
  }

  SCAN_CODE
  input_handler_list::get_next_page_with_fix (THREAD_ENTRY *thread_p,
      PAGE_PTR &out_page,
      QMGR_TEMP_FILE *&out_tfile)
  {
    out_page = nullptr;
    out_tfile = nullptr;

    if (m_new_tapeset != nullptr)
      {
	while (true)
	  {
	    if (!m_tl_new_have_chunk
		|| m_tl_new_page_offset >= m_tl_new_range.start_page + m_tl_new_range.page_count)
	      {
		if (!m_new_dist->next_chunk (m_tl_new_reader_id, m_tl_new_range))
		  {
		    return S_END;
		  }
		m_tl_new_have_chunk = true;
		m_tl_new_page_offset = m_tl_new_range.start_page;
	      }

	    qfile::tape *tape_p = m_new_tapeset->get_tape (m_tl_new_range.tape_idx);
	    if (tape_p == nullptr)
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
		return S_ERROR;
	      }

	    PAGE_PTR page_p = tape_p->read_page_into (thread_p, m_tl_new_page_offset,
			       (char *) m_tl_new_page_buf, m_tl_new_tde);
	    m_tl_new_page_offset++;
	    if (page_p == nullptr)
	      {
		assert_release_error (er_errid () != NO_ERROR);
		return S_ERROR;
	      }

	    if (QFILE_GET_TUPLE_COUNT (page_p) == QFILE_OVERFLOW_TUPLE_COUNT_FLAG)
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
		return S_ERROR;
	      }
	    out_page = page_p;
	    out_tfile = nullptr;	/* NEW pages are borrowed RAM/scratch, never qmgr/pgbuf fixes. */
	    return S_SUCCESS;
	  }
      }

    while (true)
      {
	VPID vpid;

	/* Phase 1: membuf owner drains membuf pages (NULL_VOLID, sequential pageid). */
	if (m_tl_is_membuf_worker
	    && m_sector_scan.sector_info.membuf_tfile != nullptr
	    && m_tl_membuf_pageid <= m_sector_scan.sector_info.membuf_tfile->membuf_last)
	  {
	    vpid.volid = NULL_VOLID;
	    vpid.pageid = m_tl_membuf_pageid++;
	    m_tl_current_tfile = m_sector_scan.sector_info.membuf_tfile;
	  }
	else
	  {
	    /* Phase 2: refill bitmap from next sector via dynamic atomic claim. */
	    if (m_tl_bitmap == 0)
	      {
		int sidx = m_sector_scan.next_sector_index.fetch_add (1, std::memory_order_relaxed);
		if (sidx >= m_sector_scan.sector_info.sector_cnt)
		  {
		    return S_END;
		  }
		m_tl_vsid = m_sector_scan.sector_info.sectors[sidx].vsid;
		m_tl_bitmap = m_sector_scan.sector_info.sectors[sidx].page_bitmap;
		m_tl_current_tfile = (QMGR_TEMP_FILE *) m_sector_scan.sector_info.tfiles[sidx];
		if (m_tl_bitmap == 0)
		  {
		    continue;	/* defensive: empty sector */
		  }
	      }

	    if (!qfile_sector_bitmap_next_vpid (&m_tl_vsid, &m_tl_bitmap, &vpid))
	      {
		continue;	/* defensive: helper false despite non-zero check above */
	      }
	  }

	/* Single READ-latch fix; transfer ownership to caller on S_SUCCESS. */
	PAGE_PTR page_p = qmgr_get_old_page_read_only (thread_p, &vpid, m_tl_current_tfile);
	if (page_p == nullptr)
	  {
	    assert_release_error (er_errid () != NO_ERROR);
	    return S_ERROR;
	  }

	/* Overflow continuations: the start-page owner walks the chain via qfile_assemble_overflow_tuple. */
	if (QFILE_GET_TUPLE_COUNT (page_p) == QFILE_OVERFLOW_TUPLE_COUNT_FLAG)
	  {
	    qmgr_free_old_page (thread_p, page_p, m_tl_current_tfile);
	    continue;
	  }

	out_page = page_p;
	out_tfile = m_tl_current_tfile;
	return S_SUCCESS;
      }
  }

  int
  input_handler_list::finalize (THREAD_ENTRY *thread_p)
  {
    m_tl_bitmap = 0;
    VSID_SET_NULL (&m_tl_vsid);
    m_tl_current_tfile = nullptr;
    m_tl_is_membuf_worker = false;
    m_tl_membuf_pageid = 0;
    m_tl_new_reader_id = -1;
    m_tl_new_have_chunk = false;
    m_tl_new_page_offset = 0;
    free (m_tl_new_page_raw);
    m_tl_new_page_raw = nullptr;
    m_tl_new_page_buf = nullptr;
    delete m_tl_new_tde;
    m_tl_new_tde = nullptr;
    return NO_ERROR;
  }

  void
  input_handler_list::cleanup_on_main (THREAD_ENTRY *thread_p)
  {
    qfile_close_list_sector_scan (thread_p, &m_sector_scan);
    delete m_new_dist;
    m_new_dist = nullptr;
    m_new_tapeset = nullptr;
    m_new_parallelism = 0;
    m_next_new_reader_id.store (0, std::memory_order_relaxed);
    m_list_id = nullptr;
  }
}
