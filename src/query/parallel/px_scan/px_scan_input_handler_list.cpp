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
#include "query_list.h"
#include "query_manager.h"

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace parallel_scan
{
  thread_local int input_handler_list::m_tl_new_reader_id = -1;
  thread_local qfile::tapeset_reader *input_handler_list::m_tl_new_reader = nullptr;
  thread_local bool input_handler_list::m_tl_new_reader_exhausted = false;


  int
  input_handler_list::init_on_main (THREAD_ENTRY *thread_p, QFILE_LIST_ID *list_id, int parallelism)
  {
    /* Idempotent reset before re-open. */
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
	if (ts == nullptr)
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

    /* #113/#130: an OLD-backed list never opens a parallel list scan (the
     * scan-open guard demotes it to a serial list scan) and the OLD sector
     * reader is deleted; reaching here is a guard violation. */
    assert (false);
    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
    return ER_FAILED;
  }

  int
  input_handler_list::initialize (THREAD_ENTRY *thread_p, HFID *hfid, SCAN_ID *scan_id)
  {
    m_tl_new_reader_id = -1;
    delete m_tl_new_reader;
    m_tl_new_reader = nullptr;
    m_tl_new_reader_exhausted = false;

    if (m_new_tapeset != nullptr)
      {
	m_tl_new_reader_id = m_next_new_reader_id.fetch_add (1, std::memory_order_relaxed);
	if (m_tl_new_reader_id < 0 || m_tl_new_reader_id >= m_new_parallelism)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	    return ER_FAILED;
	  }

	m_tl_new_reader = new qfile::tapeset_reader (m_new_tapeset, m_new_dist, m_tl_new_reader_id);
	if (m_tl_new_reader == nullptr)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
		    sizeof (qfile::tapeset_reader));
	    return ER_FAILED;
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
	if (m_tl_new_reader == nullptr || m_tl_new_reader_exhausted)
	  {
	    return S_END;
	  }

	/* NEW Tapeset input is tuple-sourced by the worker's tapeset_reader; the
	 * null page sentinel only drives the existing task set_page/drain loop. */
	return S_SUCCESS;
      }

    /* #113/#130: an OLD-backed list never reaches a parallel list scan; the OLD
     * sector page walk is deleted. */
    assert (false);
    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
    return S_ERROR;
  }

  int
  input_handler_list::finalize (THREAD_ENTRY *thread_p)
  {
    m_tl_new_reader_id = -1;
    delete m_tl_new_reader;
    m_tl_new_reader = nullptr;
    m_tl_new_reader_exhausted = false;
    return NO_ERROR;
  }

  void
  input_handler_list::cleanup_on_main (THREAD_ENTRY *thread_p)
  {
    delete m_new_dist;
    m_new_dist = nullptr;
    m_new_tapeset = nullptr;
    m_new_parallelism = 0;
    m_next_new_reader_id.store (0, std::memory_order_relaxed);
    m_list_id = nullptr;
  }
}
