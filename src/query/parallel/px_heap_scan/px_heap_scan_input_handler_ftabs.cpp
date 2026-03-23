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
 * px_heap_scan_input_handler_ftabs.cpp
 */


#include "px_heap_scan_input_handler_ftabs.hpp"
#include "dbtype_def.h"
#include "error_code.h"
#include "page_buffer.h"
#include "px_heap_scan_ftab_set.hpp"
#include "storage_common.h"
#include "bit.h"
#include "file_manager.h"
#include "io_uring.hpp"
#include <exception>
#include <unordered_set>
#include "thread_compat.hpp"
#include "vpid.hpp"
#include "vpid_utilities.hpp"


#if !defined(NDEBUG)
#include <sys/syscall.h>
#include "error_manager.h"
#endif

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace parallel_heap_scan
{
  thread_local HEAP_SCANCACHE *input_handler_ftabs::m_tl_scan_cache = NULL;
  thread_local PGBUF_WATCHER input_handler_ftabs::m_tl_old_page_watcher = {0};
  thread_local ftab_set *input_handler_ftabs::m_tl_ftab_set = NULL;
  thread_local std::queue<VPID> *input_handler_ftabs::m_tl_non_io_vpid_queue = NULL;
  thread_local std::unordered_set<VPID> *input_handler_ftabs::m_tl_io_vpid_set = NULL;
  thread_local bool input_handler_ftabs::m_tl_prefetch_ended = false;
  thread_local iouring::manager *tl_io_uring_manager = NULL;
  const int IO_URING_PREFETCH_SECTORS = 16; /* 1024 request of page / 64 page per sector */

  void input_handler_ftabs::prefetch_sectors (THREAD_ENTRY *thread_p)
  {
    FILE_PARTIAL_SECTOR sec = {{ -1, -1 }, 0};
    FILE_PARTIAL_SECTOR *sec_p = &sec;
    FILE_PARTIAL_SECTOR output = {{-1, -1}, 0};
    FILE_PARTIAL_SECTOR *out_p = &output;

    for (int i = 0; i < IO_URING_PREFETCH_SECTORS; i++)
      {
	sec = m_tl_ftab_set->get_prefetch_next();
	if (VSID_IS_NULL (&sec.vsid))
	  {
	    m_tl_prefetch_ended = true;
	    break;
	  }
	pgbuf_prefetch_sector (thread_p, (UINTPTR)sec_p, (UINTPTR)out_p, tl_io_uring_manager);
	VPID start_vpid = {SECTOR_FIRST_PAGEID (sec.vsid.sectid), sec.vsid.volid};
	VPID vpid = start_vpid;
	for (int i = 0; i < DISK_SECTOR_NPAGES; i++)
	  {
	    if (bit64_is_set (sec.page_bitmap, i))
	      {
		vpid.pageid = start_vpid.pageid + i;
		if (bit64_is_set (output.page_bitmap, i))
		  {
		    /* io prefetched pages */
		    m_tl_io_vpid_set->insert (vpid);
		  }
		else
		  {
		    /* on data buffer pages */
		    m_tl_non_io_vpid_queue->push (vpid);
		  }
	      }
	  }
      }
    tl_io_uring_manager->submit();
  }

  int input_handler_ftabs::initialize (THREAD_ENTRY *thread_p, HFID *hfid, SCAN_ID *scan_id)
  {
    m_tl_scan_cache = &scan_id->s.hsid.scan_cache;
    /* open_scan should have succeeded */
    assert (m_tl_scan_cache->debug_initpattern == 12345);
    PGBUF_INIT_WATCHER (&m_tl_old_page_watcher, PGBUF_ORDERED_HEAP_NORMAL, hfid);
    int idx = m_splited_ftab_set_idx.fetch_add (1);
    if (idx < 0 || (size_t) idx >= m_splited_ftab_set.size ())
      {
	assert_release (false);
	return ER_FAILED;
      }
    m_tl_ftab_set = &m_splited_ftab_set[idx];

    try
      {
	m_tl_non_io_vpid_queue = new std::queue<VPID> ();
	m_tl_io_vpid_set = new std::unordered_set<VPID> ();
	m_tl_io_vpid_set->reserve (iouring::IO_URING_DEFAULT_QUEUE_SIZE);
	tl_io_uring_manager = new iouring::manager ();
      }
    catch (...)
      {
	er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
		sizeof (VPID) * iouring::IO_URING_DEFAULT_QUEUE_SIZE);
	return ER_FAILED;
      }
    tl_io_uring_manager->initialize();

    m_tl_prefetch_ended = false;
    prefetch_sectors (thread_p);
#if !defined(NDEBUG)
    size_t io_vpid_size = m_tl_io_vpid_set->size();
    size_t non_io_vpid_size = m_tl_non_io_vpid_queue->size();
#endif

    return NO_ERROR;
  }

  int input_handler_ftabs::init_on_main (THREAD_ENTRY *thread_p, HFID hfid, int parallelism)
  {
    FILE_FTAB_COLLECTOR collector;
    int error_code;
    m_hfid = hfid;

    error_code = file_get_all_data_sectors (thread_p, &m_hfid.vfid, &collector);
    if (error_code != NO_ERROR)
      {
	if (collector.partsect_ftab != NULL)
	  {
	    db_private_free_and_init (thread_p, collector.partsect_ftab);
	  }
	return error_code;
      }
    m_ftab_set.convert (&collector);
    m_splited_ftab_set = m_ftab_set.split (parallelism);
    m_splited_ftab_set_idx.store (0);
    m_ftab_set.clear();

    if (collector.partsect_ftab != NULL)
      {
	db_private_free_and_init (thread_p, collector.partsect_ftab);
      }
    return NO_ERROR;
  }

  SCAN_CODE input_handler_ftabs::get_next_vpid_with_fix (THREAD_ENTRY *thread_p, VPID *vpid)
  {
    SCAN_CODE ret = S_SUCCESS;
    VPID curr_vpid;
    int error_code;
    size_t io_vpid_size, non_io_vpid_size;
    bool is_valid_page = false;



    if (m_tl_scan_cache->page_watcher.pgptr != NULL)
      {
	pgbuf_replace_watcher (thread_p, &m_tl_scan_cache->page_watcher, &m_tl_old_page_watcher);
      }

    while (!is_valid_page)
      {
	io_vpid_size = m_tl_io_vpid_set->size();
	non_io_vpid_size = m_tl_non_io_vpid_queue->size();
	/* get next vpid */
	if (non_io_vpid_size > 0)
	  {
	    curr_vpid = m_tl_non_io_vpid_queue->front();
	    m_tl_non_io_vpid_queue->pop();
	  }
	else if (io_vpid_size > 0)
	  {
	    error_code = pgbuf_get_prefetched_vpid (thread_p, &curr_vpid, tl_io_uring_manager);
	    if (error_code != NO_ERROR)
	      {
		er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
		return S_ERROR;
	      }
	    m_tl_io_vpid_set->erase (curr_vpid);
	  }
	else if (m_tl_prefetch_ended)
	  {
	    pgbuf_ordered_unfix (thread_p, &m_tl_old_page_watcher);
	    return S_END;
	  }
	else
	  {
	    assert_release_error (false);
	    return S_ERROR;
	  }

	/* read-ahead */
	if (!m_tl_prefetch_ended && io_vpid_size < iouring::IO_URING_DEFAULT_QUEUE_SIZE/2)
	  {
	    prefetch_sectors (thread_p);
	  }

#if defined(NDEBUG)
	error_code = pgbuf_ordered_fix_release (thread_p, &curr_vpid, OLD_PAGE_PREVENT_DEALLOC, PGBUF_LATCH_READ,
						&m_tl_scan_cache->page_watcher, true);
#else
	error_code = pgbuf_ordered_fix_debug (thread_p, &curr_vpid, OLD_PAGE_PREVENT_DEALLOC, PGBUF_LATCH_READ,
					      &m_tl_scan_cache->page_watcher, true, ARG_FILE_LINE_FUNC);
#endif
	if (m_tl_old_page_watcher.pgptr != NULL)
	  {
	    pgbuf_ordered_unfix (thread_p, &m_tl_old_page_watcher);
	  }

	if (error_code != NO_ERROR)
	  {
	    return S_ERROR;
	  }

	is_valid_page = (m_tl_scan_cache->page_watcher.pgptr != NULL);
	*vpid = curr_vpid;
      }

    return ret;
  }

  int input_handler_ftabs::finalize (THREAD_ENTRY *thread_p)
  {
    while (!m_tl_io_vpid_set->empty())
      {
	VPID curr_vpid;
	pgbuf_get_prefetched_vpid (thread_p, &curr_vpid, tl_io_uring_manager);
	m_tl_io_vpid_set->erase (curr_vpid);
      }

    if (m_tl_old_page_watcher.pgptr != NULL)
      {
	pgbuf_ordered_unfix (thread_p, &m_tl_old_page_watcher);
      }
    if (m_tl_scan_cache->page_watcher.pgptr != NULL)
      {
	pgbuf_ordered_unfix (thread_p, &m_tl_scan_cache->page_watcher);
      }
    m_tl_scan_cache = NULL;
    m_tl_old_page_watcher.pgptr = NULL;
    delete tl_io_uring_manager;
    tl_io_uring_manager = NULL;
    delete m_tl_non_io_vpid_queue;
    m_tl_non_io_vpid_queue = NULL;
    delete m_tl_io_vpid_set;
    m_tl_io_vpid_set = NULL;
    return NO_ERROR;
  }
}
