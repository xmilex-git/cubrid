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
 * px_scan_list_page_steal_pool.cpp -- page-level work-stealing for parallel list scan
 */

#include "px_scan_list_page_steal_pool.hpp"

#include "error_code.h"
#include "error_manager.h"
#include "list_file.h"
#include "object_representation.h"
#include "query_manager.h"

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace parallel_scan
{
  list_page_steal_pool::list_page_steal_pool ()
    : m_bitmaps (nullptr)
    , m_mutex ()
    , m_cv ()
    , m_next_steal_cursor (0)
    , m_active_workers (0)
    , m_no_more_sectors (false)
    , m_sector_cnt (0)
    , m_initialized (false)
  {
  }

  list_page_steal_pool::~list_page_steal_pool ()
  {
    delete[] m_bitmaps;
    m_bitmaps = nullptr;
  }

  void
  list_page_steal_pool::init (int sector_cnt, FILE_PARTIAL_SECTOR *sectors, void **tfiles)
  {
    m_sector_cnt = sector_cnt;
    m_bitmaps = new sector_bitmap_entry[sector_cnt];
    for (int i = 0; i < sector_cnt; i++)
      {
	m_bitmaps[i].bitmap.store (sectors[i].page_bitmap, std::memory_order_relaxed);
	m_bitmaps[i].vsid = sectors[i].vsid;
	m_bitmaps[i].tfile = (QMGR_TEMP_FILE *) tfiles[i];
      }
    m_next_steal_cursor.store (0, std::memory_order_relaxed);
    m_active_workers = 0;
    m_no_more_sectors = false;
    m_initialized = true;
  }

  bool
  list_page_steal_pool::pop_page (int sector_idx, VPID &out_vpid, QMGR_TEMP_FILE *&out_tfile)
  {
    if (sector_idx < 0 || sector_idx >= m_sector_cnt)
      {
	return false;
      }
    sector_bitmap_entry &entry = m_bitmaps[sector_idx];
    UINT64 old_bmp = entry.bitmap.load (std::memory_order_acquire);
    while (old_bmp != 0)
      {
	int bit = __builtin_ctzll (old_bmp);
	UINT64 new_bmp = old_bmp & ~(1ULL << bit);
	if (entry.bitmap.compare_exchange_weak (old_bmp, new_bmp,
						std::memory_order_acq_rel,
						std::memory_order_acquire))
	  {
	    out_vpid.volid = entry.vsid.volid;
	    out_vpid.pageid = SECTOR_FIRST_PAGEID (entry.vsid.sectid) + bit;
	    out_tfile = entry.tfile;
	    return true;
	  }
      }
    return false;
  }

  bool
  list_page_steal_pool::try_pop_any_page (VPID &out_vpid, QMGR_TEMP_FILE *&out_tfile)
  {
    int start = m_next_steal_cursor.fetch_add (1, std::memory_order_relaxed) % m_sector_cnt;
    for (int i = 0; i < m_sector_cnt; i++)
      {
	int idx = (start + i) % m_sector_cnt;
	if (pop_page (idx, out_vpid, out_tfile))
	  {
	    return true;
	  }
      }
    return false;
  }

  SCAN_CODE
  list_page_steal_pool::wait_or_help (THREAD_ENTRY *thread_p, PAGE_PTR &out_page, QMGR_TEMP_FILE *&out_tfile)
  {
    out_page = nullptr;
    out_tfile = nullptr;

    while (true)
      {
	VPID vpid;
	QMGR_TEMP_FILE *tfile = nullptr;
	if (try_pop_any_page (vpid, tfile))
	  {
	    PAGE_PTR page_p = qmgr_get_old_page_read_only (thread_p, &vpid, tfile);
	    if (page_p == nullptr)
	      {
		return S_ERROR;
	      }
	    /* skip overflow continuation pages */
	    if (QFILE_GET_TUPLE_COUNT (page_p) == QFILE_OVERFLOW_TUPLE_COUNT_FLAG)
	      {
		qmgr_free_old_page (thread_p, page_p, tfile);
		continue;
	      }
	    out_page = page_p;
	    out_tfile = tfile;
	    return S_SUCCESS;
	  }

	std::unique_lock<std::mutex> lock (m_mutex);
	if (m_no_more_sectors && m_active_workers <= 0)
	  {
	    return S_END;
	  }
	/* double-check after lock: active worker may have left pages while we were locking */
	VPID recheck_vpid;
	QMGR_TEMP_FILE *recheck_tfile = nullptr;
	if (try_pop_any_page (recheck_vpid, recheck_tfile))
	  {
	    lock.unlock ();
	    PAGE_PTR page_p = qmgr_get_old_page_read_only (thread_p, &recheck_vpid, recheck_tfile);
	    if (page_p == nullptr)
	      {
		return S_ERROR;
	      }
	    if (QFILE_GET_TUPLE_COUNT (page_p) == QFILE_OVERFLOW_TUPLE_COUNT_FLAG)
	      {
		qmgr_free_old_page (thread_p, page_p, recheck_tfile);
		continue;
	      }
	    out_page = page_p;
	    out_tfile = recheck_tfile;
	    return S_SUCCESS;
	  }
	if (m_no_more_sectors && m_active_workers <= 0)
	  {
	    return S_END;
	  }
	m_cv.wait_for (lock, std::chrono::milliseconds (50));
      }
  }

  bool
  list_page_steal_pool::has_stealable_pages () const
  {
    for (int i = 0; i < m_sector_cnt; i++)
      {
	if (m_bitmaps[i].bitmap.load (std::memory_order_relaxed) != 0)
	  {
	    return true;
	  }
      }
    return false;
  }

  void
  list_page_steal_pool::enter_worker ()
  {
    std::lock_guard<std::mutex> lock (m_mutex);
    m_active_workers++;
  }

  void
  list_page_steal_pool::leave_worker ()
  {
    std::lock_guard<std::mutex> lock (m_mutex);
    m_active_workers--;
    if (m_active_workers <= 0)
      {
	m_cv.notify_all ();
      }
  }

  void
  list_page_steal_pool::signal_no_more_sectors ()
  {
    std::lock_guard<std::mutex> lock (m_mutex);
    m_no_more_sectors = true;
    m_cv.notify_all ();
  }
}
