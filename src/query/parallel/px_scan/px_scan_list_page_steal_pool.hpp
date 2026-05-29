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
 * px_scan_list_page_steal_pool.hpp -- page-level work-stealing for parallel list scan
 */

#ifndef _PX_SCAN_LIST_PAGE_STEAL_POOL_HPP_
#define _PX_SCAN_LIST_PAGE_STEAL_POOL_HPP_

#include <atomic>
#include <mutex>
#include <condition_variable>
#include <vector>

#include "storage_common.h"
#include "query_manager.h"

namespace parallel_scan
{
  struct sector_bitmap_entry
  {
    std::atomic<UINT64> bitmap;
    VSID vsid;
    QMGR_TEMP_FILE *tfile;
  };

  class list_page_steal_pool
  {
    public:
      list_page_steal_pool ();
      ~list_page_steal_pool ();

      void init (int sector_cnt, FILE_PARTIAL_SECTOR *sectors, void **tfiles);

      /* lock-free fast path: pop one page from sector's shared bitmap via CAS */
      bool pop_page (int sector_idx, VPID &out_vpid, QMGR_TEMP_FILE *&out_tfile);

      /* stealer entry: scan all sectors round-robin for remaining pages, fix via qmgr_get_old_page_read_only */
      SCAN_CODE wait_or_help (THREAD_ENTRY *thread_p, PAGE_PTR &out_page, QMGR_TEMP_FILE *&out_tfile);

      bool has_stealable_pages () const;

      void enter_worker ();
      void leave_worker ();
      void signal_no_more_sectors ();

      bool is_initialized () const
      {
	return m_initialized;
      }

    private:
      bool try_pop_any_page (VPID &out_vpid, QMGR_TEMP_FILE *&out_tfile);

      sector_bitmap_entry *m_bitmaps;
      std::mutex m_mutex;
      std::condition_variable m_cv;
      std::atomic<int> m_next_steal_cursor;
      int m_active_workers;
      bool m_no_more_sectors;
      int m_sector_cnt;
      bool m_initialized;
  };
}

#endif /* _PX_SCAN_LIST_PAGE_STEAL_POOL_HPP_ */
