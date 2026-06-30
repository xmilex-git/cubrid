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
 * px_scan_input_handler_list.hpp
 */

#ifndef _PX_SCAN_INPUT_HANDLER_LIST_HPP_
#define _PX_SCAN_INPUT_HANDLER_LIST_HPP_

#include "px_interrupt.hpp"
#include "query_list.h"
#include "query_manager.h"
#include "scan_manager.h"
#include "qfile_tape.hpp"

#include <atomic>

namespace parallel_scan
{
  class input_handler_list
  {
      using interrupt = parallel_query::interrupt;
      using err_messages_with_lock = parallel_query::err_messages_with_lock;

    public:
      input_handler_list (interrupt *interrupt_p, err_messages_with_lock *err_messages_p)
	: m_sector_scan (),
	  m_new_dist (nullptr),
	  m_new_tapeset (nullptr),
	  m_new_parallelism (0),
	  m_next_new_reader_id (0),
	  m_list_id (nullptr),
	  m_interrupt_p (interrupt_p),
	  m_err_messages_p (err_messages_p)
      {
      }

      int init_on_main (THREAD_ENTRY *thread_p, QFILE_LIST_ID *list_id, int parallelism);

      /*
       * OLD backing: single READ-latch page fix, ownership transfers to caller on S_SUCCESS.
       * NEW backing: arms this worker's thread-local tapeset_reader and returns a null page sentinel once.
       */
      SCAN_CODE get_next_page_with_fix (THREAD_ENTRY *thread_p,
					PAGE_PTR &out_page,
					QMGR_TEMP_FILE *&out_tfile);

      int initialize (THREAD_ENTRY *thread_p, HFID *hfid, SCAN_ID *scan_id);
      int finalize (THREAD_ENTRY *thread_p);

      /* manager must call before destruction (implicit dtor lacks THREAD_ENTRY); idempotent. */
      void cleanup_on_main (THREAD_ENTRY *thread_p);

      QFILE_LIST_ID *get_list_id ()
      {
	return m_list_id;
      }

      static qfile::tapeset_reader *get_thread_new_reader ()
      {
	return m_tl_new_reader;
      }

      static void mark_thread_new_reader_exhausted ()
      {
	m_tl_new_reader_exhausted = true;
      }

    private:
      QFILE_LIST_SECTOR_SCAN_INFO m_sector_scan;
      qfile::chunk_distributor *m_new_dist;
      qfile::tapeset *m_new_tapeset;
      int m_new_parallelism;
      std::atomic<int> m_next_new_reader_id;


      QFILE_LIST_ID *m_list_id;
      interrupt *m_interrupt_p;
      err_messages_with_lock *m_err_messages_p;

      thread_local static UINT64 m_tl_bitmap;
      thread_local static VSID m_tl_vsid;
      thread_local static QMGR_TEMP_FILE *m_tl_current_tfile;
      thread_local static bool m_tl_is_membuf_worker;
      thread_local static int m_tl_membuf_pageid;
      thread_local static int m_tl_new_reader_id;
      thread_local static qfile::tapeset_reader *m_tl_new_reader;
      thread_local static bool m_tl_new_reader_exhausted;
  };
}

#endif /* _PX_SCAN_INPUT_HANDLER_LIST_HPP_ */
