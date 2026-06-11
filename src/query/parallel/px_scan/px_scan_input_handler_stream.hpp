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
 * px_scan_input_handler_stream.hpp - per-worker batch intake of the Streamed Result
 *                                    Source (merged component C3+C5, bottom half)
 *
 * Sibling of input_handler_list, selected by scan_traits<SCAN_TYPE::STREAM>.  Where the
 * list handler claims frozen QFILE pages from a sector snapshot, this handler pops live
 * row_batch payloads (whole length-prefixed T_MERGE tuple bytes, NO page header) from the
 * C2 channel through the stream_source.  The popped batch is bound to the NEW
 * slot_iterator_stream (set_batch) -- never to slot_iterator_list, which assumes a 32-byte
 * QFILE page header, a qmgr-owned PAGE_PTR and a tfile_vfid overflow chain.
 *
 * Inert: nothing constructs this handler yet (SSOT R2 -- OFF observable-identical);
 * wiring into scan_open_parallel_list_scan happens in a later step.
 *
 * Ownership: on S_SUCCESS the popped batch buffer is owned by the calling worker, who must
 * hand it to slot_iterator_stream::set_batch (which pins it until the batch's last tuple
 * is consumed, then frees it) or free it directly on an error path.
 */

#ifndef _PX_SCAN_INPUT_HANDLER_STREAM_HPP_
#define _PX_SCAN_INPUT_HANDLER_STREAM_HPP_

#include "px_interrupt.hpp"
#include "px_stream_source.hpp"
#include "scan_manager.h"

namespace parallel_scan
{
  class input_handler_stream
  {
      using interrupt = parallel_query::interrupt;
      using err_messages_with_lock = parallel_query::err_messages_with_lock;

    public:
      input_handler_stream (interrupt *interrupt_p, err_messages_with_lock *err_messages_p)
	: m_source_p (nullptr),
	  m_interrupt_p (interrupt_p),
	  m_err_messages_p (err_messages_p)
      {
      }

      /* Binds (and opens) the single consumer drain on the source.  Fan-out = 1 and the
       * R4 re-open guard are enforced inside stream_source::open (). */
      int init_on_main (THREAD_ENTRY *thread_p, parallel_query::stream_source *source_p, int parallelism);

      /* Worker-side intake (mirrors input_handler_list::get_next_page_with_fix):
       *  S_SUCCESS - out_batch owned by the caller (bind via slot_iterator_stream::set_batch)
       *  S_END     - clean end-of-stream (after ALL producers are done, INV-EOS)
       *  S_ERROR   - channel aborted / interrupt set. */
      SCAN_CODE get_next_batch (THREAD_ENTRY *thread_p, parallel_query::row_batch &out_batch);

      int initialize (THREAD_ENTRY *thread_p, HFID *hfid, SCAN_ID *scan_id);
      int finalize (THREAD_ENTRY *thread_p);

      /* manager must call before destruction; idempotent.  Mid-stream this is the R11
       * early-close: stream_source::close () aborts the channel. */
      void cleanup_on_main (THREAD_ENTRY *thread_p);

      parallel_query::stream_source *get_source ()
      {
	return m_source_p;
      }

    private:
      parallel_query::stream_source *m_source_p;	/* non-owning (lifetime owned by the pipeline, C7) */
      interrupt *m_interrupt_p;
      err_messages_with_lock *m_err_messages_p;
  };
}

#endif /* _PX_SCAN_INPUT_HANDLER_STREAM_HPP_ */
