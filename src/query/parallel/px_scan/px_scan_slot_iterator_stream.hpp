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
 * px_scan_slot_iterator_stream.hpp - stream slot iterator (Option B of the C3+C5 byte
 *                                    representation; merged component C3+C5, bottom half)
 *
 * Sibling of slot_iterator_list, selected by scan_traits<SCAN_TYPE::STREAM>.  Walks a
 * channel-owned row_batch buffer holding whole qfile T_MERGE tuples in the SAME wire
 * format the materialized list path stores (per-tuple QFILE_TUPLE_LENGTH_SIZE-prefixed
 * length, then column data), laid end-to-end with NO 32-byte QFILE page header.  The
 * per-tuple eval body (fetch_val_list + predicate) is identical to slot_iterator_list;
 * only the buffer layout (no header), the tuple-count source (row_batch.tuple_cnt) and
 * the free path (db_private_free, never qmgr_free_old_page) differ.
 *
 * No overflow chain: an oversized tuple travels inline as its own single-tuple batch
 * (channel INV-OVERSIZE), so there is no qfile_assemble_overflow_tuple branch and no
 * tfile_vfid coupling.  LOB/DB_ELO column bytes ride inside the tuple verbatim (locator
 * representation), exactly as on a materialized list page.
 *
 * BATCH LIFETIME / PINNING (R7 -- the load-bearing rule): set_batch () adopts ownership
 * of the batch buffer and PINS it; DB_VALUEs bound by fetch_val_list PEEK into the buffer,
 * so the buffer is freed only when the iterator moves PAST it -- on the next set_batch ()
 * (the worker requests a new batch only after the upper operator finished per-tuple work
 * for every tuple of the current one, exactly the lifetime a materialized list page gives
 * today) or at finalize ().  No DB_VALUE ever points into a freed batch.
 */

#ifndef _PX_SCAN_SLOT_ITERATOR_STREAM_HPP_
#define _PX_SCAN_SLOT_ITERATOR_STREAM_HPP_

#include "px_stream_channel.hpp"
#include "query_evaluator.h"
#include "query_list.h"
#include "scan_manager.h"
#include "storage_common.h"

namespace parallel_scan
{
  class slot_iterator_stream
  {
    public:
      slot_iterator_stream ();
      ~slot_iterator_stream ();

      int initialize (THREAD_ENTRY *thread_p, SCAN_ID *scan_id, val_descr *vd);

      /* frees the pinned batch (if any); the caller must not bind values past this */
      int finalize (THREAD_ENTRY *thread_p);

      /* Adopts ownership of batch.buf (whole length-prefixed T_MERGE tuples, no page
       * header).  Frees the previously pinned batch first -- by then its last tuple has
       * been fully consumed by the upper operator (see pinning rule above). */
      int set_batch (THREAD_ENTRY *thread_p, const parallel_query::row_batch &batch);

      /* same eval contract as slot_iterator_list::next_qualified_slot_with_peek */
      SCAN_CODE next_qualified_slot_with_peek (THREAD_ENTRY *thread_p);

    private:
      void free_pinned_batch (THREAD_ENTRY *thread_p);

      parallel_query::row_batch m_batch;	/* pinned batch; owns m_batch.buf while non-NULL */
      QFILE_TUPLE m_curr_tpl;
      int m_curr_tplno;
      int m_tuple_count;
      SCAN_PRED m_scan_pred;
      regu_variable_list_node *m_rest_regu_list;
      QFILE_TUPLE_RECORD *m_tplrecp;
      val_list_node *m_val_list;
      val_descr *m_vd;
      SCAN_STATS *m_scan_stats;
      bool m_on_trace;
  };
}

#endif /* _PX_SCAN_SLOT_ITERATOR_STREAM_HPP_ */
