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
 * px_scan_slot_iterator_stream.cpp
 *
 * The per-tuple eval body intentionally mirrors slot_iterator_list::
 * next_qualified_slot_with_peek (px_scan_slot_iterator_list.cpp) so the streamed walk is
 * behavior-identical to the materialized list walk over the same T_MERGE bytes (R1).
 * Differences are exactly the representation ones (Option B): no 32-byte page header, the
 * tuple count comes from row_batch.tuple_cnt instead of the page header, there is no
 * overflow-page branch (oversized tuples arrive inline as single-tuple batches), and the
 * pinned buffer is freed with db_private_free, never qmgr_free_old_page.
 */

#include "px_scan_slot_iterator_stream.hpp"

#include <cassert>

#include "error_manager.h"
#include "fetch.h"
#include "memory_alloc.h"
#include "object_representation.h"

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace parallel_scan
{
  slot_iterator_stream::slot_iterator_stream ()
    : m_curr_tpl (nullptr),
      m_curr_tplno (0),
      m_tuple_count (0),
      m_rest_regu_list (nullptr),
      m_tplrecp (nullptr),
      m_val_list (nullptr),
      m_vd (nullptr),
      m_scan_stats (nullptr),
      m_on_trace (false)
  {
    m_batch.buf = nullptr;
    m_batch.len = 0;
    m_batch.tuple_cnt = 0;
    m_scan_pred = { nullptr, nullptr, nullptr };
  }

  slot_iterator_stream::~slot_iterator_stream ()
  {
    /* finalize () must have freed the pinned batch (implicit dtor lacks THREAD_ENTRY) */
    assert (m_batch.buf == nullptr);
  }

  int
  slot_iterator_stream::initialize (THREAD_ENTRY *thread_p, SCAN_ID *scan_id, val_descr *vd)
  {
    /* The streamed TARGET_LIST consumer spec carries its predicates/regu lists in the
     * same LLIST_SCAN_ID fields a list scan uses; only the byte source differs. */
    LLIST_SCAN_ID *llsidp = &scan_id->s.llsid;
    m_scan_pred = llsidp->scan_pred;
    m_rest_regu_list = llsidp->rest_regu_list;
    m_tplrecp = llsidp->tplrecp;
    m_val_list = scan_id->val_list;
    m_vd = vd;
    m_scan_stats = &scan_id->scan_stats;
    m_on_trace = thread_p->on_trace;
    m_batch.buf = nullptr;
    m_batch.len = 0;
    m_batch.tuple_cnt = 0;
    m_curr_tpl = nullptr;
    m_curr_tplno = 0;
    m_tuple_count = 0;
    return NO_ERROR;
  }

  void
  slot_iterator_stream::free_pinned_batch (THREAD_ENTRY *thread_p)
  {
    if (m_batch.buf != nullptr)
      {
	db_private_free (thread_p, m_batch.buf);
	m_batch.buf = nullptr;
      }
    m_batch.len = 0;
    m_batch.tuple_cnt = 0;
    m_curr_tpl = nullptr;
    m_curr_tplno = 0;
    m_tuple_count = 0;
  }

  int
  slot_iterator_stream::finalize (THREAD_ENTRY *thread_p)
  {
    free_pinned_batch (thread_p);
    return NO_ERROR;
  }

  int
  slot_iterator_stream::set_batch (THREAD_ENTRY *thread_p, const parallel_query::row_batch &batch)
  {
    /* Free the previous pinned batch: by the worker-loop contract a new batch is
     * requested only after per-tuple work for the previous batch's LAST tuple completed,
     * so no bound DB_VALUE still points into it (R7 pinning rule). */
    free_pinned_batch (thread_p);

    assert (batch.buf != nullptr);
    assert (batch.len >= 0);
    assert (batch.tuple_cnt >= 0);

    m_batch = batch;
    m_curr_tpl = m_batch.buf;	/* tuples start at offset 0 -- NO QFILE page header */
    m_curr_tplno = 0;
    m_tuple_count = m_batch.tuple_cnt;
    return NO_ERROR;
  }

  SCAN_CODE
  slot_iterator_stream::next_qualified_slot_with_peek (THREAD_ENTRY *thread_p)
  {
    DB_LOGICAL ev_res;

    while (m_curr_tplno < m_tuple_count)
      {
	QFILE_TUPLE tpl = m_curr_tpl;

	/* whole-tuple invariant (INV-OVERSIZE): every tuple lies fully inside the batch;
	 * a malformed batch is a producer bug, not a recoverable state */
	assert (tpl != nullptr);
	assert (tpl + QFILE_TUPLE_LENGTH_SIZE <= m_batch.buf + m_batch.len);
	assert (QFILE_GET_TUPLE_LENGTH (tpl) >= QFILE_TUPLE_LENGTH_SIZE);
	assert (tpl + QFILE_GET_TUPLE_LENGTH (tpl) <= m_batch.buf + m_batch.len);

	m_curr_tpl += QFILE_GET_TUPLE_LENGTH (m_curr_tpl);
	m_curr_tplno++;

	if (m_val_list)
	  {
	    if (fetch_val_list (thread_p, m_scan_pred.regu_list, m_vd, nullptr, nullptr, tpl, PEEK) != NO_ERROR)
	      {
		return S_ERROR;
	      }
	  }

	if (m_on_trace)
	  {
	    m_scan_stats->read_rows++;
	  }

	ev_res = V_TRUE;
	if (m_scan_pred.pr_eval_fnc && m_scan_pred.pred_expr)
	  {
	    ev_res = (*m_scan_pred.pr_eval_fnc) (thread_p, m_scan_pred.pred_expr, m_vd, nullptr);
	    if (ev_res == V_ERROR)
	      {
		return S_ERROR;
	      }
	  }

	if (ev_res != V_TRUE)
	  {
	    continue;
	  }

	if (m_on_trace)
	  {
	    m_scan_stats->qualified_rows++;
	  }

	if (m_val_list && m_rest_regu_list)
	  {
	    if (fetch_val_list (thread_p, m_rest_regu_list, m_vd, nullptr, nullptr, tpl, PEEK) != NO_ERROR)
	      {
		return S_ERROR;
	      }
	  }

	if (m_tplrecp)
	  {
	    m_tplrecp->tpl = tpl;
	  }

	return S_SUCCESS;
      }

    return S_END;
  }
}
