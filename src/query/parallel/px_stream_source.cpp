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
 * px_stream_source.cpp - Streamed Result Source (merged component C3+C5, top half)
 *
 * See px_stream_source.hpp for the full contract (fan-out=1, R4 reset guard, R11 early
 * close, INV-EOS, batch ownership).  State transitions are forward-only and CAS-based so
 * concurrent consumer workers and an aborting/closing driver converge without locks.
 */

#include "px_stream_source.hpp"

#include <cassert>

#include "error_code.h"
#include "error_manager.h"
#include "memory_alloc.h"

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace parallel_query
{
  stream_source::stream_source (stream_channel<row_batch> *channel_p, interrupt *interrupt_p, int degree)
    : m_channel_p (channel_p),
      m_interrupt_p (interrupt_p),
      m_degree (degree),
      m_state ((int) source_state::CREATED),
      m_batches_delivered (0)
  {
    assert (channel_p != NULL);
    assert (interrupt_p != NULL);
    assert (degree >= 1);
  }

  stream_source::~stream_source ()
  {
    /* Non-owning: never frees the channel/interrupt.  The pipeline owner (C7) must have
     * drained residue before destroying the channel; nothing to verify here without
     * thread context. */
  }

  int
  stream_source::open ()
  {
    int expected = (int) source_state::CREATED;

    if (m_state.compare_exchange_strong (expected, (int) source_state::OPEN, std::memory_order_acq_rel))
      {
	return NO_ERROR;
      }

    /* Second consumer (fan-out > 1) or re-open after close/EOS -- a single-pass transport
     * cannot be shared or rewound (SSOT section 6 / R4).  Refuse loudly, never silently. */
    assert_release (false);
    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
    return ER_QPROC_INVALID_XASLNODE;
  }

  SCAN_CODE
  stream_source::next_batch (THREAD_ENTRY *thread_p, row_batch &out_batch)
  {
    source_state state = get_state ();

    (void) thread_p;

    out_batch.buf = NULL;
    out_batch.len = 0;
    out_batch.tuple_cnt = 0;

    if (state == source_state::END_OF_STREAM)
      {
	/* sticky clean EOS */
	return S_END;
      }
    if (state != source_state::OPEN)
      {
	/* pop on a never-opened or already-closed source is a caller contract violation;
	 * fail loud rather than hang on a channel nobody fills/drains. */
	assert_release (false);
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
	return S_ERROR;
      }

    if (m_channel_p->pop (out_batch, *m_interrupt_p))
      {
	/* INV-OWN: ownership of out_batch.buf transferred to the caller */
	m_batches_delivered.fetch_add (1, std::memory_order_relaxed);
	return S_SUCCESS;
      }

    if (m_channel_p->is_aborted ()
	|| m_interrupt_p->get_code () != interrupt::interrupt_code::NO_INTERRUPT)
      {
	/* The aborting side already er_set + set the shared interrupt (channel error
	 * model); raising another error here would mask or duplicate it. */
	return S_ERROR;
      }

    /* Clean EOS: ring empty AND the last registered producer is done (INV-EOS, R10).
     * Any worker may win the CAS; losers re-observe END_OF_STREAM above. */
    int expected = (int) source_state::OPEN;
    m_state.compare_exchange_strong (expected, (int) source_state::END_OF_STREAM, std::memory_order_acq_rel);
    return S_END;
  }

  int
  stream_source::reset ()
  {
    /* R4 -- restartability guard.  A streamed source is single-pass: a backward reset /
     * rewind / re-open would silently deliver nothing (or re-deliver), i.e. corrupt
     * results.  Hard-fail on every attempt: assert in debug, error in release; NEVER a
     * silent re-read. */
    assert_release (false);
    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
    return ER_QPROC_INVALID_XASLNODE;
  }

  void
  stream_source::close (THREAD_ENTRY *thread_p)
  {
    (void) thread_p;

    /* Idempotent, callable from any state; forward-only into CLOSED. */
    source_state prev = (source_state) m_state.exchange ((int) source_state::CLOSED, std::memory_order_acq_rel);

    if (prev == source_state::OPEN)
      {
	/* R11 -- early termination while producers may still be live: abort the channel
	 * so a push-blocked producer wakes promptly (push () == false, ownership
	 * retained) and no producer ever blocks on a buffer no one will drain.  This is
	 * a NORMAL early close -- no er_set here, no spurious error to the client. */
	m_channel_p->abort ();
      }
    /* prev == END_OF_STREAM: clean completion, nothing to wake.
     * prev == CREATED: never opened; nothing launched against this handle.
     * prev == CLOSED: repeat close, no-op. */
  }

  int
  stream_source::drain_residue (THREAD_ENTRY *thread_p)
  {
    row_batch batch;
    int drained = 0;

    /* Channel contract: drain_one only after all producers are joined; the pipeline
     * teardown runner guarantees that ordering (C7 OWN-3). */
    assert (get_state () == source_state::CLOSED || get_state () == source_state::END_OF_STREAM);

    while (m_channel_p->drain_one (batch))
      {
	if (batch.buf != NULL)
	  {
	    db_private_free (thread_p, batch.buf);
	  }
	drained++;
      }

    return drained;
  }
}
