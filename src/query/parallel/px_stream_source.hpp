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
 * px_stream_source.hpp - Streamed Result Source, consumer-side handle over one
 *                        streamed hash-join output edge (merged component C3+C5)
 *
 * The top half of the merged "Streamed Result Source" (architecture_deepening_designs.md
 * C3+C5): the per-spec object the consumer scan opens INSTEAD of a list-file scan when the
 * hash-join output is streamed.  It pops parallel_query::row_batch payloads from the C2
 * stream_channel and hands them to the bottom half (parallel_scan::input_handler_stream +
 * slot_iterator_stream), which walks the whole qfile-tuple (T_MERGE wire format) bytes.
 *
 * Inert unless explicitly constructed by a streamed pipeline: no existing code path
 * constructs a stream_source, so with the feature OFF the engine is observable-identical
 * (SSOT R2).
 *
 * Contract (streaming_hashjoin_SSOT.md):
 *  - NON-OWNING.  The channel, the interrupt, and the pipeline they belong to are owned by
 *    the streaming pipeline owner (C7); this handle never frees the channel.  The handle
 *    must not be used after the pipeline owner reaches CONSUMER_CLOSED.
 *  - Fan-out = 1 (SSOT section 6).  A streamed source is single-drain: open () succeeds
 *    exactly once; a second open is refused with an error, never silently shared.
 *  - R4 single-pass.  reset () / re-open ALWAYS hard-fails: assert in debug builds,
 *    er_set + error return in release.  A rewound single-pass transport would silently
 *    re-read nothing (corruption); this guard makes any miss loud.
 *  - EOS (R10).  next_batch () returns S_END exactly when the channel reports clean
 *    end-of-stream: ring empty AND the LAST registered producer called producer_done ().
 *    S_END is sticky/idempotent.
 *  - R11 early close.  close () before EOS aborts the channel so blocked producers wake
 *    and stop; close never raises an error itself (a normal early termination is not an
 *    error).  Residual in-flight batches are then freed exactly once via drain_residue (),
 *    which the teardown runner calls AFTER all producers are joined (the channel's
 *    drain_one contract).
 *  - R7 ownership.  A batch returned by next_batch () is owned by the caller, who must
 *    free its buf (the bottom-half slot iterator pins it until the batch's last tuple has
 *    been consumed, then frees it on the next set_batch/finalize).
 *
 * Error model: C error model only -- no exceptions; er_set + return codes / SCAN_CODE.
 */

#ifndef _PX_STREAM_SOURCE_HPP_
#define _PX_STREAM_SOURCE_HPP_

#include <atomic>
#include <cstdint>

#include "px_interrupt.hpp"
#include "px_stream_channel.hpp"
#include "storage_common.h"	/* SCAN_CODE */
#include "thread_compat.hpp"

namespace parallel_query
{
  class stream_source
  {
    public:
      enum class source_state : int
      {
	CREATED = 0,		/* constructed, not yet opened */
	OPEN,			/* draining; next_batch is legal */
	END_OF_STREAM,		/* clean EOS observed (sticky S_END) */
	CLOSED			/* close () called; no further next_batch */
      };

      /* channel_p/interrupt_p are NON-OWNING handles into the pipeline owner's state (C7);
       * degree is the consumer parallel degree decided from the probe-INPUT estimate
       * (never from the unknown output page_cnt -- spec I2). */
      stream_source (stream_channel<row_batch> *channel_p, interrupt *interrupt_p, int degree);
      ~stream_source ();

      stream_source (const stream_source &) = delete;
      stream_source &operator= (const stream_source &) = delete;

      /* Open the single consumer drain.  Fan-out = 1: the first open returns NO_ERROR;
       * any later open (second consumer, or re-open after close -- an R4 rewind shape)
       * is refused: assert in debug, er_set + error code in release. */
      int open ();

      /* Pop the next owned batch.
       *  S_SUCCESS - out_batch populated; the CALLER now owns out_batch.buf.
       *  S_END     - clean end-of-stream (INV-EOS); sticky.
       *  S_ERROR   - channel aborted or interrupt set (the aborting side already
       *              er_set + set the shared interrupt; no error is raised here). */
      SCAN_CODE next_batch (THREAD_ENTRY *thread_p, row_batch &out_batch);

      /* R4 guard: a streamed source can NEVER be rewound.  Always fails loud:
       * assert in debug; er_set + negative error code in release.  Never a silent no-op. */
      int reset ();

      /* R11: idempotent close from any state.  Mid-stream (state OPEN, EOS not reached)
       * it aborts the channel so blocked producers wake promptly and stop; after clean
       * EOS it is a quiet state transition.  Never raises an error (a normal early
       * termination must not surface a spurious error to the client). */
      void close (THREAD_ENTRY *thread_p);

      /* Teardown drain: frees every residual in-flight batch exactly once via the
       * channel's drain_one protocol.  PRECONDITION (channel contract): all producers
       * have been joined; the source is CLOSED or at EOS.  Returns # of batches freed.
       * Called by the pipeline teardown runner (C7), not by consumer workers. */
      int drain_residue (THREAD_ENTRY *thread_p);

      int get_degree () const
      {
	return m_degree;
      }

      source_state get_state () const
      {
	return (source_state) m_state.load (std::memory_order_acquire);
      }

      bool is_open () const
      {
	return get_state () == source_state::OPEN;
      }

      /* batches handed out so far (test/trace visibility) */
      std::uint64_t get_batches_delivered () const
      {
	return m_batches_delivered.load (std::memory_order_relaxed);
      }

    private:
      stream_channel<row_batch> *m_channel_p;	/* non-owning (owned by the pipeline, C7) */
      interrupt *m_interrupt_p;			/* non-owning shared pipeline interrupt */
      int m_degree;				/* consumer degree, from the probe-input estimate */

      std::atomic<int> m_state;			/* source_state; forward-only transitions */
      std::atomic<std::uint64_t> m_batches_delivered;
  };
}

#endif /* _PX_STREAM_SOURCE_HPP_ */
