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
 * px_stream_pipeline.hpp - streaming hash-join pipeline lifetime owner (component C7)
 *
 * THE single owner of every object with a cross-worker lifetime on one streamed
 * hash-join edge (architecture_deepening_designs.md C7).  It owns:
 *   - the C2 stream_channel (created at create (), freed in the RELEASED transition),
 *   - the shared interrupt (member; alive until destroy ()),
 *   - the whole-pipeline worker reservation (exactly 2*D, acquired atomically by the C4
 *     policy and handed in at create (); released EXACTLY ONCE, post-JOINED -- OWN-1),
 *   - the consumer-side stream_source it hands out as a NON-OWNING handle,
 *   - opaque producer-side state (XASL_STATE / hash table / HASHJOIN_CONTEXT bundle)
 *     registered at launch_producers (); freed exactly once in RELEASED (OWN-2).
 *
 * State machine (forward-only; values ordered so `<` means "earlier"):
 *
 *   BEGIN -> PRODUCER_STARTED -> CONSUMER_OPEN -> CONSUMER_CLOSED -> JOINED -> RELEASED
 *
 * Ordered teardown (OWN-3), executed by the SINGLE teardown runner inside join_all ():
 *
 *   close consumer -> join producers -> drain channel residue -> release workers once
 *                  -> free owned state (producer bundle, source, channel)
 *
 * join_all () MUST complete (state RELEASED) before qexec_clear_head_lists-time cleanup
 * (query_executor.c, mainblock epilogue ~:16390) touches any resource the producer or a
 * consumer worker may still reference; destroy () asserts that contract.
 *
 * Concurrency contract (architecture_deepening_designs.md C7 SYNC / TEARDOWN-RUNNER /
 * ABORT-JOIN-ALL / HANDLE-LIFETIME):
 *  - State transitions are single CAS edges on an atomic state; the compound teardown
 *    body runs on exactly ONE runner, elected by an atomic claim flag.  A join_all ()
 *    loser observes (yield-spins) until RELEASED and then returns; it never re-runs any
 *    teardown step.
 *  - DRIVER OPERATIONS -- create / launch_producers / open_consumer / join_all /
 *    destroy -- are called by the one driving (mainblock) thread; they are not
 *    thread-safe against each other.  ANY-THREAD operations -- abort (),
 *    close_consumer (), producer_task_finished () -- are safe from any thread, any
 *    state, any number of times, with exactly-once side effects.
 *  - abort () is the always-safe wake (R6/R11): it publishes the first interrupt code
 *    (first writer wins) and calls channel abort () under the pointer-guard mutex, so a
 *    late abort can never touch a channel the runner already freed.  abort () does NOT
 *    execute the teardown itself: a producer task aborting on its own error path must
 *    never become the runner that joins producers (it would self-deadlock).  The sole
 *    teardown executor is join_all (), which the driver always calls before cleanup --
 *    so abort-from-any-state still converges on the one ordered teardown.  (This is the
 *    one deliberate narrowing of the design doc's "abort CASes into the teardown".)
 *  - HANDLE-LIFETIME: the stream_source returned by open_consumer () and any injected
 *    pool handle are non-owning and INVALID once the pipeline reaches CONSUMER_CLOSED;
 *    the consumer never waits on, releases, or destroys pipeline-owned state.  The
 *    worker reservation is released exactly once, only on the JOINED -> RELEASED edge.
 *  - close_consumer () is the R11 feedback edge: consumer early termination advances
 *    the state to CONSUMER_CLOSED and aborts the channel if producers may still be
 *    running, so a push-blocked producer wakes promptly.  A normal early close is NOT
 *    an error: no interrupt is set, no er_set.
 *
 * Deadlock freedom of the teardown (R5/R6/R11): before the runner waits for producers it
 * has ALWAYS made them unblockable -- the close-consumer step aborts the channel unless
 * clean EOS was reached, and clean EOS (INV-EOS) implies every producer already finished
 * pushing.  A producer can therefore never be joined while still blocked on a full
 * channel (no lost-wakeup).
 *
 * Teardown-order instrumentation: each teardown step records a monotonic sequence stamp
 * (0 = not yet executed) so tests assert join < drain < release < free strictly.
 *
 * Inert unless explicitly constructed: no production path creates a stream_pipeline yet
 * (R2: feature OFF = observable-identical).
 *
 * Error model: C error model only -- no exceptions; er_set + return codes.
 */

#ifndef _PX_STREAM_PIPELINE_HPP_
#define _PX_STREAM_PIPELINE_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>

#include "px_interrupt.hpp"
#include "px_stream_channel.hpp"
#include "px_stream_source.hpp"
#include "thread_compat.hpp"

namespace parallel_query
{
  class stream_pipeline
  {
    public:
      enum class pipe_state : int
      {
	BEGIN = 0,		/* created; exact 2*D reservation owned; nothing launched */
	PRODUCER_STARTED,	/* producer tasks registered/launched; channel being fed */
	CONSUMER_OPEN,		/* consumer source opened on the channel, draining */
	CONSUMER_CLOSED,	/* no further pop will occur; consumer handles now INVALID */
	JOINED,			/* all producer tasks joined; no worker references owned state */
	RELEASED		/* residue drained, workers released once, owned state freed */
      };

      /* The whole-pipeline worker reservation the pipeline owns from creation on.
       * reserved_workers is EXACTLY 2*D by construction (the C4 policy's atomic
       * all-or-nothing reservation, OWN-5); release (handle) is called EXACTLY ONCE,
       * by the single teardown runner, only after JOINED (OWN-1/HANDLE-LIFETIME).
       * C-style hook so the lifetime logic links without the engine pool in unit
       * tests; the engine binds { worker_manager*, 2*D,
       * stream_policy_release_pool_handle }.  release == NULL means "no pool owned"
       * (test pipelines); handle may be anything release understands. */
      struct pool_binding
      {
	void *handle;
	int reserved_workers;
	void (*release) (void *handle);
      };

      /* frees the opaque producer-side state bundle exactly once, post-JOINED */
      typedef void (*producer_state_free_fn) (THREAD_ENTRY *thread_p, void *producer_state);

      /* DRIVER: construct the owner in state BEGIN.  Takes ownership of pool
       * IMMEDIATELY: on any construction failure the reservation is released here
       * (never leaked, never partial) and NULL is returned -- the caller falls back
       * to the materialized path (legal: still before any emit, R3/R5). */
      static stream_pipeline *create (THREAD_ENTRY *thread_p, const pool_binding &pool,
				      std::size_t channel_capacity);

      /* DRIVER: destruction; requires join_all () to have completed (RELEASED) --
       * i.e. strictly before qexec_clear_head_lists-time cleanup.  On contract
       * violation: assert_release, then a defensive join_all () so release builds
       * still tear down in order rather than leak.  pipe_ref is NULLed. */
      static void destroy (THREAD_ENTRY *thread_p, stream_pipeline *&pipe_ref);

      /* DRIVER: BEGIN -> PRODUCER_STARTED.  Registers producer_count producers on the
       * channel (all registered before the first producer_done -- INV-EOS) and takes
       * ownership of the opaque producer state bundle.  The caller launches the actual
       * tasks AFTER this returns; every launched task must, on EVERY exit path, call
       * channel producer_done () exactly once and then producer_task_finished () as its
       * very LAST touch of pipeline-owned state. */
      int launch_producers (THREAD_ENTRY *thread_p, int producer_count,
			    void *producer_state, producer_state_free_fn free_fn);

      /* ANY-THREAD: a producer task's last action.  After the final producer's call the
       * teardown runner's join completes.  Deliberately a single atomic decrement --
       * no lock, no condition variable -- so the task's last touch of the pipeline can
       * never race its destruction (the runner yield-spins on the counter, mirroring
       * worker_manager::wait_workers). */
      void producer_task_finished ();

      /* DRIVER: PRODUCER_STARTED -> CONSUMER_OPEN.  Returns the NON-OWNING consumer
       * handle (HANDLE-LIFETIME: invalid once the pipeline reaches CONSUMER_CLOSED).
       * The pipeline keeps ownership of the source object itself. */
      stream_source *open_consumer (THREAD_ENTRY *thread_p, int degree);

      /* ANY-THREAD, idempotent: advance to CONSUMER_CLOSED (R11 feedback edge).  The
       * winner of the transition closes the source; if EOS was not reached (or no
       * consumer ever opened) that aborts the channel so producers still running
       * unblock promptly.  Normal early termination: never sets the interrupt, never
       * raises an error. */
      void close_consumer (THREAD_ENTRY *thread_p);

      /* DRIVER (TEARDOWN-RUNNER): the ordered teardown to RELEASED --
       *   close consumer -> join producers -> drain residue -> release workers once ->
       *   free owned state.
       * Idempotent: the runner is elected by an atomic claim; a loser yield-spins until
       * RELEASED and returns without re-running any step.  MUST complete before
       * qexec_clear_head_lists-time cleanup. */
      int join_all (THREAD_ENTRY *thread_p);

      /* ANY-THREAD, idempotent, any state, any number of times (R6/R11): the
       * always-safe wake.  Publishes code into the shared interrupt (first abort wins)
       * and aborts the channel (INV-ABORT) so blocked producers AND a blocked consumer
       * wake; pointer-guarded so re-entry after RELEASED is a strict no-op.  Does not
       * run the teardown (see header comment); the driver's join_all () completes it. */
      void abort (THREAD_ENTRY *thread_p,
		  interrupt::interrupt_code code = interrupt::interrupt_code::ERROR_INTERRUPTED_FROM_MAIN_THREAD);

      pipe_state get_state () const
      {
	return (pipe_state) m_state.load (std::memory_order_acquire);
      }

      /* non-owning accessors (producer sink binding / tests) */
      stream_channel<row_batch> *get_channel ()
      {
	return m_channel_p;
      }
      interrupt *get_interrupt ()
      {
	return &m_interrupt;
      }

      int get_reserved_workers () const
      {
	return m_pool.reserved_workers;
      }

      /* NON-OWNING pool handle (engine: the worker_manager holding the 2*D
       * reservation) for dispatching consumer tasks; valid until the teardown
       * runner's release step (HANDLE-LIFETIME) */
      void *get_pool_handle () const
      {
	return m_pool.handle;
      }
      bool is_abort_requested () const
      {
	return m_abort_requested.load (std::memory_order_acquire);
      }

      /* teardown-order instrumentation (test/trace visibility): monotonic stamps,
       * 0 = step not executed yet; strictly joined < drained < released < freed */
      std::uint64_t get_seq_producers_joined () const
      {
	return m_seq_producers_joined.load (std::memory_order_acquire);
      }
      std::uint64_t get_seq_residue_drained () const
      {
	return m_seq_residue_drained.load (std::memory_order_acquire);
      }
      std::uint64_t get_seq_workers_released () const
      {
	return m_seq_workers_released.load (std::memory_order_acquire);
      }
      std::uint64_t get_seq_state_freed () const
      {
	return m_seq_state_freed.load (std::memory_order_acquire);
      }
      int get_pool_release_calls () const
      {
	return m_pool_release_calls.load (std::memory_order_acquire);
      }
      int get_residue_drained_count () const
      {
	return m_residue_drained_count.load (std::memory_order_acquire);
      }

      /* public only for placement_new inside create () (db_private_alloc pattern, like
       * worker_manager); use create ()/destroy (), never construct directly */
      explicit stream_pipeline (const pool_binding &pool);

    private:
      ~stream_pipeline ();

      stream_pipeline (const stream_pipeline &) = delete;
      stream_pipeline &operator= (const stream_pipeline &) = delete;

      /* forward-only multi-CAS: advance to CONSUMER_CLOSED from any earlier state;
       * true iff THIS call performed the transition (exactly-once side effects) */
      bool transition_to_consumer_closed ();

      std::uint64_t next_seq ()
      {
	return m_seq_counter.fetch_add (1, std::memory_order_acq_rel) + 1;
      }

      std::atomic<int> m_state;			/* pipe_state; forward-only */
      std::atomic<bool> m_teardown_claimed;	/* TEARDOWN-RUNNER election */
      std::atomic<bool> m_abort_requested;

      /* guards m_channel_p / m_source_p pointer use by ANY-THREAD ops (abort /
       * close_consumer) against the runner nulling them in the free step; the runner
       * never holds it across a blocking wait, so abort can always get in to wake a
       * blocked side */
      std::mutex m_ptr_mtx;

      stream_channel<row_batch> *m_channel_p;	/* owned; freed in RELEASED step */
      stream_source *m_source_p;		/* owned; handed out as non-owning handle */
      interrupt m_interrupt;			/* owned; alive until destroy () */

      pool_binding m_pool;			/* owned reservation; released exactly once */

      std::atomic<int> m_producers_active;	/* producer tasks not yet finished */
      int m_producer_count;

      void *m_producer_state;			/* opaque owned bundle (OWN-2) */
      producer_state_free_fn m_producer_state_free;

      /* instrumentation */
      std::atomic<std::uint64_t> m_seq_counter;
      std::atomic<std::uint64_t> m_seq_producers_joined;
      std::atomic<std::uint64_t> m_seq_residue_drained;
      std::atomic<std::uint64_t> m_seq_workers_released;
      std::atomic<std::uint64_t> m_seq_state_freed;
      std::atomic<int> m_pool_release_calls;
      std::atomic<int> m_residue_drained_count;
  };
}

#endif /* _PX_STREAM_PIPELINE_HPP_ */
