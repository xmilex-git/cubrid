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
 * px_stream_pipeline.cpp - streaming hash-join pipeline lifetime owner (component C7)
 *
 * See px_stream_pipeline.hpp for the full ownership/concurrency contract.  The load-
 * bearing rules implemented here:
 *  - exactly ONE teardown runner (atomic claim), running the ordered teardown
 *    close -> join -> drain -> release-once -> free; losers observe until RELEASED;
 *  - before the runner waits for producers they are always unblockable (channel aborted
 *    unless clean EOS, which itself implies all producers finished) -- no lost-wakeup;
 *  - any-thread abort/close touch the channel/source only under the pointer-guard
 *    mutex, so a late abort cannot use a channel the runner already freed;
 *  - a producer task's last touch of the pipeline is one atomic decrement, so joining
 *    never races task exit (the runner yield-spins, like worker_manager::wait_workers).
 */

#include "px_stream_pipeline.hpp"

#include <cassert>
#include <thread>

#include "error_code.h"
#include "error_manager.h"
#include "memory_alloc.h"
#include "system_parameter.h"	/* er_log_debug gate (PRM_ID_ER_LOG_DEBUG) */

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace parallel_query
{
  stream_pipeline::stream_pipeline (const pool_binding &pool)
    : m_state ((int) pipe_state::BEGIN),
      m_teardown_claimed (false),
      m_abort_requested (false),
      m_ptr_mtx (),
      m_channel_p (NULL),
      m_source_p (NULL),
      m_interrupt (),
      m_pool (pool),
      m_producers_active (0),
      m_producer_count (0),
      m_producer_state (NULL),
      m_producer_state_free (NULL),
      m_seq_counter (0),
      m_seq_producers_joined (0),
      m_seq_residue_drained (0),
      m_seq_workers_released (0),
      m_seq_state_freed (0),
      m_pool_release_calls (0),
      m_residue_drained_count (0)
  {
  }

  stream_pipeline::~stream_pipeline ()
  {
    /* destroy () guarantees RELEASED before delete; all owned state already freed */
    assert (get_state () == pipe_state::RELEASED);
    assert (m_channel_p == NULL);
    assert (m_source_p == NULL);
  }

  stream_pipeline *
  stream_pipeline::create (THREAD_ENTRY *thread_p, const pool_binding &pool, std::size_t channel_capacity)
  {
    /* OWN-5: only an exact whole-pipeline reservation may reach an owner; the C4
     * policy's all-or-nothing CAS guarantees it.  From here on the reservation is
     * owned: every failure path below releases it exactly once before returning.
     * Allocation pattern: db_private_alloc + placement_new (like worker_manager);
     * db_private_alloc reports its own OOM error. */
    assert (pool.release == NULL || pool.reserved_workers > 0);

    stream_pipeline *pipe = (stream_pipeline *) db_private_alloc (thread_p, sizeof (stream_pipeline));
    if (pipe == NULL)
      {
	if (pool.release != NULL)
	  {
	    pool.release (pool.handle);
	  }
	return NULL;
      }
    pipe = placement_new (pipe, pool);

    stream_channel<row_batch> *channel_p =
	    (stream_channel<row_batch> *) db_private_alloc (thread_p, sizeof (stream_channel<row_batch>));
    if (channel_p == NULL)
      {
	if (pool.release != NULL)
	  {
	    pool.release (pool.handle);
	    pipe->m_pool.release = NULL;	/* already released; teardown must not double-release */
	    pipe->m_pool.handle = NULL;
	  }
	pipe->m_state.store ((int) pipe_state::RELEASED, std::memory_order_release);
	pipe->~stream_pipeline ();
	db_private_free (thread_p, pipe);
	return NULL;
      }
    pipe->m_channel_p = placement_new (channel_p, channel_capacity);

    pipe->m_metrics.t_create_us.store (stream_metrics_now_us (), std::memory_order_relaxed);

    return pipe;		/* state BEGIN */
  }

  void
  stream_pipeline::destroy (THREAD_ENTRY *thread_p, stream_pipeline *&pipe_ref)
  {
    stream_pipeline *pipe = pipe_ref;

    pipe_ref = NULL;
    if (pipe == NULL)
      {
	return;
      }

    if (pipe->get_state () != pipe_state::RELEASED)
      {
	/* contract: join_all () completes (RELEASED) before destruction -- and thus
	 * before qexec_clear_head_lists-time cleanup touches shared resources.
	 * Recover defensively in release builds rather than leak or free live state. */
	assert_release (false);
	(void) pipe->join_all (thread_p);
      }

    pipe->~stream_pipeline ();
    db_private_free (thread_p, pipe);
  }

  int
  stream_pipeline::launch_producers (THREAD_ENTRY *thread_p, int producer_count,
				     void *producer_state, producer_state_free_fn free_fn)
  {
    int expected = (int) pipe_state::BEGIN;

    (void) thread_p;

    if (producer_count < 1
	|| !m_state.compare_exchange_strong (expected, (int) pipe_state::PRODUCER_STARTED,
	    std::memory_order_acq_rel))
      {
	/* driver contract violation (double launch, launch after teardown, bad count) */
	assert_release (false);
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
	return ER_QPROC_INVALID_XASLNODE;
      }

    /* OWN-2: the pipeline now owns the producer-side bundle; freed exactly once in the
     * RELEASED transition, strictly after producers are joined. */
    m_producer_state = producer_state;
    m_producer_state_free = free_fn;

    m_producer_count = producer_count;
    m_producers_active.store (producer_count, std::memory_order_release);

    /* INV-EOS: ALL producers registered before any task can call producer_done () --
     * the caller launches tasks only after this returns. */
    for (int i = 0; i < producer_count; i++)
      {
	m_channel_p->register_producer ();
      }

    m_metrics.t_launch_us.store (stream_metrics_now_us (), std::memory_order_relaxed);

    return NO_ERROR;
  }

  void
  stream_pipeline::producer_task_finished ()
  {
    int remaining = m_producers_active.fetch_sub (1, std::memory_order_acq_rel) - 1;

    /* more finishes than launches would mean a task exited twice */
    assert (remaining >= 0);
    (void) remaining;

    /* deliberately nothing else: this atomic decrement is the task's LAST touch of
     * pipeline-owned state, so task exit can never race teardown/destruction */
  }

  stream_source *
  stream_pipeline::open_consumer (THREAD_ENTRY *thread_p, int degree)
  {
    stream_source *source_p;
    int expected;

    source_p = (stream_source *) db_private_alloc (thread_p, sizeof (stream_source));
    if (source_p == NULL)
      {
	/* db_private_alloc reports its own OOM error */
	return NULL;
      }
    source_p = placement_new (source_p, m_channel_p, &m_interrupt, degree, &m_metrics);

    expected = (int) pipe_state::PRODUCER_STARTED;
    if (!m_state.compare_exchange_strong (expected, (int) pipe_state::CONSUMER_OPEN,
					  std::memory_order_acq_rel))
      {
	/* driver contract violation (no producer launched yet, double open, or open
	 * after close/teardown -- the fan-out=1 / R4 shapes) */
	source_p->~stream_source ();
	db_private_free (thread_p, source_p);
	assert_release (false);
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
	return NULL;
      }

    {
      std::lock_guard<std::mutex> lk (m_ptr_mtx);
      m_source_p = source_p;
    }

    /* first open on a fresh source always succeeds (fan-out=1 is enforced inside) */
    (void) source_p->open ();

    m_metrics.t_consumer_open_us.store (stream_metrics_now_us (), std::memory_order_relaxed);

    /* NON-OWNING handle (HANDLE-LIFETIME): invalid once state reaches CONSUMER_CLOSED */
    return source_p;
  }

  bool
  stream_pipeline::transition_to_consumer_closed ()
  {
    int cur = m_state.load (std::memory_order_acquire);

    while (cur < (int) pipe_state::CONSUMER_CLOSED)
      {
	if (m_state.compare_exchange_weak (cur, (int) pipe_state::CONSUMER_CLOSED,
					   std::memory_order_acq_rel))
	  {
	    return true;	/* this call performed the transition */
	  }
	/* cur reloaded; re-check */
      }

    return false;		/* already at/past CONSUMER_CLOSED */
  }

  void
  stream_pipeline::close_consumer (THREAD_ENTRY *thread_p)
  {
    if (!transition_to_consumer_closed ())
      {
	/* idempotent: someone already closed (or teardown already past it) */
	return;
      }

    m_metrics.t_consumer_closed_us.store (stream_metrics_now_us (), std::memory_order_relaxed);

    /* Exactly-once close side effect, by the transition winner (R11): make every
     * producer that may still be running unblockable.  Touch the source/channel only
     * under the pointer guard so this can never race the runner's free step. */
    std::lock_guard<std::mutex> lk (m_ptr_mtx);

    if (m_source_p != NULL)
      {
	/* aborts the channel iff the consumer had NOT reached clean EOS; a normal
	 * early termination -- no interrupt, no er_set, no spurious error (R11) */
	m_source_p->close (thread_p);
      }
    else if (m_channel_p != NULL)
      {
	/* no consumer was ever opened: producers (if any) could block forever on a
	 * full channel no one will drain -- abort is the only correct wake (R11) */
	m_channel_p->abort ();
      }
  }

  int
  stream_pipeline::join_all (THREAD_ENTRY *thread_p)
  {
    if (m_teardown_claimed.exchange (true, std::memory_order_acq_rel))
      {
	/* TEARDOWN-RUNNER loser: observe until the single runner reaches RELEASED;
	 * never re-run any teardown step (no double-join / double-release) */
	while (get_state () != pipe_state::RELEASED)
	  {
	    std::this_thread::yield ();
	  }
	return NO_ERROR;
      }

    /* ---- the SINGLE teardown runner: ordered teardown (OWN-3) ---- */

    m_metrics.t_join_claim_us.store (stream_metrics_now_us (), std::memory_order_relaxed);

    /* 0. close the consumer: after this no new pop occurs, consumer handles are
     *    invalid (HANDLE-LIFETIME), and any producer still running was made
     *    unblockable (channel aborted unless clean EOS -- which implies all
     *    producers already finished pushing).  Idempotent if already closed. */
    close_consumer (thread_p);

    /* 1. join the producer tasks: wait until every launched task performed its final
     *    producer_task_finished ().  Yield-spin (worker_manager::wait_workers pattern):
     *    the tasks' last touch is an atomic decrement, so no lock/cv lifetime races. */
    while (m_producers_active.load (std::memory_order_acquire) > 0)
      {
	std::this_thread::yield ();
      }

    {
      int expected = (int) pipe_state::CONSUMER_CLOSED;
      bool advanced = m_state.compare_exchange_strong (expected, (int) pipe_state::JOINED,
		      std::memory_order_acq_rel);

      /* only the unique runner advances past CONSUMER_CLOSED */
      assert (advanced);
      (void) advanced;
    }
    m_seq_producers_joined.store (next_seq (), std::memory_order_release);

    /* 2. drain channel residue: free every in-flight batch exactly once (INV-OWN).
     *    Safe without the pointer guard: only the runner ever frees the pointers, and
     *    the free step below has not run yet. */
    {
      int drained = 0;

      if (m_source_p != NULL)
	{
	  /* idempotent re-close: covers the window where a concurrent close_consumer
	   * won the state transition before open_consumer published the source */
	  m_source_p->close (thread_p);
	  drained = m_source_p->drain_residue (thread_p);
	}
      else
	{
	  row_batch batch;

	  while (m_channel_p->drain_one (batch))
	    {
	      /* batch payloads cross threads: they live on the global heap (malloc),
	       * never on a per-thread private heap */
	      if (batch.buf != NULL)
		{
		  free_and_init (batch.buf);
		}
	      drained++;
	    }
	}

      m_residue_drained_count.store (drained, std::memory_order_release);
    }
    m_seq_residue_drained.store (next_seq (), std::memory_order_release);

    /* 3. release the worker reservation EXACTLY ONCE (OWN-1), only now -- after the
     *    join -- so the pool is never waited on/released while a producer task could
     *    still be accounted against it (HANDLE-LIFETIME). */
    if (m_pool.release != NULL)
      {
	void (*release_fn) (void *) = m_pool.release;

	m_pool.release = NULL;	/* single-shot by construction */
	release_fn (m_pool.handle);
	m_pool.handle = NULL;
	m_pool_release_calls.fetch_add (1, std::memory_order_acq_rel);
      }
    m_seq_workers_released.store (next_seq (), std::memory_order_release);

    /* 4. free owned state (OWN-2): producer bundle, then source, then channel.  The
     *    pointers are nulled under the guard FIRST so a late any-thread abort/close
     *    observes NULL instead of touching freed memory; deletion follows outside the
     *    lock (any abort that loaded the old pointer did so -- and finished -- under
     *    the same guard). */
    if (m_producer_state_free != NULL)
      {
	m_producer_state_free (thread_p, m_producer_state);
      }
    m_producer_state = NULL;
    m_producer_state_free = NULL;

    {
      stream_source *source_p;
      stream_channel<row_batch> *channel_p;

      {
	std::lock_guard<std::mutex> lk (m_ptr_mtx);
	source_p = m_source_p;
	m_source_p = NULL;
	channel_p = m_channel_p;
	m_channel_p = NULL;
      }

      if (source_p != NULL)
	{
	  source_p->~stream_source ();
	  db_private_free (thread_p, source_p);
	}
      if (channel_p != NULL)
	{
	  channel_p->~stream_channel ();
	  db_private_free (thread_p, channel_p);
	}
    }
    m_seq_state_freed.store (next_seq (), std::memory_order_release);

    m_metrics.t_join_done_us.store (stream_metrics_now_us (), std::memory_order_relaxed);
    emit_metrics ();

    /* publish RELEASED: observers (join_all losers, destroy) may proceed */
    m_state.store ((int) pipe_state::RELEASED, std::memory_order_release);

    return NO_ERROR;
  }

  /*
   * emit_metrics () - A7 overlap metrics: ONE summary line from the teardown runner.
   *
   * Emission is gated by the er_log_debug system parameter (server error log); the
   * counters themselves only ever updated on the gated streamed path, so OFF stays
   * observable-identical (R2).  All times relative to t_create (pipeline arm) in ms.
   */
  void
  stream_pipeline::emit_metrics ()
  {
    const stream_metrics &m = m_metrics;
    std::uint64_t t0 = m.t_create_us.load (std::memory_order_relaxed);

    if (t0 == 0)
      {
	return;
      }

    /* relative ms helper; 0 stays 0 ("never happened") */
#define PX_REL_MS(t) ((t) == 0 ? -1.0 : ((double) ((t) - t0)) / 1000.0)

    std::uint64_t prod_s = m.prod_first_start_us.load (std::memory_order_relaxed);
    std::uint64_t prod_e = m.prod_last_end_us.load (std::memory_order_relaxed);
    std::uint64_t cons_s = m.cons_first_pop_us.load (std::memory_order_relaxed);
    std::uint64_t cons_e = m.cons_last_ret_us.load (std::memory_order_relaxed);

    double overlap_ms = 0.0;
    double prod_ms = (prod_s != 0 && prod_e > prod_s) ? (double) (prod_e - prod_s) / 1000.0 : 0.0;
    double cons_ms = (cons_s != 0 && cons_e > cons_s) ? (double) (cons_e - cons_s) / 1000.0 : 0.0;

    if (prod_s != 0 && cons_s != 0)
      {
	std::uint64_t lo = (prod_s > cons_s) ? prod_s : cons_s;
	std::uint64_t hi = (prod_e < cons_e) ? prod_e : cons_e;

	if (hi > lo)
	  {
	    overlap_ms = (double) (hi - lo) / 1000.0;
	  }
      }

    er_log_debug (ARG_FILE_LINE,
		  "HJSTREAM metrics (ms rel to arm): launch=%.1f consumer_open=%.1f consumer_closed=%.1f "
		  "join_claim=%.1f join_done=%.1f | producer=[%.1f..%.1f] dur=%.1f | consumer=[%.1f..%.1f] dur=%.1f | "
		  "overlap=%.1f (%.0f%% of producer) | push_block=%.1fms (%llu waits) pop_block=%.1fms (%llu waits) | "
		  "batches=%llu bytes=%llu tuples=%llu | gather_tail(closed->join_claim)=%.1f teardown=%.1f | "
		  "degree=%d reserved=%d\n",
		  PX_REL_MS (m.t_launch_us.load (std::memory_order_relaxed)),
		  PX_REL_MS (m.t_consumer_open_us.load (std::memory_order_relaxed)),
		  PX_REL_MS (m.t_consumer_closed_us.load (std::memory_order_relaxed)),
		  PX_REL_MS (m.t_join_claim_us.load (std::memory_order_relaxed)),
		  PX_REL_MS (m.t_join_done_us.load (std::memory_order_relaxed)),
		  PX_REL_MS (prod_s), PX_REL_MS (prod_e), prod_ms,
		  PX_REL_MS (cons_s), PX_REL_MS (cons_e), cons_ms,
		  overlap_ms, (prod_ms > 0.0) ? (overlap_ms * 100.0 / prod_ms) : 0.0,
		  (double) m.push_block_us.load (std::memory_order_relaxed) / 1000.0,
		  (unsigned long long) m.push_blocked_cnt.load (std::memory_order_relaxed),
		  (double) m.pop_block_us.load (std::memory_order_relaxed) / 1000.0,
		  (unsigned long long) m.pop_blocked_cnt.load (std::memory_order_relaxed),
		  (unsigned long long) m.batches_pushed.load (std::memory_order_relaxed),
		  (unsigned long long) m.bytes_pushed.load (std::memory_order_relaxed),
		  (unsigned long long) m.tuples_pushed.load (std::memory_order_relaxed),
		  (m.t_consumer_closed_us.load (std::memory_order_relaxed) != 0
		   && m.t_join_claim_us.load (std::memory_order_relaxed)
		   > m.t_consumer_closed_us.load (std::memory_order_relaxed))
		  ? (double) (m.t_join_claim_us.load (std::memory_order_relaxed)
			      - m.t_consumer_closed_us.load (std::memory_order_relaxed)) / 1000.0 : 0.0,
		  (m.t_join_done_us.load (std::memory_order_relaxed)
		   > m.t_join_claim_us.load (std::memory_order_relaxed))
		  ? (double) (m.t_join_done_us.load (std::memory_order_relaxed)
			      - m.t_join_claim_us.load (std::memory_order_relaxed)) / 1000.0 : 0.0,
		  m_producer_count, m_pool.reserved_workers);

#undef PX_REL_MS
  }

  void
  stream_pipeline::abort (THREAD_ENTRY *thread_p, interrupt::interrupt_code code)
  {
    (void) thread_p;

    /* exactly-once interrupt publication: the FIRST abort's code wins; later aborts do
     * not overwrite the original cause */
    interrupt::interrupt_code expected = interrupt::interrupt_code::NO_INTERRUPT;
    m_interrupt.m_code.compare_exchange_strong (expected, code);

    m_abort_requested.store (true, std::memory_order_release);

    /* the always-safe wake (INV-ABORT): wakes a push-blocked producer and an
     * empty-blocked consumer.  Pointer-guarded: after the runner freed the channel
     * (and nulled it under this mutex) a late abort is a strict no-op -- never a
     * use-after-free.  The teardown itself is executed only by join_all () (see
     * header: a producer task aborting must never join itself). */
    std::lock_guard<std::mutex> lk (m_ptr_mtx);

    if (m_channel_p != NULL)
      {
	m_channel_p->abort ();
      }
  }
}
