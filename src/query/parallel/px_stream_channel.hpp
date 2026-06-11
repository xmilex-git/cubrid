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
 * px_stream_channel.hpp - bounded MPMC channel of owned row-batch payloads
 *                         (streaming hash-join transport, component C2)
 *
 * A self-contained transport for the streaming/pipelined hash-join: D producer workers push
 * owned row-batch payloads, the consumer side pops them, with first-class abort and a
 * producer-registry end-of-stream.  Inert unless explicitly instantiated by a streamed
 * pipeline -- no existing code path uses it (R2: OFF = observable-identical).
 *
 * Channel-state invariants (streaming_hashjoin_SSOT.md R6/R7/R10/R11;
 * architecture_deepening_designs.md C2):
 *
 *  (INV-OWN)      Single-owner-at-a-time.  push()==true  => payload ownership TRANSFERRED to
 *                 the slot; the producer must not touch the buffer again.  push()==false =>
 *                 ownership RETAINED by the caller (free explicitly or retry).  The consumer
 *                 owns (and must free) every popped payload.  The channel never allocates or
 *                 frees a payload; the destructor frees nothing, so residue MUST be drained
 *                 via drain_one() before destruction.  Net: every batch is freed exactly once
 *                 -- consumer-on-pop, producer-on-failed-push, or the teardown drainer.
 *  (INV-FAILPUSH) A push that fails because the channel is aborted, push-completed (EOS), or
 *                 interrupted returns false WITH ownership retained, so the producer's
 *                 explicit free of the un-pushed batch is unambiguous and immediate.
 *  (INV-EOS)      Producer registry.  End-of-stream is observable to consumers ONLY when the
 *                 ring is empty AND the LAST registered producer has called producer_done()
 *                 -- never earlier (R10/O6).  All producers must be registered before the
 *                 first producer_done(); producer_done() is called exactly once per
 *                 registered producer on EVERY exit path (clean EOS, error, abort); calls
 *                 beyond the registered count are clamped no-ops (assert in debug builds).
 *  (INV-ABORT)    abort() stores m_aborted with release semantics, THEN notify_all on BOTH
 *                 condition variables under the lock.  Blocked pushers and poppers wake and
 *                 return the failure sentinel; the lock-free fast paths acquire-load
 *                 m_aborted as their FIRST check, so a nonempty channel delivers no residual
 *                 row once the abort is observed.  drain_one() is the explicit teardown
 *                 exception: it pops residue DESPITE the abort flag so the single teardown
 *                 drainer frees each in-flight batch exactly once.
 *  (INV-BOUND)    Bounded.  In-flight batches <= capacity (ctor clamp to [2, 65535]; the
 *                 per-slot sequence ring needs >= 2 slots); backpressure is push()
 *                 blocking while the ring is full.
 *  (INV-OVERSIZE) Payload-policy invariant the channel relies on: a tuple is never split
 *                 across batches; an oversized tuple travels as its own single-tuple batch.
 *                 The channel accepts any batch byte-size -- it bounds only the NUMBER of
 *                 in-flight batches; batch sizing is the producer sink's (C1) policy.
 *
 * Error model: no exceptions.  push()==false / pop()==false are the only signaling channels;
 * disambiguate with is_aborted() and the shared interrupt code.  Setting an interrupt alone
 * does NOT wake a blocked side -- the error path is always:
 *   er_set (...) + set the shared parallel_query::interrupt + channel.abort ().
 * Clean EOS (producer_done) must NEVER be used to wake a blocked producer; that is abort().
 */

#ifndef _PX_STREAM_CHANNEL_HPP_
#define _PX_STREAM_CHANNEL_HPP_

#include <cstddef>
#include <cstdint>
#include <vector>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include "px_interrupt.hpp"

namespace parallel_query
{
  /* Non-owning POD handle for one batch of serialized whole tuples -- NOT an RAII owner
   * (CUBRID rule: no RAII-for-memory in engine paths).  The implicit destructor frees
   * nothing; buf is freed explicitly by whoever owns the batch per INV-OWN. */
  struct row_batch
  {
    char *buf;			/* owned payload bytes (db_private_alloc'd by the producer) */
    int len;			/* used bytes in buf */
    int tuple_cnt;		/* whole tuples in buf; a tuple is never split (INV-OVERSIZE) */
  };

  template<typename T>
  class stream_channel
  {
    public:
      explicit stream_channel (std::size_t capacity);
      ~stream_channel ();

      stream_channel (const stream_channel &) = delete;
      stream_channel &operator= (const stream_channel &) = delete;

      /* Blocking ownership-aware push (INV-OWN/INV-BOUND): blocks while full.
       * true  => ownership transferred to the channel.
       * false => ownership RETAINED by the caller (INV-FAILPUSH): channel aborted,
       *          EOS already signaled, or the interrupt is set. */
      bool push (const T &value, const interrupt &interrupt_check = interrupt ());

      /* Blocking pop: blocks while empty (until data, EOS, abort or interrupt).
       * true  => value populated; the caller now owns the payload and must free it.
       * false => end-of-stream (empty AND all registered producers done, INV-EOS),
       *          aborted (INV-ABORT), or the interrupt is set.  After an abort is
       *          observed no residual row is ever delivered through pop(). */
      bool pop (T &value, const interrupt &interrupt_check = interrupt ());

      /* Non-blocking variants; same ownership sentinel; also false on full/empty. */
      bool try_push (const T &value);
      bool try_pop (T &value);

      /* Producer registry (INV-EOS).  Contract: register every producer before the first
       * producer_done(); call producer_done() exactly once per registered producer on every
       * exit path.  The last producer_done() makes EOS observable; surplus calls are
       * clamped no-ops (assert in debug). */
      void register_producer ();
      void producer_done ();

      /* First-class abort (INV-ABORT): idempotent, callable from any thread any number of
       * times; wakes BOTH blocked sides; observed by the lock-free fast paths. */
      void abort ();
      bool is_aborted () const;

      /* Teardown raw drain: pops residue DESPITE abort/EOS so the single teardown drainer
       * frees each in-flight batch exactly once (INV-OWN).  Call only after all producers
       * are joined.  false => nothing left. */
      bool drain_one (T &value);

      bool is_empty () const;
      bool is_full () const;
      std::size_t size () const;
      std::size_t capacity () const;

    private:
      struct channel_slot
      {
	std::atomic<std::uint64_t> sequence;
	T data;

	channel_slot () : sequence (0), data () {}
      };

      std::vector<channel_slot> m_slots;
      std::size_t m_capacity;

      std::atomic<std::uint64_t> m_enqueue_pos;
      std::atomic<std::uint64_t> m_dequeue_pos;

      std::atomic<bool> m_aborted;		/* INV-ABORT; never cleared */
      std::atomic<bool> m_push_completed;	/* INV-EOS; set once by the last producer_done */
      std::atomic<std::uint32_t> m_producers_registered;
      std::atomic<std::uint32_t> m_producers_done;

      /* sleeping-waiter counts; Dekker-paired (seq_cst fences) with the wake_* helpers so a
       * lock-free fast-path op never loses a wakeup without taking the mutex itself */
      std::atomic<std::uint32_t> m_push_waiters;
      std::atomic<std::uint32_t> m_pop_waiters;

      mutable std::mutex m_mutex;
      std::condition_variable m_not_empty;
      std::condition_variable m_not_full;

      /* lock-free bounded MPMC ring ops (sequence-per-slot); false only on full/empty */
      bool try_enqueue (const T &value);
      bool try_dequeue (T &value);
      void wake_poppers ();
      void wake_pushers ();
  };
}

#endif /* _PX_STREAM_CHANNEL_HPP_ */
