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
 * px_stream_channel.cpp - bounded MPMC channel of owned row-batch payloads (C2)
 *
 * Ring algorithm: per-slot sequence counters (the classic bounded MPMC scheme).  A slot's
 * sequence equals the enqueue position when it is free for that lap's pusher, and equals
 * position+1 when it holds data for that lap's popper.  Positions only ever grow; 64-bit
 * positions make wraparound a non-issue (centuries at 1 op/ns).
 *
 * Blocking layer: a mutex + two condition variables provide backpressure (INV-BOUND).  The
 * lock-free fast paths never take the mutex unless a waiter is registered; the waiter
 * registration (seq_cst RMW + fence) and the waker's fence + waiter-count load form a
 * Dekker pair, so either the waker sees the registered waiter (and notifies under the lock)
 * or the waiter's re-check (made after registration, before sleeping) sees the state change.
 * Either way no wakeup is lost.
 *
 * See px_stream_channel.hpp for the full invariant contract
 * (INV-OWN / INV-FAILPUSH / INV-OVERSIZE / INV-EOS / INV-ABORT / INV-BOUND).
 */

#include "px_stream_channel.hpp"

#include <cassert>

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace parallel_query
{
  static const std::size_t STREAM_CHANNEL_MAX_CAPACITY = 65535;	/* matches DB_UINT16_MAX clamp */

  /* minimum 2: with one slot the per-slot sequence scheme cannot distinguish "published,
   * not yet popped" (seq == pos + 1) from "free for the next position" (next pos == pos + 1,
   * same residue mod capacity), so a second pusher would overwrite an unpopped batch */
  static const std::size_t STREAM_CHANNEL_MIN_CAPACITY = 2;

  static std::size_t
  stream_channel_clamp_capacity (std::size_t capacity)
  {
    if (capacity < STREAM_CHANNEL_MIN_CAPACITY)
      {
	return STREAM_CHANNEL_MIN_CAPACITY;
      }
    if (capacity > STREAM_CHANNEL_MAX_CAPACITY)
      {
	return STREAM_CHANNEL_MAX_CAPACITY;
      }
    return capacity;
  }

  template<typename T>
  stream_channel<T>::stream_channel (std::size_t capacity)
    : m_slots (stream_channel_clamp_capacity (capacity)),
      m_capacity (stream_channel_clamp_capacity (capacity)),
      m_enqueue_pos (0), m_dequeue_pos (0),
      m_aborted (false), m_push_completed (false),
      m_producers_registered (0), m_producers_done (0),
      m_push_waiters (0), m_pop_waiters (0)
  {
    for (std::size_t i = 0; i < m_capacity; ++i)
      {
	m_slots[i].sequence.store (i, std::memory_order_relaxed);
      }
  }

  template<typename T>
  stream_channel<T>::~stream_channel ()
  {
    /* INV-OWN: the channel frees no payload; residue must have been drained (drain_one)
     * by the single teardown drainer before destruction, or the batches leak. */
    assert (is_empty ());
  }

  /*
   * try_enqueue - lock-free bounded-MPMC enqueue; returns false ONLY when the ring is full
   *               (a CAS lost to a sibling producer retries internally)
   */
  template<typename T>
  bool
  stream_channel<T>::try_enqueue (const T &value)
  {
    std::uint64_t pos = m_enqueue_pos.load (std::memory_order_relaxed);

    for (;;)
      {
	channel_slot &s = m_slots[pos % m_capacity];
	std::uint64_t seq = s.sequence.load (std::memory_order_acquire);
	std::int64_t dif = (std::int64_t) (seq - pos);

	if (dif == 0)
	  {
	    /* slot free for this position; claim it */
	    if (m_enqueue_pos.compare_exchange_weak (pos, pos + 1, std::memory_order_relaxed))
	      {
		s.data = value;
		s.sequence.store (pos + 1, std::memory_order_release);
		return true;
	      }
	    /* CAS failure reloaded pos; retry */
	  }
	else if (dif < 0)
	  {
	    /* the slot one lap back has not been popped yet => full */
	    return false;
	  }
	else
	  {
	    pos = m_enqueue_pos.load (std::memory_order_relaxed);
	  }
      }
  }

  /*
   * try_dequeue - lock-free bounded-MPMC dequeue; returns false ONLY when no published
   *               element is available (empty, or a pusher claimed but not yet published)
   */
  template<typename T>
  bool
  stream_channel<T>::try_dequeue (T &value)
  {
    std::uint64_t pos = m_dequeue_pos.load (std::memory_order_relaxed);

    for (;;)
      {
	channel_slot &s = m_slots[pos % m_capacity];
	std::uint64_t seq = s.sequence.load (std::memory_order_acquire);
	std::int64_t dif = (std::int64_t) (seq - (pos + 1));

	if (dif == 0)
	  {
	    if (m_dequeue_pos.compare_exchange_weak (pos, pos + 1, std::memory_order_relaxed))
	      {
		value = s.data;
		s.sequence.store (pos + m_capacity, std::memory_order_release);
		return true;
	      }
	  }
	else if (dif < 0)
	  {
	    return false;
	  }
	else
	  {
	    pos = m_dequeue_pos.load (std::memory_order_relaxed);
	  }
      }
  }

  /*
   * wake_poppers/wake_pushers - notify sleeping waiters after a fast-path op.  The seq_cst
   * fence pairs with the waiter's seq_cst registration: if the waiter-count load misses a
   * registered waiter, the waiter's post-registration re-check is guaranteed to see this
   * op's state change, so it does not sleep.  Notification is taken under the mutex so it
   * cannot slip between a waiter's re-check and its wait().
   */
  template<typename T>
  void
  stream_channel<T>::wake_poppers ()
  {
    std::atomic_thread_fence (std::memory_order_seq_cst);
    if (m_pop_waiters.load (std::memory_order_relaxed) > 0)
      {
	std::lock_guard<std::mutex> lock (m_mutex);
	m_not_empty.notify_all ();
      }
  }

  template<typename T>
  void
  stream_channel<T>::wake_pushers ()
  {
    std::atomic_thread_fence (std::memory_order_seq_cst);
    if (m_push_waiters.load (std::memory_order_relaxed) > 0)
      {
	std::lock_guard<std::mutex> lock (m_mutex);
	m_not_full.notify_all ();
      }
  }

  template<typename T>
  bool
  stream_channel<T>::push (const T &value, const interrupt &interrupt_check)
  {
    for (;;)
      {
	/* INV-ABORT / INV-FAILPUSH: observe abort/EOS/interrupt BEFORE committing anything;
	 * every false return leaves payload ownership with the caller. */
	if (m_aborted.load (std::memory_order_acquire))
	  {
	    return false;
	  }
	if (m_push_completed.load (std::memory_order_acquire))
	  {
	    /* EOS already signaled: late push is refused, ownership retained */
	    return false;
	  }
	if (interrupt_check.get_code () != interrupt::interrupt_code::NO_INTERRUPT)
	  {
	    return false;
	  }

	if (try_enqueue (value))
	  {
	    wake_poppers ();
	    return true;
	  }

	/* ring full -- backpressure (INV-BOUND): sleep until a pop frees a slot or
	 * abort()/producer_done() fires.  An interrupt alone does not wake a sleeper;
	 * the error protocol pairs it with abort(). */
	{
	  std::unique_lock<std::mutex> lock (m_mutex);
	  m_push_waiters.fetch_add (1, std::memory_order_seq_cst);
	  std::atomic_thread_fence (std::memory_order_seq_cst);
	  if (!m_aborted.load (std::memory_order_acquire)
	      && !m_push_completed.load (std::memory_order_acquire)
	      && interrupt_check.get_code () == interrupt::interrupt_code::NO_INTERRUPT
	      && is_full ())
	    {
	      m_not_full.wait (lock);
	    }
	  m_push_waiters.fetch_sub (1, std::memory_order_relaxed);
	}
	/* loop: re-check abort/EOS/interrupt on every wake (spurious or real) */
      }
  }

  template<typename T>
  bool
  stream_channel<T>::pop (T &value, const interrupt &interrupt_check)
  {
    for (;;)
      {
	/* INV-ABORT: abort is the FIRST check -- a nonempty channel delivers no residual
	 * row through pop() once the abort is observed (the teardown drainer uses
	 * drain_one instead). */
	if (m_aborted.load (std::memory_order_acquire))
	  {
	    return false;
	  }

	/* INV-EOS: read EOS BEFORE attempting the dequeue.  Every producer's pushes
	 * happen-before the EOS store (release-RMW chain on m_producers_done), so
	 * "EOS observed, then found empty" is final; the reverse order could miss a row
	 * pushed between the two reads and report a false end-of-stream (R10 loss). */
	bool eos = m_push_completed.load (std::memory_order_acquire);

	if (try_dequeue (value))
	  {
	    wake_pushers ();
	    return true;
	  }

	if (eos)
	  {
	    /* empty AND the last registered producer is done => clean end-of-stream */
	    return false;
	  }

	if (interrupt_check.get_code () != interrupt::interrupt_code::NO_INTERRUPT)
	  {
	    return false;
	  }

	{
	  std::unique_lock<std::mutex> lock (m_mutex);
	  m_pop_waiters.fetch_add (1, std::memory_order_seq_cst);
	  std::atomic_thread_fence (std::memory_order_seq_cst);
	  if (!m_aborted.load (std::memory_order_acquire)
	      && !m_push_completed.load (std::memory_order_acquire)
	      && interrupt_check.get_code () == interrupt::interrupt_code::NO_INTERRUPT
	      && is_empty ())
	    {
	      m_not_empty.wait (lock);
	    }
	  m_pop_waiters.fetch_sub (1, std::memory_order_relaxed);
	}
      }
  }

  template<typename T>
  bool
  stream_channel<T>::try_push (const T &value)
  {
    if (m_aborted.load (std::memory_order_acquire)
	|| m_push_completed.load (std::memory_order_acquire))
      {
	return false;		/* INV-FAILPUSH: ownership retained */
      }
    if (!try_enqueue (value))
      {
	return false;		/* full; ownership retained */
      }
    wake_poppers ();
    return true;
  }

  template<typename T>
  bool
  stream_channel<T>::try_pop (T &value)
  {
    if (m_aborted.load (std::memory_order_acquire))
      {
	return false;		/* INV-ABORT: no residual row after abort */
      }
    if (!try_dequeue (value))
      {
	return false;
      }
    wake_pushers ();
    return true;
  }

  template<typename T>
  void
  stream_channel<T>::register_producer ()
  {
    /* contract (INV-EOS): all registrations precede the first producer_done; a transient
     * zero of the registry mid-registration would otherwise fire EOS early */
    assert (!m_push_completed.load (std::memory_order_acquire));
    assert (m_producers_done.load (std::memory_order_acquire) == 0);

    m_producers_registered.fetch_add (1, std::memory_order_release);
  }

  template<typename T>
  void
  stream_channel<T>::producer_done ()
  {
    const std::uint32_t registered = m_producers_registered.load (std::memory_order_acquire);
    std::uint32_t done = m_producers_done.load (std::memory_order_relaxed);

    assert (registered > 0);

    for (;;)
      {
	if (done >= registered)
	  {
	    /* more producer_done() calls than registered producers: protocol violation;
	     * clamped so EOS is never fired early or twice (INV-EOS) */
	    assert (false);
	    return;
	  }
	if (m_producers_done.compare_exchange_weak (done, done + 1, std::memory_order_acq_rel,
	    std::memory_order_relaxed))
	  {
	    done = done + 1;
	    break;
	  }
      }

    if (done == registered)
      {
	/* LAST producer: EOS becomes observable only now (INV-EOS, R10/O6).  The acq_rel
	 * RMW chain on m_producers_done orders every producer's pushes before this store,
	 * so a consumer that acquires m_push_completed sees every pushed batch. */
	m_push_completed.store (true, std::memory_order_release);

	std::lock_guard<std::mutex> lock (m_mutex);
	/* wake consumers sleeping on empty (they will observe EOS).  Also wake any pusher
	 * as a safety net: it re-checks and returns false with ownership retained
	 * (INV-FAILPUSH).  Clean EOS is still never the designated wake for a blocked
	 * producer -- that is abort()'s job (INV-ABORT). */
	m_not_empty.notify_all ();
	m_not_full.notify_all ();
      }
  }

  template<typename T>
  void
  stream_channel<T>::abort ()
  {
    /* INV-ABORT: release-store FIRST, then notify_all on BOTH condition variables under
     * the lock.  Idempotent: any number of callers, from any thread, in any state. */
    m_aborted.store (true, std::memory_order_release);

    std::lock_guard<std::mutex> lock (m_mutex);
    m_not_empty.notify_all ();
    m_not_full.notify_all ();
  }

  template<typename T>
  bool
  stream_channel<T>::is_aborted () const
  {
    return m_aborted.load (std::memory_order_acquire);
  }

  template<typename T>
  bool
  stream_channel<T>::drain_one (T &value)
  {
    /* teardown raw drain (INV-ABORT exception): pops residue DESPITE m_aborted /
     * m_push_completed so each in-flight batch is freed exactly once (INV-OWN).
     * Contract: producers are already joined; the consumer no longer pops. */
    if (!try_dequeue (value))
      {
	return false;
      }
    wake_pushers ();		/* safety net only; no producer should be blocked at teardown */
    return true;
  }

  template<typename T>
  bool
  stream_channel<T>::is_empty () const
  {
    std::uint64_t dequeue_pos = m_dequeue_pos.load (std::memory_order_acquire);
    std::uint64_t enqueue_pos = m_enqueue_pos.load (std::memory_order_acquire);
    return dequeue_pos >= enqueue_pos;
  }

  template<typename T>
  bool
  stream_channel<T>::is_full () const
  {
    std::uint64_t dequeue_pos = m_dequeue_pos.load (std::memory_order_acquire);
    std::uint64_t enqueue_pos = m_enqueue_pos.load (std::memory_order_acquire);
    return (enqueue_pos - dequeue_pos) >= m_capacity;
  }

  template<typename T>
  std::size_t
  stream_channel<T>::size () const
  {
    std::uint64_t dequeue_pos = m_dequeue_pos.load (std::memory_order_acquire);
    std::uint64_t enqueue_pos = m_enqueue_pos.load (std::memory_order_acquire);
    return enqueue_pos > dequeue_pos ? (std::size_t) (enqueue_pos - dequeue_pos) : 0;
  }

  template<typename T>
  std::size_t
  stream_channel<T>::capacity () const
  {
    return m_capacity;
  }
}

template class parallel_query::stream_channel<parallel_query::row_batch>;
