/*
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
 * test_stream_pipeline.cpp - C7+C4 lifetime prototype (the codex-mandated gate:
 *                            "no real join") over the REAL pipeline + channel + source
 *
 * FAKE producer tasks (threads pushing tagged batches through the real C2 channel under
 * the real C7 stream_pipeline) + the real C3+C5 stream_source consumer.  Scenarios:
 *
 *   S1  normal run: full lifecycle to RELEASED; exact-once tags; workers released
 *       exactly once; producer state freed exactly once; alloc ledger empty
 *   S2  slow consumer (backpressure; producers block in push) + consumer closes early
 *       (R11): abort propagates, producers unblock, join completes, no deadlock, no
 *       spurious error, ledger empty
 *   S3  abort injected at EVERY state (BEGIN .. the CONSUMER_CLOSED->JOINED->RELEASED
 *       window), concurrently and twice (double-abort): idempotent, no leak, no hang,
 *       release exactly once; abort after RELEASED is a strict no-op
 *   S4  reservation atomicity (C4/OWN-5, R5/O3) with the REAL primitive
 *       stream_try_reserve_exact: shortfall reserves NOTHING; concurrent pipelines
 *       racing for the last slots -> exactly one wins or all fall back, NEVER partial
 *   S5  teardown-order: sequence stamps prove join < drain < release < free strictly
 *       (asserted after every join_all in every scenario)
 *
 * Iteration counts scale with argv[1] (float multiplier) so the same binary serves as
 * the in-tree unit test (scale 1) and the standalone ASan/TSan stress harness
 * (scale >> 1).  A watchdog _exit(86)s on any deadlock.
 *
 * Link stubs: er_set / db_private_free_release are defined HERE and double as the
 * error and allocation ledgers (no engine library link; THREAD_ENTRY is void in
 * mode-less compilation, NDEBUG selects the *_release allocator names and the
 * er_set-based assert_release).
 */

#include "px_stream_pipeline.hpp"
#include "px_stream_policy.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

using parallel_query::interrupt;
using parallel_query::row_batch;
using parallel_query::stream_channel;
using parallel_query::stream_pipeline;
using parallel_query::stream_source;
using parallel_query::stream_try_reserve_exact;

static int g_failed = 0;

#define TEST_CHECK(cond)                                                        \
  do                                                                            \
    {                                                                           \
      if (!(cond))                                                              \
	{                                                                       \
	  std::printf ("FAILED %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
	  g_failed++;                                                           \
	}                                                                       \
    }                                                                           \
  while (0)

/* ---------------- link stubs + ledgers ---------------- */

static std::atomic<int> g_er_set_calls (0);
static std::atomic<int> g_live_buffers (0);	/* malloc'd batch buffers not yet freed */

extern "C" void
er_set (int severity, const char *file_name, const int line_no, int err_id, int num_args, ...)
{
  (void) severity;
  (void) file_name;
  (void) line_no;
  (void) err_id;
  (void) num_args;
  g_er_set_calls.fetch_add (1);
}

extern "C" void *
db_private_alloc_release (THREAD_ENTRY *thrd, size_t size, bool rc_track)
{
  (void) thrd;
  (void) rc_track;
  void *ptr = std::malloc (size);
  if (ptr != NULL)
    {
      g_live_buffers.fetch_add (1);
    }
  return ptr;
}

extern "C" void
db_private_free_release (THREAD_ENTRY *thrd, void *ptr, bool rc_track)
{
  (void) thrd;
  (void) rc_track;
  if (ptr != NULL)
    {
      std::free (ptr);
      g_live_buffers.fetch_sub (1);
    }
}

/* ---------------- batch helpers ---------------- */

static row_batch
make_batch (std::uint64_t tag)
{
  row_batch b;
  b.buf = (char *) std::malloc (sizeof (std::uint64_t));
  std::memcpy (b.buf, &tag, sizeof (tag));
  b.len = (int) sizeof (tag);
  b.tuple_cnt = 1;
  g_live_buffers.fetch_add (1);
  return b;
}

static std::uint64_t
batch_tag (const row_batch &b)
{
  std::uint64_t tag;
  std::memcpy (&tag, b.buf, sizeof (tag));
  return tag;
}

static void
free_batch_direct (row_batch &b)
{
  if (b.buf != NULL)
    {
      std::free (b.buf);
      b.buf = NULL;
      g_live_buffers.fetch_sub (1);
    }
}

/* ---------------- fake worker pool (REAL reservation primitive) ---------------- */

struct fake_pool
{
  std::atomic<int> available;
  std::atomic<int> release_calls;
  int capacity;

  explicit fake_pool (int cap) : available (cap), release_calls (0), capacity (cap) {}
};

struct fake_reservation
{
  fake_pool *pool;
  int workers;
};

static void
fake_pool_release (void *handle)
{
  fake_reservation *res = (fake_reservation *) handle;
  res->pool->available.fetch_add (res->workers);
  res->pool->release_calls.fetch_add (1);
  delete res;
}

/* C4 policy shape over the fake pool: atomic exact-2*D-or-fallback */
static bool
fake_policy_try_begin (fake_pool &pool, int degree, stream_pipeline::pool_binding &binding_out)
{
  int workers_2d = 2 * degree;
  int got = stream_try_reserve_exact (pool.available, workers_2d);

  if (got == 0)
    {
      /* fallback reported: NOTHING reserved */
      binding_out.handle = NULL;
      binding_out.reserved_workers = 0;
      binding_out.release = NULL;
      return false;
    }

  TEST_CHECK (got == workers_2d);	/* never partial */
  binding_out.handle = new fake_reservation { &pool, workers_2d };
  binding_out.reserved_workers = workers_2d;
  binding_out.release = fake_pool_release;
  return true;
}

/* ---------------- fake producer-side state bundle ---------------- */

static void
fake_producer_state_free (THREAD_ENTRY *thread_p, void *state)
{
  (void) thread_p;
  ((std::atomic<int> *) state)->fetch_add (1);
}

/* ---------------- fake producer task ---------------- */

static void
producer_task (stream_pipeline *pipe, int producer_id, int batches, std::atomic<int> *pushed)
{
  stream_channel<row_batch> *chan = pipe->get_channel ();

  for (int i = 0; i < batches; i++)
    {
      row_batch b = make_batch (((std::uint64_t) producer_id << 32) | (std::uint64_t) i);
      if (chan->push (b, *pipe->get_interrupt ()))
	{
	  if (pushed != NULL)
	    {
	      pushed->fetch_add (1);
	    }
	}
      else
	{
	  /* INV-FAILPUSH: ownership retained -> free and stop promptly (R11) */
	  free_batch_direct (b);
	  break;
	}
    }

  chan->producer_done ();		/* exactly once, on every exit path (INV-EOS) */
  pipe->producer_task_finished ();	/* the LAST touch of pipeline-owned state */
}

/* ---------------- common assertions ---------------- */

/* S5: strict teardown order -- join producers < drain residue < release workers < free */
static void
check_teardown_order (stream_pipeline *pipe)
{
  std::uint64_t j = pipe->get_seq_producers_joined ();
  std::uint64_t d = pipe->get_seq_residue_drained ();
  std::uint64_t r = pipe->get_seq_workers_released ();
  std::uint64_t f = pipe->get_seq_state_freed ();

  TEST_CHECK (j > 0);
  TEST_CHECK (d > j);
  TEST_CHECK (r > d);
  TEST_CHECK (f > r);
}

static void
check_released_clean (stream_pipeline *pipe, fake_pool *pool, std::atomic<int> *state_frees)
{
  TEST_CHECK (pipe->get_state () == stream_pipeline::pipe_state::RELEASED);
  check_teardown_order (pipe);
  if (pool != NULL)
    {
      TEST_CHECK (pipe->get_pool_release_calls () == 1);	/* released exactly once */
    }
  if (state_frees != NULL)
    {
      TEST_CHECK (state_frees->load () == 1);	/* producer bundle freed exactly once */
    }
  /* alloc ledger: every batch and engine object freed except the pipeline shell
   * itself, which destroy () releases (asserted == 0 after destroy in each scenario) */
  TEST_CHECK (g_live_buffers.load () == 1);
}

/* ---------------- S1: normal full lifecycle ---------------- */

static void
test_normal_lifecycle (int iterations)
{
  const int D = 2;
  const int BATCHES = 50;

  for (int iter = 0; iter < iterations; iter++)
    {
      fake_pool pool (8);
      stream_pipeline::pool_binding binding;
      std::atomic<int> state_frees (0);
      int er_before = g_er_set_calls.load ();

      TEST_CHECK (fake_policy_try_begin (pool, D, binding));
      TEST_CHECK (pool.available.load () == 8 - 2 * D);

      stream_pipeline *pipe = stream_pipeline::create (NULL, binding, 8);
      TEST_CHECK (pipe != NULL);
      TEST_CHECK (pipe->get_state () == stream_pipeline::pipe_state::BEGIN);
      TEST_CHECK (pipe->get_reserved_workers () == 2 * D);

      TEST_CHECK (pipe->launch_producers (NULL, D, &state_frees, fake_producer_state_free) == 0);
      TEST_CHECK (pipe->get_state () == stream_pipeline::pipe_state::PRODUCER_STARTED);

      std::vector<std::thread> producers;
      for (int p = 0; p < D; p++)
	{
	  producers.emplace_back (producer_task, pipe, p, BATCHES, (std::atomic<int> *) NULL);
	}

      stream_source *src = pipe->open_consumer (NULL, D);
      TEST_CHECK (src != NULL);
      TEST_CHECK (pipe->get_state () == stream_pipeline::pipe_state::CONSUMER_OPEN);

      /* exact-once check: every tag delivered exactly once, then sticky EOS */
      std::vector<int> seen (D * BATCHES, 0);
      row_batch out;
      int delivered = 0;
      while (src->next_batch (NULL, out) == S_SUCCESS)
	{
	  std::uint64_t tag = batch_tag (out);
	  int idx = (int) (tag >> 32) * BATCHES + (int) (tag & 0xffffffffU);
	  TEST_CHECK (idx >= 0 && idx < D * BATCHES);
	  seen[idx]++;
	  delivered++;
	  free_batch_direct (out);
	}
      TEST_CHECK (delivered == D * BATCHES);
      for (int i = 0; i < D * BATCHES; i++)
	{
	  TEST_CHECK (seen[i] == 1);
	}
      TEST_CHECK (src->next_batch (NULL, out) == S_END);	/* sticky */

      for (auto &t : producers)
	{
	  t.join ();
	}

      pipe->close_consumer (NULL);
      TEST_CHECK (pipe->get_state () == stream_pipeline::pipe_state::CONSUMER_CLOSED);

      TEST_CHECK (pipe->join_all (NULL) == 0);
      check_released_clean (pipe, &pool, &state_frees);
      TEST_CHECK (pipe->get_residue_drained_count () == 0);	/* clean EOS: nothing left */
      TEST_CHECK (g_er_set_calls.load () == er_before);	/* no error on the normal path */
      TEST_CHECK (!pipe->is_abort_requested ());

      stream_pipeline::destroy (NULL, pipe);
      TEST_CHECK (pipe == NULL);
      TEST_CHECK (pool.available.load () == 8);	/* reservation returned */
      TEST_CHECK (pool.release_calls.load () == 1);
      TEST_CHECK (g_live_buffers.load () == 0);	/* alloc ledger balanced */
    }

  std::printf ("S1 normal lifecycle: %d iterations\n", iterations);
}

/* ---------------- S2: slow consumer (backpressure) + early close (R11) ----------- */

static void
test_slow_consumer_early_close (int iterations)
{
  const int D = 2;
  const int BATCHES = 200;	/* far more than capacity: producers WILL block */

  for (int iter = 0; iter < iterations; iter++)
    {
      fake_pool pool (4);
      stream_pipeline::pool_binding binding;
      std::atomic<int> state_frees (0);
      std::atomic<int> pushed (0);
      int er_before = g_er_set_calls.load ();

      TEST_CHECK (fake_policy_try_begin (pool, D, binding));

      stream_pipeline *pipe = stream_pipeline::create (NULL, binding, 2 /* tiny: force backpressure */);
      TEST_CHECK (pipe != NULL);
      TEST_CHECK (pipe->launch_producers (NULL, D, &state_frees, fake_producer_state_free) == 0);

      std::vector<std::thread> producers;
      for (int p = 0; p < D; p++)
	{
	  producers.emplace_back (producer_task, pipe, p, BATCHES, &pushed);
	}

      stream_source *src = pipe->open_consumer (NULL, D);
      TEST_CHECK (src != NULL);

      /* slow consumer: drain a few batches, slowly, then terminate early (LIMIT) */
      row_batch out;
      for (int i = 0; i < 3; i++)
	{
	  if (src->next_batch (NULL, out) == S_SUCCESS)
	    {
	      free_batch_direct (out);
	    }
	  std::this_thread::sleep_for (std::chrono::microseconds (200));
	}

      /* R11 feedback edge: consumer-close aborts the channel; push-blocked producers
       * MUST wake promptly (the join below would otherwise deadlock -> watchdog) */
      pipe->close_consumer (NULL);
      TEST_CHECK (pipe->get_state () == stream_pipeline::pipe_state::CONSUMER_CLOSED);

      for (auto &t : producers)
	{
	  t.join ();		/* no deadlock: abort propagated */
	}
      TEST_CHECK (pushed.load () < D * BATCHES);	/* stopped early, no unbounded waste */

      TEST_CHECK (pipe->join_all (NULL) == 0);
      check_released_clean (pipe, &pool, &state_frees);

      /* normal early termination is NOT an error (R11): no interrupt, no er_set */
      TEST_CHECK (!pipe->is_abort_requested ());
      TEST_CHECK (g_er_set_calls.load () == er_before);

      stream_pipeline::destroy (NULL, pipe);
      TEST_CHECK (pool.available.load () == 4);
      TEST_CHECK (pool.release_calls.load () == 1);
      TEST_CHECK (g_live_buffers.load () == 0);	/* alloc ledger balanced */
    }

  std::printf ("S2 slow consumer + early close: %d iterations\n", iterations);
}

/* ---------------- S3: abort injected at every state, doubled + raced ------------- */

enum abort_point
{
  AT_BEGIN = 0,
  AT_PRODUCER_STARTED,
  AT_CONSUMER_OPEN,
  AT_CONSUMER_CLOSED,
  AT_TEARDOWN_RACE,		/* abort raced against join_all: covers the
				 * CONSUMER_CLOSED->JOINED->RELEASED window */
  ABORT_POINT_COUNT
};

static void
fire_double_abort (stream_pipeline *pipe)
{
  /* two concurrent aborts from foreign threads: idempotent, exactly-once side effects */
  std::thread a1 ([pipe] ()
  {
    pipe->abort (NULL, interrupt::interrupt_code::ERROR_INTERRUPTED_FROM_WORKER_THREAD);
  });
  std::thread a2 ([pipe] ()
  {
    pipe->abort (NULL, interrupt::interrupt_code::ERROR_INTERRUPTED_FROM_MAIN_THREAD);
  });
  a1.join ();
  a2.join ();
}

static void
test_abort_every_state (int iterations_per_state)
{
  const int D = 2;
  const int BATCHES = 100;

  for (int point = 0; point < ABORT_POINT_COUNT; point++)
    {
      for (int iter = 0; iter < iterations_per_state; iter++)
	{
	  fake_pool pool (4);
	  stream_pipeline::pool_binding binding;
	  std::atomic<int> state_frees (0);

	  TEST_CHECK (fake_policy_try_begin (pool, D, binding));

	  stream_pipeline *pipe = stream_pipeline::create (NULL, binding, 2);
	  TEST_CHECK (pipe != NULL);

	  std::vector<std::thread> producers;
	  std::thread consumer;
	  bool launched = false;
	  bool opened = false;

	  if (point >= AT_PRODUCER_STARTED)
	    {
	      TEST_CHECK (pipe->launch_producers (NULL, D, &state_frees, fake_producer_state_free) == 0);
	      for (int p = 0; p < D; p++)
		{
		  producers.emplace_back (producer_task, pipe, p, BATCHES, (std::atomic<int> *) NULL);
		}
	      launched = true;
	    }

	  if (point >= AT_CONSUMER_OPEN)
	    {
	      stream_source *src = pipe->open_consumer (NULL, D);
	      TEST_CHECK (src != NULL);
	      opened = true;
	      /* consumer worker pops until ERROR/EOS; abort while it may be blocked */
	      consumer = std::thread ([src] ()
	      {
		row_batch out;
		SCAN_CODE code;
		while ((code = src->next_batch (NULL, out)) == S_SUCCESS)
		  {
		    free_batch_direct (out);
		  }
	      });
	    }

	  if (point == AT_CONSUMER_CLOSED)
	    {
	      pipe->close_consumer (NULL);
	    }

	  if (point != AT_TEARDOWN_RACE)
	    {
	      fire_double_abort (pipe);
	      TEST_CHECK (pipe->is_abort_requested ());
	    }

	  /* HANDLE-LIFETIME: the consumer worker is joined BEFORE teardown frees the
	   * source/channel (in the engine the scan manager joins its workers before
	   * the mainblock calls join_all).  The abort above guarantees it exits. */
	  if (opened)
	    {
	      consumer.join ();
	    }

	  if (point == AT_TEARDOWN_RACE)
	    {
	      /* abort fired from two threads at random offsets WHILE the driver runs
	       * join_all: races the CONSUMER_CLOSED -> JOINED -> RELEASED window,
	       * including the free step (a late abort must hit the NULLed pointer,
	       * never the freed channel) */
	      std::thread a1 ([pipe, iter] ()
	      {
		std::this_thread::sleep_for (std::chrono::microseconds (iter % 7));
		pipe->abort (NULL, interrupt::interrupt_code::ERROR_INTERRUPTED_FROM_WORKER_THREAD);
	      });
	      std::thread a2 ([pipe, iter] ()
	      {
		std::this_thread::sleep_for (std::chrono::microseconds (iter % 13));
		pipe->abort (NULL, interrupt::interrupt_code::ERROR_INTERRUPTED_FROM_WORKER_THREAD);
	      });
	      TEST_CHECK (pipe->join_all (NULL) == 0);
	      a1.join ();
	      a2.join ();
	    }
	  else
	    {
	      TEST_CHECK (pipe->join_all (NULL) == 0);
	    }

	  /* abort after RELEASED: strict no-op (channel already freed and nulled) */
	  pipe->abort (NULL);
	  TEST_CHECK (pipe->join_all (NULL) == 0);	/* re-entrant join_all: loser path */

	  for (auto &t : producers)
	    {
	      t.join ();	/* no deadlock at any abort point */
	    }

	  check_released_clean (pipe, &pool, launched ? &state_frees : NULL);
	  if (!launched)
	    {
	      TEST_CHECK (state_frees.load () == 0);	/* nothing registered, nothing freed */
	    }

	  stream_pipeline::destroy (NULL, pipe);
	  TEST_CHECK (pool.available.load () == 4);	/* reservation back, exactly once */
	  TEST_CHECK (pool.release_calls.load () == 1);
	  TEST_CHECK (g_live_buffers.load () == 0);
	}
    }

  std::printf ("S3 abort at every state (x%d each, %d points, incl. double-abort + teardown race)\n",
	       iterations_per_state, (int) ABORT_POINT_COUNT);
}

/* ---------------- S4: reservation atomicity / contention (C4, OWN-5) ------------- */

static void
test_reservation_atomicity (int race_iterations, int stress_iterations)
{
  /* a) shortfall reserves NOTHING */
  {
    fake_pool pool (3);
    stream_pipeline::pool_binding binding;
    TEST_CHECK (!fake_policy_try_begin (pool, 2 /* needs 4 */, binding));
    TEST_CHECK (pool.available.load () == 3);	/* untouched: no partial grant */
    TEST_CHECK (binding.handle == NULL && binding.reserved_workers == 0);
  }

  /* b) two pipelines race for the LAST slots: exactly one wins, or (with capacity
   *    below 2*D) both fall back -- never partial */
  for (int iter = 0; iter < race_iterations; iter++)
    {
      fake_pool pool (4);	/* room for exactly ONE 2*D=4 reservation */
      stream_pipeline::pool_binding b1, b2;
      bool won1 = false, won2 = false;

      std::thread t1 ([&] ()
      {
	won1 = fake_policy_try_begin (pool, 2, b1);
      });
      std::thread t2 ([&] ()
      {
	won2 = fake_policy_try_begin (pool, 2, b2);
      });
      t1.join ();
      t2.join ();

      TEST_CHECK (won1 != won2);	/* exactly one winner */
      TEST_CHECK (pool.available.load () == 0);

      /* the winner runs a full mini-lifecycle; the loser reported fallback and holds
       * NOTHING that needs cleanup */
      stream_pipeline::pool_binding &winner = won1 ? b1 : b2;
      std::atomic<int> state_frees (0);
      stream_pipeline *pipe = stream_pipeline::create (NULL, winner, 4);
      TEST_CHECK (pipe != NULL);
      TEST_CHECK (pipe->launch_producers (NULL, 1, &state_frees, fake_producer_state_free) == 0);
      std::thread prod (producer_task, pipe, 0, 3, (std::atomic<int> *) NULL);
      stream_source *src = pipe->open_consumer (NULL, 2);
      TEST_CHECK (src != NULL);
      row_batch out;
      while (src->next_batch (NULL, out) == S_SUCCESS)
	{
	  free_batch_direct (out);
	}
      prod.join ();
      pipe->close_consumer (NULL);
      TEST_CHECK (pipe->join_all (NULL) == 0);
      check_released_clean (pipe, &pool, &state_frees);
      stream_pipeline::destroy (NULL, pipe);

      TEST_CHECK (pool.available.load () == 4);	/* everything returned */
      TEST_CHECK (pool.release_calls.load () == 1);	/* by exactly one release */
      TEST_CHECK (g_live_buffers.load () == 0);	/* alloc ledger balanced */
    }

  /* c) randomized contention: invariants -- every grant is exactly 2*D, the available
   *    count never goes negative, and after all releases the pool is whole again */
  {
    fake_pool pool (8);
    std::atomic<bool> stop (false);
    std::atomic<int> grants (0);

    std::thread checker ([&] ()
    {
      while (!stop.load ())
	{
	  int avail = pool.available.load ();
	  TEST_CHECK (avail >= 0 && avail <= pool.capacity);
	  std::this_thread::yield ();
	}
    });

    std::vector<std::thread> contenders;
    for (int t = 0; t < 4; t++)
      {
	contenders.emplace_back ([ &, t] ()
	{
	  for (int i = 0; i < stress_iterations; i++)
	    {
	      int degree = 1 + ((i + t) % 3);	/* 2*D in {2,4,6} */
	      stream_pipeline::pool_binding binding;
	      if (fake_policy_try_begin (pool, degree, binding))
		{
		  grants.fetch_add (1);
		  TEST_CHECK (binding.reserved_workers == 2 * degree);
		  std::this_thread::yield ();
		  binding.release (binding.handle);	/* exactly-once per grant */
		}
	    }
	});
      }
    for (auto &t : contenders)
      {
	t.join ();
      }
    stop.store (true);
    checker.join ();

    TEST_CHECK (pool.available.load () == pool.capacity);	/* nothing leaked */
    TEST_CHECK (pool.release_calls.load () == grants.load ());
    std::printf ("S4 reservation: %d race iters, %d stress iters x4 threads, %d grants\n",
		 race_iterations, stress_iterations, grants.load ());
  }
}

/* ---------------- main ---------------- */

int
main (int argc, char **argv)
{
  double scale = 1.0;

  if (argc > 1)
    {
      scale = std::atof (argv[1]);
      if (scale <= 0)
	{
	  scale = 1.0;
	}
    }

  /* watchdog: any deadlock (the bug class this prototype exists to catch) aborts the
   * run loudly instead of hanging CI */
  static std::atomic<bool> done (false);
  int watchdog_secs = 120 + (int) (240 * scale);
  std::thread ([watchdog_secs] ()
  {
    for (int i = 0; i < watchdog_secs * 10; i++)
      {
	std::this_thread::sleep_for (std::chrono::milliseconds (100));
	if (done.load ())
	  {
	    return;
	  }
      }
    std::fprintf (stderr, "WATCHDOG: stream_pipeline test deadlocked\n");
    std::_Exit (86);
  }).detach ();

  test_normal_lifecycle ((int) (50 * scale));
  test_slow_consumer_early_close ((int) (50 * scale));
  test_abort_every_state ((int) (200 * scale));
  test_reservation_atomicity ((int) (300 * scale), (int) (1000 * scale));

  done.store (true);

  if (g_failed == 0)
    {
      std::printf ("ALL STREAM PIPELINE TESTS PASSED\n");
      return 0;
    }
  std::printf ("%d STREAM PIPELINE CHECK(S) FAILED\n", g_failed);
  return 1;
}
