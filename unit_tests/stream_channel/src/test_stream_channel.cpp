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
 * test_stream_channel.cpp - deterministic unit tests for the C2 row-batch channel
 *
 * Covers, in isolation and deterministically (the heavy randomized stress lives in the
 * standalone harness):
 *   1. abort() wakes BOTH a push-blocked producer and a pop-blocked consumer (INV-ABORT)
 *   2. failed push retains ownership: aborted / push-completed channel (INV-FAILPUSH)
 *   3. MPMC exact-once: 4 producers x 2 consumers, no dup/loss, EOS after last producer (INV-EOS)
 *   4. fast-path abort: nonempty channel + abort => pop delivers nothing; drain_one still
 *      pops the residue exactly once (INV-ABORT / INV-OWN)
 *   5. EOS is observable only after the LAST registered producer calls producer_done()
 */

#include "px_stream_channel.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

using parallel_query::row_batch;
using parallel_query::stream_channel;

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

static row_batch
make_batch (std::uint64_t tag)
{
  row_batch b;
  b.buf = (char *) std::malloc (sizeof (std::uint64_t));
  std::memcpy (b.buf, &tag, sizeof (tag));
  b.len = (int) sizeof (tag);
  b.tuple_cnt = 1;
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
free_batch (row_batch &b)
{
  std::free (b.buf);
  b.buf = NULL;
}

/* 1. abort() wakes a push-blocked producer AND a pop-blocked consumer (no hang) */
static void
test_abort_wakes_both_sides (void)
{
  stream_channel<row_batch> full_ch (2);	/* capacity requests < 2 are clamped to 2 */
  full_ch.register_producer ();

  /* fill the ring so the next push blocks */
  std::uint64_t fill_tag = 1;
  int filled = 0;
  while (filled < (int) full_ch.capacity ())
    {
      row_batch b = make_batch (fill_tag + (std::uint64_t) filled);
      TEST_CHECK (full_ch.push (b));
      filled++;
    }
  TEST_CHECK (full_ch.is_full ());

  std::atomic<bool> pusher_returned (false);
  std::thread pusher ([&]
  {
    row_batch b = make_batch (1000);
    bool ok = full_ch.push (b);	/* blocks: ring already full */
    TEST_CHECK (!ok);		/* INV-FAILPUSH: woken by abort, ownership retained */
    free_batch (b);
    pusher_returned.store (true);
  });

  stream_channel<row_batch> empty_ch (4);
  empty_ch.register_producer ();

  std::atomic<bool> popper_returned (false);
  std::thread popper ([&]
  {
    row_batch b;
    bool ok = empty_ch.pop (b);	/* blocks: empty, no EOS */
    TEST_CHECK (!ok);
    TEST_CHECK (empty_ch.is_aborted ());
    popper_returned.store (true);
  });

  std::this_thread::sleep_for (std::chrono::milliseconds (50));
  TEST_CHECK (!pusher_returned.load ());
  TEST_CHECK (!popper_returned.load ());

  full_ch.abort ();
  empty_ch.abort ();

  pusher.join ();
  popper.join ();
  TEST_CHECK (pusher_returned.load ());
  TEST_CHECK (popper_returned.load ());

  /* teardown: residue freed exactly once via drain_one */
  row_batch residue;
  int drained = 0;
  while (full_ch.drain_one (residue))
    {
      TEST_CHECK (batch_tag (residue) == fill_tag + (std::uint64_t) drained);
      free_batch (residue);
      drained++;
    }
  TEST_CHECK (drained == filled);

  std::printf ("test_abort_wakes_both_sides: OK\n");
}

/* 2. failed push retains ownership on aborted and push-completed channels */
static void
test_failed_push_ownership (void)
{
  {
    stream_channel<row_batch> ch (4);
    ch.register_producer ();
    ch.abort ();

    row_batch b = make_batch (42);
    TEST_CHECK (!ch.push (b));		/* aborted => false, retained */
    TEST_CHECK (!ch.try_push (b));
    free_batch (b);			/* caller's unambiguous free */
    ch.producer_done ();
  }
  {
    stream_channel<row_batch> ch (4);
    ch.register_producer ();
    ch.producer_done ();		/* EOS signaled */

    row_batch b = make_batch (43);
    TEST_CHECK (!ch.push (b));		/* push-completed => false, retained */
    TEST_CHECK (!ch.try_push (b));
    free_batch (b);
  }

  std::printf ("test_failed_push_ownership: OK\n");
}

/* 3. MPMC exact-once: no dup, no loss, EOS only after the last producer_done */
static void
test_mpmc_exact_once (void)
{
  const int producers = 4;
  const int consumers = 2;
  const int per_producer = 500;
  const int total = producers * per_producer;

  stream_channel<row_batch> ch (8);
  for (int p = 0; p < producers; p++)
    {
      ch.register_producer ();
    }

  std::atomic<int> done_calls (0);
  std::vector<std::atomic<int>> seen ((std::size_t) total);
  for (auto &s : seen)
    {
      s.store (0);
    }

  std::vector<std::thread> threads;
  for (int p = 0; p < producers; p++)
    {
      threads.emplace_back ([&, p]
      {
	for (int i = 0; i < per_producer; i++)
	  {
	    row_batch b = make_batch ((std::uint64_t) (p * per_producer + i));
	    bool ok = ch.push (b);
	    TEST_CHECK (ok);
	    if (!ok)
	      {
		free_batch (b);
	      }
	  }
	done_calls.fetch_add (1);
	ch.producer_done ();
      });
    }

  std::atomic<int> received (0);
  for (int c = 0; c < consumers; c++)
    {
      threads.emplace_back ([&]
      {
	row_batch b;
	while (ch.pop (b))
	  {
	    std::uint64_t tag = batch_tag (b);
	    TEST_CHECK (tag < (std::uint64_t) total);
	    TEST_CHECK (seen[(std::size_t) tag].fetch_add (1) == 0);	/* no duplicate */
	    free_batch (b);
	    received.fetch_add (1);
	  }
	/* INV-EOS: a clean end-of-stream is observable only after ALL producers done */
	TEST_CHECK (!ch.is_aborted ());
	TEST_CHECK (done_calls.load () == producers);
      });
    }

  for (auto &t : threads)
    {
      t.join ();
    }

  TEST_CHECK (received.load () == total);	/* no loss */
  for (auto &s : seen)
    {
      TEST_CHECK (s.load () == 1);		/* exact-once */
    }
  TEST_CHECK (ch.is_empty ());

  std::printf ("test_mpmc_exact_once: OK\n");
}

/* 4. fast-path abort: nonempty channel + abort => pop/try_pop deliver nothing;
 *    drain_one still pops the residue exactly once */
static void
test_fast_path_abort_residual (void)
{
  stream_channel<row_batch> ch (8);
  ch.register_producer ();

  for (int i = 0; i < 5; i++)
    {
      row_batch b = make_batch ((std::uint64_t) i);
      TEST_CHECK (ch.push (b));
    }

  ch.abort ();

  row_batch b;
  TEST_CHECK (!ch.pop (b));	/* nonempty, but aborted: no residual row delivered */
  TEST_CHECK (!ch.try_pop (b));

  int drained = 0;
  while (ch.drain_one (b))
    {
      free_batch (b);
      drained++;
    }
  TEST_CHECK (drained == 5);	/* residue freed exactly once, by the drainer alone */
  TEST_CHECK (ch.is_empty ());

  ch.producer_done ();		/* safe on the abort exit path too */

  std::printf ("test_fast_path_abort_residual: OK\n");
}

/* 5. EOS strictly after the LAST registered producer's producer_done */
static void
test_eos_only_after_last_producer (void)
{
  stream_channel<row_batch> ch (4);
  ch.register_producer ();
  ch.register_producer ();

  std::atomic<bool> pop_returned (false);
  std::thread consumer ([&]
  {
    row_batch b;
    bool ok = ch.pop (b);	/* must block: empty but producer 2 still registered */
    TEST_CHECK (!ok);
    TEST_CHECK (!ch.is_aborted ());
    pop_returned.store (true);
  });

  ch.producer_done ();		/* first producer done: NOT EOS yet */
  std::this_thread::sleep_for (std::chrono::milliseconds (50));
  TEST_CHECK (!pop_returned.load ());

  ch.producer_done ();		/* last producer done: EOS observable now */
  consumer.join ();
  TEST_CHECK (pop_returned.load ());

  std::printf ("test_eos_only_after_last_producer: OK\n");
}

int
main (int, char **)
{
  test_abort_wakes_both_sides ();
  test_failed_push_ownership ();
  test_mpmc_exact_once ();
  test_fast_path_abort_residual ();
  test_eos_only_after_last_producer ();

  if (g_failed > 0)
    {
      std::printf ("test_stream_channel: %d FAILURE(S)\n", g_failed);
      return 1;
    }
  std::printf ("test_stream_channel: all tests passed\n");
  return 0;
}
