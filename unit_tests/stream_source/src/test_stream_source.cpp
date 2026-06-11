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
 * test_stream_source.cpp - deterministic unit tests for the C3+C5 Streamed Result Source
 *                          (top half) over the real C2 channel
 *
 * Covers:
 *   1. happy path: N batches in, N out via next_batch, then sticky S_END (INV-EOS)
 *   2. EOS only after the LAST producer is done (R10): with one producer still live the
 *      consumer never sees S_END
 *   3. fan-out = 1: second open refused with an error + er_set (SSOT section 6)
 *   4. R4 guard: reset () always hard-fails (er_set + error), never a silent no-op
 *   5. R11 early close: close () mid-stream aborts the channel, a push-blocked producer
 *      wakes with push()==false and frees its retained batch, drain_residue () frees the
 *      in-flight residue exactly once, close raises NO error -- alloc ledger balanced
 *   6. abort/interrupt => next_batch returns S_ERROR (not S_END)
 *
 * The byte-level walk (slot iterator) is exercised by the engine-flag harness
 * (.not_git_tracking/scratch/stream_source_harness) against the real px_scan TUs.
 *
 * Link stubs: er_set / db_private_free_release are defined HERE and double as the error
 * and allocation ledgers (no engine library link; THREAD_ENTRY is void in mode-less
 * compilation, and NDEBUG selects the *_release allocator names).
 */

#include "px_stream_source.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

using parallel_query::interrupt;
using parallel_query::row_batch;
using parallel_query::stream_channel;
using parallel_query::stream_source;

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
static std::atomic<int> g_db_private_frees (0);

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

extern "C" void
db_private_free_release (THREAD_ENTRY *thrd, void *ptr, bool rc_track)
{
  (void) thrd;
  (void) rc_track;
  if (ptr != NULL)
    {
      std::free (ptr);
      g_live_buffers.fetch_sub (1);
      g_db_private_frees.fetch_add (1);
    }
}

/* ---------------- helpers ---------------- */

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

/* ---------------- 1. happy path + sticky EOS ---------------- */

static void
test_happy_path_and_eos ()
{
  stream_channel<row_batch> chan (8);
  interrupt intr;
  stream_source src (&chan, &intr, 4);

  TEST_CHECK (src.get_degree () == 4);
  TEST_CHECK (src.get_state () == stream_source::source_state::CREATED);
  TEST_CHECK (src.open () == 0 /* NO_ERROR */);
  TEST_CHECK (src.is_open ());

  const int N = 5;
  chan.register_producer ();
  for (int i = 0; i < N; i++)
    {
      row_batch b = make_batch (i);
      TEST_CHECK (chan.push (b));
    }
  chan.producer_done ();

  row_batch out;
  for (int i = 0; i < N; i++)
    {
      TEST_CHECK (src.next_batch (NULL, out) == S_SUCCESS);
      std::uint64_t tag;
      std::memcpy (&tag, out.buf, sizeof (tag));
      TEST_CHECK (tag == (std::uint64_t) i);	/* SPSC: order preserved */
      free_batch_direct (out);
    }

  TEST_CHECK (src.next_batch (NULL, out) == S_END);
  TEST_CHECK (src.get_state () == stream_source::source_state::END_OF_STREAM);
  TEST_CHECK (src.next_batch (NULL, out) == S_END);	/* sticky */
  TEST_CHECK (src.get_batches_delivered () == (std::uint64_t) N);

  src.close (NULL);
  TEST_CHECK (src.drain_residue (NULL) == 0);
  TEST_CHECK (g_live_buffers.load () == 0);
}

/* ---------------- 2. EOS only after the LAST producer (R10) ---------------- */

static void
test_eos_after_last_producer ()
{
  stream_channel<row_batch> chan (8);
  interrupt intr;
  stream_source src (&chan, &intr, 2);
  TEST_CHECK (src.open () == 0);

  chan.register_producer ();
  chan.register_producer ();

  row_batch b = make_batch (7);
  TEST_CHECK (chan.push (b));
  chan.producer_done ();	/* producer 1 done; producer 2 still live */

  row_batch out;
  TEST_CHECK (src.next_batch (NULL, out) == S_SUCCESS);
  free_batch_direct (out);

  /* channel is empty but one producer is live: consumer must BLOCK, not see S_END.
   * Verify with a popper thread + a delayed last producer. */
  std::atomic<bool> got_result (false);
  SCAN_CODE late_code = S_ERROR;
  std::thread popper ([&] ()
  {
    late_code = src.next_batch (NULL, out);
    got_result.store (true);
  });

  std::this_thread::sleep_for (std::chrono::milliseconds (50));
  TEST_CHECK (!got_result.load ());	/* no early EOS while a producer is live */

  row_batch b2 = make_batch (8);
  TEST_CHECK (chan.push (b2));
  chan.producer_done ();	/* LAST producer done */

  popper.join ();
  TEST_CHECK (got_result.load ());
  TEST_CHECK (late_code == S_SUCCESS);
  free_batch_direct (out);

  TEST_CHECK (src.next_batch (NULL, out) == S_END);
  src.close (NULL);
  TEST_CHECK (g_live_buffers.load () == 0);
}

/* ---------------- 3. fan-out = 1 + 4. R4 reset guard ---------------- */

static void
test_fanout_and_reset_guard ()
{
  stream_channel<row_batch> chan (4);
  interrupt intr;
  stream_source src (&chan, &intr, 1);

  TEST_CHECK (src.open () == 0);

  int er_before = g_er_set_calls.load ();
  TEST_CHECK (src.open () != 0);	/* second consumer refused (fan-out = 1) */
  TEST_CHECK (g_er_set_calls.load () > er_before);

  er_before = g_er_set_calls.load ();
  TEST_CHECK (src.reset () != 0);	/* R4: rewind ALWAYS hard-fails */
  TEST_CHECK (g_er_set_calls.load () > er_before);

  /* still drainable after the refused calls: guard must refuse, not corrupt */
  chan.register_producer ();
  chan.producer_done ();
  row_batch out;
  TEST_CHECK (src.next_batch (NULL, out) == S_END);

  src.close (NULL);

  er_before = g_er_set_calls.load ();
  TEST_CHECK (src.open () != 0);	/* re-open after close refused too (R4) */
  TEST_CHECK (g_er_set_calls.load () > er_before);
}

/* ---------------- 5. R11 early close: prompt stop, drain, no leak, no error ------- */

static void
test_early_close_no_leak_no_error ()
{
  /* tiny capacity so the producer actually blocks mid-stream */
  stream_channel<row_batch> chan (2);
  interrupt intr;
  stream_source src (&chan, &intr, 2);
  TEST_CHECK (src.open () == 0);

  chan.register_producer ();

  const int TOTAL = 64;
  std::atomic<int> pushed (0);
  std::atomic<int> retained_frees (0);
  std::thread producer ([&] ()
  {
    for (int i = 0; i < TOTAL; i++)
      {
	row_batch b = make_batch (i);
	if (chan.push (b))
	  {
	    pushed.fetch_add (1);
	  }
	else
	  {
	    /* INV-FAILPUSH: ownership retained -> free and stop, no error raised */
	    free_batch_direct (b);
	    retained_frees.fetch_add (1);
	    break;
	  }
      }
    chan.producer_done ();
  });

  /* consume a couple of batches, then early-terminate (LIMIT satisfied) */
  row_batch out;
  for (int i = 0; i < 2; i++)
    {
      if (src.next_batch (NULL, out) == S_SUCCESS)
	{
	  free_batch_direct (out);
	}
    }

  int er_before = g_er_set_calls.load ();
  src.close (NULL);		/* R11: aborts the channel, wakes the blocked producer */
  producer.join ();		/* must not hang */
  TEST_CHECK (g_er_set_calls.load () == er_before);	/* close raised NO spurious error */
  TEST_CHECK (retained_frees.load () <= 1);
  TEST_CHECK (pushed.load () < TOTAL);	/* producer stopped early, no unbounded waste */

  /* teardown drain (producers joined): residue freed exactly once via db_private_free */
  int residue = src.drain_residue (NULL);
  TEST_CHECK (residue >= 0);
  TEST_CHECK (g_db_private_frees.load () == residue);
  TEST_CHECK (g_live_buffers.load () == 0);	/* ledger balanced: no leak */

  /* after close, the channel delivers nothing through pop */
  TEST_CHECK (chan.drain_one (out) == false);
}

/* ---------------- 6. abort / interrupt => S_ERROR, never S_END ---------------- */

static void
test_abort_is_error_not_eos ()
{
  stream_channel<row_batch> chan (4);
  interrupt intr;
  stream_source src (&chan, &intr, 1);
  TEST_CHECK (src.open () == 0);

  chan.register_producer ();
  row_batch b = make_batch (42);
  TEST_CHECK (chan.push (b));

  /* error path protocol: set the shared interrupt, then abort the channel */
  intr.set_code (interrupt::interrupt_code::ERROR_INTERRUPTED_FROM_WORKER_THREAD);
  chan.abort ();

  row_batch out;
  TEST_CHECK (src.next_batch (NULL, out) == S_ERROR);	/* aborted: no residual row, no EOS */
  TEST_CHECK (src.get_state () == stream_source::source_state::OPEN);	/* not a clean EOS */

  chan.producer_done ();	/* producer exits via its own abort observation */
  src.close (NULL);
  int residue = src.drain_residue (NULL);
  TEST_CHECK (residue == 1);	/* the in-flight batch freed exactly once */
  TEST_CHECK (g_live_buffers.load () == 0);
}

int
main ()
{
  test_happy_path_and_eos ();
  test_eos_after_last_producer ();
  test_fanout_and_reset_guard ();
  test_early_close_no_leak_no_error ();
  test_abort_is_error_not_eos ();

  if (g_failed == 0)
    {
      std::printf ("ALL STREAM SOURCE TESTS PASSED\n");
      return 0;
    }
  std::printf ("%d STREAM SOURCE CHECK(S) FAILED\n", g_failed);
  return 1;
}
