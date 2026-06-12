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
 * px_stream_metrics.hpp - A7 overlap metrics for one streamed hash-join edge
 *                         (streaming_hashjoin_SSOT.md section 8, A7)
 *
 * Lightweight aggregate counters/timestamps the streamed pipeline updates while it
 * runs and emits as ONE er_log_debug summary line from the teardown runner:
 *   - producer active interval  [prod_first_start_us, prod_last_end_us]
 *   - consumer active interval  [cons_first_pop_us,  cons_last_ret_us]
 *   - overlap duration/ratio    (derived at emission)
 *   - push-block / pop-block time (time spent inside the channel's blocking ops)
 *   - output-gather tail        (consumer close -> teardown, derived at emission)
 *
 * OFF-identity (R2): every update site is reachable only when a stream_pipeline
 * exists (the gated streamed path); emission additionally goes through
 * er_log_debug, which is gated by the er_log_debug system parameter.  With the
 * feature OFF nothing here ever executes.
 *
 * All members are atomics updated with relaxed/acq_rel ordering -- the numbers are
 * diagnostics, not synchronization; correctness of the pipeline never depends on
 * them.  No exceptions, no allocation.
 */

#ifndef _PX_STREAM_METRICS_HPP_
#define _PX_STREAM_METRICS_HPP_

#include <atomic>
#include <chrono>
#include <cstdint>

namespace parallel_query
{
  /* monotonic microsecond stamp; one shared epoch (steady_clock) for every site */
  inline std::uint64_t
  stream_metrics_now_us ()
  {
    return (std::uint64_t) std::chrono::duration_cast<std::chrono::microseconds>
	   (std::chrono::steady_clock::now ().time_since_epoch ()).count ();
  }

  struct stream_metrics
  {
    /* driver-side lifecycle stamps */
    std::atomic<std::uint64_t> t_create_us {0};		/* pipeline created (arm; build table ready) */
    std::atomic<std::uint64_t> t_launch_us {0};		/* producers registered/launched (detach) */
    std::atomic<std::uint64_t> t_consumer_open_us {0};	/* consumer source opened on the channel */
    std::atomic<std::uint64_t> t_consumer_closed_us {0};	/* CONSUMER_CLOSED transition (gather done) */
    std::atomic<std::uint64_t> t_join_claim_us {0};	/* teardown runner claimed (mainblock done) */
    std::atomic<std::uint64_t> t_join_done_us {0};	/* teardown complete (RELEASED imminent) */

    /* producer active interval (min start / max end over the D probe tasks) */
    std::atomic<std::uint64_t> prod_first_start_us {0};
    std::atomic<std::uint64_t> prod_last_end_us {0};

    /* consumer active interval (min first-pop / max last-return over D workers) */
    std::atomic<std::uint64_t> cons_first_pop_us {0};
    std::atomic<std::uint64_t> cons_last_ret_us {0};

    /* time spent inside the channel's blocking push/pop (backpressure evidence) */
    std::atomic<std::uint64_t> push_block_us {0};
    std::atomic<std::uint64_t> pop_block_us {0};
    std::atomic<std::uint64_t> push_blocked_cnt {0};	/* pushes that waited > 100us */
    std::atomic<std::uint64_t> pop_blocked_cnt {0};	/* pops that waited > 100us */

    /* traffic */
    std::atomic<std::uint64_t> batches_pushed {0};
    std::atomic<std::uint64_t> bytes_pushed {0};
    std::atomic<std::uint64_t> tuples_pushed {0};

    /* probe-input chase (D1) -- all zero unless a chase engaged */
    std::atomic<int> chase_engaged {0};			/* 1 = probe consumed via the chase iterator */
    std::atomic<int> chase_stopped {0};			/* 1 = writer ended on a requested stop (R11) */
    std::atomic<std::uint64_t> chase_pages {0};		/* pages published at the last frontier update */
    std::atomic<std::uint64_t> chase_wait_us {0};	/* probe time spent waiting on writer progress */
    std::atomic<std::uint64_t> chase_wait_cnt {0};
    std::atomic<std::uint64_t> chase_writer_end_us {0};	/* writer closed (epoch us) */
    std::atomic<std::uint64_t> chase_stop_request_us {0};	/* first stop request (epoch us) */

    /* store-min (first writer of a nonzero value wins races conservatively) */
    void
    note_min (std::atomic<std::uint64_t> &slot, std::uint64_t value)
    {
      std::uint64_t cur = slot.load (std::memory_order_relaxed);
      while ((cur == 0 || value < cur)
	     && !slot.compare_exchange_weak (cur, value, std::memory_order_acq_rel))
	{
	  ;			/* cur reloaded; re-check */
	}
    }

    /* store-max */
    void
    note_max (std::atomic<std::uint64_t> &slot, std::uint64_t value)
    {
      std::uint64_t cur = slot.load (std::memory_order_relaxed);
      while (value > cur
	     && !slot.compare_exchange_weak (cur, value, std::memory_order_acq_rel))
	{
	  ;			/* cur reloaded; re-check */
	}
    }
  };
}

#endif /* _PX_STREAM_METRICS_HPP_ */
