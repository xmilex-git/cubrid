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
 * px_stream_policy.hpp - streaming hash-join orchestration policy (component C4)
 *
 * The THIN policy layer over the C7 lifetime owner (px_stream_pipeline.hpp): decide, for
 * one eligible hash-join edge of degree D, whether the whole pipeline's 2*D workers can be
 * reserved ATOMICALLY -- and on any shortfall report fallback to the materialized path.
 * The policy owns nothing with a lifetime; a successful reservation is handed to the C7
 * stream_pipeline, which releases it exactly once (architecture_deepening_designs.md C4/C7,
 * OWN-1/OWN-5).
 *
 * Whole-pipeline reservation atomicity (SSOT R5 / O3):
 *  - The streamed pipeline needs producer D + consumer D = 2*D workers BEFORE either side
 *    can block on the bounded channel.  Reserving them in two steps (the two independent
 *    sites today: query_hash_join.c producer reservation and px_scan.cpp consumer
 *    reservation) can deadlock against the shared global pool and other concurrent
 *    pipelines: each of two queries grabs D and waits forever for the other D.
 *  - Therefore the reservation is ONE atomic all-or-nothing step:
 *    stream_try_reserve_exact () below -- a single successful CAS on the pool's available
 *    count subtracts exactly 2*D, or nothing at all.  A partial reservation is impossible
 *    by construction: there is no code path that subtracts fewer than the requested
 *    workers (contrast worker_manager_global::try_reserve_workers, which may legally
 *    grant fewer, down to min_degree -- that path is NOT used for pipelines).
 *  - Fallback (materialize) on shortfall is legal ONLY before the first emitted row
 *    (SSOT R3/R5); this decision point runs before the producer is launched, hence before
 *    any emit, by construction.
 *
 * Inert: nothing calls stream_policy_try_begin () in production paths yet (R2: feature
 * OFF = observable-identical).  This header is deliberately light (<atomic> +
 * thread_compat.hpp only) so worker_manager_global.cpp and standalone unit tests can both
 * include the one shared reservation primitive.
 *
 * Error model: C error model only -- no exceptions; a fallback decision is an expected
 * outcome, not an error (no er_set on shortfall).
 */

#ifndef _PX_STREAM_POLICY_HPP_
#define _PX_STREAM_POLICY_HPP_

#include <atomic>

#include "thread_compat.hpp"

namespace parallel_query
{
  class worker_manager;

  /* The one atomic all-or-nothing reservation primitive (R5/O3).  Either EXACTLY
   * num_workers are subtracted from available_count in one successful CAS, or nothing is
   * subtracted and 0 is returned.  Shared by worker_manager_global::try_reserve_workers_exact
   * (the engine pool) and the lifetime-prototype tests (a fake pool) so the algorithm under
   * test is the algorithm in production. */
  inline int
  stream_try_reserve_exact (std::atomic<int> &available_count, const int num_workers)
  {
    /* safe-guard */
    if (num_workers <= 0)
      {
	return 0;
      }

    int available = available_count.load ();

    while (true)
      {
	if (available < num_workers)
	  {
	    /* all-or-nothing: a shortfall reserves NOTHING (never a partial grant) */
	    return 0;
	  }

	if (available_count.compare_exchange_weak (available, available - num_workers))
	  {
	    /* exactly num_workers, subtracted in this one CAS */
	    return num_workers;
	  }

	/* CAS failed: available was reloaded with the actual value, retry */
      }
  }

  /* C4 decision: stream this edge, or fall back to the materialized path */
  enum class stream_policy_kind : int
  {
    STREAM = 0,			/* exactly 2*D workers reserved; caller hands them to the C7 pipeline */
    FALLBACK_INELIGIBLE,	/* edge/degree ineligible -- materialized path */
    FALLBACK_NO_WORKERS		/* atomic 2*D reservation failed -- materialized path (pre-emit only, R3/R5) */
  };

  struct stream_policy_decision
  {
    stream_policy_kind policy;
    int degree;			/* D: producer degree == consumer degree (C6 deferred) */
    int pipeline_workers;	/* exactly 2*D when policy == STREAM; otherwise 0 */
    worker_manager *pool;	/* the atomically reserved pool; NULL unless policy == STREAM.
				 * Ownership: hand to stream_pipeline::create () (which then owns
				 * the exactly-once release), or return it via
				 * stream_policy_release_fallback () if pipeline construction
				 * fails before the pipeline takes ownership. */
  };

  /* Decide-to-stream for ONE eligible edge: attempt the single atomic 2*D reservation;
   * on any shortfall report fallback.  Never holds (and can never hold) a partial
   * reservation.  Pure decision + reservation -- no producer/consumer is launched here. */
  stream_policy_decision stream_policy_try_begin (THREAD_ENTRY *thread_p, int degree);

  /* Release a STREAM decision's reservation when the pipeline could not be constructed
   * (still strictly before any emit, so the materialized fallback remains legal). */
  void stream_policy_release_fallback (THREAD_ENTRY *thread_p, stream_policy_decision &decision);

  /* stream_pipeline::pool_binding-compatible release hook for an engine worker_manager
   * handle: the C7 pipeline calls it EXACTLY ONCE on the JOINED -> RELEASED transition. */
  void stream_policy_release_pool_handle (void *pool_handle);
}

#endif /* _PX_STREAM_POLICY_HPP_ */
