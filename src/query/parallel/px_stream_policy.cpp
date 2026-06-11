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
 * px_stream_policy.cpp - streaming hash-join orchestration policy (component C4)
 *
 * See px_stream_policy.hpp for the contract.  Inert: no production path calls
 * stream_policy_try_begin () yet (R2).
 */

#if !defined (SERVER_MODE) && !defined (SA_MODE)
#error Belongs to server module
#endif /* !defined (SERVER_MODE) && !defined (SA_MODE) */

#include "px_stream_policy.hpp"

#include <cassert>

#include "px_worker_manager.hpp"

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace parallel_query
{
  stream_policy_decision
  stream_policy_try_begin (THREAD_ENTRY *thread_p, int degree)
  {
    stream_policy_decision decision;

    (void) thread_p;

    decision.policy = stream_policy_kind::FALLBACK_INELIGIBLE;
    decision.degree = degree;
    decision.pipeline_workers = 0;
    decision.pool = NULL;

    if (degree < 1)
      {
	/* not a parallelizable edge; materialized path */
	return decision;
      }

    /* one atomic whole-pipeline reservation (R5/O3): producer D + consumer D.
     * try_reserve_workers_exact is all-or-nothing by construction, so a partial
     * reservation can never reach a pipeline (OWN-5 exact-2*D-or-fallback). */
    int pipeline_workers = 2 * degree;

    decision.pool = worker_manager::try_reserve_workers_exact (pipeline_workers);
    if (decision.pool == NULL)
      {
	/* shortfall: NOTHING was reserved; the caller takes the materialized path.
	 * This fallback is legal only before the first emit (R3/R5) -- and this
	 * decision point runs before any producer is launched, by construction.
	 * Expected outcome, not an error: no er_set. */
	decision.policy = stream_policy_kind::FALLBACK_NO_WORKERS;
	return decision;
      }

    assert (decision.pool->get_reserved_workers () == pipeline_workers);

    decision.policy = stream_policy_kind::STREAM;
    decision.pipeline_workers = pipeline_workers;
    return decision;
  }

  void
  stream_policy_release_fallback (THREAD_ENTRY *thread_p, stream_policy_decision &decision)
  {
    (void) thread_p;

    if (decision.pool != NULL)
      {
	/* no producer/consumer task was ever launched against this reservation, so
	 * release_workers () returns it to the global pool immediately (and frees the
	 * manager -- the handle is dead after this call). */
	decision.pool->release_workers ();
	decision.pool = NULL;
      }

    decision.pipeline_workers = 0;
    decision.policy = stream_policy_kind::FALLBACK_NO_WORKERS;
  }

  void
  stream_policy_release_pool_handle (void *pool_handle)
  {
    assert (pool_handle != NULL);

    if (pool_handle != NULL)
      {
	/* waits for any task still accounted against the reservation, returns the
	 * workers to the global pool, and frees the manager (release_workers () frees
	 * `this`; the C7 pipeline guarantees this runs exactly once, post-JOINED). */
	((worker_manager *) pool_handle)->release_workers ();
      }
  }
}
