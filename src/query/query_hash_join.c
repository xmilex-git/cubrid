/*
 * Copyright 2008 Search Solution Corporation
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
 * query_hash_join.c
 */

#include "query_hash_join.h"

#include "dbtype.h"		/* db_make_null */
#include "error_manager.h"	/* er_errid, NO_ERROR, assert_release_error */
#include "fetch.h"		/* fetch_val_list */
#include "list_file.h"		/* qfile_open_list, qfile_close_list */
#include "memory_alloc.h"	/* CEIL_PTVDIV */
#include "object_representation.h"	/* TP_DOMAIN */
#include "perf_monitor.h"	/* perfmon_get_from_statistic, PSTAT_... */
#include "px_hash_join.hpp"	/* parallel_query::hash_join::... */
#include "px_parallel.hpp"	/* parallel_query::compute_parallel_degree */
#include "px_worker_manager.hpp"	/* parallel_query::worker_manager */
#include "qfile_batch_record_store.hpp"	/* issue #147 T1 S2: raw batch record store (Grace batch files) */
#include "query_executor.h"	/* issue #149 P3: HASHJOIN_STREAM_SINK, XASL_STATE, qexec_execute_mainblock */
#include "query_list.h"		/* JOIN_TYPE */
#include "query_opfunc.h"	/* issue #149 P3: qdata_get_valptr_type_list */
#include "query_manager.h"	/* QMGR_TEMP_FILE */
#include "query_workmem.hpp"	/* temp_page_store::op_limit_bytes (hash_mem accessor, #146 S0) */
#include "system_parameter.h"	/* prm_get_bigint_value, PRM_ID_... */
#include <stdlib.h>
#include "thread_entry.hpp"	/* THREAD_ENTRY */
#include "xasl.h"		/* XASL_NODE, HASHJOIN_PROC_NODE, XASL_INCLUDES_TDE_CLASS */

#include <atomic>
#include <vector>

/* issue #147 T1 S2 gate 1 (structure goal): process-wide count of
 * hjoin_probe_key's random-read branches (HYBRID / HASH_FILE ->
 * qfile_jump_scan_tuple_position). The Grace path (hjoin_execute_grace)
 * forces every batch through HASH_METH_IN_MEM (never HYBRID/HASH_FILE), so
 * this must stay 0 across a Grace-executed join; the SINGLE/old-PARTITION/
 * PARALLEL paths may still legitimately use HYBRID/HASH_FILE for their own
 * (unrelated, unchanged) tiering, so this counter's value is only meaningful
 * scoped to a query run known to have taken the Grace path. */
static std::atomic<long> hjoin_debug_random_probe_reads { 0 };

long
hjoin_debug_random_probe_read_count (void)
{
  return hjoin_debug_random_probe_reads.load (std::memory_order_relaxed);
}

/* issue #147 T1 S4: mid-build/mid-reload nbatch growth observability (same
 * debug-counter pattern as hjoin_debug_random_probe_reads above -- no full
 * HASHJOIN_STATS wiring, see the S2 stop-and-report's deferred gap). */
static std::atomic<long> hjoin_debug_grace_nbatch_grows { 0 };
static std::atomic<long> hjoin_debug_grace_skew_overflow { 0 };

long
hjoin_debug_grace_nbatch_grow_count (void)
{
  return hjoin_debug_grace_nbatch_grows.load (std::memory_order_relaxed);
}

long
hjoin_debug_grace_skew_overflow_bytes (void)
{
  return hjoin_debug_grace_skew_overflow.load (std::memory_order_relaxed);
}

/* issue #149 P2: outer-streaming coverage observability (same pattern). */
static std::atomic<long> hjoin_debug_outer_streamed { 0 };
static std::atomic<long> hjoin_debug_outer_fallback_materialize { 0 };

long
hjoin_debug_outer_streamed_count (void)
{
  return hjoin_debug_outer_streamed.load (std::memory_order_relaxed);
}

long
hjoin_debug_outer_fallback_materialize_count (void)
{
  return hjoin_debug_outer_fallback_materialize.load (std::memory_order_relaxed);
}

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

/*
 * Debug Macros
 */

#define PARTITION_FILL_FACTOR 0.8

#define DUMP_HASH_TABLE_LIMIT 100
#define DUMP_PROBE_LIMIT 20


/*
 * Macro Function Declarations
 */

#if HASHJOIN_DUMP_HASH_TABLE
#define HJOIN_DUMP_HASH_TABLE(thread_p, hash_scan_p, list_id_p) \
  hjoin_dump_hash_table ((thread_p), (hash_scan_p), (list_id_p))
#else
#define HJOIN_DUMP_HASH_TABLE(thread_p, hash_scan_p, list_id_p) ((void) 0)
#endif /* HASHJOIN_DUMP_HASH_TABLE */

/*
 * Function Declarations
 */

/* Hash Join Execution */
static int hjoin_execute_partitions (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager);
static int hjoin_outer_fill_null_values (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager,
					 HASHJOIN_CONTEXT * context);
static int hjoin_execute_internal (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context);

/* Hash Join Manager */
static int hjoin_init_manager (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, XASL_NODE * xasl,
			       QUERY_ID query_id, VAL_DESCR * val_descr, XASL_STATE * xasl_state);
static void hjoin_clear_manager (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager);

/* Hash Join Domain Info */
static int hjoin_init_domain_info (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager,
				   HASHJOIN_DOMAIN_INFO * domain_info);

/* Hash Join Partitioning */
static HASHJOIN_STATUS hjoin_try_partition (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager,
					    HASHJOIN_CONTEXT * single_context);
static HASHJOIN_STATUS hjoin_check_partition (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager,
					      HASHJOIN_CONTEXT * single_context);
static int hjoin_prepare_partition (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager,
				    HASHJOIN_SPLIT_INFO * split_info);
static int hjoin_build_partitions (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager,
				   HASHJOIN_SPLIT_INFO * split_info);
static int hjoin_split_qlist (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager,
			      HASHJOIN_INPUT_SPLIT_INFO * split_info, QFILE_LIST_ID ** temp_part_list_id,
			      HASH_SCAN_KEY * temp_key);

/* Hash Join Grace (issue #147 T1 S2/S6): lazy forward-spill batch state
 * machine. Originally serial-only (replacing hjoin_build_partitions for the
 * non-px fallback of hjoin_try_partition's HASHJOIN_STATUS_PARTITION branch);
 * issue #147 S6 reuses it verbatim per-partition for PARALLEL mode too (see
 * hjoin_execute_grace_px / join_task::execute) -- D-S2-1's nbatch clamp
 * guarantees every px-bound partition already fits hash_mem, so forcing
 * IN_MEM per partition is always safe there. `worker_id` disambiguates
 * concurrently-running callers' batch record store filenames (buffile's
 * naming folds in worker_id + pid; multiple px workers share one pid) --
 * every simultaneous caller MUST pass a distinct worker_id (the serial call
 * site always passes 0, since it never runs concurrently with anything). */
static int hjoin_execute_grace (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context,
				UINT32 worker_id);

/* issue #147 S6: per-partition PARALLEL entry point -- mirrors hjoin_execute()'s
 * empty-input dispatch (FILL_NULL_VALUES / TRY / END, including the px-only
 * last-partition-is-the-NULL-partition rule) but calls hjoin_execute_grace()
 * instead of hjoin_execute_internal() for the TRY case. hjoin_execute() itself
 * stays untouched (still used by the plain SINGLE path). Declared in
 * query_hash_join.h (public, used cross-TU by join_task::execute) -- no
 * separate forward declaration needed here. */

/* Hash Join Parallel */
static HASHJOIN_STATUS hjoin_try_parallel (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager,
					   HASHJOIN_CONTEXT * single_context);
static HASHJOIN_STATUS hjoin_try_parallel_probe (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager,
						 HASHJOIN_CONTEXT * single_context);

/* Hash Join Split Info */
static int hjoin_init_split_info (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager,
				  HASHJOIN_SPLIT_INFO * split_info);
static void hjoin_clear_split_info (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager,
				    HASHJOIN_SPLIT_INFO * split_info, bool clear_all);

/* Hash Join Context */
static int hjoin_init_context (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context,
			       bool * use_grace);
static void hjoin_clear_context (THREAD_ENTRY * thread_p, HASHJOIN_CONTEXT * context);
static void hjoin_destroy_qlist (THREAD_ENTRY * thread_p, HASHJOIN_CONTEXT * context);

/* Hash Join Processing */
static HASHJOIN_STATUS hjoin_check_empty_inputs (HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context);

/* Build Phase */
static int hjoin_build (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context);
static int hjoin_build_key (THREAD_ENTRY * thread_p, HASH_LIST_SCAN * hash_scan,
			    QFILE_LIST_SCAN_ID * list_scan_id, QFILE_TUPLE_RECORD * tuple_record);

/* Probe Phase */
static int hjoin_probe (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context);
static int hjoin_inner_probe (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context,
			      QFILE_LIST_ID * list_id);
static int hjoin_outer_probe (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context,
			      QFILE_LIST_ID * list_id);
static int hjoin_outer_probe_fill_empty (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager,
					 HASHJOIN_CONTEXT * context, QFILE_LIST_ID * list_id,
					 QFILE_TUPLE_RECORD * overflow_record);

/* Merge QFILE_LIST_ID */
static int hjoin_merge_tuple (THREAD_ENTRY * thread_p, QFILE_TUPLE_RECORD * outer_record,
			      QFILE_TUPLE_RECORD * inner_record, QFILE_LIST_MERGE_INFO * merge_info,
			      QFILE_TUPLE_RECORD * overflow_record);

/* Dump */
#if HASHJOIN_DUMP_HASH_TABLE
static void hjoin_dump_hash_table (THREAD_ENTRY * thread_p, HASH_LIST_SCAN * hash_scan, QFILE_LIST_ID * list_id);
#endif /* HASHJOIN_DUMP_HASH_TABLE */

#if !defined(NDEBUG) && HASHJOIN_DUMP_PROBE
static void hjoin_print_tuple (QFILE_LIST_ID * list_id, QFILE_TUPLE tuple, HASHJOIN_PRINT_STEP step);
#endif /* !NDEBUG && HASHJOIN_DUMP_PROBE */

/*
 * Function Definitions
 */

/*
 * qexec_hash_join() -
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry.
 *   xasl(in): XASL node for hash join execution.
 *   query_id(in): Query identifier.
 *   val_descr(in): Value descriptor for positional values.
 */
int
qexec_hash_join (THREAD_ENTRY * thread_p, XASL_NODE * xasl, QUERY_ID query_id, VAL_DESCR * val_descr,
		 XASL_STATE * xasl_state)
{
  HASHJOIN_MANAGER manager;
  HASHJOIN_CONTEXT *single_context;
  HASHJOIN_STATUS status, part_status;

  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (xasl != NULL);
  assert (query_id != NULL_QUERY_ID);

  error = hjoin_init_manager (thread_p, &manager, xasl, query_id, val_descr, xasl_state);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  single_context = &manager.single_context;

  /* issue #149 P3: in push-stream mode the outer's emptiness is unknowable
   * until it runs (during the probe phase) and the inner-empty fast paths
   * read the outer list too -- skip the shortcut entirely; the Grace state
   * machine handles an empty build table (every probe misses -> fill-empty)
   * and an empty outer (zero pushes -> zero rows) without special cases. */
  status = manager.outer_stream_push ? HASHJOIN_STATUS_TRY : hjoin_check_empty_inputs (&manager, single_context);
  single_context->status = status;
  switch (status)
    {
    case HASHJOIN_STATUS_FILL_NULL_VALUES:
      error = hjoin_outer_fill_null_values (thread_p, &manager, single_context);
      break;

    case HASHJOIN_STATUS_TRY:
      part_status = hjoin_try_partition (thread_p, &manager, single_context);
      single_context->status = part_status;
      switch (part_status)
	{
	case HASHJOIN_STATUS_SINGLE:
	  /* monitor */
	  perfmon_inc_stat (thread_p, PSTAT_QM_NUM_HASHJOINS);

	  error = hjoin_execute (thread_p, &manager, single_context);

	  if (thread_is_on_trace (thread_p))
	    {
	      xasl->executed_parallelism = manager.num_parallel_threads;
	    }
	  break;

	case HASHJOIN_STATUS_PARTITION:
	  /* monitor */
	  perfmon_inc_stat (thread_p, PSTAT_QM_NUM_HASHJOINS_PARTITIONED);

	  error = hjoin_execute_partitions (thread_p, &manager);
	  break;

	case HASHJOIN_STATUS_GRACE_DONE:
	  /* issue #147 T1 S2: hjoin_try_partition already ran the Grace batch
	   * state machine to completion -- single_context->list_id is the
	   * final result. Nothing left to execute here (unlike PARTITION). */
	  perfmon_inc_stat (thread_p, PSTAT_QM_NUM_HASHJOINS_PARTITIONED);
	  break;

#if defined (SERVER_MODE)
	case HASHJOIN_STATUS_PARALLEL:
	  /* monitor */
	  perfmon_inc_stat (thread_p, PSTAT_QM_NUM_HASHJOINS_PARALLEL);

	  if (thread_is_on_trace (thread_p))
	    {
	      xasl->executed_parallelism = manager.num_parallel_threads;
	    }

	  // *INDENT-OFF*
	  error = parallel_query::hash_join::execute_partitions (*thread_p, &manager);
	  // *INDENT-ON*
	  break;
#endif /* defined (SERVER_MODE) */

	case HASHJOIN_STATUS_END:
	  /* impossible case */
	  /* hjoin_check_empty_inputs guarantees HASHJOIN_STATUS_END cannot occur here */
	  assert_release_error (false);
	  goto error_exit;

	case HASHJOIN_STATUS_ERROR:
	  /* hjoin_try_partition always retries as HASHJOIN_STATUS_SINGLE;
	   * except for ER_INTERRUPTED, never returns HASHJOIN_STATUS_ERROR */
	  error = er_errid ();
	  assert_release_error (error == ER_INTERRUPTED);
	  goto error_exit;

	default:
	  /* impossible case */
	  assert_release_error (false);
	  goto error_exit;
	}
      break;

    case HASHJOIN_STATUS_END:
      /* Nothing to do */
      assert (single_context->list_id == NULL);
      break;

    case HASHJOIN_STATUS_ERROR:
      [[fallthrough]];
    default:
      /* impossible case */
      assert_release_error (false);
      goto error_exit;
    }

  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  if (single_context->list_id != NULL)
    {
      /* Check if qfile_close_list was called */
      assert (single_context->list_id->last_pgptr == NULL);

      qfile_destroy_list (thread_p, xasl->list_id);	/* may be unnecessary */
      qfile_copy_list_id (xasl->list_id, single_context->list_id, false, QFILE_MOVE_DEPENDENT);
      QFILE_FREE_AND_INIT_LIST_ID (single_context->list_id);

      ASSERT_NO_ERROR_OR_INTERRUPTED ();

      /* The ST/partition path builds a pgbuf-paged (appendable) output list, but
       * a nested hash join consuming it can only run its parallel split when the
       * input is tapeset-backed (the split guards require qfile_list_has_tapeset
       * on both inputs).  Promote the finished list to tapeset via
       * qfile_list_promote_old_to_new (a no-op when the parallel path already
       * produced a tapeset list).  Skip a to-be-cached result (copy-out keeps
       * the pgbuf-paged list). */
      if (!XASL_IS_FLAGED (xasl, XASL_TO_BE_CACHED))
	{
	  error = qfile_list_promote_old_to_new (thread_p, xasl->list_id);
	  if (error != NO_ERROR)
	    {
	      goto error_exit;
	    }
	}
    }
  else if (status == HASHJOIN_STATUS_END)
    {
      ASSERT_NO_ERROR_OR_INTERRUPTED ();
    }
  else
    {
      /* list_id can be NULL when the join result is empty.
       * In this case, it is NO_ERROR.
       * Otherwise, an error may be set. */
      error = er_errid ();
    }

cleanup:
  if (thread_is_on_trace (thread_p) && manager.stats_group != NULL)
    {
      manager.stats_group->status = single_context->status;
    }

  hjoin_clear_manager (thread_p, &manager);

  return error;

error_exit:
  if (error == NO_ERROR || er_errid () == NO_ERROR)
    {
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
    }

  goto cleanup;
}

/*
 * hjoin_execute_partitions() -
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 */
static int
hjoin_execute_partitions (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager)
{
  HASHJOIN_CONTEXT *current_context;
  UINT32 context_cnt, context_index;

  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (manager != NULL);

  HASHJOIN_STATS *stats = manager->single_context.stats;
#if HASHJOIN_PROFILE_TIME
  HASHJOIN_START_STATS profile_start_stats = HASHJOIN_START_STATS_INITIALIZER;
#endif /* HASHJOIN_PROFILE_TIME */
  assert (!thread_is_on_trace (thread_p) || stats != NULL);

  context_cnt = manager->context_cnt;

  for (context_index = 0; context_index < context_cnt; context_index++)
    {
      current_context = &manager->contexts[context_index];

      error = hjoin_execute (thread_p, manager, current_context);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}

      if (thread_is_on_trace (thread_p))
	{
	  hjoin_trace_merge_stats (stats, current_context->stats, manager->single_context.status);
	}

      if (current_context->list_id == NULL)
	{
	  error = er_errid ();
	  if (error != NO_ERROR)
	    {
	      goto error_exit;
	    }
	  else
	    {
	      /* list_id can be NULL when the join result is empty.
	       * In this case, it is NO_ERROR. */
	      continue;
	    }
	}

      HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_MERGE);
      error = hjoin_merge_qlist (thread_p, manager, current_context);
      HJOIN_PROFILE_MERGE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_MERGE,
			       (manager->single_context.list_id !=
				nullptr) ? manager->single_context.list_id->tuple_cnt : 0);

      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
    }

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;

error_exit:
  if (error == NO_ERROR || er_errid () == NO_ERROR)
    {
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
    }

  return error;
}

/*
 * hjoin_execute() -
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   context(in): Hash join context containing per-partition state.
 */
int
hjoin_execute (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context)
{
  HASHJOIN_STATUS status;
  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (context != NULL);

  status = hjoin_check_empty_inputs (manager, context);

  /* In outer joins, tuples with NULL in any join column are placed in the last partition.
   * HASHJOIN_STATUS_FILL_NULL_VALUES is triggered for all tuples in that partition. */
  if (IS_OUTER_JOIN_TYPE (manager->join_type) && context == &manager->contexts[manager->context_cnt - 1])
    {
      status = (status == HASHJOIN_STATUS_TRY) ? HASHJOIN_STATUS_FILL_NULL_VALUES : status;
    }

  context->status = status;

  switch (status)
    {
    case HASHJOIN_STATUS_FILL_NULL_VALUES:
      assert (context != &manager->single_context);
      error = hjoin_outer_fill_null_values (thread_p, manager, context);
      break;

    case HASHJOIN_STATUS_TRY:
      error = hjoin_execute_internal (thread_p, manager, context);
      break;

    case HASHJOIN_STATUS_END:
      /* Nothing to do */
      break;

    case HASHJOIN_STATUS_ERROR:
    default:
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
      break;
    }

  /* Check if qfile_close_list was called */
  assert (context->list_id == NULL || context->list_id->last_pgptr == NULL);

  return error;
}

/*
 * hjoin_outer_fill_null_values() -
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   context(in): Hash join context containing per-partition state.
 */
static int
hjoin_outer_fill_null_values (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context)
{
  QFILE_LIST_ID *list_id = NULL;
  QFILE_TUPLE_RECORD overflow_record = { NULL, 0 };
  SCAN_CODE scan_code;

  HASHJOIN_FETCH_INFO *outer, *inner;
  HASHJOIN_FETCH_INFO *build, *probe;

  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (context != NULL);

  HASHJOIN_STATS *stats = context->stats;
  HASHJOIN_START_STATS start_stats = HASHJOIN_START_STATS_INITIALIZER;
  assert (!thread_is_on_trace (thread_p) || stats != NULL);

  outer = &context->outer;
  inner = &context->inner;

  switch (manager->join_type)
    {
    case JOIN_LEFT:
      context->build = inner;
      context->probe = outer;
      break;

    case JOIN_RIGHT:
      context->build = outer;
      context->probe = inner;
      break;

    default:
      /* impossible case */
      assert_release_error (false);
      goto error_exit;
    }

  build = context->build;
  probe = context->probe;

  /* Prevent faults when qfile_close_scan is called */
  probe->list_scan_id.status = S_CLOSED;

  // *INDENT-OFF*
  probe->tuple_record = { NULL, 0 };
  // *INDENT-ON*

  build->fill_record = NULL;
  probe->fill_record = &probe->tuple_record;

  list_id = qfile_open_list (thread_p, &manager->type_list, NULL, manager->query_id, manager->qlist_flag, NULL);
  if (list_id == NULL)
    {
      goto error_exit;
    }

  error = qfile_open_list_scan (probe->list_id, &probe->list_scan_id);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  if (thread_is_on_trace (thread_p))
    {
      hjoin_trace_start (thread_p, &start_stats);

      assert (stats->build.read_rows == 0);
      assert (stats->build.read_keys == 0);
      stats->build.qualified_rows = build->list_id->tuple_cnt;
    }

  while ((scan_code = qfile_scan_list_next (thread_p, &probe->list_scan_id, &probe->tuple_record, PEEK)) == S_SUCCESS)
    {
      error =
	hjoin_merge_tuple_to_list_id (thread_p, list_id, outer->fill_record, inner->fill_record, manager->merge_info,
				      &overflow_record);
      if (error != NO_ERROR)
	{
	  break;
	}
    }

  if (thread_is_on_trace (thread_p))
    {
      hjoin_trace_end (thread_p, &stats->probe, &start_stats);
      stats->probe.read_rows = probe->list_id->tuple_cnt;
      assert (stats->probe.read_keys == 0);
      stats->probe.qualified_rows = list_id->tuple_cnt;
    }

  /* After qfile_open_list_scan, if an error occurs,
   * ensure qfile_close_scan runs here
   * before jumping to error_exit. */
  qfile_close_scan (thread_p, &probe->list_scan_id);

  if (scan_code == S_ERROR || error != NO_ERROR)
    {
      goto error_exit;
    }

  qfile_close_list (thread_p, list_id);
  context->list_id = list_id;

  ASSERT_NO_ERROR_OR_INTERRUPTED ();

cleanup:
  if (overflow_record.tpl != NULL)
    {
      db_private_free_and_init (thread_p, overflow_record.tpl);
    }

  hjoin_destroy_qlist (thread_p, context);

  /* Check if qfile_close_list was called */
  assert (list_id == NULL || list_id->last_pgptr == NULL);

  return error;

error_exit:
  if (list_id != NULL)
    {
      qfile_close_list (thread_p, list_id);
      qfile_destroy_list (thread_p, list_id);
      QFILE_FREE_AND_INIT_LIST_ID (list_id);
    }

  if (error == NO_ERROR || er_errid () == NO_ERROR)
    {
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
    }

  goto cleanup;
}

/*
 * hjoin_execute_internal() -
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   context(in): Hash join context containing per-partition state.
 */
static int
hjoin_execute_internal (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context)
{
  HASHJOIN_FETCH_INFO *outer, *inner;
  HASHJOIN_FETCH_INFO *build = NULL, *probe = NULL;
  bool use_grace = false;

  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (context != NULL);
  assert (context->list_id == NULL);

  outer = &context->outer;
  inner = &context->inner;

  /* Prevent faults when qfile_close_scan is called */
  outer->list_scan_id.status = S_CLOSED;
  inner->list_scan_id.status = S_CLOSED;

  error = hjoin_init_context (thread_p, manager, context, &use_grace);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }
  if (use_grace)
    {
      /* issue #147 S5-lite: SINGLE path's IN_MEM reserve was rejected by the
       * layer-2 accountant despite fitting the static byte estimate --
       * hjoin_init_context/hjoin_scan_init already bailed out without
       * touching build->list_scan_id, so it's safe to hand this context
       * straight to Grace (nbatch recomputed from the REAL build size)
       * instead of accepting the old HYBRID/HASH_FILE degrade. context is
       * always &manager->single_context here (this is the only live caller
       * of hjoin_execute_internal), matching hjoin_execute_grace's contract;
       * worker_id 0 -- serial, never concurrent. */
      return hjoin_execute_grace (thread_p, manager, context, 0);
    }

  build = context->build;
  probe = context->probe;
  assert (build != NULL);
  assert (probe != NULL);

  error = qfile_open_list_scan (build->list_id, &build->list_scan_id);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  error = hjoin_build (thread_p, manager, context);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  error = qfile_open_list_scan (probe->list_id, &probe->list_scan_id);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  error = hjoin_probe (thread_p, manager, context);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  ASSERT_NO_ERROR_OR_INTERRUPTED ();

cleanup:
  qfile_close_scan (thread_p, &build->list_scan_id);
  qfile_close_scan (thread_p, &probe->list_scan_id);

  hjoin_destroy_qlist (thread_p, context);

  hjoin_scan_clear (thread_p, &context->hash_scan);

  return error;

error_exit:
  if (error == NO_ERROR || er_errid () == NO_ERROR)
    {
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
    }

  goto cleanup;
}

/*
 * hjoin_init_manager() -
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry.
 *   manager(in/out): Hash join manager to initialize.
 *   xasl(in): XASL node for hash join execution.
 *   query_id(in): Query identifier.
 *   val_descr(in): Value descriptor for positional values.
 */
/*
 * hjoin_outer_stream_push_eligible () - issue #149 P3: can this join's
 *   plan-time-detached outer actually be push-streamed at runtime?
 *   (see the header for the full contract)
 */
bool
hjoin_outer_stream_push_eligible (const XASL_NODE * hashjoin_xasl)
{
  const XASL_NODE *outer;
  const BUILDLIST_PROC_NODE *bl;

  if (hashjoin_xasl == NULL || hashjoin_xasl->type != HASHJOIN_PROC)
    {
      return false;
    }

  outer = hashjoin_xasl->proc.hashjoin.outer.xasl;
  if (outer == NULL || !XASL_IS_FLAGED (outer, XASL_HASHJOIN_OUTER_STREAMED))
    {
      return false;
    }

  /* probe side must be the physical outer: LEFT only (RIGHT swaps the outer
   * into the build side, which needs real materialized sizes; INNER picks
   * sides by comparing real sizes -- both already excluded at plan time,
   * re-checked defensively here). */
  if (hashjoin_xasl->proc.hashjoin.merge_info.join_type != JOIN_LEFT)
    {
      return false;
    }

  /* narrow shape: a plain scan+filter+projection BUILDLIST.  Anything that
   * gives the materialized list its own semantics (sort, group, aggregate,
   * analytic, connect-by, top-n, click-counter update) must keep the
   * materializing fallback path. */
  if (outer->type != BUILDLIST_PROC)
    {
      return false;
    }
  if (outer->orderby_list != NULL || outer->topn_items != NULL || outer->connect_by_ptr != NULL
      || outer->selected_upd_list != NULL || outer->option == Q_DISTINCT)
    {
      return false;
    }
  bl = &outer->proc.buildlist;
  if (bl->groupby_list != NULL || bl->g_agg_list != NULL || bl->g_hash_eligible || bl->a_eval_list != NULL)
    {
      return false;
    }

  return true;
}

static int
hjoin_init_manager (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, XASL_NODE * xasl, QUERY_ID query_id,
		    VAL_DESCR * val_descr, XASL_STATE * xasl_state)
{
  HASHJOIN_PROC_NODE *proc;
  QFILE_LIST_MERGE_INFO *merge_info;
  QFILE_LIST_ID *outer_list_id, *inner_list_id;
  HASHJOIN_DOMAIN_INFO *domain_info;
  HASHJOIN_CONTEXT *context;

  QFILE_TUPLE_VALUE_TYPE_LIST *type_list = NULL;
  int type_cnt, type_index;

  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (xasl != NULL);

  memset (manager, 0, sizeof (HASHJOIN_MANAGER));

  proc = &xasl->proc.hashjoin;

  merge_info = &proc->merge_info;
  assert (merge_info->ls_pos_cnt > 0);
  assert (merge_info->ls_pos_list != NULL);
  manager->merge_info = merge_info;

  manager->outer = &proc->outer;
  manager->inner = &proc->inner;
  assert (manager->outer->xasl != NULL);
  assert (manager->inner->xasl != NULL);

  /* issue #149 P2: a streamed outer (XASL_HASHJOIN_OUTER_STREAMED) was
   * excluded from aptr_list at plan time (see pt_to_hashjoin_proc), but
   * qexec_execute_mainblock_internal's aptr-loop area now runs it manually
   * in the exact spot/context the old aptr loop always ran it in (see the
   * HASHJOIN_PROC branch right before the `for (xptr2 = xptr->aptr_list...)`
   * loop) -- calling qexec_execute_mainblock from *here* instead left a
   * resource-tracker entry unreleased (issue #149 P2 stop-and-report:
   * SIGABRT via cubbase::restrack_assert, net_server_request's end-of-request
   * pop_resource_tracks). By the time hjoin_init_manager runs, outer's
   * list_id is therefore already materialized like any other consumer
   * (hjoin_check_empty_inputs, hjoin_execute_grace,
   * hjoin_grace_select_build_probe, ...) expects -- nothing further to do
   * here. */
  manager->xasl_state = xasl_state;

  /* issue #149 P3: if the detached outer is push-eligible the executor's
   * aptr-area pre-run left it untouched (IS_XASL_INITIAL_STATUS) -- its rows
   * will be pushed straight into the probe phase and its list file never
   * receives a tuple.  Every other detached-outer case (RIGHT, non-narrow
   * shape) was pre-materialized by the executor exactly as before P3. */
  manager->outer_stream_push = (hjoin_outer_stream_push_eligible (xasl)
				&& IS_XASL_INITIAL_STATUS (manager->outer->xasl->status));
  if (XASL_IS_FLAGED (manager->outer->xasl, XASL_HASHJOIN_OUTER_STREAMED) && !manager->outer_stream_push)
    {
      hjoin_debug_outer_fallback_materialize.fetch_add (1, std::memory_order_relaxed);
    }

  outer_list_id = manager->outer->xasl->list_id;
  inner_list_id = manager->inner->xasl->list_id;
  assert (outer_list_id != NULL);
  assert (inner_list_id != NULL);

  /* issue #149 P3: the push-streamed outer never ran, so its list has no
   * type list yet -- but the domain/fetch machinery below needs the outer's
   * tuple types now.  Install them from the outer's own output column list
   * (the same source qexec_start_mainblock_iterations' qfile_open_list uses;
   * when the outer subplan actually runs during the probe phase its own
   * list-open replaces this). */
  if (manager->outer_stream_push && outer_list_id->type_list.type_cnt == 0)
    {
      QFILE_TUPLE_VALUE_TYPE_LIST tmp_type_list = { NULL, 0 };

      if (qdata_get_valptr_type_list (thread_p, manager->outer->xasl->outptr_list, &tmp_type_list) != NO_ERROR)
	{
	  return ER_FAILED;
	}
      /* qdata_get_valptr_type_list allocates domp with db_private_alloc, but
       * qfile_clear_list_id frees a list's domp with free() -- hand the list
       * a malloc'd copy so its own cleanup discipline applies unchanged. */
      if (tmp_type_list.type_cnt > 0)
	{
	  outer_list_id->type_list.domp = (TP_DOMAIN **) malloc (sizeof (TP_DOMAIN *) * tmp_type_list.type_cnt);
	  if (outer_list_id->type_list.domp == NULL)
	    {
	      db_private_free_and_init (thread_p, tmp_type_list.domp);
	      return ER_FAILED;
	    }
	  memcpy (outer_list_id->type_list.domp, tmp_type_list.domp, sizeof (TP_DOMAIN *) * tmp_type_list.type_cnt);
	  outer_list_id->type_list.type_cnt = tmp_type_list.type_cnt;
	}
      if (tmp_type_list.domp != NULL)
	{
	  db_private_free_and_init (thread_p, tmp_type_list.domp);
	}
    }

  /* When aptr_list is executed in qexec_execute_mainblock_internal,
   * it checks the results from outer_xasl and inner_xasl in merge_info.
   * If either has no result, the other is skipped,
   * and the skipped node can have a type count of 0 in list_id.type_list. */
  if ((!manager->outer_stream_push && outer_list_id->type_list.type_cnt == 0)
      || inner_list_id->type_list.type_cnt == 0)
    {
      return NO_ERROR;
    }

  assert (outer_list_id->type_list.domp != NULL);
  assert (inner_list_id->type_list.domp != NULL);

  manager->join_type = merge_info->join_type;
  manager->key_cnt = merge_info->ls_column_cnt;

  manager->during_join_pred = xasl->during_join_pred;
  manager->num_parallel_threads = xasl->parallelism;

  manager->query_id = query_id;
  manager->val_descr = val_descr;

  domain_info = &proc->domain_info;
  error = hjoin_init_domain_info (thread_p, manager, domain_info);
  if (error != NO_ERROR)
    {
      assert_release_error (er_errid () != NO_ERROR);
      return er_errid ();
    }

  /* single_context */
  context = &manager->single_context;

  assert (context->list_id == NULL);

  context->outer.list_id = outer_list_id;
  context->outer.input = &domain_info->outer;
  context->outer.coerce_domains = domain_info->coerce_domains;
  context->outer.need_coerce_domains = domain_info->need_coerce_domains;
  context->outer.regu_list_pred = proc->outer.regu_list_pred;

  context->inner.list_id = inner_list_id;
  context->inner.input = &domain_info->inner;
  context->inner.coerce_domains = domain_info->coerce_domains;
  context->inner.need_coerce_domains = domain_info->need_coerce_domains;
  context->inner.regu_list_pred = proc->inner.regu_list_pred;

  /* Set in hjoin_init_context or hjoin_outer_fill_null_values. */
  assert (context->build == NULL);
  assert (context->probe == NULL);

  context->during_join_pred = manager->during_join_pred;
  context->val_descr = manager->val_descr;

  assert (context->status == HASHJOIN_STATUS_NONE);

  /* contexts */
  assert (manager->contexts == NULL);
  assert (manager->context_cnt == 0);

  /* type_list */
  type_list = &manager->type_list;
  assert (type_list->domp == NULL);
  assert (type_list->type_cnt == 0);

  type_cnt = merge_info->ls_pos_cnt;

  type_list->domp = (TP_DOMAIN **) db_private_alloc (thread_p, type_cnt * sizeof (TP_DOMAIN *));
  if (type_list->domp == NULL)
    {
      assert_release_error (er_errid () != NO_ERROR);
      return er_errid ();
    }

  type_list->type_cnt = type_cnt;

  for (type_index = 0; type_index < type_cnt; type_index++)
    {
      if (merge_info->ls_outer_inner_list[type_index] == QFILE_OUTER_LIST)
	{
	  type_list->domp[type_index] = outer_list_id->type_list.domp[merge_info->ls_pos_list[type_index]];
	}
      else
	{
	  type_list->domp[type_index] = inner_list_id->type_list.domp[merge_info->ls_pos_list[type_index]];
	}
    }

  manager->qlist_merge_method = HASHJOIN_MERGE_CONNECT;
  manager->qlist_flag = QFILE_FLAG_ALL;

  assert (manager->px_worker_manager == NULL);

  /* stats_group */
  if (thread_is_on_trace (thread_p))
    {
      manager->stats_group = &proc->stats_group;
      memset (manager->stats_group, 0, sizeof (HASHJOIN_STATS_GROUP));

      context->stats = &manager->stats_group->stats;
    }
  else
    {
      assert (manager->stats_group == NULL);
      assert (context->stats == NULL);
    }

#if defined (SERVER_MODE) && HASHJOIN_DUMP_HASH_TABLE
  pthread_mutex_init (&manager->dump_hash_table_mutex, NULL);
#endif /* defined (SERVER_MODE) && HASHJOIN_DUMP_HASH_TABLE */

  /* issue #149 P3: the temporary outer type list installed above has served
   * every consumer in this function (domain_info and the merged result
   * type_list both copy the interned TP_DOMAIN pointers, not the array).
   * Revert the outer's list to its virgin never-opened state: anything that
   * treats a non-zero type_cnt as "this list was opened" (aptr skip checks,
   * the per-query qlist open/close balance assert, clear paths) must see the
   * truth -- the outer only really runs during the probe phase. */
  if (manager->outer_stream_push && outer_list_id->type_list.domp != NULL)
    {
      free (outer_list_id->type_list.domp);
      outer_list_id->type_list.domp = NULL;
      outer_list_id->type_list.type_cnt = 0;
    }

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;
}

/*
 * hjoin_clear_manager() -
 *   return: None.
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager to clear.
 */
static void
hjoin_clear_manager (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager)
{
  HASHJOIN_CONTEXT *single_context;
  HASHJOIN_CONTEXT *contexts = NULL, *current_context;
  UINT32 context_cnt, context_index;

  assert (thread_p != NULL);
  assert (manager != NULL);

  single_context = &manager->single_context;

  if (single_context->list_id != NULL)
    {
      qfile_close_list (thread_p, single_context->list_id);
      qfile_destroy_list (thread_p, single_context->list_id);
      QFILE_FREE_AND_INIT_LIST_ID (single_context->list_id);
    }

  hjoin_destroy_qlist (thread_p, single_context);

  if (manager->contexts != NULL)
    {
      contexts = manager->contexts;
      context_cnt = manager->context_cnt;
      assert (context_cnt > 1);

      for (context_index = 0; context_index < context_cnt; context_index++)
	{
	  current_context = &contexts[context_index];

#if defined (SERVER_MODE)
	  if (current_context->status == HASHJOIN_STATUS_PARALLEL_PROBE)
	    {
	      parallel_query::hash_join::clear_context (*thread_p, current_context);
	    }
	  else
#endif /* defined (SERVER_MODE) */
	    {
	      hjoin_clear_context (thread_p, current_context);
	    }
	}

      db_private_free_and_init (thread_p, contexts);

      manager->contexts = NULL;
      manager->context_cnt = 0;
    }
  else
    {
      assert (manager->context_cnt == 0);
    }

  if (manager->type_list.domp != NULL)
    {
      db_private_free_and_init (thread_p, manager->type_list.domp);
    }

#if defined (SERVER_MODE)
  if (manager->px_worker_manager != NULL)
    {
      manager->px_worker_manager->release_workers ();
      manager->px_worker_manager = NULL;
    }

  if (manager->px_worker_stats != NULL)
    {
      db_private_free_and_init (thread_p, manager->px_worker_stats);
    }

  THREAD_ENTRY *main_thread_p = thread_get_main_thread (thread_p);

  /* only top-level parent */
  if (main_thread_p == thread_p)
    {
      if (thread_p->m_px_stats != NULL && !thread_p->m_uses_px_stats)
	{
	  perfmon_merge_parallel_stats_to_tran_stats (thread_p);
	  free_and_init (thread_p->m_px_stats);
	}
    }
#else
  assert (manager->px_worker_manager == NULL);
  assert (manager->px_worker_stats == NULL);
  assert (thread_p->m_px_stats == NULL);
#endif /* defined (SERVER_MODE) */

#if defined (SERVER_MODE) && HASHJOIN_DUMP_HASH_TABLE
  pthread_mutex_destroy (&manager->dump_hash_table_mutex);
#endif /* defined (SERVER_MODE) && HASHJOIN_DUMP_HASH_TABLE */
}

/*
 * hjoin_init_domain_info() -
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   domain_info(in/out): Domain information for join columns.
 */
static int
hjoin_init_domain_info (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_DOMAIN_INFO * domain_info)
{
  QFILE_LIST_MERGE_INFO *merge_info;
  QFILE_LIST_ID *outer_list_id, *inner_list_id;

  TP_DOMAIN **outer_domains, **inner_domains, **coerce_domains;
  int *outer_value_indexes, *inner_value_indexes;
  int outer_value_index, inner_value_index;
  int domain_cnt, domain_index;
  bool need_coerce_domains;

  DB_TYPE outer_type, inner_type, common_type;
  int outer_precision, inner_precision;
  int outer_scale, inner_scale;
  int outer_integral, inner_integral;
  int common_precision, common_scale;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (domain_info != NULL);

  merge_info = manager->merge_info;
  assert (merge_info != NULL);

  assert (manager->outer != NULL);
  assert (manager->outer->xasl != NULL);
  assert (manager->outer->xasl->list_id != NULL);
  outer_list_id = manager->outer->xasl->list_id;

  assert (manager->inner != NULL);
  assert (manager->inner->xasl != NULL);
  assert (manager->inner->xasl->list_id != NULL);
  inner_list_id = manager->inner->xasl->list_id;

  /* domain_info */
  domain_cnt = merge_info->ls_column_cnt;

  outer_domains = domain_info->outer.domains;
  outer_value_indexes = domain_info->outer.value_indexes;
  assert (outer_domains != NULL);
  assert (outer_value_indexes != NULL);

  inner_domains = domain_info->inner.domains;
  inner_value_indexes = domain_info->inner.value_indexes;
  assert (inner_domains != NULL);
  assert (inner_value_indexes != NULL);

  coerce_domains = domain_info->coerce_domains;
  need_coerce_domains = domain_info->need_coerce_domains = false;

  memset (coerce_domains, 0, domain_cnt * sizeof (TP_DOMAIN *));

  /* This code references tp_infer_common_domain but reduces unnecessary calls to tp_domain_new. */
  for (domain_index = 0; domain_index < domain_cnt; domain_index++)
    {
      outer_value_index = outer_value_indexes[domain_index];
      inner_value_index = inner_value_indexes[domain_index];

      outer_domains[domain_index] = outer_list_id->type_list.domp[outer_value_index];
      inner_domains[domain_index] = inner_list_id->type_list.domp[inner_value_index];
      assert (outer_domains[domain_index] != NULL);
      assert (inner_domains[domain_index] != NULL);

      outer_type = TP_DOMAIN_TYPE (outer_domains[domain_index]);
      inner_type = TP_DOMAIN_TYPE (inner_domains[domain_index]);

      /* common_type */
      if (outer_type == inner_type)
	{
	  common_type = outer_type;
	}
      else
	{
	  need_coerce_domains = true;

	  if (outer_type == DB_TYPE_NULL)
	    {
	      assert (false);
	      coerce_domains[domain_index] = inner_domains[domain_index];
	      continue;
	    }
	  else if (inner_type == DB_TYPE_NULL)
	    {
	      assert (false);
	      coerce_domains[domain_index] = outer_domains[domain_index];
	      continue;
	    }
	  else
	    {
	      common_type = (tp_more_general_type (outer_type, inner_type) > 0) ? outer_type : inner_type;
	    }
	}

      /* common_precision, common_scale */
      outer_precision = outer_domains[domain_index]->precision;
      outer_scale = outer_domains[domain_index]->scale;

      inner_precision = inner_domains[domain_index]->precision;
      inner_scale = inner_domains[domain_index]->scale;

      if (outer_precision == inner_precision && outer_scale == inner_scale)
	{
	  common_precision = inner_precision;
	  common_scale = inner_scale;
	}
      else
	{
	  need_coerce_domains = true;

	  if (outer_precision == TP_FLOATING_PRECISION_VALUE || inner_precision == TP_FLOATING_PRECISION_VALUE)
	    {
	      common_precision = TP_FLOATING_PRECISION_VALUE;
	      common_scale = 0;
	    }
	  else if (common_type == DB_TYPE_NUMERIC)
	    {
	      if (outer_precision == DB_DEFAULT_NUMERIC_PRECISION || inner_precision == DB_DEFAULT_NUMERIC_PRECISION)
		{
		  common_precision = DB_DEFAULT_NUMERIC_PRECISION;
		  common_scale = DB_DEFAULT_NUMERIC_SCALE;
		}
	      else
		{
		  common_scale = MAX (outer_scale, inner_scale);

		  outer_integral = outer_precision - outer_scale;
		  inner_integral = inner_precision - inner_scale;

		  common_precision = MAX (outer_integral, inner_integral) + common_scale;
		  common_precision = MIN (common_precision, DB_MAX_FIXED_NUMERIC_PRECISION);
		}
	    }
	  else
	    {
	      common_precision = MAX (outer_precision, inner_precision);
	      common_scale = 0;
	    }
	}

      /* need_coerce_domains, coerce_domains */
      if (need_coerce_domains)
	{
	  if (common_type == outer_type && common_precision == outer_precision && common_scale == outer_scale)
	    {
	      coerce_domains[domain_index] = outer_domains[domain_index];
	    }
	  else if (common_type == inner_type && common_precision == inner_precision && common_scale == inner_scale)
	    {
	      coerce_domains[domain_index] = inner_domains[domain_index];
	    }
	  else
	    {
	      coerce_domains[domain_index] =
		tp_domain_copy ((common_type == outer_type) ? outer_domains[domain_index] : inner_domains[domain_index],
				false);
	      if (coerce_domains[domain_index] == NULL)
		{
		  assert_release_error (er_errid () != NO_ERROR);
		  return er_errid ();
		}

	      coerce_domains[domain_index]->precision = common_precision;
	      coerce_domains[domain_index]->scale = common_scale;

	      coerce_domains[domain_index] = tp_domain_cache (coerce_domains[domain_index]);
	    }
	}
    }				/* for (domain_index < domain_cnt) */

#if !defined (NDEBUG)
  if (!need_coerce_domains)
    {
      for (domain_index = 0; domain_index < domain_cnt; domain_index++)
	{
	  assert (coerce_domains[domain_index] == NULL);
	}
    }
#endif /* !NDEBUG */

  /* If join predicates compare different types, need_coerce_domains is set to true;
   * otherwise, it is false.
   * 
   * When need_coerce_domains is true, coerce_domains is set to inner_domains,
   * outer_domains, or a common domain for comparison.
   *
   * If either inner_domains or outer_domains matches coerce_domains, 
   * no coercion is needed. Otherwise, values are coerced to the common domain. */
  domain_info->need_coerce_domains = need_coerce_domains;

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;
}

/*
 * hjoin_try_partition() -
 *   return: One of the following HASHJOIN_STATUS values:
 *           - HASHJOIN_STATUS_SINGLE: Partitioning is not needed or falls back on error.
 *           - HASHJOIN_STATUS_PARTITION: Partitioning is applied.
 *           - HASHJOIN_STATUS_PARALLEL: Parallel execution is applied.
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   single_context(in): Hash join context for single-threaded execution.
 */
static HASHJOIN_STATUS
hjoin_try_partition (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * single_context)
{
  HASHJOIN_STATUS status;
  HASHJOIN_SPLIT_INFO split_info;
  UINT32 context_index;

  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (single_context != NULL);
  assert (single_context == &manager->single_context);

  /* issue #149 P3: push-stream mode always runs the Grace state machine on
   * the single context -- the old #123 partition-count formula reads the
   * outer's (not yet existing) tuple count, and the plain SINGLE path scans
   * the outer's (never populated) list file.  Grace with nbatch computed
   * from the materialized build (inner) side handles every case, nbatch==1
   * included (batch 0 only, no batch files -- same work as SINGLE). */
  if (manager->outer_stream_push)
    {
      error = hjoin_execute_grace (thread_p, manager, single_context, 0 /* worker_id: serial */ );
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
      return HASHJOIN_STATUS_GRACE_DONE;
    }

  status = hjoin_check_partition (thread_p, manager, single_context);
  if (status == HASHJOIN_STATUS_SINGLE)
    {
      return status;
    }

  assert (status == HASHJOIN_STATUS_PARTITION);

  error = hjoin_prepare_partition (thread_p, manager, &split_info);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

#if defined (SERVER_MODE)
  status = hjoin_try_parallel (thread_p, manager, single_context);
  single_context->status = status;
#endif /* defined (SERVER_MODE) */

  switch (status)
    {
    case HASHJOIN_STATUS_PARTITION:
      if (thread_is_on_trace (thread_p))
	{
	  assert (single_context->stats != NULL);
	  assert (single_context->stats->num_parallel_threads == 0);
	}

      /* issue #147 T1 S2: serial (non-px) fallback -- Grace batch state
       * machine replaces the old hjoin_build_partitions + hjoin_execute_partitions
       * pair. Grace runs build+probe to completion itself (single_context->list_id
       * is the final result), so the status is switched to GRACE_DONE below
       * instead of staying PARTITION (which would otherwise make qexec_hash_join
       * call hjoin_execute_partitions again over the never-populated
       * manager->contexts[]). split_info (from hjoin_prepare_partition above,
       * needed by px's build_partitions) is unused here -- Grace computes its
       * own nbatch/routing independently (D4, hash_mem-based) rather than
       * reusing the #123-formula-derived partition count. */
      error = hjoin_execute_grace (thread_p, manager, single_context, 0 /* worker_id: serial, never concurrent */ );
      if (error == NO_ERROR)
	{
	  status = HASHJOIN_STATUS_GRACE_DONE;
	}
      break;

#if defined (SERVER_MODE)
    case HASHJOIN_STATUS_PARALLEL:
      if (thread_is_on_trace (thread_p))
	{
	  assert (single_context->stats != NULL);
	  single_context->stats->num_parallel_threads = manager->num_parallel_threads;
	}

      // *INDENT-OFF*
      error = parallel_query::hash_join::build_partitions (*thread_p, manager, &split_info);
      // *INDENT-ON*
      break;
#endif /* defined (SERVER_MODE) */

    case HASHJOIN_STATUS_ERROR:
      goto error_exit;

    default:
      /* impossible case */
      assert_release_error (false);
      goto error_exit;
    }

  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  /* call hjoin_destroy_qlist after build_partitions is done,
   * since HASHJOIN_STATUS_SINGLE may retry on error*/
  hjoin_destroy_qlist (thread_p, single_context);

  assert (status == HASHJOIN_STATUS_GRACE_DONE || status == HASHJOIN_STATUS_PARALLEL);

  ASSERT_NO_ERROR_OR_INTERRUPTED ();

cleanup:
  hjoin_clear_split_info (thread_p, manager, &split_info, false);

  return status;

error_exit:
#if defined (SERVER_MODE)
  if (manager->px_worker_manager != NULL)
    {
      manager->px_worker_manager->release_workers ();
      manager->px_worker_manager = NULL;
    }
#else
  assert (manager->px_worker_manager == NULL);
#endif /* defined (SERVER_MODE) */

  hjoin_clear_split_info (thread_p, manager, &split_info, true);

  if (manager->contexts != NULL)
    {
      assert (manager->context_cnt > 1);

      for (context_index = 0; context_index < manager->context_cnt; context_index++)
	{
	  hjoin_clear_context (thread_p, &manager->contexts[context_index]);
	}

      db_private_free_and_init (thread_p, manager->contexts);
      manager->context_cnt = 0;
    }

  if (thread_is_on_trace (thread_p))
    {
      assert (manager->stats_group != NULL);

      if (manager->stats_group->context_stats != NULL)
	{
	  free_and_init (manager->stats_group->context_stats);
	}
      manager->stats_group->context_cnt = 0;
    }
  else
    {
      assert (manager->stats_group == NULL);
    }

  if (error == ER_INTERRUPTED || er_errid () == ER_INTERRUPTED)
    {
      status = HASHJOIN_STATUS_ERROR;
    }
  else
    {
      /* fallback to HASHJOIN_STATUS_SINGLE */
      er_clear ();
      status = HASHJOIN_STATUS_SINGLE;
    }

  goto cleanup;
}

/* Bounded halving, mirrors HLS_SPILL_MAX_NBATCH's role for the batch-spill
 * hash (query_hash_scan.c) -- an escape hatch against a pathological
 * build_bytes/hash_mem ratio spinning nbatch up without bound. */
static const int HJOIN_GRACE_MAX_NBATCH = 512;

/*
 * hjoin_grace_bucket_bytes_estimate () - rough per-entry bucket-array
 *   overhead for an in-memory hash table sized to hold `tuple_cnt` entries
 *   (mht_create_hls's bucket vector, one pointer-ish slot per bucket at
 *   ~0.7 load factor -- see memory_hash.c). An approximation: the halving
 *   loop below is the same tolerance mechanism PG's own estimate-then-halve
 *   sizing relies on, so a rough constant-per-tuple estimate is sufficient.
 *   (File-scope, not the anonymous namespace with the rest of the Grace
 *   helpers, so hjoin_check_partition -- defined earlier in the file -- can
 *   call it; see D-S2-1.)
 */
static UINT64
hjoin_grace_bucket_bytes_estimate (INT64 tuple_cnt)
{
  if (tuple_cnt <= 0)
    {
      return 0;
    }
  return (UINT64) tuple_cnt * sizeof (void *);
}

/*
 * hjoin_grace_choose_nbatch () - issue #147 D4: precompute nbatch (power of
 *   two) so each batch's build footprint fits hash_mem (session work_mem *
 *   hash_mem_multiplier, #146 S0 accessor -- op_limit_bytes(hash) is first
 *   consumed here). Uses the build side's REAL materialized size
 *   (page_cnt/tuple_cnt), not an estimate; halving loop bounded by
 *   HJOIN_GRACE_MAX_NBATCH (PG ExecChooseHashTableSize equivalent).
 */
static int
hjoin_grace_choose_nbatch (INT64 build_tuple_cnt, INT64 build_page_cnt)
{
  if (build_tuple_cnt <= 0 || build_page_cnt <= 0)
    {
      return 1;
    }

  const UINT64 hash_mem = (UINT64) temp_page_store::op_limit_bytes (temp_page_store::op_workmem_kind::hash);
  const UINT64 build_bytes = (UINT64) build_page_cnt * DB_PAGESIZE;

  int nbatch = 1;
  while (nbatch < HJOIN_GRACE_MAX_NBATCH)
    {
      const UINT64 per_batch_tuples = (UINT64) build_tuple_cnt / (UINT64) nbatch;
      const UINT64 per_batch_bytes = build_bytes / (UINT64) nbatch;
      const UINT64 bucket_bytes = hjoin_grace_bucket_bytes_estimate ((INT64) per_batch_tuples);

      if (per_batch_bytes + bucket_bytes <= hash_mem)
	{
	  break;
	}
      nbatch <<= 1;
    }
  return nbatch;
}

/*
 * hjoin_grace_build_side_size () - read-only lookup of which side
 *   hjoin_check_partition's Grace-nbatch gate (D-S2-1) should size against:
 *   mirrors hjoin_init_context/hjoin_grace_select_build_probe's build-side
 *   pick (JOIN_INNER: smaller side; JOIN_LEFT: inner; JOIN_RIGHT: outer)
 *   without mutating context (build/probe assignment proper happens later,
 *   once execution actually starts down the SINGLE or Grace path).
 */
static void
hjoin_grace_build_side_size (const HASHJOIN_MANAGER * manager, const HASHJOIN_CONTEXT * single_context,
			     INT64 * tuple_cnt_out, INT64 * page_cnt_out)
{
  const QFILE_LIST_ID *outer_list_id = single_context->outer.list_id;
  const QFILE_LIST_ID *inner_list_id = single_context->inner.list_id;

  switch (manager->join_type)
    {
    case JOIN_INNER:
      if (outer_list_id->tuple_cnt < inner_list_id->tuple_cnt
	  || (outer_list_id->tuple_cnt == inner_list_id->tuple_cnt
	      && outer_list_id->page_cnt < inner_list_id->page_cnt))
	{
	  *tuple_cnt_out = outer_list_id->tuple_cnt;
	  *page_cnt_out = outer_list_id->page_cnt;
	}
      else
	{
	  *tuple_cnt_out = inner_list_id->tuple_cnt;
	  *page_cnt_out = inner_list_id->page_cnt;
	}
      break;

    case JOIN_LEFT:
      *tuple_cnt_out = inner_list_id->tuple_cnt;
      *page_cnt_out = inner_list_id->page_cnt;
      break;

    case JOIN_RIGHT:
      *tuple_cnt_out = outer_list_id->tuple_cnt;
      *page_cnt_out = outer_list_id->page_cnt;
      break;

    default:
      /* impossible case */
      assert_release_error (false);
      *tuple_cnt_out = 0;
      *page_cnt_out = 0;
      break;
    }
}

/*
 * hjoin_check_partition() -
 *   return: One of the following HASHJOIN_STATUS values:
 *           - HASHJOIN_STATUS_SINGLE: Partitioning is not needed.
 *           - HASHJOIN_STATUS_PARTITION: Partitioning is applied.
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   single_context(in): Hash join context for single-threaded execution.
 *
 * issue #147 T1 S2 D-S2-1 correction: the #123 entry-metadata formula below
 * stays (px's hjoin_prepare_partition/hjoin_split_qlist NULL-partition scheme
 * still consumes its part_cnt as ITS OWN partition-count parameter), but it
 * no longer gates SINGLE vs PARTITION alone. That formula sizes off
 * fixed-width index entries (HENTRY_HLS + SIMPLE_POS), so a small-entry-count/
 * wide-tuple build set could slip through as SINGLE while its actual bytes
 * exceed work_mem -- hjoin_scan_init would then pick HYBRID/HASH_FILE for the
 * plain SINGLE path, and the probe-random-read pathology Grace exists to
 * eliminate would reappear. The gate is now `part_cnt > 1 || grace_nbatch > 1`
 * (either formula can trigger PARTITION); part_cnt is clamped up to at least
 * grace_nbatch so px, if it does engage, still gets a fine-enough partition
 * count. The residual mismatch direction (#123 says PARTITION, Grace says
 * nbatch == 1) is safe: hjoin_execute_grace recomputes its own nbatch
 * independently and simply degenerates to a single in-memory batch.
 */
static HASHJOIN_STATUS
hjoin_check_partition (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * single_context)
{
  QFILE_LIST_ID *outer_list_id, *inner_list_id;

  UINT64 mem_limit;
  INT64 min_tuple_cnt;
  UINT32 part_cnt;
  INT64 build_tuple_cnt, build_page_cnt;
  int grace_nbatch;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (single_context != NULL);
  assert (single_context == &manager->single_context);

  outer_list_id = single_context->outer.list_id;
  inner_list_id = single_context->inner.list_id;
  assert (outer_list_id != NULL);
  assert (inner_list_id != NULL);

  mem_limit = prm_get_bigint_value (PRM_ID_WORK_MEM);
  assert (mem_limit > 0);

  min_tuple_cnt =
    (outer_list_id->tuple_cnt < inner_list_id->tuple_cnt) ? outer_list_id->tuple_cnt : inner_list_id->tuple_cnt;
  assert (min_tuple_cnt >= 0);

  part_cnt =
    CEIL_PTVDIV ((sizeof (HENTRY_HLS) + sizeof (QFILE_TUPLE_SIMPLE_POS)) * min_tuple_cnt,
		 mem_limit * PARTITION_FILL_FACTOR);

  hjoin_grace_build_side_size (manager, single_context, &build_tuple_cnt, &build_page_cnt);
  grace_nbatch = hjoin_grace_choose_nbatch (build_tuple_cnt, build_page_cnt);
  if (part_cnt < (UINT32) grace_nbatch)
    {
      part_cnt = (UINT32) grace_nbatch;
    }

#if defined (SERVER_MODE)
  /* issue #149 (materialize-preserving concurrency recovery, part 2): a
   * SINGLE verdict here means "try the in-memory build" -- but if the
   * layer-2 accountant cap cannot possibly grant that build,
   * hjoin_scan_init's reserve gets rejected at runtime and S5-lite degrades
   * to *serial* Grace, serializing the whole join on a px-eligible plan
   * (observed: wmmid @ db=256M/wm=256M, cap=128M < ~150M build -> 1.4x).
   * Predict the rejection up front and partition instead, so the px pool
   * stays engaged; partitions small enough to fit are joined IN_MEM by the
   * workers exactly as before. */
  if (part_cnt <= 1 && grace_nbatch <= 1 && manager->num_parallel_threads > 1)
    {
      UINT64 build_bytes = (UINT64) build_page_cnt * DB_PAGESIZE;
      /* in-memory footprint expands ~1.8x over the raw list bytes (hash
       * entries + value arena; measured on the campaign fixtures: 174M list
       * -> ~320M footprint, 82M -> ~150M).  Predict a runtime cap rejection
       * only when that footprint genuinely cannot fit -- an over-eager
       * prediction here demotes joins whose in-memory build was optimal. */
      UINT64 est_footprint = build_bytes + (build_bytes * 4) / 5;

      /* threshold cap/2, not cap: (a) measured, the partitioned px build
       * (parallel build + parallel probe) already beats the SINGLE path's
       * serial build + parallel probe once the build is this size (wmmid
       * 2.4M: 4.4s partitioned vs 6.9s single); (b) a near-cap in-memory
       * build monopolizes the layer-2 budget every other operator in the
       * query shares.  Genuinely small builds keep PARALLEL_PROBE. */
      if (est_footprint > (UINT64) temp_page_store::cap_bytes () / 2)
	{
	  part_cnt = 2;		/* enter the PARTITION branch; the floor below widens it */
	}
    }

  /* issue #149 (materialize-preserving concurrency recovery): the px execute
   * phase reserves MIN(parallel hint, part_cnt) workers and each partition is
   * then joined serially by one worker -- a large work_mem shrinks part_cnt
   * to 2..3 and starves the pool (observed on the same data: work_mem
   * 64M -> 256M drops HASHJOIN from 8 workers/2.5s to 3 workers/4.0s purely
   * through this cap).  Floor the partition count at twice the parallel hint
   * so every worker gets pull-work and stays balanced; the bytes routed are
   * identical, only the partition-file granularity changes.  Serial Grace is
   * unaffected (it ignores part_cnt and computes its own nbatch); tiny
   * inputs are exempt (>= 1024 rows per floored partition required). */
  if (part_cnt > 1 && manager->num_parallel_threads > 1)
    {
      UINT32 floor_cnt = (UINT32) (2 * manager->num_parallel_threads);

      if (part_cnt < floor_cnt && min_tuple_cnt >= (INT64) floor_cnt * 1024)
	{
	  part_cnt = floor_cnt;
	}
    }
#endif /* defined (SERVER_MODE) */

  if (part_cnt > 1 || grace_nbatch > 1)
    {
      if (IS_OUTER_JOIN_TYPE (manager->join_type))
	{
	  /* In outer joins, tuples with NULL in any join column are placed in the last partition.
	   * HASHJOIN_STATUS_FILL_NULL_VALUES is triggered for all tuples in this partition.
	   * (px-only scheme -- Grace's own NULL handling is need_skip_next-based, no
	   * dedicated partition; see hjoin_execute_grace.) */
	  part_cnt += 1;
	}

      manager->context_cnt = part_cnt;

      return HASHJOIN_STATUS_PARTITION;
    }
  else
    {
      assert (manager->context_cnt == 0);

      return HASHJOIN_STATUS_SINGLE;
    }
}

/* ==================================================================== */
/* Hash Join Grace (issue #147 T1 S2 -- PG-serial-equivalent Grace       */
/* rebuild). Serial-only: px (PARALLEL / PARALLEL_PROBE) is untouched    */
/* and keeps using hjoin_check_partition / hjoin_prepare_partition /     */
/* hjoin_split_qlist / hjoin_build_partitions exactly as before -- those */
/* functions are NOT modified by this slice.  This block replaces only  */
/* the non-px serial fallback (hjoin_try_partition's HASHJOIN_STATUS_    */
/* PARTITION branch, see the call site below).                          */
/*                                                                       */
/* Design (see issue #147 D2/D4/D5, S2 slice-plan comment):              */
/*   - nbatch is precomputed once from the REAL materialized build size */
/*     (page_cnt/tuple_cnt), sized so every batch's build set fits      */
/*     hash_mem (#146 accessor) -- so every batch always selects the    */
/*     existing HASH_METH_IN_MEM tier (mht + #144 P3 D2 arena), and      */
/*     HYBRID/HASH_FILE (qfile_jump_scan_tuple_position's random reads) */
/*     are simply never reached from this path.  No new "avoid random   */
/*     read" code is needed -- IN_MEM already has none.                 */
/*   - Routing is bit-split: bucket bits (low) stay whatever mht's own   */
/*     internal hashing already uses; batch bits are the hash's high    */
/*     bits, taken via rotate_right so they don't correlate with the    */
/*     bucket bits.  batch 0 is always in memory and never touches a    */
/*     file (build 1-pass + probe 1-pass, forward-only: only nbatch-1   */
/*     files, not nbatch, ever get created).                            */
/*   - NULL-keyed tuples: no dedicated NULL batch/side-store. The        */
/*     existing hjoin_fetch_key "need_skip_next" signal already gives    */
/*     the exact minimal contract needed here: a NULL build key is       */
/*     simply never inserted (no batching decision needed -- it can     */
/*     never equal anything), a NULL probe key is skipped for INNER, or  */
/*     immediately emitted as an outer unmatched row for LEFT/RIGHT      */
/*     (hjoin_outer_probe_fill_empty) -- exactly like the existing       */
/*     single-batch code, reused verbatim.                               */
/*                                                                        */
/*     D-S3-1 (issue #147 T1 S3, investigated/not implemented): the      */
/*     design record calls for a PG v19-style NULL side-store (isolate   */
/*     NULL-keyed tuples into their own hash_mem/16-capped store on      */
/*     first read, batch-routing never sees them, drained once right     */
/*     after batch 0). Investigated and NOT built, because it would add  */
/*     cost for no gain in this engine's shape: a NULL join key can      */
/*     never equal anything under SQL NULL semantics, so its outer-join  */
/*     disposition (INNER: contributes nothing; LEFT/RIGHT: contributes  */
/*     exactly one fill-empty row) is knowable the instant it's fetched, */
/*     independent of which batch is active or whether the build side   */
/*     has even been loaded yet -- there is no future state a side-store */
/*     could let the decision wait for. Immediate emission costs zero    */
/*     extra memory; a side-store would cost up to hash_mem/16 for the   */
/*     same outcome. The one property it could change is output row      */
/*     ORDER (NULL-driven fill-empty rows land in scan order today,      */
/*     would land in a separate post-batch-0 burst under a side-store),  */
/*     and hash-join output order is not a documented guarantee. Cost:   */
/*     an intentional deviation from the design record's PG parity, on   */
/*     record here rather than re-litigated. Escape hatch: build S3      */
/*     properly if a future requirement needs NULL-driven rows in a      */
/*     specific batch-relative position (none identified so far); the    */
/*     gate this slice ran (S2's ③, LEFT/INNER/RIGHT + both-null +       */
/*     unmatched combinations, merge-join parity) already covers this    */
/*     path's correctness exhaustively and needs no new gate for this    */
/*     decision alone.                                                   */
/* ==================================================================== */

namespace
{
  /* number of bits nbatch (a power of two) occupies, e.g. 8 -> 3 */
  int
  hjoin_grace_nbatch_bits (int nbatch)
  {
    int nbits = 0;
    for (int n = nbatch; n > 1; n >>= 1)
      {
	nbits++;
      }
    return nbits;
  }

  UINT32
  hjoin_grace_ror32 (UINT32 v, unsigned int shift)
  {
    shift &= 31;
    return (shift == 0) ? v : (UINT32) ((v >> shift) | (v << (32 - shift)));
  }

  /*
   * hjoin_grace_route () - issue #147 D2: bit-split routing.  batch = the
   *   hash's high bits, brought down via rotate_right so they don't overlap
   *   the low bits mht's own internal bucket hashing already consumes.
   *   nbatch == 1 always routes to batch 0 (no rotation needed).
   */
  int
  hjoin_grace_route (UINT32 hash_key, int nbatch, int nbatch_bits)
  {
    if (nbatch <= 1)
      {
	return 0;
      }
    return (int) (hjoin_grace_ror32 (hash_key, (unsigned int) nbatch_bits) & (UINT32) (nbatch - 1));
  }

  /*
   * hjoin_grace_tde_algo () - resolve the TDE algorithm for Grace batch files
   *   from the join's own real flag (issue #147 fixed confirmation: unlike
   *   hls_spill_new_file's TDE_ALGORITHM_NONE hardcode -- "no user data" is
   *   not true here, batch files carry full tuples), matching the existing
   *   qfile_producer_create_for_list convention.
   */
  TDE_ALGORITHM
  hjoin_grace_tde_algo (const HASHJOIN_MANAGER * manager)
  {
    const bool tde_encrypted =
      XASL_IS_FLAGED (manager->outer->xasl, XASL_INCLUDES_TDE_CLASS)
      || XASL_IS_FLAGED (manager->inner->xasl, XASL_INCLUDES_TDE_CLASS);
    if (!tde_encrypted)
      {
	return TDE_ALGORITHM_NONE;
      }
    return (TDE_ALGORITHM) prm_get_integer_value (PRM_ID_TDE_DEFAULT_ALGORITHM);
  }

  /*
   * hjoin_grace_scan_init_in_mem () - forced HASH_METH_IN_MEM setup for a
   *   Grace batch, sized for `tuple_cnt_hint` entries.  Deliberately separate
   *   from hjoin_scan_init's list_id-driven tier auto-select: a Grace batch's
   *   source is not (always) a QFILE_LIST_ID (reload batches read from a
   *   qfile::batch_record_store), and by construction (nbatch sizing, D4)
   *   every batch must select IN_MEM -- HYBRID/HASH_FILE never apply here.
   */
  int
  hjoin_grace_scan_init_in_mem (THREAD_ENTRY * thread_p, HASH_LIST_SCAN * hash_scan, int key_cnt,
				INT64 tuple_cnt_hint)
  {
    hash_scan->wm_bytes = 0;
    hash_scan->wm_shard = -1;

    hash_scan->temp_key = qdata_alloc_hscan_key (thread_p, key_cnt, true);
    if (hash_scan->temp_key == NULL)
      {
	goto error_exit;
      }

    hash_scan->temp_new_key = qdata_alloc_hscan_key (thread_p, key_cnt, true);
    if (hash_scan->temp_new_key == NULL)
      {
	goto error_exit;
      }

    hash_scan->hash_list_scan_type = HASH_METH_IN_MEM;
    hash_scan->memory.hash_table =
      mht_create_hls ("Hash Join (Grace batch)", (int) MAX (tuple_cnt_hint, 2), NULL, NULL);
    if (hash_scan->memory.hash_table == NULL)
      {
	goto error_exit;
      }
    hash_scan->memory.curr_hash_entry = NULL;
    /* #144 P3 D2 arena; NULL (OOM) falls back to per-entry alloc, same as
     * hjoin_scan_init's IN_MEM branch. */
    hash_scan->memory.value_arena = hscan_value_arena_create ();

    hash_scan->curr_hash_key = 0;
    hash_scan->need_coerce_type = false;

    ASSERT_NO_ERROR_OR_INTERRUPTED ();
    return NO_ERROR;

  error_exit:
    hjoin_scan_clear (thread_p, hash_scan);

    if (er_errid () == NO_ERROR)
      {
	assert_release_error (er_errid () != NO_ERROR);
      }
    return er_errid ();
  }

  /*
   * hjoin_grace_select_build_probe () - mirrors hjoin_init_context's
   *   build/probe assignment (JOIN_INNER smaller-side pick; JOIN_LEFT/RIGHT
   *   fixed, preserved side = probe). Duplicated rather than reused because
   *   hjoin_init_context also unconditionally calls hjoin_scan_init against
   *   the FULL (unsplit) build list_id -- exactly the oversized tier check
   *   Grace exists to avoid.
   */
  int
  hjoin_grace_select_build_probe (HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context)
  {
    HASHJOIN_FETCH_INFO *outer = &context->outer;
    HASHJOIN_FETCH_INFO *inner = &context->inner;

    switch (manager->join_type)
      {
      case JOIN_INNER:
	if (outer->list_id->tuple_cnt < inner->list_id->tuple_cnt
	    || (outer->list_id->tuple_cnt == inner->list_id->tuple_cnt
		&& outer->list_id->page_cnt < inner->list_id->page_cnt))
	  {
	    context->build = outer;
	    context->probe = inner;
	  }
	else
	  {
	    context->build = inner;
	    context->probe = outer;
	  }
	break;

      case JOIN_LEFT:
	outer->fill_record = &outer->tuple_record;
	inner->fill_record = NULL;
	context->build = inner;
	context->probe = outer;
	break;

      case JOIN_RIGHT:
	outer->fill_record = NULL;
	inner->fill_record = &inner->tuple_record;
	context->build = outer;
	context->probe = inner;
	break;

      default:
	/* impossible case */
	assert_release_error (false);
	return er_errid ();
      }

    return NO_ERROR;
  }

  /*
   * hjoin_grace_build_insert () - insert one build tuple (already routed to
   *   the current batch) into the IN_MEM table. `scan_id_source` is any
   *   already-constructed QFILE_LIST_SCAN_ID (never dereferenced -- the
   *   IN_MEM branch of hjoin_build_key ignores it; it exists only to satisfy
   *   that function's non-NULL assert).
   */
  int
  hjoin_grace_build_insert (THREAD_ENTRY * thread_p, HASH_LIST_SCAN * hash_scan, QFILE_LIST_SCAN_ID * scan_id_source,
			    UINT32 hash_key, QFILE_TUPLE tuple)
  {
    QFILE_TUPLE_RECORD rec;
    rec.tpl = tuple;
    rec.size = QFILE_GET_TUPLE_LENGTH (tuple);
    hash_scan->curr_hash_key = hash_key;
    return hjoin_build_key (thread_p, hash_scan, scan_id_source, &rec);
  }

  /*
   * hjoin_grace_probe_match () - probe one already-fetched, already-hashed
   *   probe tuple against the current batch's IN_MEM table: search every
   *   matching hash entry (hjoin_probe_key / hjoin_fetch_key-compare, same as
   *   hjoin_inner_probe/hjoin_outer_probe's do-while), evaluate during-join
   *   predicate if present, merge each qualifying pair into result_list_id,
   *   and -- for outer joins -- emit exactly one unmatched row via the
   *   existing hjoin_outer_probe_fill_empty if nothing matched. Caller has
   *   already: fetched the probe key into hash_scan->temp_key, confirmed it
   *   is non-NULL, computed hash_scan->curr_hash_key, and set
   *   context->probe->tuple_record to the probe tuple.
   */
  int
  hjoin_grace_probe_match (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context,
			   QFILE_LIST_ID * result_list_id, QFILE_TUPLE_RECORD * overflow_record)
  {
    HASHJOIN_FETCH_INFO *outer = &context->outer;
    HASHJOIN_FETCH_INFO *inner = &context->inner;
    HASHJOIN_FETCH_INFO *build = context->build;
    HASHJOIN_FETCH_INFO *probe = context->probe;
    HASH_LIST_SCAN *hash_scan = &context->hash_scan;
    HASH_SCAN_KEY *key = hash_scan->temp_key;
    HASH_SCAN_KEY *found_key = hash_scan->temp_new_key;
    bool need_skip_next = false;
    bool any_record_added = false;
    int error = NO_ERROR;

    build->tuple_record.tpl = NULL;
    build->tuple_record.size = 0;

    do
      {
	error = hjoin_probe_key (thread_p, hash_scan, &build->list_scan_id, &build->tuple_record);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
	if (build->tuple_record.tpl == NULL)
	  {
	    break;		/* not found */
	  }

	error = hjoin_fetch_key (thread_p, build, &build->tuple_record, found_key, key /* compare_key */ ,
				 &need_skip_next);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
	else if (need_skip_next)
	  {
	    need_skip_next = false;
	    continue;
	  }

	if (context->during_join_pred != NULL)
	  {
	    DB_LOGICAL ev_res;

	    error = fetch_val_list (thread_p, probe->regu_list_pred, context->val_descr, NULL, NULL,
				    probe->tuple_record.tpl, PEEK);
	    if (error != NO_ERROR)
	      {
		return error;
	      }
	    error = fetch_val_list (thread_p, build->regu_list_pred, context->val_descr, NULL, NULL,
				    build->tuple_record.tpl, PEEK);
	    if (error != NO_ERROR)
	      {
		return error;
	      }
	    ev_res = eval_pred (thread_p, context->during_join_pred, context->val_descr, NULL);
	    if (ev_res == V_ERROR)
	      {
		return ER_FAILED;
	      }
	    if (ev_res != V_TRUE)
	      {
		continue;
	      }
	  }

	error = hjoin_merge_tuple_to_list_id (thread_p, result_list_id, &outer->tuple_record, &inner->tuple_record,
					      manager->merge_info, overflow_record);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
	any_record_added = true;
      }
    while (true);

    if (!any_record_added && IS_OUTER_JOIN_TYPE (manager->join_type))
      {
	return hjoin_outer_probe_fill_empty (thread_p, manager, context, result_list_id, overflow_record);
      }
    return NO_ERROR;
  }

  /* One Grace batch's forward-spill files (index 0 is always structurally
   * absent -- batch 0 never touches a file, per D2/S2). */
  struct hjoin_grace_batch_files
  {
    std::vector<qfile::batch_record_store *> inner;
    std::vector<qfile::batch_record_store *> outer;

    explicit hjoin_grace_batch_files (int nbatch)
    {
      /* issue #147 T1 S4: reserve the max upfront so grow()'s resize() below
       * never reallocates -- hjoin_execute_grace's reload loop holds
       * inner_store/outer_store as references into these vectors' elements
       * across a mid-reload grow(), which would otherwise dangle. */
      inner.reserve ((size_t) HJOIN_GRACE_MAX_NBATCH);
      outer.reserve ((size_t) HJOIN_GRACE_MAX_NBATCH);
      inner.resize ((size_t) nbatch, nullptr);
      outer.resize ((size_t) nbatch, nullptr);
    }

    /* issue #147 T1 S4: widen for a mid-build/mid-reload nbatch doubling; new
     * slots start unassigned (NULL). Never reallocates (see the reserve()
     * calls above) -- existing element references stay valid. */
    void
    grow (int new_nbatch)
    {
      assert (new_nbatch <= HJOIN_GRACE_MAX_NBATCH);
      inner.resize ((size_t) new_nbatch, nullptr);
      outer.resize ((size_t) new_nbatch, nullptr);
    }

    ~hjoin_grace_batch_files ()
    {
      for (qfile::batch_record_store * store : inner)
	{
	  delete store;
	}
      for (qfile::batch_record_store * store : outer)
	{
	  delete store;
	}
    }
  };

  /*
   * hjoin_grace_ensure_batch_file () - lazily create the batch file for
   *   (is_outer, batchno) on first spill. `next_seq` is a per-call counter
   *   the caller bumps for every file created (uniqueness within this join's
   *   files); `worker_id` is issue #147 S6's addition -- distinguishes
   *   concurrently-running px workers' batch files (folded into the
   *   underlying buffile's name alongside pid, which px workers share).
   */
  int
  hjoin_grace_ensure_batch_file (THREAD_ENTRY * thread_p, const std::string & dir, TDE_ALGORITHM tde_algo,
				 UINT32 worker_id, std::uint64_t * next_seq, qfile::batch_record_store ** slot)
  {
    if (*slot != NULL)
      {
	return NO_ERROR;
      }
    int os_error = 0;
    *slot = qfile::batch_record_store::create (thread_p, dir.c_str (), (*next_seq)++, worker_id, tde_algo, &os_error);
    if (*slot == NULL)
      {
	qfile::spill_file::set_os_error (os_error);
	assert_release_error (er_errid () != NO_ERROR);
	return er_errid ();
      }
    return NO_ERROR;
  }

  /*
   * hjoin_grace_arena_charge_estimate () - issue #147 T1 S4: bytes the arena
   *   bump-allocates for one build tuple of `tuple_size` (mirrors
   *   qdata_alloc_hscan_value_arena's own math -- 8-byte aligned
   *   sizeof(HASH_SCAN_VALUE) + tuple_size). Used to track the CURRENTLY
   *   in-memory batch's real footprint in real time, as an alternative to
   *   hjoin_grace_choose_nbatch's one-shot upfront estimate (which averages
   *   build_bytes/nbatch over the whole build set and can't see per-tuple
   *   skew or a per-tuple size far from that average).
   */
  UINT64
  hjoin_grace_arena_charge_estimate (int tuple_size)
  {
    UINT64 n = sizeof (HASH_SCAN_VALUE) + (UINT64) tuple_size;
    return (n + 7) & ~((UINT64) 7);
  }

  /*
   * issue #147 T1 S4 (D2 routing invariant this growth scheme depends on):
   * hjoin_grace_route()'s batch bits are ror32(hash, nbatch_bits) & (nbatch-1)
   * -- nbatch_bits is fixed once, from the INITIAL (pre-growth) nbatch, and
   * never recomputed as nbatch itself grows (hjoin_execute_grace never
   * reassigns its local `nbatch_bits` after the first hjoin_grace_choose_nbatch
   * call). That is deliberate: with the rotate amount fixed and only the mask
   * (nbatch-1) widening, doubling nbatch is a clean bit-trie bisection --
   * every key currently routed to some batch X (X < old_nbatch) reroutes,
   * under the wider mask, to EITHER X again or to X + old_nbatch, never to
   * any other (in particular never to an already-finished, already-deleted
   * lower batch). If nbatch_bits were instead recomputed from the grown
   * nbatch (widening the rotate amount along with the mask), the *window* of
   * hash bits sampled would shift too, and growth could silently reroute an
   * in-memory or already-spilled record to an arbitrary unrelated batch --
   * including one already probed and torn down. Do not "fix" nbatch_bits to
   * track nbatch; that would both break this invariant and change existing
   * (non-growth) golden output ordering for every fixture with nbatch > 1.
   */

  /* Context for hjoin_grace_grow_route_func (issue #147 T1 S4). */
  struct hjoin_grace_grow_ctx
  {
    THREAD_ENTRY *thread_p;
    hjoin_grace_batch_files *batches;
    int old_nbatch;		/* mask width before this doubling -- also the eviction threshold */
    int new_nbatch;
    int nbatch_bits;		/* fixed rotate amount, see comment above -- never changes */
    const std::string *dir;
    TDE_ALGORITHM tde_algo;
    UINT32 worker_id;
    std::uint64_t *next_seq;
    UINT64 freed_bytes;
  };

  /*
   * hjoin_grace_grow_route_func () - mht_rehash_out_hls callback: re-route one
   *   currently in-memory entry against the widened mask; entries that still
   *   land under old_nbatch stay (evict=false), everything else is spilled to
   *   its (guaranteed not-yet-visited, per the invariant above) new batch
   *   file and evicted from the table. The evicted value's arena bytes are
   *   NOT reclaimed here (the arena is bump-only, freed en masse at the next
   *   hscan_value_arena_reset / hjoin_scan_clear) -- only its charge is
   *   backed out of the caller's running in-mem byte count.
   */
  int
  hjoin_grace_grow_route_func (unsigned int key, void *data, void *args, bool * evict)
  {
    hjoin_grace_grow_ctx *ctx = (hjoin_grace_grow_ctx *) args;
    HASH_SCAN_VALUE *value = (HASH_SCAN_VALUE *) data;
    QFILE_TUPLE tpl = value->tuple;
    int batchno_new = hjoin_grace_route ((UINT32) key, ctx->new_nbatch, ctx->nbatch_bits);

    if (batchno_new < ctx->old_nbatch)
      {
	*evict = false;
	return NO_ERROR;
      }

    *evict = true;
    int error = hjoin_grace_ensure_batch_file (ctx->thread_p, *ctx->dir, ctx->tde_algo, ctx->worker_id, ctx->next_seq,
						&ctx->batches->inner[batchno_new]);
    if (error == NO_ERROR)
      {
	error = ctx->batches->inner[batchno_new]->append (ctx->thread_p, key, tpl);
      }
    if (error == NO_ERROR)
      {
	ctx->freed_bytes += hjoin_grace_arena_charge_estimate (QFILE_GET_TUPLE_LENGTH (tpl));
      }
    return error;
  }

  /* issue #149 P3: per-push context for a push-streamed outer.  The push
   * callback below executes the exact same per-tuple body as
   * hjoin_execute_grace's probe 1-pass list-scan loop -- KEEP THE TWO IN
   * SYNC (the loop is the source of truth). */
  struct hjoin_grace_push_ctx
  {
    HASHJOIN_MANAGER *manager;
    HASHJOIN_CONTEXT *context;
    HASHJOIN_FETCH_INFO *probe;
    HASH_LIST_SCAN *hash_scan;
    HASH_SCAN_KEY *key;
    QFILE_LIST_ID *result_list_id;
    QFILE_TUPLE_RECORD *overflow_record;
    hjoin_grace_batch_files *batches;
    const std::string *dir;
    TDE_ALGORITHM tde_algo;
    UINT32 worker_id;
    std::uint64_t *next_seq;
    int nbatch;
    int nbatch_bits;
  };

  int
  hjoin_grace_stream_push_fn (THREAD_ENTRY * thread_p, void *arg, QFILE_TUPLE_RECORD * tplrec)
  {
    hjoin_grace_push_ctx *c = (hjoin_grace_push_ctx *) arg;
    bool need_skip_next = false;
    int batchno;
    int error;

    c->probe->tuple_record = *tplrec;

    error = hjoin_fetch_key (thread_p, c->probe, &c->probe->tuple_record, c->key, NULL /* compare_key */ ,
			     &need_skip_next);
    if (error != NO_ERROR)
      {
	return error;
      }
    if (need_skip_next)
      {
	if (IS_OUTER_JOIN_TYPE (c->manager->join_type))
	  {
	    return hjoin_outer_probe_fill_empty (thread_p, c->manager, c->context, c->result_list_id,
						 c->overflow_record);
	  }
	return NO_ERROR;
      }

    c->hash_scan->curr_hash_key = qdata_hash_scan_key (c->key, UINT_MAX, HASH_METH_IN_MEM);
    batchno = hjoin_grace_route (c->hash_scan->curr_hash_key, c->nbatch, c->nbatch_bits);

    if (batchno == 0)
      {
	return hjoin_grace_probe_match (thread_p, c->manager, c->context, c->result_list_id, c->overflow_record);
      }

    error = hjoin_grace_ensure_batch_file (thread_p, *c->dir, c->tde_algo, c->worker_id, c->next_seq,
					   &c->batches->outer[batchno]);
    if (error == NO_ERROR)
      {
	error = c->batches->outer[batchno]->append (thread_p, c->hash_scan->curr_hash_key,
						    c->probe->tuple_record.tpl);
      }
    return error;
  }

  static const int HJOIN_GRACE_EFFECTIVE_CAP_MULT = 8;	/* D3 trade-off guard's own escape hatch,
							 * same order of magnitude as HJOIN_GRACE_MAX_NBATCH's halving bound */

  /*
   * hjoin_grace_maybe_grow () - issue #147 T1 S4 (D3/D4): mid-build/mid-reload
   *   nbatch doubling. Call after every IN_MEM insert with the batch's
   *   really-tracked footprint (`*in_mem_bytes_p` / `*in_mem_tuple_cnt_p`); if
   *   it exceeds `*effective_hash_mem_p` and growth is still enabled, doubles
   *   nbatch in one hash-table walk (hjoin_grace_grow_route_func): entries
   *   that still route under the old mask stay, the rest spill to a fresh
   *   batch file. Safe whether the currently-loaded chunk is the initial
   *   build's batch 0 or a reload's batch X -- see the routing-invariant
   *   comment above hjoin_grace_grow_ctx. The reload loop's own bound
   *   (`batchno < nbatch`) picks up any newly-created higher batches
   *   automatically once nbatch grows.
   *
   *   D3 trade-off guard: doubling nbatch can create up to `old_nbatch` new
   *   batch-file write buffers (BUFFILE_BATCH_PAGES=8 pages each side, see
   *   qfile_buffile.cpp) -- cheap at small nbatch, but as nbatch approaches
   *   HJOIN_GRACE_MAX_NBATCH the aggregate buffer cost can exceed simply
   *   letting this one batch run over its budget. When the estimated buffer
   *   cost would exceed hash_mem itself, this raises `*effective_hash_mem_p`
   *   (capped at HJOIN_GRACE_EFFECTIVE_CAP_MULT * hash_mem) instead of
   *   splitting, and leaves nbatch/the table untouched this round.
   *
   *   Give-up heuristic: if a split happens but frees nothing (nfreed == 0,
   *   e.g. one dominant key repeated ninmemory times -- no bit split can ever
   *   separate identical hash keys) or frees everything (nfreed ==
   *   ninmemory, e.g. this batch's whole content is uniform in the new bit --
   *   no future doubling from here helps either), `*grow_enabled_p` latches
   *   false: subsequent overflow is accepted over-budget (soft-charged to
   *   hjoin_debug_grace_skew_overflow_bytes) rather than retried.
   *
   *   MCV (most-common-value) skew handling -- detecting a single dominant
   *   key ahead of time and routing it to a dedicated structure instead of
   *   relying on this give-up fallback -- is out of scope for this slice
   *   (issue #147 S4 slice plan); the give-up branch below is the hook point
   *   where that logic would plug in.
   */
  int
  hjoin_grace_maybe_grow (THREAD_ENTRY * thread_p, HASH_LIST_SCAN * hash_scan, hjoin_grace_batch_files & batches,
			  int *nbatch_p, int nbatch_bits, const std::string & dir, TDE_ALGORITHM tde_algo,
			  UINT32 worker_id, std::uint64_t * next_seq, UINT64 hash_mem, UINT64 * effective_hash_mem_p,
			  UINT64 * in_mem_bytes_p, INT64 * in_mem_tuple_cnt_p, bool * grow_enabled_p,
			  UINT64 last_insert_charge)
  {
    const UINT64 charge = *in_mem_bytes_p + hjoin_grace_bucket_bytes_estimate (*in_mem_tuple_cnt_p);
    if (charge <= *effective_hash_mem_p)
      {
	return NO_ERROR;
      }

    /* Once over budget, charge just the tuple that was last inserted (not the
     * running total) -- `charge - *effective_hash_mem_p` would double-count
     * everything already over budget on every subsequent insert, making the
     * counter's magnitude meaningless past the first give-up. */
    if (!*grow_enabled_p)
      {
	hjoin_debug_grace_skew_overflow.fetch_add ((long) last_insert_charge, std::memory_order_relaxed);
	return NO_ERROR;
      }

    const int old_nbatch = *nbatch_p;
    const int new_nbatch = old_nbatch * 2;
    if (new_nbatch > HJOIN_GRACE_MAX_NBATCH)
      {
	*grow_enabled_p = false;
	hjoin_debug_grace_skew_overflow.fetch_add ((long) last_insert_charge, std::memory_order_relaxed);
	return NO_ERROR;
      }

    const UINT64 buffer_cost_estimate = (UINT64) old_nbatch * 8 /* BUFFILE_BATCH_PAGES */  * DB_PAGESIZE * 2;
    if (buffer_cost_estimate > hash_mem && *effective_hash_mem_p < (UINT64) HJOIN_GRACE_EFFECTIVE_CAP_MULT * hash_mem)
      {
	*effective_hash_mem_p *= 2;
	return NO_ERROR;
      }

    batches.grow (new_nbatch);

    hjoin_grace_grow_ctx ctx = { thread_p, &batches, old_nbatch, new_nbatch, nbatch_bits, &dir, tde_algo, worker_id,
      next_seq, 0
    };
    unsigned int nevicted = 0;
    int error = mht_rehash_out_hls (hash_scan->memory.hash_table, hjoin_grace_grow_route_func, &ctx, &nevicted);
    if (error != NO_ERROR)
      {
	return error;
      }

    const INT64 ninmemory_before = *in_mem_tuple_cnt_p;
    *nbatch_p = new_nbatch;
    *in_mem_tuple_cnt_p = ninmemory_before - (INT64) nevicted;
    *in_mem_bytes_p -= ctx.freed_bytes;
    hjoin_debug_grace_nbatch_grows.fetch_add (1, std::memory_order_relaxed);

    if (nevicted == 0 || (INT64) nevicted == ninmemory_before)
      {
	*grow_enabled_p = false;
      }

    return NO_ERROR;
  }

  /*
   * hjoin_grace_build_insert_tracked () - hjoin_grace_build_insert wrapper
   *   that also tracks the current in-mem batch's real bytes/tuple count and
   *   invokes hjoin_grace_maybe_grow (issue #147 T1 S4). Used at every site
   *   that inserts into the currently-loaded IN_MEM table (initial build's
   *   batch 0 and the reload loop's inner-read sub-loop).
   */
  int
  hjoin_grace_build_insert_tracked (THREAD_ENTRY * thread_p, HASH_LIST_SCAN * hash_scan,
				    QFILE_LIST_SCAN_ID * scan_id_source, UINT32 hash_key, QFILE_TUPLE tuple,
				    hjoin_grace_batch_files & batches, int *nbatch_p, int nbatch_bits,
				    const std::string & dir, TDE_ALGORITHM tde_algo, UINT32 worker_id,
				    std::uint64_t * next_seq, UINT64 hash_mem, UINT64 * effective_hash_mem_p,
				    UINT64 * in_mem_bytes_p, INT64 * in_mem_tuple_cnt_p, bool * grow_enabled_p)
  {
    int error = hjoin_grace_build_insert (thread_p, hash_scan, scan_id_source, hash_key, tuple);
    if (error != NO_ERROR)
      {
	return error;
      }

    const UINT64 this_tuple_charge = hjoin_grace_arena_charge_estimate (QFILE_GET_TUPLE_LENGTH (tuple));
    *in_mem_tuple_cnt_p += 1;
    *in_mem_bytes_p += this_tuple_charge;

    return hjoin_grace_maybe_grow (thread_p, hash_scan, batches, nbatch_p, nbatch_bits, dir, tde_algo, worker_id,
				   next_seq, hash_mem, effective_hash_mem_p, in_mem_bytes_p, in_mem_tuple_cnt_p,
				   grow_enabled_p, this_tuple_charge);
  }
}				/* anonymous namespace */

/*
 * hjoin_execute_grace () - issue #147 T1 S2/S6: Grace hash join, serial or
 *   per-px-partition. Precomputes nbatch (D4); build 1-pass (batch 0
 *   direct-insert, other batches forward-spilled) then probe 1-pass (batch 0
 *   immediate match, other batches forward-spilled); then reloads batches
 *   1..nbatch-1 one at a time into the same IN_MEM table (mht_clear_hls +
 *   arena reset, no destroy/recreate churn) and probes each against its
 *   outer batch file. nbatch == 1 degenerates to the existing single-batch
 *   path unchanged. `worker_id` must be unique among any callers that may
 *   run concurrently (distinct px workers each processing their own claimed
 *   partition); the serial call site passes 0 (never concurrent).
 */
static int
hjoin_execute_grace (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context,
		     UINT32 worker_id)
{
  HASHJOIN_FETCH_INFO *build, *probe;
  HASH_LIST_SCAN *hash_scan;
  QFILE_LIST_ID *result_list_id = NULL;
  QFILE_TUPLE_RECORD overflow_record = { NULL, 0 };
  SCAN_CODE scan_code;
  HASH_SCAN_KEY *key;
  bool need_skip_next = false;
  bool build_scan_open = false, probe_scan_open = false;
  int nbatch, nbatch_bits;
  int batchno;
  int error = NO_ERROR;
  std::string dir;
  TDE_ALGORITHM tde_algo = TDE_ALGORITHM_NONE;
  std::uint64_t next_seq = 0;
  /* issue #147 T1 S4: mid-build/mid-reload nbatch growth state. hash_mem is
   * fetched once here (same accessor hjoin_grace_choose_nbatch used for the
   * upfront estimate); effective_hash_mem is the D3 trade-off guard's own
   * (possibly raised) threshold. in_mem_bytes/in_mem_tuple_cnt track
   * whatever conceptual batch (0 during build, X during a reload) is
   * currently resident; reset to 0 at each reload iteration's table clear. */
  const UINT64 hash_mem = (UINT64) temp_page_store::op_limit_bytes (temp_page_store::op_workmem_kind::hash);
  UINT64 effective_hash_mem = hash_mem;
  UINT64 in_mem_bytes = 0;
  INT64 in_mem_tuple_cnt = 0;
  bool grace_grow_enabled = true;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (context != NULL);
  assert (context == &manager->single_context
	  || (manager->contexts != NULL && context >= manager->contexts
	      && context < manager->contexts + manager->context_cnt));
  assert (context->list_id == NULL);

  error = hjoin_grace_select_build_probe (manager, context);
  if (error != NO_ERROR)
    {
      return error;
    }

  build = context->build;
  probe = context->probe;
  hash_scan = &context->hash_scan;

  nbatch = hjoin_grace_choose_nbatch (build->list_id->tuple_cnt, build->list_id->page_cnt);
  nbatch_bits = hjoin_grace_nbatch_bits (nbatch);

  error = hjoin_grace_scan_init_in_mem (thread_p, hash_scan, manager->key_cnt,
				       build->list_id->tuple_cnt / MAX (nbatch, 1));
  if (error != NO_ERROR)
    {
      return error;
    }

  key = hash_scan->temp_key;

  hjoin_grace_batch_files batches (nbatch);
  /* issue #147 T1 S4: resolved unconditionally (not just nbatch > 1) -- a
   * mid-build grow can create the first batch file even when the upfront
   * estimate chose nbatch == 1. */
  if (!qfile::buffile::default_scratch_dir (dir))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OUT_OF_TEMP_SPACE, 0);
      error = er_errid ();
      goto error_exit;
    }
  tde_algo = hjoin_grace_tde_algo (manager);
  next_seq = ((std::uint64_t) manager->query_id << 20) ^ 1;

  result_list_id = qfile_open_list (thread_p, &manager->type_list, NULL, manager->query_id, manager->qlist_flag, NULL);
  if (result_list_id == NULL)
    {
      goto error_exit;
    }

  /* ---- build 1-pass: batch 0 direct-insert, other batches forward-spilled (D2) ---- */
  error = qfile_open_list_scan (build->list_id, &build->list_scan_id);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }
  build_scan_open = true;

  build->tuple_record = { NULL, 0 };
  while ((scan_code = qfile_scan_list_next (thread_p, &build->list_scan_id, &build->tuple_record, PEEK)) == S_SUCCESS)
    {
      error = hjoin_fetch_key (thread_p, build, &build->tuple_record, key, NULL /* compare_key */ , &need_skip_next);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
      if (need_skip_next)
	{
	  /* NULL build key: never inserted, never batched (D2/S2 minimal NULL contract). */
	  need_skip_next = false;
	  continue;
	}

      const UINT32 hash_key = qdata_hash_scan_key (key, UINT_MAX, HASH_METH_IN_MEM);
      batchno = hjoin_grace_route (hash_key, nbatch, nbatch_bits);

      if (batchno == 0)
	{
	  error = hjoin_grace_build_insert_tracked (thread_p, hash_scan, &build->list_scan_id, hash_key,
						    build->tuple_record.tpl, batches, &nbatch, nbatch_bits, dir,
						    tde_algo, worker_id, &next_seq, hash_mem, &effective_hash_mem,
						    &in_mem_bytes, &in_mem_tuple_cnt, &grace_grow_enabled);
	}
      else
	{
	  error = hjoin_grace_ensure_batch_file (thread_p, dir, tde_algo, worker_id, &next_seq, &batches.inner[batchno]);
	  if (error == NO_ERROR)
	    {
	      error = batches.inner[batchno]->append (thread_p, hash_key, build->tuple_record.tpl);
	    }
	}
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
    }
  if (scan_code == S_ERROR)
    {
      error = er_errid ();
      goto error_exit;
    }
  qfile_close_scan (thread_p, &build->list_scan_id);
  build_scan_open = false;

  /* ---- probe 1-pass: batch 0 immediate match, other batches forward-spilled (D2) ---- */

  /* issue #149 P3: push-stream mode -- instead of scanning a materialized
   * outer list, run the outer subplan to completion under its normal driver
   * with a sink installed: qexec_end_one_iteration redirects every
   * qualifying outer row into hjoin_grace_stream_push_fn (same per-tuple
   * body as the loop below), so the outer's list file never receives a
   * tuple.  Future-batch rows land in the outer batch stores exactly as in
   * the list path; the reload loop below is shared unchanged. */
  if (manager->outer_stream_push && context == &manager->single_context)
    {
      hjoin_grace_push_ctx pctx;
      HASHJOIN_STREAM_SINK sink;
      HASHJOIN_STREAM_SINK *saved_sink;

      pctx.manager = manager;
      pctx.context = context;
      pctx.probe = probe;
      pctx.hash_scan = hash_scan;
      pctx.key = key;
      pctx.result_list_id = result_list_id;
      pctx.overflow_record = &overflow_record;
      pctx.batches = &batches;
      pctx.dir = &dir;
      pctx.tde_algo = tde_algo;
      pctx.worker_id = worker_id;
      pctx.next_seq = &next_seq;
      pctx.nbatch = nbatch;
      pctx.nbatch_bits = nbatch_bits;

      sink.owner = manager->outer->xasl;
      sink.push_fn = hjoin_grace_stream_push_fn;
      sink.ctx = &pctx;
      sink.tplrec.tpl = NULL;
      sink.tplrec.size = 0;

      assert (manager->xasl_state != NULL);
      saved_sink = manager->xasl_state->stream_sink;
      manager->xasl_state->stream_sink = &sink;
      hjoin_debug_outer_streamed.fetch_add (1, std::memory_order_relaxed);

      error = qexec_execute_mainblock (thread_p, manager->outer->xasl, manager->xasl_state, NULL);

      manager->xasl_state->stream_sink = saved_sink;
      if (sink.tplrec.tpl != NULL)
	{
	  db_private_free_and_init (thread_p, sink.tplrec.tpl);
	}
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
      goto probe_pass_done;
    }

  error = qfile_open_list_scan (probe->list_id, &probe->list_scan_id);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }
  probe_scan_open = true;

  probe->tuple_record = { NULL, 0 };
  while ((scan_code = qfile_scan_list_next (thread_p, &probe->list_scan_id, &probe->tuple_record, PEEK)) == S_SUCCESS)
    {
      error = hjoin_fetch_key (thread_p, probe, &probe->tuple_record, key, NULL /* compare_key */ , &need_skip_next);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
      if (need_skip_next)
	{
	  need_skip_next = false;
	  if (IS_OUTER_JOIN_TYPE (manager->join_type))
	    {
	      error = hjoin_outer_probe_fill_empty (thread_p, manager, context, result_list_id, &overflow_record);
	      if (error != NO_ERROR)
		{
		  goto error_exit;
		}
	    }
	  continue;
	}

      hash_scan->curr_hash_key = qdata_hash_scan_key (key, UINT_MAX, HASH_METH_IN_MEM);
      batchno = hjoin_grace_route (hash_scan->curr_hash_key, nbatch, nbatch_bits);

      if (batchno == 0)
	{
	  /* probe->tuple_record already set by qfile_scan_list_next above */
	  error = hjoin_grace_probe_match (thread_p, manager, context, result_list_id, &overflow_record);
	}
      else
	{
	  error = hjoin_grace_ensure_batch_file (thread_p, dir, tde_algo, worker_id, &next_seq, &batches.outer[batchno]);
	  if (error == NO_ERROR)
	    {
	      error = batches.outer[batchno]->append (thread_p, hash_scan->curr_hash_key, probe->tuple_record.tpl);
	    }
	}
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
    }
  if (scan_code == S_ERROR)
    {
      error = er_errid ();
      goto error_exit;
    }
  qfile_close_scan (thread_p, &probe->list_scan_id);
  probe_scan_open = false;

probe_pass_done:

  /* ---- reload loop: batches 1..nbatch-1 ---- */
  for (batchno = 1; batchno < nbatch; batchno++)
    {
      qfile::batch_record_store *&inner_store = batches.inner[batchno];
      qfile::batch_record_store *&outer_store = batches.outer[batchno];

      if (inner_store == NULL && outer_store == NULL)
	{
	  continue;		/* empty-batch rule 1: neither side ever spilled here */
	}
      if (outer_store == NULL)
	{
	  /* empty-batch rule 3: nothing to probe -- reloading the inner
	   * table would be wasted work (no consumer). */
	  delete inner_store;
	  inner_store = NULL;
	  continue;
	}

      /* reload the inner batch (rule 2: if inner_store is NULL the table
       * simply stays empty -- every probe below then naturally misses,
       * which is exactly the desired outer-join fill-empty / inner-join
       * zero-rows behavior, with no special-case code). */
      mht_clear_hls (hash_scan->memory.hash_table, qdata_noop_free_hscan_entry, NULL);
      if (hash_scan->memory.value_arena != NULL)
	{
	  hscan_value_arena_reset (hash_scan->memory.value_arena);
	}
      /* issue #147 T1 S4: this reload iteration's own in-mem footprint starts fresh. */
      in_mem_bytes = 0;
      in_mem_tuple_cnt = 0;

      if (inner_store != NULL)
	{
	  UINT32 rd_hash;
	  QFILE_TUPLE rd_tuple;
	  bool rd_eof = false;

	  while (true)
	    {
	      error = inner_store->read (thread_p, &rd_hash, &rd_tuple, &rd_eof);
	      if (error != NO_ERROR)
		{
		  goto error_exit;
		}
	      if (rd_eof)
		{
		  break;
		}

	      /* issue #147 T1 S4: a growth triggered while reloading an earlier
	       * batch (nbatch only ever grows) may have moved this record's true
	       * home past `batchno` since it was written -- recheck and
	       * forward-spill instead of inserting if so; guaranteed to land on
	       * a not-yet-visited batch (see the routing-invariant comment above
	       * hjoin_grace_grow_ctx). */
	      {
		const int rd_batchno = hjoin_grace_route (rd_hash, nbatch, nbatch_bits);
		if (rd_batchno != batchno)
		  {
		    error = hjoin_grace_ensure_batch_file (thread_p, dir, tde_algo, worker_id, &next_seq,
							   &batches.inner[rd_batchno]);
		    if (error == NO_ERROR)
		      {
			error = batches.inner[rd_batchno]->append (thread_p, rd_hash, rd_tuple);
		      }
		    if (error != NO_ERROR)
		      {
			goto error_exit;
		      }
		    continue;
		  }
	      }

	      error = hjoin_grace_build_insert_tracked (thread_p, hash_scan, &build->list_scan_id, rd_hash, rd_tuple,
							batches, &nbatch, nbatch_bits, dir, tde_algo, worker_id,
							&next_seq, hash_mem, &effective_hash_mem, &in_mem_bytes,
							&in_mem_tuple_cnt, &grace_grow_enabled);
	      if (error != NO_ERROR)
		{
		  goto error_exit;
		}
	    }
	  /* §4: inner batch file closes immediately once its reload completes. */
	  delete inner_store;
	  inner_store = NULL;
	}

      {
	UINT32 rd_hash;
	QFILE_TUPLE rd_tuple;
	bool rd_eof = false;

	while (true)
	  {
	    error = outer_store->read (thread_p, &rd_hash, &rd_tuple, &rd_eof);
	    if (error != NO_ERROR)
	      {
		goto error_exit;
	      }
	    if (rd_eof)
	      {
		break;
	      }

	    /* issue #147 T1 S4: same staleness concern as the inner-read loop
	     * above -- a growth since this outer record was written (whether
	     * during this same batch's own inner reload just above, or an
	     * earlier batch's) may have moved its true home past `batchno`;
	     * recheck and forward-spill instead of probing now if so. */
	    {
	      const int rd_batchno = hjoin_grace_route (rd_hash, nbatch, nbatch_bits);
	      if (rd_batchno != batchno)
		{
		  error = hjoin_grace_ensure_batch_file (thread_p, dir, tde_algo, worker_id, &next_seq,
							 &batches.outer[rd_batchno]);
		  if (error == NO_ERROR)
		    {
		      error = batches.outer[rd_batchno]->append (thread_p, rd_hash, rd_tuple);
		    }
		  if (error != NO_ERROR)
		    {
		      goto error_exit;
		    }
		  continue;
		}
	    }

	    probe->tuple_record.tpl = rd_tuple;
	    probe->tuple_record.size = QFILE_GET_TUPLE_LENGTH (rd_tuple);

	    /* The hash was already computed once (stored in the batch file,
	     * S1) and is reused as-is, but `key`'s decoded DB_VALUEs are
	     * stale from whatever tuple hjoin_fetch_key last ran on -- they
	     * must be re-fetched from THIS tuple before hjoin_grace_probe_match
	     * uses `key` as the compare_key for the build-side match. A NULL
	     * key can't recur here (this tuple was only spilled after an
	     * earlier NULL check passed), so need_skip_next is not expected. */
	    error = hjoin_fetch_key (thread_p, probe, &probe->tuple_record, key, NULL /* compare_key */ ,
				     &need_skip_next);
	    if (error != NO_ERROR)
	      {
		goto error_exit;
	      }
	    assert (!need_skip_next);
	    need_skip_next = false;

	    hash_scan->curr_hash_key = rd_hash;
	    error = hjoin_grace_probe_match (thread_p, manager, context, result_list_id, &overflow_record);
	    if (error != NO_ERROR)
	      {
		goto error_exit;
	      }
	  }
	/* §4: outer batch file closes once its probe pass completes. */
	delete outer_store;
	outer_store = NULL;
      }
    }

  qfile_close_list (thread_p, result_list_id);
  context->list_id = result_list_id;

  hjoin_scan_clear (thread_p, hash_scan);

  if (overflow_record.tpl != NULL)
    {
      db_private_free_and_init (thread_p, overflow_record.tpl);
    }

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;

error_exit:
  if (build_scan_open)
    {
      qfile_close_scan (thread_p, &build->list_scan_id);
    }
  if (probe_scan_open)
    {
      qfile_close_scan (thread_p, &probe->list_scan_id);
    }
  if (overflow_record.tpl != NULL)
    {
      db_private_free_and_init (thread_p, overflow_record.tpl);
    }
  if (result_list_id != NULL)
    {
      qfile_close_list (thread_p, result_list_id);
      qfile_destroy_list (thread_p, result_list_id);
      QFILE_FREE_AND_INIT_LIST_ID (result_list_id);
    }
  hjoin_scan_clear (thread_p, hash_scan);

  /* Any batch file left over from a mid-write error never got its write
   * phase finalized under a real thread_p; close it here (idempotent) so
   * batches' destructor -- which runs with thread_p = NULL -- only does the
   * already-closed no-op instead of finalizing under a NULL thread context. */
  for (qfile::batch_record_store * store : batches.inner)
    {
      if (store != NULL)
	{
	  store->close (thread_p);
	}
    }
  for (qfile::batch_record_store * store : batches.outer)
    {
      if (store != NULL)
	{
	  store->close (thread_p);
	}
    }

  if (error == NO_ERROR || er_errid () == NO_ERROR)
    {
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
    }
  return error;
}

/*
 * hjoin_execute_grace_px () - issue #147 S6: per-partition PARALLEL entry
 *   point (join_task::execute). Mirrors hjoin_execute()'s empty-input
 *   dispatch (FILL_NULL_VALUES / TRY / END, including the px-only
 *   last-partition-is-the-NULL-partition rule) but calls hjoin_execute_grace()
 *   instead of hjoin_execute_internal() for the TRY case, forcing the Grace
 *   IN_MEM reload (D-S2-1's nbatch clamp guarantees it fits hash_mem) so this
 *   partition never falls into hjoin_scan_init's old auto-tier-select, which
 *   could otherwise still pick HYBRID/HASH_FILE (probe random reads) for a
 *   large partition -- exactly the pathology D-S6-1 identified as the reason
 *   PARALLEL benchmark cells weren't moving. `worker_id` (this worker's
 *   stable task slot index, see join_task::execute) must be unique among
 *   concurrently-running callers -- distinct px workers never share one.
 */
int
hjoin_execute_grace_px (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context,
			UINT32 worker_id)
{
  HASHJOIN_STATUS status;
  int error = NO_ERROR;
#if !defined (NDEBUG)
  long random_reads_before;
#endif /* !NDEBUG */

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (context != NULL);

  status = hjoin_check_empty_inputs (manager, context);

  if (IS_OUTER_JOIN_TYPE (manager->join_type) && context == &manager->contexts[manager->context_cnt - 1])
    {
      status = (status == HASHJOIN_STATUS_TRY) ? HASHJOIN_STATUS_FILL_NULL_VALUES : status;
    }

  context->status = status;

  switch (status)
    {
    case HASHJOIN_STATUS_FILL_NULL_VALUES:
      assert (context != &manager->single_context);
      error = hjoin_outer_fill_null_values (thread_p, manager, context);
      break;

    case HASHJOIN_STATUS_TRY:
#if !defined (NDEBUG)
      /* issue #147 S6 D-S6-1 item 4: defensive check -- the Grace path this
       * call forces must never take a random-read (HYBRID/HASH_FILE) probe
       * branch. Structurally guaranteed today (hjoin_execute_grace only ever
       * selects HASH_METH_IN_MEM), but this catches a future regression at
       * runtime instead of silently reintroducing the exact pathology S6
       * exists to remove. Reuses the existing process-wide counter (gate ①)
       * rather than adding a new one. */
      random_reads_before = hjoin_debug_random_probe_read_count ();
#endif /* !NDEBUG */

      error = hjoin_execute_grace (thread_p, manager, context, worker_id);

#if !defined (NDEBUG)
      assert (hjoin_debug_random_probe_read_count () == random_reads_before);
#endif /* !NDEBUG */
      break;

    case HASHJOIN_STATUS_END:
      /* Nothing to do */
      break;

    case HASHJOIN_STATUS_ERROR:
    default:
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
      break;
    }

  /* Check if qfile_close_list was called */
  assert (context->list_id == NULL || context->list_id->last_pgptr == NULL);

  return error;
}

/*
 * hjoin_prepare_partition() -
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   split_info(in): Split information.
 */
static int
hjoin_prepare_partition (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_SPLIT_INFO * split_info)
{
  QFILE_LIST_ID *outer_list_id, *inner_list_id;
  QFILE_LIST_ID **outer_part_list_id = NULL, **inner_part_list_id = NULL;

  HASHJOIN_CONTEXT *single_context;
  HASHJOIN_CONTEXT *contexts = NULL, *current_context;
  HASHJOIN_STATS *context_stats = NULL;

  UINT32 part_cnt, part_index;

  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (split_info != NULL);

  single_context = &manager->single_context;
  outer_list_id = single_context->outer.list_id;
  inner_list_id = single_context->inner.list_id;
  assert (outer_list_id != NULL);
  assert (inner_list_id != NULL);

  error = hjoin_init_split_info (thread_p, manager, split_info);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  outer_part_list_id = split_info->outer.part_list_id;
  inner_part_list_id = split_info->inner.part_list_id;
  assert (outer_part_list_id != NULL);
  assert (inner_part_list_id != NULL);

  part_cnt = manager->context_cnt;
  assert (part_cnt > 1);

  contexts = (HASHJOIN_CONTEXT *) db_private_alloc (thread_p, part_cnt * sizeof (HASHJOIN_CONTEXT));
  if (contexts == NULL)
    {
      goto error_exit;
    }
  memset (contexts, 0, part_cnt * sizeof (HASHJOIN_CONTEXT));

  for (part_index = 0; part_index < part_cnt; part_index++)
    {
      current_context = &contexts[part_index];

      outer_part_list_id[part_index] =
	qfile_open_list (thread_p, &outer_list_id->type_list, NULL, outer_list_id->query_id, QFILE_FLAG_ALL, NULL);
      if (outer_part_list_id[part_index] == NULL)
	{
	  goto error_exit;
	}

      inner_part_list_id[part_index] =
	qfile_open_list (thread_p, &inner_list_id->type_list, NULL, inner_list_id->query_id, QFILE_FLAG_ALL, NULL);
      if (inner_part_list_id[part_index] == NULL)
	{
	  goto error_exit;
	}

      assert (current_context->list_id == NULL);

      current_context->outer.list_id = outer_part_list_id[part_index];
      current_context->outer.input = single_context->outer.input;
      current_context->outer.coerce_domains = single_context->outer.coerce_domains;
      current_context->outer.need_coerce_domains = single_context->outer.need_coerce_domains;
      current_context->outer.regu_list_pred = single_context->outer.regu_list_pred;

      current_context->inner.list_id = inner_part_list_id[part_index];
      current_context->inner.input = single_context->inner.input;
      current_context->inner.coerce_domains = single_context->inner.coerce_domains;
      current_context->inner.need_coerce_domains = single_context->inner.need_coerce_domains;
      current_context->inner.regu_list_pred = single_context->inner.regu_list_pred;

      assert (current_context->build == NULL);
      assert (current_context->probe == NULL);

      current_context->during_join_pred = single_context->during_join_pred;
      current_context->val_descr = single_context->val_descr;
    }

  manager->contexts = contexts;

  if (thread_is_on_trace (thread_p))
    {
      context_stats = (HASHJOIN_STATS *) malloc (part_cnt * sizeof (HASHJOIN_STATS));
      if (context_stats == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, part_cnt * sizeof (HASHJOIN_STATS));
	  goto error_exit;
	}
      memset (context_stats, 0, part_cnt * sizeof (HASHJOIN_STATS));

      for (part_index = 0; part_index < part_cnt; part_index++)
	{
	  contexts[part_index].stats = &context_stats[part_index];
	}

      assert (manager->stats_group != NULL);
      manager->stats_group->context_stats = context_stats;
      manager->stats_group->context_cnt = part_cnt;
    }
  else
    {
      assert (manager->stats_group == NULL);
    }

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;

error_exit:
  if (contexts != NULL)
    {
      db_private_free_and_init (thread_p, contexts);
    }

  hjoin_clear_split_info (thread_p, manager, split_info, true);

  if (thread_is_on_trace (thread_p))
    {
      if (context_stats != NULL)
	{
	  free_and_init (context_stats);
	}

      assert (manager->stats_group != NULL);
      manager->stats_group->context_stats = NULL;
      manager->stats_group->context_cnt = 0;
    }
  else
    {
      assert (context_stats == NULL);
      assert (manager->stats_group == NULL);
    }

  manager->contexts = NULL;

  if (error == NO_ERROR || er_errid () == NO_ERROR)
    {
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
    }

  return error;
}

/*
 * hjoin_build_partitions() -
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   split_info(in): Split information.
 */
static int
hjoin_build_partitions (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_SPLIT_INFO * split_info)
{
  QFILE_LIST_ID **temp_part_list_id = NULL;
  HASH_SCAN_KEY *temp_key = NULL;
  UINT32 part_cnt, part_index;
  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (split_info != NULL);

  HASHJOIN_STATS *stats = manager->single_context.stats;
  HASHJOIN_START_STATS start_stats = HASHJOIN_START_STATS_INITIALIZER;
  assert (!thread_is_on_trace (thread_p) || stats != NULL);

  if (thread_is_on_trace (thread_p))
    {
      hjoin_trace_start (thread_p, &start_stats);
    }

  part_cnt = manager->context_cnt;

  temp_part_list_id = (QFILE_LIST_ID **) db_private_alloc (thread_p, part_cnt * sizeof (QFILE_LIST_ID *));
  if (temp_part_list_id == NULL)
    {
      goto error_exit;
    }
  memset (temp_part_list_id, 0, part_cnt * sizeof (QFILE_LIST_ID *));

  temp_key = qdata_alloc_hscan_key (thread_p, manager->key_cnt, true);
  if (temp_key == NULL)
    {
      goto error_exit;
    }

  error = hjoin_split_qlist (thread_p, manager, &split_info->outer, temp_part_list_id, temp_key);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  error = hjoin_split_qlist (thread_p, manager, &split_info->inner, temp_part_list_id, temp_key);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  ASSERT_NO_ERROR_OR_INTERRUPTED ();

cleanup:
  if (thread_is_on_trace (thread_p))
    {
      hjoin_trace_end (thread_p, &stats->split, &start_stats);
    }

  if (temp_part_list_id != NULL)
    {
      for (part_index = 0; part_index < part_cnt; part_index++)
	{
	  if (temp_part_list_id[part_index] != NULL)
	    {
	      qfile_close_list (thread_p, temp_part_list_id[part_index]);
	      qfile_destroy_list (thread_p, temp_part_list_id[part_index]);
	      QFILE_FREE_AND_INIT_LIST_ID (temp_part_list_id[part_index]);
	    }
	}
      db_private_free_and_init (thread_p, temp_part_list_id);
    }

  if (temp_key != NULL)
    {
      qdata_free_hscan_key (thread_p, temp_key, manager->key_cnt);
    }

  return error;

error_exit:
  if (error == NO_ERROR || er_errid () == NO_ERROR)
    {
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
    }

  goto cleanup;
}

/*
 * hjoin_split_qlist() -
 *   return: Error code (NO_ERROR if successful, error code otherwise)
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   split_info(in): Split information.
 *   key(in/out): Space for reading join column values.
 */
static int
hjoin_split_qlist (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_INPUT_SPLIT_INFO * split_info,
		   QFILE_LIST_ID ** temp_part_list_id, HASH_SCAN_KEY * temp_key)
{
  QFILE_LIST_ID *list_id;
  QFILE_LIST_ID **part_list_id;
  QFILE_LIST_SCAN_ID list_scan_id;
  QFILE_TUPLE_RECORD tuple_record = { NULL, 0 };
  SCAN_CODE scan_code;

  unsigned int hash_key;
  UINT32 part_cnt, part_index, part_id;

  bool is_outer_join = false;
  bool need_skip_next = false;

  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (split_info != NULL);
  assert (split_info->fetch_info != NULL);
  assert (temp_part_list_id != NULL);
  assert (temp_key != NULL);

  list_id = split_info->fetch_info->list_id;
  part_list_id = split_info->part_list_id;
  part_cnt = manager->context_cnt;
  assert (list_id != NULL);
  assert (part_list_id != NULL);
  assert (part_cnt > 1);

  /* Prevent faults when qfile_close_scan is called */
  list_scan_id.status = S_CLOSED;

  is_outer_join = IS_OUTER_JOIN_TYPE (manager->join_type);

  error = qfile_open_list_scan (list_id, &list_scan_id);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  while ((scan_code = qfile_scan_list_next (thread_p, &list_scan_id, &tuple_record, PEEK)) == S_SUCCESS)
    {
      error = hjoin_fetch_key (thread_p, split_info->fetch_info, &tuple_record, temp_key, NULL /* compare_key */ ,
			       &need_skip_next);
      if (error != NO_ERROR)
	{
	  break;		/* error_exit */
	}
      else if (need_skip_next)
	{
	  need_skip_next = false;	/* init */

	  if (is_outer_join)
	    {
	      /* In outer joins, tuples with NULL in any join column are placed in the last partition.
	       * HASHJOIN_STATUS_FILL_NULL_VALUES is triggered for all tuples in that partition. */
	      part_id = part_cnt - 1;
	    }
	  else
	    {
	      /* next tuple */
	      continue;
	    }
	}			/* else if (need_skip_next) */
      else
	{
	  hash_key = qdata_hash_scan_key (temp_key, UINT_MAX, HASH_METH_IN_MEM);
	  part_id = (is_outer_join) ? hash_key % (part_cnt - 1) : hash_key % (part_cnt);

	  hjoin_update_tuple_hash_key (thread_p, &tuple_record, hash_key);
	}

      /* overflow page */
      if (QFILE_GET_OVERFLOW_PAGE_ID (list_scan_id.curr_pgptr) != NULL_PAGEID)
	{
	  assert (part_list_id[part_id]->last_pgptr == NULL);

	  if (qfile_reopen_list_as_append_mode (thread_p, part_list_id[part_id]) != NO_ERROR)
	    {
	      break;		/* error_exit */
	    }

	  error = qfile_add_tuple_to_list (thread_p, part_list_id[part_id], tuple_record.tpl);
	  if (error != NO_ERROR)
	    {
	      break;		/* error_exit */
	    }

	  qfile_close_list (thread_p, part_list_id[part_id]);

	  /* next tuple */
	  continue;
	}

      if (temp_part_list_id[part_id] != NULL &&
	  (QFILE_LIST_ID_TFILE_VFID(temp_part_list_id[part_id])->membuf_last ==
	   QFILE_LIST_ID_TFILE_VFID(temp_part_list_id[part_id])->membuf_npages - 1) &&
	  (temp_part_list_id[part_id]->last_offset + QFILE_GET_TUPLE_LENGTH (tuple_record.tpl)) > DB_PAGESIZE)
	{
	  qfile_close_list (thread_p, temp_part_list_id[part_id]);	/* may be meaningless since only memory buffer is used */

	  assert (part_list_id[part_id]->last_pgptr == NULL);

	  if (part_list_id[part_id]->tuple_cnt > 0)
	    {
	      error = qfile_append_list (thread_p, part_list_id[part_id], temp_part_list_id[part_id]);
	      if (error != NO_ERROR)
		{
		  break;	/* error_exit */
		}

	      error = qfile_truncate_list (thread_p, temp_part_list_id[part_id]);
	      if (error != NO_ERROR)
		{
		  break;	/* error_exit */
		}
	    }
	  else
	    {
	      qfile_destroy_list (thread_p, part_list_id[part_id]);
	      qfile_copy_list_id (part_list_id[part_id], temp_part_list_id[part_id], false, QFILE_PROHIBIT_DEPENDENT);
	      QFILE_FREE_AND_INIT_LIST_ID (temp_part_list_id[part_id]);
	    }
	}

      if (temp_part_list_id[part_id] == NULL)
	{
	  temp_part_list_id[part_id] =
	    qfile_open_list (thread_p, &list_id->type_list, NULL, list_id->query_id, QFILE_FLAG_ALL, NULL);
	  if (temp_part_list_id[part_id] == NULL)
	    {
	      break;		/* error_exit */
	    }
	}

      error = qfile_add_tuple_to_list (thread_p, temp_part_list_id[part_id], tuple_record.tpl);
      if (error != NO_ERROR)
	{
	  break;		/* error_exit */
	}
      assert (VFID_ISNULL (&QFILE_LIST_ID_TFILE_VFID(temp_part_list_id[part_id])->temp_vfid));
    }				/* while (qfile_scan_list_next (list_scan_id)) */

  /* After qfile_open_list_scan, if an error occurs,
   * ensure qfile_close_scan runs here
   * before jumping to error_exit. */
  qfile_close_scan (thread_p, &list_scan_id);

  for (part_index = 0; part_index < part_cnt; part_index++)
    {
      if (temp_part_list_id[part_index] != NULL)
	{
	  qfile_close_list (thread_p, temp_part_list_id[part_index]);	/* may be meaningless since only memory buffer is used */

	  if (temp_part_list_id[part_index]->tuple_cnt > 0)
	    {
	      assert (part_list_id[part_index]->last_pgptr == NULL);

	      if (part_list_id[part_index]->tuple_cnt > 0)
		{
		  error = qfile_append_list (thread_p, part_list_id[part_index], temp_part_list_id[part_index]);
		  if (error != NO_ERROR)
		    {
		      break;	/* error_exit */
		    }

		  qfile_destroy_list (thread_p, temp_part_list_id[part_index]);
		}
	      else
		{
		  qfile_destroy_list (thread_p, part_list_id[part_index]);
		  qfile_copy_list_id (part_list_id[part_index], temp_part_list_id[part_index], false,
				      QFILE_PROHIBIT_DEPENDENT);
		}
	    }
	  else
	    {
	      qfile_destroy_list (thread_p, temp_part_list_id[part_index]);
	    }

	  QFILE_FREE_AND_INIT_LIST_ID (temp_part_list_id[part_index]);
	}
    }

  if (scan_code == S_ERROR || error != NO_ERROR)
    {
      goto error_exit;
    }

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;

error_exit:
  if (error == NO_ERROR || er_errid () == NO_ERROR)
    {
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
    }

  return error;
}

/*
 * hjoin_merge_qlist() -
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   context(in): Hash join context containing per-partition state.
 */
int
hjoin_merge_qlist (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context)
{
  QFILE_LIST_ID *new_list_id = NULL;

  HASHJOIN_CONTEXT *single_context;

  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (context != NULL);

  /* Check if qfile_close_list was called */
  assert (context->list_id != NULL);
  assert (context->list_id->last_pgptr == NULL);

  single_context = &manager->single_context;
  assert (single_context != context);

  if (single_context->list_id == NULL)
    {
      single_context->list_id = context->list_id;
      context->list_id = NULL;
      return NO_ERROR;
    }
  else if (single_context->list_id->tuple_cnt == 0)
    {
      qfile_destroy_list (thread_p, single_context->list_id);
      QFILE_FREE_AND_INIT_LIST_ID (single_context->list_id);

      single_context->list_id = context->list_id;
      context->list_id = NULL;
      return NO_ERROR;
    }
  else if (context->list_id->tuple_cnt == 0)
    {
      qfile_destroy_list (thread_p, context->list_id);
      QFILE_FREE_AND_INIT_LIST_ID (context->list_id);
      return NO_ERROR;
    }

  /* Check if qfile_close_list was called */
  assert (single_context->list_id->last_pgptr == NULL);

  switch (manager->qlist_merge_method)
    {
    case HASHJOIN_MERGE_COMBINE:
      {
	new_list_id =
	  qfile_combine_two_list (thread_p, single_context->list_id, context->list_id,
				  QFILE_FLAG_ALL | QFILE_FLAG_UNION);
	if (new_list_id == NULL)
	  {
	    goto error_exit;
	  }

	qfile_destroy_list (thread_p, single_context->list_id);
	QFILE_FREE_AND_INIT_LIST_ID (single_context->list_id);

	qfile_destroy_list (thread_p, context->list_id);
	QFILE_FREE_AND_INIT_LIST_ID (context->list_id);

	single_context->list_id = new_list_id;
	new_list_id = NULL;

	break;
      }

    case HASHJOIN_MERGE_APPEND:
      {
	error = qfile_append_list (thread_p, single_context->list_id, context->list_id);
	if (error != NO_ERROR)
	  {
	    goto error_exit;
	  }

	qfile_destroy_list (thread_p, context->list_id);
	QFILE_FREE_AND_INIT_LIST_ID (context->list_id);

	break;
      }

    case HASHJOIN_MERGE_CONNECT:
      {
	/* connect now always appends, matching HASHJOIN_MERGE_APPEND. */
	error = qfile_append_list (thread_p, single_context->list_id, context->list_id);
	if (error != NO_ERROR)
	  {
	    goto error_exit;
	  }

	qfile_destroy_list (thread_p, context->list_id);
	QFILE_FREE_AND_INIT_LIST_ID (context->list_id);
	break;
      }

    default:
      /* impossible case */
      assert_release_error (false);
      goto error_exit;
    }

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;

error_exit:
  if (error == NO_ERROR || er_errid () == NO_ERROR)
    {
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
    }

  return error;
}

/*
 * hjoin_try_parallel() -
 *   return: One of the following HASHJOIN_STATUS values:
 *           - HASHJOIN_STATUS_PARTITION: Parallel execution is not applied or falls back on error.
 *           - HASHJOIN_STATUS_PARALLEL: Parallel execution is applied.
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   single_context(in): Hash join context for single-threaded execution.
 */
static HASHJOIN_STATUS
hjoin_try_parallel (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * single_context)
{
  QFILE_LIST_ID *outer_list_id, *inner_list_id;
  INT64 max_page_cnt;

  parallel_query::worker_manager * px_worker_manager = NULL;
  UINT64 *px_worker_stats = NULL;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (single_context != NULL);
  assert (single_context == &manager->single_context);

#if !defined (SERVER_MODE)
  assert (false);
#endif /* defined (SERVER_MODE) */

  outer_list_id = single_context->outer.list_id;
  inner_list_id = single_context->inner.list_id;
  assert (outer_list_id != NULL);
  assert (inner_list_id != NULL);

  /* Mirror hjoin_try_parallel_probe's guard: the parallel split requires
   * tapeset backing on both inputs.  If either side is pgbuf-paged (e.g. worker
   * pool exhaustion forced a serial scan upstream), force serial partitioning
   * (HASHJOIN_STATUS_PARTITION). */
  if (!qfile_list_has_tapeset (outer_list_id) || !qfile_list_has_tapeset (inner_list_id))
    {
      manager->num_parallel_threads = 0;
      assert (manager->px_worker_manager == NULL);
      return HASHJOIN_STATUS_PARTITION;
    }

  /* the parallel partition split reads tapeset input via chunk_distributor +
   * per-worker tapeset_reader (mirroring the probe path). */

  /* immutable */
  static const size_t stats_size = perfmon_get_number_of_statistic_values () * sizeof (UINT64);

  /* check if pages are enough for parallel-thread hash join */
  max_page_cnt =
    (outer_list_id->page_cnt > inner_list_id->page_cnt) ? outer_list_id->page_cnt : inner_list_id->page_cnt;
  assert (max_page_cnt >= 0);

  UINT32 degree = parallel_query::compute_parallel_degree (parallel_query::parallel_type::HASH_JOIN, max_page_cnt,
							   manager->num_parallel_threads);
  if (degree < 2)
    {
      /* try single-thread hash join */
      assert (degree == 0);
      manager->num_parallel_threads = 0;
      assert (manager->px_worker_manager == NULL);
      return HASHJOIN_STATUS_PARTITION;
    }

  degree = MIN (degree, manager->context_cnt /* part_cnt */ );

  px_worker_manager = parallel_query::worker_manager::try_reserve_workers (degree);
  if (px_worker_manager == NULL)
    {
      goto error_exit;
    }

  /* update to actual reserved workers */
  degree = px_worker_manager->get_reserved_workers ();
  manager->num_parallel_threads = degree;

  if (thread_is_on_trace (thread_p))
    {
      px_worker_stats = (UINT64 *) db_private_alloc (thread_p, degree * stats_size);
      if (px_worker_stats == NULL)
	{
	  assert_release_error (er_errid () != NO_ERROR);
	  goto error_exit;
	}
      memset (px_worker_stats, 0, degree * stats_size);

      /* only top-level parent */
      if (thread_p->m_px_stats == NULL)
	{
	  thread_p->m_px_stats = perfmon_allocate_values ();
	  if (thread_p->m_px_stats == NULL)
	    {
	      assert_release_error (er_errid () != NO_ERROR);
	      goto error_exit;
	    }
	  memset (thread_p->m_px_stats, 0, stats_size);
	}

      manager->px_worker_stats = px_worker_stats;
    }
  else
    {
      assert (manager->px_worker_stats == NULL);
    }

  manager->px_worker_manager = px_worker_manager;

  return HASHJOIN_STATUS_PARALLEL;

error_exit:
  manager->num_parallel_threads = 0;

  if (px_worker_manager != NULL)
    {
      px_worker_manager->release_workers ();
    }

  if (px_worker_stats != NULL)
    {
      db_private_free_and_init (thread_p, px_worker_stats);
    }

  if (er_errid () == ER_INTERRUPTED)
    {
      return HASHJOIN_STATUS_ERROR;
    }
  else
    {
      /* fallback to HASHJOIN_STATUS_PARTITION */
      er_clear ();
      return HASHJOIN_STATUS_PARTITION;
    }
}

/*
 * hjoin_try_parallel_probe() -
 *   return: One of the following HASHJOIN_STATUS values:
 *           - HASHJOIN_STATUS_SINGLE: Parallel probe is not applied or falls back on error.
 *           - HASHJOIN_STATUS_PARALLEL_PROBE: Parallel probe is applied.
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   single_context(in): Hash join context for single-threaded execution.
 */
static HASHJOIN_STATUS
hjoin_try_parallel_probe (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * single_context)
{
  parallel_query::worker_manager * px_worker_manager = NULL;
  UINT64 *px_worker_stats = NULL;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (manager->context_cnt == 0);
  assert (single_context != NULL);
  assert (single_context == &manager->single_context);

#if !defined (SERVER_MODE)
  assert (false);
#endif /* defined (SERVER_MODE) */

  /* The parallel probe path needs tapeset input (chunk_distributor +
   * tapeset_reader).  On re-execution the worker pool can be exhausted, dropping
   * the probe input back to pgbuf backing; force serial (HASHJOIN_STATUS_SINGLE)
   * for that case. */
  if (!qfile_list_has_tapeset (single_context->probe->list_id))
    {
      manager->num_parallel_threads = 0;
      assert (manager->px_worker_manager == NULL);
      return HASHJOIN_STATUS_SINGLE;
    }

  /* immutable */
  static const size_t stats_size = perfmon_get_number_of_statistic_values () * sizeof (UINT64);

  UINT32 degree = parallel_query::compute_parallel_degree (parallel_query::parallel_type::HASH_JOIN,
							   single_context->probe->list_id->page_cnt,
							   manager->num_parallel_threads);
  if (degree < 2)
    {
      /* try single-thread hash join */
      assert (degree == 0);
      manager->num_parallel_threads = 0;
      assert (manager->px_worker_manager == NULL);
      return HASHJOIN_STATUS_SINGLE;
    }

  px_worker_manager = parallel_query::worker_manager::try_reserve_workers (degree);
  if (px_worker_manager == NULL)
    {
      goto error_exit;
    }

  /* update to actual reserved workers */
  degree = px_worker_manager->get_reserved_workers ();
  manager->num_parallel_threads = degree;

  if (thread_is_on_trace (thread_p))
    {
      px_worker_stats = (UINT64 *) db_private_alloc (thread_p, degree * stats_size);
      if (px_worker_stats == NULL)
	{
	  assert_release_error (er_errid () != NO_ERROR);
	  goto error_exit;
	}
      memset (px_worker_stats, 0, degree * stats_size);

      /* only top-level parent */
      if (thread_p->m_px_stats == NULL)
	{
	  thread_p->m_px_stats = perfmon_allocate_values ();
	  if (thread_p->m_px_stats == NULL)
	    {
	      assert_release_error (er_errid () != NO_ERROR);
	      goto error_exit;
	    }
	  memset (thread_p->m_px_stats, 0, stats_size);
	}

      manager->px_worker_stats = px_worker_stats;
    }
  else
    {
      assert (manager->px_worker_stats == NULL);
    }

  manager->px_worker_manager = px_worker_manager;

  return HASHJOIN_STATUS_PARALLEL_PROBE;

error_exit:
  manager->num_parallel_threads = 0;

  if (px_worker_manager != NULL)
    {
      px_worker_manager->release_workers ();
    }

  if (px_worker_stats != NULL)
    {
      db_private_free_and_init (thread_p, px_worker_stats);
    }

  if (er_errid () == ER_INTERRUPTED)
    {
      return HASHJOIN_STATUS_ERROR;
    }
  else
    {
      /* fallback to HASHJOIN_STATUS_SINGLE */
      er_clear ();
      return HASHJOIN_STATUS_SINGLE;
    }
}

/*
 * hjoin_init_split_info() -
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   split_info(in/out): Split information.
 */
static int
hjoin_init_split_info (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_SPLIT_INFO * split_info)
{
  HASHJOIN_CONTEXT *single_context;
  HASHJOIN_INPUT_SPLIT_INFO *outer, *inner;
  UINT32 part_cnt;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (split_info != NULL);

  single_context = &manager->single_context;

  outer = &split_info->outer;
  inner = &split_info->inner;

  outer->fetch_info = &single_context->outer;
  outer->part_list_id = NULL;

  inner->fetch_info = &single_context->inner;
  inner->part_list_id = NULL;

  part_cnt = manager->context_cnt;
  assert (part_cnt > 1);

  outer->part_list_id = (QFILE_LIST_ID **) db_private_alloc (thread_p, part_cnt * sizeof (QFILE_LIST_ID *));
  if (outer->part_list_id == NULL)
    {
      goto error_exit;
    }
  memset (outer->part_list_id, 0, part_cnt * sizeof (QFILE_LIST_ID *));

  inner->part_list_id = (QFILE_LIST_ID **) db_private_alloc (thread_p, part_cnt * sizeof (QFILE_LIST_ID *));
  if (inner->part_list_id == NULL)
    {
      goto error_exit;
    }
  memset (inner->part_list_id, 0, part_cnt * sizeof (QFILE_LIST_ID *));

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;

error_exit:
  hjoin_clear_split_info (thread_p, manager, split_info, true);

  assert_release_error (er_errid () != NO_ERROR);
  return er_errid ();
}

/*
 * hjoin_clear_split_info() -
 *   return: None.
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   split_info(in): Split information.
 *   clear_all(in): True to destroy list identifiers; false otherwise.
 */
static void
hjoin_clear_split_info (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_SPLIT_INFO * split_info,
			bool clear_all)
{
  HASHJOIN_INPUT_SPLIT_INFO *outer, *inner;
  UINT32 part_cnt, part_index;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (split_info != NULL);

  outer = &split_info->outer;
  inner = &split_info->inner;

  part_cnt = manager->context_cnt;
  if (part_cnt <= 1)
    {
      assert (outer->part_list_id == NULL);
      assert (inner->part_list_id == NULL);
      return;			/* nothing to do */
    }

  /* The list identifier is still used through contexts[].outer/inner.list_id,
   * even though part_list_id is freed. */

  if (outer->part_list_id != NULL)
    {
      if (clear_all)
	{
	  for (part_index = 0; part_index < part_cnt; part_index++)
	    {
	      qfile_close_list (thread_p, outer->part_list_id[part_index]);
	      qfile_destroy_list (thread_p, outer->part_list_id[part_index]);
	      QFILE_FREE_AND_INIT_LIST_ID (outer->part_list_id[part_index]);

	      if (manager->contexts != NULL)
		{
		  manager->contexts[part_index].outer.list_id = NULL;
		}
	    }
	}
      db_private_free_and_init (thread_p, outer->part_list_id);
    }

  if (inner->part_list_id != NULL)
    {
      if (clear_all)
	{
	  for (part_index = 0; part_index < part_cnt; part_index++)
	    {
	      qfile_close_list (thread_p, inner->part_list_id[part_index]);
	      qfile_destroy_list (thread_p, inner->part_list_id[part_index]);
	      QFILE_FREE_AND_INIT_LIST_ID (inner->part_list_id[part_index]);

	      if (manager->contexts != NULL)
		{
		  manager->contexts[part_index].inner.list_id = NULL;
		}
	    }
	}
      db_private_free_and_init (thread_p, inner->part_list_id);
    }
}

/*
 * hjoin_init_shared_split_info() -
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   shared_info(in/out): Shared split information.
 */
int
hjoin_init_shared_split_info (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager,
			      HASHJOIN_SHARED_SPLIT_INFO * shared_info)
{
  UINT32 part_cnt, part_index;
  UINT32 init_cnt = 0;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (shared_info != NULL);

  part_cnt = manager->context_cnt;
  assert (part_cnt > 1);

  if (manager->px_worker_manager != NULL)
    {
      assert (shared_info->part_mutexes == NULL);

      shared_info->part_mutexes = (std::mutex *) db_private_alloc (thread_p, part_cnt * sizeof (std::mutex));
      if (shared_info->part_mutexes == NULL)
	{
	  goto error_exit;
	}

      try
      {
	for (part_index = 0; part_index < part_cnt; part_index++)
	  {
	    placement_new < std::mutex > (&shared_info->part_mutexes[part_index]);
	    ++init_cnt;
	  }
      }
      catch ( ...)
      {
	goto error_exit;
      }
    }

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;

error_exit:
  if (shared_info->part_mutexes != NULL)
    {
      for (part_index = 0; part_index < init_cnt; part_index++)
	{
	  // *INDENT-OFF*
	  shared_info->part_mutexes[part_index].~mutex ();
	  // *INDENT-ON*
	}
      db_private_free_and_init (thread_p, shared_info->part_mutexes);
    }

  assert_release_error (er_errid () != NO_ERROR);
  return er_errid ();
}

/*
 * hjoin_clear_shared_split_info() -
 *   return: None.
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   shared_info(in): Shared split information.
 */
void
hjoin_clear_shared_split_info (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager,
			       HASHJOIN_SHARED_SPLIT_INFO * shared_info)
{
  UINT32 part_cnt, part_index;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (shared_info != NULL);

  part_cnt = manager->context_cnt;
  if (part_cnt <= 1)
    {
      assert (shared_info->part_mutexes == NULL);
      return;			/* nothing more to do */
    }

  if (shared_info->part_mutexes != NULL)
    {
      for (part_index = 0; part_index < part_cnt; part_index++)
	{
	  // *INDENT-OFF*
	  shared_info->part_mutexes[part_index].~mutex ();
	  // *INDENT-ON*
	}
      db_private_free_and_init (thread_p, shared_info->part_mutexes);
    }

  /* clean up any leaked per-worker output lists (e.g. on an error path where
   * the leader merge did not run). */
  if (shared_info->worker_part_lists != NULL)
    {
      for (UINT32 wi = 0; wi < shared_info->worker_count; wi++)
	{
	  if (shared_info->worker_part_lists[wi] != NULL)
	    {
	      for (part_index = 0; part_index < part_cnt; part_index++)
		{
		  if (shared_info->worker_part_lists[wi][part_index] != NULL)
		    {
		      qfile_close_list (thread_p, shared_info->worker_part_lists[wi][part_index]);
		      qfile_destroy_list (thread_p, shared_info->worker_part_lists[wi][part_index]);
		      QFILE_FREE_AND_INIT_LIST_ID (shared_info->worker_part_lists[wi][part_index]);
		    }
		}
	      db_private_free_and_init (thread_p, shared_info->worker_part_lists[wi]);
	    }
	}
      db_private_free_and_init (thread_p, shared_info->worker_part_lists);
      shared_info->worker_count = 0;
    }
}

/*
 * hjoin_init_context() -
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   context(in/out): Hash join context to initialize.
 *   use_grace(out): issue #147 S5-lite -- set true when hjoin_scan_init
 *     signals that the SINGLE path's IN_MEM reserve was rejected and the
 *     caller must re-route into Grace instead. When true, this function
 *     returns NO_ERROR early without touching stats/parallel-probe dispatch
 *     (context->build/probe are still valid; nothing else is).
 */
static int
hjoin_init_context (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context, bool * use_grace)
{
  HASHJOIN_FETCH_INFO *outer, *inner;
  HASHJOIN_FETCH_INFO *build = NULL;
  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (context != NULL);
  assert (!thread_is_on_trace (thread_p) || context->stats != NULL);

  outer = &context->outer;
  inner = &context->inner;
  assert (outer->list_id != NULL && outer->list_id->tuple_cnt > 0);
  assert (inner->list_id != NULL && inner->list_id->tuple_cnt > 0);

  switch (manager->join_type)
    {
    case JOIN_INNER:
      if (outer->list_id->tuple_cnt < inner->list_id->tuple_cnt)
	{
	  context->build = outer;
	  context->probe = inner;
	}
      else if (outer->list_id->tuple_cnt == inner->list_id->tuple_cnt
	       && outer->list_id->page_cnt < inner->list_id->page_cnt)
	{
	  context->build = outer;
	  context->probe = inner;
	}
      else
	{
	  context->build = inner;
	  context->probe = outer;
	}
      break;

    case JOIN_LEFT:
      outer->fill_record = &outer->tuple_record;
      inner->fill_record = NULL;

      context->build = inner;
      context->probe = outer;
      break;

    case JOIN_RIGHT:
      outer->fill_record = NULL;
      inner->fill_record = &inner->tuple_record;

      context->build = outer;
      context->probe = inner;
      break;

    default:
      /* impossible case */
      assert_release_error (false);
      goto error_exit;
    }

  build = context->build;
  assert (build != NULL);

  error = hjoin_scan_init (thread_p, &context->hash_scan, manager->key_cnt, build->list_id, use_grace);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }
  if (use_grace != NULL && *use_grace)
    {
      /* issue #147 S5-lite: caller (hjoin_execute_internal) re-routes into
       * Grace -- build/probe are already set above; nothing else here
       * (stats, parallel-probe dispatch) applies to that path. */
      return NO_ERROR;
    }

  if (thread_is_on_trace (thread_p))
    {
      context->stats->hash_method = context->hash_scan.hash_list_scan_type;
      context->stats->swap_join_inputs = (context->build == outer) ? true : false;
    }

#if defined (SERVER_MODE)
  if (context == &manager->single_context)
    {
      context->status = hjoin_try_parallel_probe (thread_p, manager, context);
      switch (context->status)
	{
	case HASHJOIN_STATUS_SINGLE:
	  if (thread_is_on_trace (thread_p))
	    {
	      assert (context->stats != NULL);
	      assert (context->stats->num_parallel_threads == 0);
	    }
	  break;

	case HASHJOIN_STATUS_PARALLEL_PROBE:
	  if (thread_is_on_trace (thread_p))
	    {
	      assert (context->stats != NULL);
	      context->stats->num_parallel_threads = manager->num_parallel_threads;
	    }
	  break;

	case HASHJOIN_STATUS_ERROR:
	  goto error_exit;

	default:
	  /* impossible case */
	  assert_release_error (false);
	  goto error_exit;
	}
    }
#endif /* defined (SERVER_MODE) */

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;

error_exit:
  hjoin_scan_clear (thread_p, &context->hash_scan);

  if (error == NO_ERROR || er_errid () == NO_ERROR)
    {
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
    }

  return error;
}

/*
 * hjoin_clear_context() -
 *   return: None.
 *   thread_p(in): Thread entry.
 *   context(in): Hash join context to clear.
 */
static void
hjoin_clear_context (THREAD_ENTRY * thread_p, HASHJOIN_CONTEXT * context)
{
  HASHJOIN_FETCH_INFO *outer, *inner;

  assert (thread_p != NULL);
  assert (context != NULL);

  outer = &context->outer;
  inner = &context->inner;

  if (context->list_id != NULL)
    {
      qfile_close_list (thread_p, context->list_id);
      qfile_destroy_list (thread_p, context->list_id);
      QFILE_FREE_AND_INIT_LIST_ID (context->list_id);
    }

  if (outer->list_id != NULL)
    {
      qfile_close_list (thread_p, outer->list_id);
      qfile_destroy_list (thread_p, outer->list_id);
      QFILE_FREE_AND_INIT_LIST_ID (outer->list_id);
    }

  if (inner->list_id != NULL)
    {
      qfile_close_list (thread_p, inner->list_id);
      qfile_destroy_list (thread_p, inner->list_id);
      QFILE_FREE_AND_INIT_LIST_ID (inner->list_id);
    }
}

/*
 * hjoin_destroy_qlist() -
 *   return: None.
 *   thread_p(in): Thread entry.
 *   context(in): Hash join context to destroy.
 */
static void
hjoin_destroy_qlist (THREAD_ENTRY * thread_p, HASHJOIN_CONTEXT * context)
{
  HASHJOIN_FETCH_INFO *outer, *inner;

  assert (thread_p != NULL);
  assert (context != NULL);

  outer = &context->outer;
  inner = &context->inner;

  if (outer->list_id != NULL)
    {
      qfile_close_list (thread_p, outer->list_id);
      qfile_destroy_list (thread_p, outer->list_id);
    }

  if (inner->list_id != NULL)
    {
      qfile_close_list (thread_p, inner->list_id);
      qfile_destroy_list (thread_p, inner->list_id);
    }
}

/*
 * hjoin_scan_init() -
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry.
 *   hash_scan(in/out): Hash scan structure to initialize.
 *   key_cnt(in): Number of join columns.
 *   list_id(in): List identifier to be used as build input.
 */
int
hjoin_scan_init (THREAD_ENTRY * thread_p, HASH_LIST_SCAN * hash_scan, int key_cnt, QFILE_LIST_ID * list_id,
		 bool * use_grace)
{
  UINT64 mem_limit;
  bool fits_in_mem_size;

  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (hash_scan != NULL);
  assert (list_id == NULL || list_id->tuple_cnt > 0);
  assert (key_cnt > 0);

  if (use_grace != NULL)
    {
      *use_grace = false;
    }

  mem_limit = prm_get_bigint_value (PRM_ID_WORK_MEM);
  assert (mem_limit > 0);

  assert (hash_scan->build_regu_list == NULL);	/* Unused */
  assert (hash_scan->probe_regu_list == NULL);	/* Unused */

  hash_scan->wm_bytes = 0;
  hash_scan->wm_shard = -1;

  hash_scan->temp_key = qdata_alloc_hscan_key (thread_p, key_cnt, true);
  if (hash_scan->temp_key == NULL)
    {
      goto error_exit;
    }

  hash_scan->temp_new_key = qdata_alloc_hscan_key (thread_p, key_cnt, true);
  if (hash_scan->temp_new_key == NULL)
    {
      goto error_exit;
    }

  if (list_id != NULL)
    {
      /* Same tier contract as check_hash_list_scan: an in-memory tier must both
       * fit work_mem AND secure its estimate from the work_mem accountant; on
       * refusal it degrades to the next tier. */
      fits_in_mem_size = (UINT64) list_id->page_cnt * DB_PAGESIZE <= mem_limit;
      if (fits_in_mem_size && qdata_hscan_wm_reserve (hash_scan, (size_t) list_id->page_cnt * DB_PAGESIZE))
	{
#if HASHJOIN_DUMP_BUILD
	  fprintf (stdout, "\nHash Join Method: In Memory\n");
	  fprintf (stdout, "  - Page Count: %d <= %lu\n", list_id->page_cnt, mem_limit / 16344);
#endif /* HASHJOIN_DUMP_BUILD */

	  hash_scan->hash_list_scan_type = HASH_METH_IN_MEM;

	  hash_scan->memory.hash_table = mht_create_hls ("Hash Join", list_id->tuple_cnt, NULL, NULL);
	  if (hash_scan->memory.hash_table == NULL)
	    {
	      goto error_exit;
	    }

	  hash_scan->memory.curr_hash_entry = NULL;
	  /* #144 P3 D2: arena for the value+tuple bump-alloc (NULL => per-entry
	   * fallback; freed en masse in hjoin_scan_clear). */
	  hash_scan->memory.value_arena = hscan_value_arena_create ();
	}
      else if (fits_in_mem_size && use_grace != NULL)
	{
	  /* issue #147 S5-lite: the static byte estimate said IN_MEM should fit,
	   * but the layer-2 accountant's live reserve just rejected it (a
	   * tighter real-time cap than the static per-op estimate above --
	   * e.g. other concurrent work_mem consumers). Degrading to HYBRID/
	   * HASH_FILE here would reintroduce the exact probe-random-read
	   * pathology Grace exists to eliminate (qfile_jump_scan_tuple_position).
	   * Signal the caller to re-route into Grace's own nbatch-based
	   * batching (which resizes against the REAL build size) instead of
	   * silently degrading tier. hash_scan is left cleared/unusable --
	   * the caller must not use it further on this path. */
	  *use_grace = true;
	  hjoin_scan_clear (thread_p, hash_scan);
	  return NO_ERROR;
	}
      else if ((UINT64) list_id->tuple_cnt * (sizeof (HENTRY_HLS) + sizeof (QFILE_TUPLE_SIMPLE_POS)) <= mem_limit
	       && qdata_hscan_wm_reserve (hash_scan,
					  (size_t) list_id->tuple_cnt * (sizeof (HENTRY_HLS) +
									 sizeof (QFILE_TUPLE_SIMPLE_POS))))
	{
#if HASHJOIN_DUMP_BUILD
	  fprintf (stdout, "\nHash Join Method: Hybrid\n");
	  fprintf (stdout, "  - Page Count: %d > %lu\n", list_id->page_cnt, mem_limit / 16344);
	  fprintf (stdout, "  - Tuple Count: %ld <= %lu\n", list_id->tuple_cnt,
		   mem_limit / (sizeof (HENTRY_HLS) + sizeof (QFILE_TUPLE_SIMPLE_POS)));
#endif /* HASHJOIN_DUMP_BUILD */

	  hash_scan->hash_list_scan_type = HASH_METH_HYBRID;

	  hash_scan->memory.hash_table = mht_create_hls ("Hash Join", list_id->tuple_cnt, NULL, NULL);
	  if (hash_scan->memory.hash_table == NULL)
	    {
	      goto error_exit;
	    }

	  hash_scan->memory.curr_hash_entry = NULL;
	  /* #144 P3 D2: HYBRID keeps per-entry OID alloc (no tuple copy) -- no arena. */
	  hash_scan->memory.value_arena = NULL;
	}
      else
	{
#if HASHJOIN_DUMP_BUILD
	  fprintf (stdout, "\nHash Join Method: File\n");
	  fprintf (stdout, "  - Page Count: %d > %lu\n", list_id->page_cnt, mem_limit / 16344);
	  fprintf (stdout, "  - Tuple Count: %ld > %lu\n", list_id->tuple_cnt,
		   mem_limit / (sizeof (HENTRY_HLS) + sizeof (QFILE_TUPLE_SIMPLE_POS)));
#endif /* HASHJOIN_DUMP_BUILD */

	  hash_scan->hash_list_scan_type = HASH_METH_HASH_FILE;

	  /* batch-spill hash table (HASH_FILE tier) */
	  hash_scan->spill.hash_table = hls_spill_create (thread_p, list_id->tuple_cnt);
	  if (hash_scan->spill.hash_table == NULL)
	    {
	      goto error_exit;
	    }

	  /* per-scan probe cursor: created alongside the table so every scan of
	   * type HASH_METH_HASH_FILE always has a cursor whenever
	   * spill.hash_table != NULL */
	  hash_scan->spill.cursor = hls_spill_cursor_create (thread_p);
	  if (hash_scan->spill.cursor == NULL)
	    {
	      goto error_exit;
	    }
	}
    }
  else
    {
      /* skip hash table */
      hash_scan->hash_list_scan_type = HASH_METH_NOT_USE;
    }

  hash_scan->curr_hash_key = 0;
  hash_scan->need_coerce_type = false;

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;

error_exit:
  hjoin_scan_clear (thread_p, hash_scan);

  if (error == NO_ERROR || er_errid () == NO_ERROR)
    {
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
    }

  return error;
}

/*
 * hjoin_scan_clear() -
 *   return: None.
 *   thread_p(in): Thread entry.
 *   hash_scan(in): Hash scan structure to clear.
 */
void
hjoin_scan_clear (THREAD_ENTRY * thread_p, HASH_LIST_SCAN * hash_scan)
{
  assert (thread_p != NULL);
  assert (hash_scan != NULL);

  if (hash_scan->temp_key != NULL)
    {
      qdata_free_hscan_key (thread_p, hash_scan->temp_key, hash_scan->temp_key->val_count);
      hash_scan->temp_key = NULL;
    }

  if (hash_scan->temp_new_key != NULL)
    {
      qdata_free_hscan_key (thread_p, hash_scan->temp_new_key, hash_scan->temp_new_key->val_count);
      hash_scan->temp_new_key = NULL;
    }

  switch (hash_scan->hash_list_scan_type)
    {
    case HASH_METH_IN_MEM:
    case HASH_METH_HYBRID:
      if (hash_scan->memory.hash_table != NULL)
	{
	  if (hash_scan->memory.value_arena != NULL)
	    {
	      /* #144 P3 D2: values live in the arena -> no per-entry free (mht clear
	       * passes key=NULL; inline uint keys need none), then drop the arena. */
	      mht_clear_hls (hash_scan->memory.hash_table, qdata_noop_free_hscan_entry, NULL);
	      mht_destroy_hls (hash_scan->memory.hash_table);
	      hash_scan->memory.hash_table = NULL;
	      hscan_value_arena_destroy (hash_scan->memory.value_arena);
	      hash_scan->memory.value_arena = NULL;
	    }
	  else
	    {
	      mht_clear_hls (hash_scan->memory.hash_table, qdata_free_hscan_entry, (void *) thread_p);
	      mht_destroy_hls (hash_scan->memory.hash_table);
	      hash_scan->memory.hash_table = NULL;
	    }
	}
      break;

    case HASH_METH_HASH_FILE:
      if (hash_scan->spill.hash_table != NULL)
	{
	  hls_spill_cursor_destroy (thread_p, hash_scan->spill.hash_table, hash_scan->spill.cursor);
	  hls_spill_destroy (thread_p, hash_scan->spill.hash_table);
	  hash_scan->spill.hash_table = NULL;
	  hash_scan->spill.cursor = NULL;
	}
      break;

    case HASH_METH_NOT_USE:
      /* fall through */
    default:
      /* Nothing to do */
      break;
    }

  /* release the IN_MEM/HYBRID build estimate charge */
  qdata_hscan_wm_release (hash_scan);

  hash_scan->hash_list_scan_type = HASH_METH_NOT_USE;
}

/*
 * hjoin_check_empty_inputs() -
 *   return: One of the following HASHJOIN_STATUS values:
 *           - HASHJOIN_STATUS_END: Inner join with one empty input, or outer join with empty preserved side.
 *           - HASHJOIN_STATUS_FILL_NULL_VALUES: Outer join with empty null-supplying side.
 *           - HASHJOIN_STATUS_TRY: Both inputs are non-empty; proceed with the join.
 *   manager(in): Hash join manager containing shared state.
 *   context(in): Hash join context containing per-partition state.
 */
static HASHJOIN_STATUS
hjoin_check_empty_inputs (HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context)
{
  HASHJOIN_FETCH_INFO *outer, *inner;
  INT64 outer_tuple_cnt, inner_tuple_cnt;
  HASHJOIN_STATUS status;

  assert (manager != NULL);
  assert (context != NULL);

  outer = &context->outer;
  inner = &context->inner;

  /* When aptr_list is executed in qexec_execute_mainblock_internal,
   * it checks the results from outer_xasl and inner_xasl in merge_info.
   * If either has no result, the other is skipped,
   * and the skipped node can have a type count of 0 in list_id.type_list. */
  if (outer->list_id == NULL || inner->list_id == NULL)
    {
      return HASHJOIN_STATUS_END;
    }

  outer_tuple_cnt = outer->list_id->tuple_cnt;
  inner_tuple_cnt = inner->list_id->tuple_cnt;

  /* HASHJOIN_STATUS_END must be checked first. */

  switch (manager->join_type)
    {
    case JOIN_INNER:
      status = (outer_tuple_cnt == 0 || inner_tuple_cnt == 0) ? HASHJOIN_STATUS_END : HASHJOIN_STATUS_TRY;
      break;

    case JOIN_LEFT:
      status =
	(outer_tuple_cnt == 0) ? HASHJOIN_STATUS_END : (inner_tuple_cnt ==
							0) ? HASHJOIN_STATUS_FILL_NULL_VALUES : HASHJOIN_STATUS_TRY;
      break;

    case JOIN_RIGHT:
      status =
	(inner_tuple_cnt == 0) ? HASHJOIN_STATUS_END : (outer_tuple_cnt ==
							0) ? HASHJOIN_STATUS_FILL_NULL_VALUES : HASHJOIN_STATUS_TRY;
      break;

    default:
      /* impossible case */
      assert_release_error (false);
      status = HASHJOIN_STATUS_ERROR;
    }

  return status;
}

/*
 * hjoin_fetch_key() -
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry.
 *   fetch_info(in): Information for reading join column values. 
 *   tuple_record(in): Tuple to read values from.
 *   key(in/out): Space for reading join column values.
 *   compare_key(in): Key for comparison with the read key. (can be NULL).
 *   need_skip_next(in/out): Set to true if the current tuple should be skipped.
 */
int
hjoin_fetch_key (THREAD_ENTRY * thread_p, HASHJOIN_FETCH_INFO * fetch_info, QFILE_TUPLE_RECORD * tuple_record,
		 HASH_SCAN_KEY * key, HASH_SCAN_KEY * compare_key, bool * need_skip_next)
{
  TP_DOMAIN **domains, **coerce_domains;
  int *value_indexes;
  bool need_coerce_domains;

  QFILE_TUPLE tuple_record_end;
  QFILE_TUPLE tuple_value;
  OR_BUF buf;
  int value_size, value_index, key_index;

  TP_DOMAIN_STATUS domain_status = DOMAIN_COMPATIBLE;
  DB_VALUE pre_coerce_value;

  DB_VALUE_COMPARE_RESULT compare_result = DB_EQ;

  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (fetch_info != NULL);
  assert (fetch_info->input != NULL);
  assert (tuple_record != NULL);
  assert (tuple_record->tpl != NULL);
  assert (key != NULL);
  assert (need_skip_next != NULL);
  assert (*need_skip_next == false);

  domains = fetch_info->input->domains;
  value_indexes = fetch_info->input->value_indexes;
  coerce_domains = fetch_info->coerce_domains;
  need_coerce_domains = fetch_info->need_coerce_domains;
  assert (domains != NULL);
  assert (value_indexes != NULL);
  assert (coerce_domains != NULL);

  db_make_null (&pre_coerce_value);

  tuple_record_end = tuple_record->tpl + QFILE_GET_TUPLE_LENGTH (tuple_record->tpl);

  /* Skip the tuple header */
  tuple_value = tuple_record->tpl + QFILE_TUPLE_LENGTH_SIZE;

  for (value_index = 0; tuple_value < tuple_record_end; value_index++)
    {
      for (key_index = 0; key_index < key->val_count; key_index++)
	{
	  /*
	   * The same tuple value can be referenced by multiple keys.
	   *
	   * e.g. value_indexes[0] = 0
	   *      value_indexes[1] = 1
	   *      value_indexes[2] = 1
	   *      value_indexes[3] = 3
	   */
	  if (value_indexes[key_index] != value_index)
	    {
	      continue;
	    }

	  /* Skip the tuple if any value is NULL */
	  if (QFILE_GET_TUPLE_VALUE_FLAG (tuple_value) == V_UNBOUND)
	    {
	      goto skip_next;
	    }

	  value_size = QFILE_GET_TUPLE_VALUE_LENGTH (tuple_value);
	  assert (value_size > 0);

	  /* Skip the tuple value header */
	  or_init (&buf, tuple_value + QFILE_TUPLE_VALUE_HEADER_SIZE, value_size);

	  pr_clear_value (key->values[key_index]);

	  if (need_coerce_domains && coerce_domains[key_index] != NULL
	      && coerce_domains[key_index] != domains[key_index])
	    {
	      error =
		domains[key_index]->type->data_readval (&buf, &pre_coerce_value, domains[key_index], -1, false, NULL,
							0);
	      if (error != NO_ERROR)
		{
		  goto error_exit;
		}

	      if (coerce_domains[key_index]->type->id == DB_TYPE_NUMERIC
		  && pre_coerce_value.domain.general_info.type == DB_TYPE_NUMERIC
		  && coerce_domains[key_index]->precision == DB_DEFAULT_NUMERIC_PRECISION
		  && pre_coerce_value.domain.numeric_info.precision != DB_DEFAULT_NUMERIC_PRECISION
		  && pre_coerce_value.domain.numeric_info.scale < 0)
		{
		  /* 
		   * for float numeric and fixed numeric, this is used to recalculate the fixed
		   * numeric's precision later, as it must be known accurately during normalization.
		   * 
		   * note: A value in numeric(38,0) column does not guarantee precision 38.
		   */
		  pre_coerce_value.domain.numeric_info.precision = DB_HJOIN_NUMERIC_PRECISION_DEFERRED;
		}

	      domain_status = tp_value_coerce (&pre_coerce_value, key->values[key_index], coerce_domains[key_index]);
	      if (domain_status != DOMAIN_COMPATIBLE)
		{
		  tp_domain_status_er_set (domain_status, ARG_FILE_LINE, &pre_coerce_value, coerce_domains[key_index]);
		  pr_clear_value (&pre_coerce_value);
		  goto error_exit;
		}

	      pr_clear_value (&pre_coerce_value);
	    }
	  else
	    {
	      error =
		domains[key_index]->type->data_readval (&buf, key->values[key_index], domains[key_index], -1, false,
							NULL, 0);
	      if (error != NO_ERROR)
		{
		  goto error_exit;
		}
	    }

	  if (compare_key != NULL)
	    {
	      /* Skip the tuple if any value does not match */
	      compare_result = tp_value_compare (key->values[key_index], compare_key->values[key_index], 0, 0);
	      if (compare_result != DB_EQ)
		{
		  goto skip_next;
		}
	    }
	}

      /* Skip the current tuple value */
      tuple_value += QFILE_TUPLE_VALUE_HEADER_SIZE + QFILE_GET_TUPLE_VALUE_LENGTH (tuple_value);
    }

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;

skip_next:
  *need_skip_next = true;

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;

error_exit:
  if (error == NO_ERROR || er_errid () == NO_ERROR)
    {
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
    }

  return error;
}

/*
 * hjoin_update_tuple_hash_key() -
 *   return: None
 *   thread_p(in): Thread entry.
 *   tuple_record(in): Tuple containing the hash key tuple value.
 *   hash_key(in): Hash key to store in the tuple value.
 */
void
hjoin_update_tuple_hash_key (THREAD_ENTRY * thread_p, QFILE_TUPLE_RECORD * tuple_record, UINT32 hash_key)
{
  QFILE_TUPLE tuple_value;

  assert (thread_p != NULL);
  assert (tuple_record != NULL);

  tuple_value = tuple_record->tpl + QFILE_TUPLE_LENGTH_SIZE;
  assert (QFILE_GET_TUPLE_VALUE_FLAG (tuple_value) == V_BOUND);
  assert (QFILE_GET_TUPLE_VALUE_LENGTH (tuple_value) == MAX_ALIGNMENT);

  tuple_value += QFILE_TUPLE_VALUE_HEADER_SIZE;
  assert (OR_GET_INT (tuple_value) == -1);

  OR_PUT_INT (tuple_value, hash_key);
}

/*
 * hjoin_build() -
 *   return: Error code (NO_ERROR if successful, error code otherwise)
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   context(in): Hash join context containing per-partition state.
 */
static int
hjoin_build (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context)
{
  SCAN_CODE scan_code;
  bool need_skip_next = false;

  HASHJOIN_FETCH_INFO *build = NULL;
  QFILE_TUPLE tuple_value;

  HASH_LIST_SCAN *hash_scan;
  HASH_METHOD hash_method;
  HASH_SCAN_KEY *key;

  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (context != NULL);

  HASHJOIN_STATS *stats = context->stats;
  HASHJOIN_START_STATS start_stats = HASHJOIN_START_STATS_INITIALIZER;
#if HASHJOIN_PROFILE_TIME
  HASHJOIN_START_STATS profile_start_stats = HASHJOIN_START_STATS_INITIALIZER;
#endif /* HASHJOIN_PROFILE_TIME */
  assert (!thread_is_on_trace (thread_p) || stats != NULL);

  build = context->build;
  assert (build != NULL);
  assert (build->list_scan_id.status != S_CLOSED);

  // *INDENT-OFF*
  build->tuple_record = { NULL, 0 };
  // *INDENT-ON*

  hash_scan = &context->hash_scan;

  hash_method = hash_scan->hash_list_scan_type;
  assert (hash_method != HASH_METH_NOT_USE);

  key = hash_scan->temp_key;
  assert (key != NULL);

  if (thread_is_on_trace (thread_p))
    {
      hjoin_trace_start (thread_p, &start_stats);
    }

  if (manager->context_cnt == 0)	/* HASHJOIN_STATUS_SINGLE */
    {
      while ((scan_code =
	      qfile_scan_list_next (thread_p, &build->list_scan_id, &build->tuple_record, PEEK)) == S_SUCCESS)
	{
	  HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_BUILD_FETCH);
	  error =
	    hjoin_fetch_key (thread_p, build, &build->tuple_record, key, NULL /* compare_key */ , &need_skip_next);
	  HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_BUILD_FETCH);

	  if (error != NO_ERROR)
	    {
	      break;		/* error_exit */
	    }
	  else if (need_skip_next)
	    {
	      need_skip_next = false;	/* init */
	      continue;
	    }
	  else
	    {
	      HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_BUILD_HASH);
	      hash_scan->curr_hash_key = qdata_hash_scan_key (key, UINT_MAX, hash_method);
	      HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_BUILD_HASH);

	      HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_BUILD_INSERT);
	      error = hjoin_build_key (thread_p, hash_scan, &build->list_scan_id, &build->tuple_record);
	      HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_BUILD_INSERT);

	      if (error != NO_ERROR)
		{
		  break;	/* error_exit */
		}
	    }
	}			/* while (qfile_scan_list_next (list_scan_id)) */
    }
  else
    {
      while ((scan_code =
	      qfile_scan_list_next (thread_p, &build->list_scan_id, &build->tuple_record, PEEK)) == S_SUCCESS)
	{
	  HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_BUILD_FETCH);
	  tuple_value = build->tuple_record.tpl + QFILE_TUPLE_LENGTH_SIZE;
	  assert (QFILE_GET_TUPLE_VALUE_FLAG (tuple_value) == V_BOUND);
	  assert (QFILE_GET_TUPLE_VALUE_LENGTH (tuple_value) == MAX_ALIGNMENT);

	  tuple_value += QFILE_TUPLE_VALUE_HEADER_LENGTH;
	  HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_BUILD_FETCH);

	  HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_BUILD_HASH);
	  hash_scan->curr_hash_key = (UINT32) OR_GET_INT (tuple_value);
	  HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_BUILD_HASH);

	  HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_BUILD_INSERT);
	  error = hjoin_build_key (thread_p, hash_scan, &build->list_scan_id, &build->tuple_record);
	  HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_BUILD_INSERT);

	  if (error != NO_ERROR)
	    {
	      break;		/* error_exit */
	    }
	}			/* while (qfile_scan_list_next (list_scan_id)) */
    }

  if (thread_is_on_trace (thread_p))
    {
      hjoin_trace_end (thread_p, &stats->build, &start_stats);
      stats->build.read_rows = build->list_id->tuple_cnt;
      assert (stats->build.read_keys == 0);
      stats->build.qualified_rows = build->list_id->tuple_cnt;

#if HASHJOIN_COLLISION_RATE
      if (hash_scan->hash_list_scan_type == HASH_METH_IN_MEM || hash_scan->hash_list_scan_type == HASH_METH_HYBRID)
	{
	  stats->collision_rate = (double) hash_scan->memory.hash_table->ncollisions / build->list_id->tuple_cnt;
	}
      else
	{
	  stats->collision_rate = 0;
	}
#endif /* HASHJOIN_COLLISION_RATE */
    }

  /* qfile_close_scan is called by the caller. */

  if (scan_code == S_ERROR || error != NO_ERROR)
    {
      error = (error == NO_ERROR) ? er_errid () : error;
      goto error_exit;
    }

  if (hash_scan->hash_list_scan_type == HASH_METH_HASH_FILE
      && hls_spill_finalize (thread_p, hash_scan->spill.hash_table) != NO_ERROR)
    {
      error = er_errid ();
      goto error_exit;
    }

#if HASHJOIN_DUMP_HASH_TABLE
  if (build->list_id->tuple_cnt <= DUMP_HASH_TABLE_LIMIT)
    {
#if defined (SERVER_MODE)
      pthread_mutex_lock (&manager->dump_hash_table_mutex);
#endif /* defined (SERVER_MODE) */
      HJOIN_DUMP_HASH_TABLE (thread_p, hash_scan, build->list_id);
#if defined (SERVER_MODE)
      pthread_mutex_unlock (&manager->dump_hash_table_mutex);
#endif /* defined (SERVER_MODE) */
    }
#endif /* HASHJOIN_DUMP_HASH_TABLE */

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;

error_exit:
  if (error == NO_ERROR || er_errid () == NO_ERROR)
    {
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
    }

  return error;
}

/*
 * hjoin_build_key() -
 *   return: Error code (NO_ERROR if successful, error code otherwise)
 *   thread_p(in): Thread entry.
 *   hash_scan(in): Hash scan structure used for hash table operations.
 *   list_scan_id(in): Scan identifier for the build input.
 *   tuple_record(in): Tuple to be inserted into the hash table
 */
static int
hjoin_build_key (THREAD_ENTRY * thread_p, HASH_LIST_SCAN * hash_scan, QFILE_LIST_SCAN_ID * list_scan_id,
		 QFILE_TUPLE_RECORD * tuple_record)
{
  HASH_SCAN_VALUE *hash_value = NULL;

  assert (thread_p != NULL);
  assert (hash_scan != NULL);
  assert (list_scan_id != NULL);
  assert (tuple_record != NULL && tuple_record->tpl != NULL);

  switch (hash_scan->hash_list_scan_type)
    {
    case HASH_METH_IN_MEM:
      assert (hash_scan->memory.hash_table != NULL);

      /* #144 P3 D2: bump-allocate value+tuple from the build arena when present
       * (falls back to per-entry alloc if arena create OOM'd). */
      hash_value = (hash_scan->memory.value_arena != NULL)
	? qdata_alloc_hscan_value_arena (hash_scan->memory.value_arena, tuple_record->tpl)
	: qdata_alloc_hscan_value (thread_p, tuple_record->tpl);
      if (hash_value == NULL)
	{
	  assert_release_error (er_errid () != NO_ERROR);
	  return er_errid ();
	}

      if (mht_put_hls (hash_scan->memory.hash_table, (void *) &hash_scan->curr_hash_key, (void *) hash_value) == NULL)
	{
	  /* arena-owned values are reclaimed en masse in hjoin_scan_clear; only a
	   * per-entry-allocated value is freed here. */
	  if (hash_scan->memory.value_arena == NULL)
	    {
	      qdata_free_hscan_value (thread_p, hash_value);
	    }

	  assert_release_error (er_errid () != NO_ERROR);
	  return er_errid ();
	}
      break;

    case HASH_METH_HYBRID:
      assert (hash_scan->memory.hash_table != NULL);

      hash_value = qdata_alloc_hscan_value_OID (thread_p, list_scan_id);
      if (hash_value == NULL)
	{
	  assert_release_error (er_errid () != NO_ERROR);
	  return er_errid ();
	}

      if (mht_put_hls (hash_scan->memory.hash_table, (void *) &hash_scan->curr_hash_key, (void *) hash_value) == NULL)
	{
	  qdata_free_hscan_value (thread_p, hash_value);

	  assert_release_error (er_errid () != NO_ERROR);
	  return er_errid ();
	}
      break;

    case HASH_METH_HASH_FILE:
      assert (hash_scan->spill.hash_table != NULL);

      /* batch spill: backing-aware SIMPLE_POS value (TAPE/SPILL/VPID) */
      {
	QFILE_TUPLE_SIMPLE_POS spill_pos;

	qdata_save_hscan_pos (list_scan_id, &spill_pos);
	if (hls_spill_insert (thread_p, hash_scan->spill.hash_table, hash_scan->curr_hash_key, &spill_pos) != NO_ERROR)
	  {
	    assert_release_error (er_errid () != NO_ERROR);
	    return er_errid ();
	  }
      }
      break;

    case HASH_METH_NOT_USE:
      [[fallthrough]];
    default:
      /* impossible case */
      assert_release_error (false);
      return er_errid ();
    }

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;
}

/*
 * hjoin_probe() -
 *   return: Error code (NO_ERROR if successful, error code otherwise)
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   context(in): Hash join context containing per-partition state.
 */
static int
hjoin_probe (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context)
{
  QFILE_LIST_ID *list_id = NULL;
  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (context != NULL);
  assert (context->list_id == NULL);

#if defined (SERVER_MODE)
  if (context->status == HASHJOIN_STATUS_PARALLEL_PROBE)
    {
      assert (context == &manager->single_context);

      // *INDENT-OFF*
      error = parallel_query::hash_join::probe_prepare (*thread_p, manager);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}

      error = parallel_query::hash_join::probe_execute (*thread_p, manager);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
      // *INDENT-ON*
    }
  else
#endif /* defined (SERVER_MODE) */
    {
      list_id = qfile_open_list (thread_p, &manager->type_list, NULL, manager->query_id, manager->qlist_flag, NULL);
      if (list_id == NULL)
	{
	  goto error_exit;
	}

      if (IS_OUTER_JOIN_TYPE (manager->join_type))
	{
	  error = hjoin_outer_probe (thread_p, manager, context, list_id);
	}
      else
	{
	  error = hjoin_inner_probe (thread_p, manager, context, list_id);
	}

      qfile_close_list (thread_p, list_id);
      context->list_id = list_id;
    }

cleanup:
  /* Check if qfile_close_list was called */
  assert (list_id == NULL || list_id->last_pgptr == NULL);

  return error;

error_exit:
  if (list_id != NULL)
    {
      qfile_close_list (thread_p, list_id);
      qfile_destroy_list (thread_p, list_id);
      QFILE_FREE_AND_INIT_LIST_ID (list_id);
    }

  if (error == NO_ERROR || er_errid () == NO_ERROR)
    {
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
    }

  goto cleanup;
}

/*
 * hjoin_inner_probe() -
 *   return: Error code (NO_ERROR if successful, error code otherwise)
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   context(in): Hash join context containing per-partition state.
 *   list_id(in/out): List identifier containing the join result.
 */
static int
hjoin_inner_probe (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context,
		   QFILE_LIST_ID * list_id)
{
  QFILE_TUPLE_RECORD overflow_record = { NULL, 0 };
  SCAN_CODE scan_code;
  bool need_skip_next = false;

  HASHJOIN_FETCH_INFO *outer, *inner;
  HASHJOIN_FETCH_INFO *build = NULL, *probe = NULL;
  QFILE_TUPLE tuple_value;

  HASH_LIST_SCAN *hash_scan;
  HASH_METHOD hash_method;
  HASH_SCAN_KEY *key, *found_key;

  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (context != NULL);
  assert (list_id != NULL);

  HASHJOIN_STATS *stats = context->stats;
  HASHJOIN_START_STATS start_stats = HASHJOIN_START_STATS_INITIALIZER;
#if HASHJOIN_PROFILE_TIME
  HASHJOIN_START_STATS profile_start_stats = HASHJOIN_START_STATS_INITIALIZER;
#endif /* HASHJOIN_PROFILE_TIME */
  assert (!thread_is_on_trace (thread_p) || stats != NULL);

  outer = &context->outer;
  inner = &context->inner;
  assert (outer->list_scan_id.status != S_CLOSED);
  assert (inner->list_scan_id.status != S_CLOSED);

  build = context->build;
  probe = context->probe;
  assert (build != NULL);
  assert (probe != NULL);

  // *INDENT-OFF*
  probe->tuple_record = { NULL, 0 };
  build->tuple_record = { NULL, 0 };
  // *INDENT-ON*

  hash_scan = &context->hash_scan;

  hash_method = hash_scan->hash_list_scan_type;
  assert (hash_method != HASH_METH_NOT_USE);

  key = hash_scan->temp_key;
  found_key = hash_scan->temp_new_key;
  assert (key != NULL);
  assert (found_key != NULL);

  if (thread_is_on_trace (thread_p))
    {
      hjoin_trace_start (thread_p, &start_stats);
    }

  while ((scan_code = qfile_scan_list_next (thread_p, &probe->list_scan_id, &probe->tuple_record, PEEK)) == S_SUCCESS)
    {
      HJOIN_PRINT_TUPLE (probe->list_id, probe->tuple_record.tpl, HASHJOIN_PRINT_READ_KEY);

      if (manager->context_cnt == 0)	/* HASHJOIN_STATUS_SINGLE */
	{
	  HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_PROBE_FETCH);
	  error = hjoin_fetch_key (thread_p, probe, &probe->tuple_record, key, NULL /* compare_key */ ,
				   &need_skip_next);
	  HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_PROBE_FETCH);

	  if (error != NO_ERROR)
	    {
	      break;		/* error_exit */
	    }
	  else if (need_skip_next)
	    {
	      need_skip_next = false;	/* init */
	      continue;
	    }
	  else
	    {
	      /* fall through */
	    }

	  HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_PROBE_HASH);
	  hash_scan->curr_hash_key = qdata_hash_scan_key (key, UINT_MAX, hash_method);
	  HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_PROBE_HASH);
	}
      else
	{
	  HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_PROBE_FETCH);
	  tuple_value = probe->tuple_record.tpl + QFILE_TUPLE_LENGTH_SIZE;
	  assert (QFILE_GET_TUPLE_VALUE_FLAG (tuple_value) == V_BOUND);
	  assert (QFILE_GET_TUPLE_VALUE_LENGTH (tuple_value) == MAX_ALIGNMENT);

	  tuple_value += QFILE_TUPLE_VALUE_HEADER_LENGTH;
	  HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_PROBE_FETCH);

	  HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_PROBE_HASH);
	  hash_scan->curr_hash_key = (UINT32) OR_GET_INT (tuple_value);
	  HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_PROBE_HASH);
	}

      do
	{
	  HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_PROBE_SEARCH);
	  error = hjoin_probe_key (thread_p, hash_scan, &build->list_scan_id, &build->tuple_record);
	  HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_PROBE_SEARCH);

	  if (error != NO_ERROR)
	    {
	      break;		/* error_exit */
	    }

	  if (build->tuple_record.tpl == NULL)
	    {
	      break;		/* not found */
	    }

	  if (thread_is_on_trace (thread_p))
	    {
	      stats->probe.read_keys++;	/* found */
	    }

	  if (manager->context_cnt != 0)	/* HASHJOIN_STATUS_PARTITION or HASHJOIN_STATUS_PARALLEL */
	    {
	      HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_PROBE_FETCH);
	      error = hjoin_fetch_key (thread_p, probe, &probe->tuple_record, key, NULL /* compare_key */ ,
				       &need_skip_next);
	      HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_PROBE_FETCH);

	      if (error != NO_ERROR)
		{
		  break;	/* error_exit */
		}
	      else if (need_skip_next)
		{
		  need_skip_next = false;	/* init */

		  /* impossible case */
		  assert_release_error (false);
		  error = er_errid ();
		  break;
		}
	      else
		{
		  /* fall through */
		}
	    }

	  HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_PROBE_MATCH);
	  error = hjoin_fetch_key (thread_p, build, &build->tuple_record, found_key, key /* compare_key */ ,
				   &need_skip_next);
	  HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_PROBE_MATCH);

	  if (error != NO_ERROR)
	    {
	      break;		/* error_exit */
	    }
	  else if (need_skip_next)
	    {
	      HJOIN_PRINT_TUPLE (build->list_id, build->tuple_record.tpl, HASHJOIN_PRINT_NOT_MATCHED_KEY);

	      need_skip_next = false;	/* init */
	      continue;
	    }
	  else
	    {
	      /* fall through */
	    }

	  HJOIN_PRINT_TUPLE (build->list_id, build->tuple_record.tpl, HASHJOIN_PRINT_QUALIFIED_KEY);

	  HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_PROBE_ADD);
	  error =
	    hjoin_merge_tuple_to_list_id (thread_p, list_id, &outer->tuple_record, &inner->tuple_record,
					  manager->merge_info, &overflow_record);
	  HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_PROBE_ADD);

	  if (error != NO_ERROR)
	    {
	      break;		/* error_exit */
	    }
	}
      while (true);

      if (error != NO_ERROR)
	{
	  break;		/* error_exit */
	}
    }				/* while (qfile_scan_list_next (list_scan_id)) */

  if (thread_is_on_trace (thread_p))
    {
      hjoin_trace_end (thread_p, &stats->probe, &start_stats);
      stats->probe.read_rows = probe->list_id->tuple_cnt;
      stats->probe.qualified_rows = list_id->tuple_cnt;
    }

  /* qfile_close_scan is called by the caller. */

  if (scan_code == S_ERROR || error != NO_ERROR)
    {
      error = (error == NO_ERROR) ? er_errid () : error;
      goto error_exit;
    }

  ASSERT_NO_ERROR_OR_INTERRUPTED ();

cleanup:
  if (overflow_record.tpl != NULL)
    {
      db_private_free_and_init (thread_p, overflow_record.tpl);
    }

  return error;

error_exit:
  if (error == NO_ERROR || er_errid () == NO_ERROR)
    {
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
    }

  goto cleanup;
}

/*
 * hjoin_outer_probe_fill_empty () - append an outer-join fill-empty (unmatched) row
 *   return: Error code (NO_ERROR if successful, error code otherwise)
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager (merge_info).
 *   context(in): Hash join context (probe / outer / inner fill records, stats).
 *   list_id(in/out): Result list to append the fill-empty row to.
 *   overflow_record(in/out): Scratch overflow tuple record.
 */
static int
hjoin_outer_probe_fill_empty (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context,
			      QFILE_LIST_ID * list_id, QFILE_TUPLE_RECORD * overflow_record)
{
  HASHJOIN_FETCH_INFO *outer = &context->outer;
  HASHJOIN_FETCH_INFO *inner = &context->inner;
  HASHJOIN_FETCH_INFO *probe = context->probe;
  HASHJOIN_STATS *stats = context->stats;
#if HASHJOIN_PROFILE_TIME
  HASHJOIN_START_STATS profile_start_stats = HASHJOIN_START_STATS_INITIALIZER;
#endif /* HASHJOIN_PROFILE_TIME */
  int error;

  HJOIN_PRINT_TUPLE (probe->list_id, probe->tuple_record.tpl, HASHJOIN_PRINT_FILL_EMPTY_KEY);

  HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_PROBE_ADD);
  error =
    hjoin_merge_tuple_to_list_id (thread_p, list_id, outer->fill_record, inner->fill_record, manager->merge_info,
				  overflow_record);
  HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_PROBE_ADD);

  return error;
}

/*
 * hjoin_outer_probe() -
 *   return: Error code (NO_ERROR if successful, error code otherwise)
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 *   context(in): Hash join context containing per-partition state.
 *   list_id(in/out): List identifier containing the join result.
 */
static int
hjoin_outer_probe (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context,
		   QFILE_LIST_ID * list_id)
{
  QFILE_TUPLE_RECORD overflow_record = { NULL, 0 };
  SCAN_CODE scan_code;
  bool need_skip_next = false;
  bool any_record_added;

  HASHJOIN_FETCH_INFO *outer, *inner;
  HASHJOIN_FETCH_INFO *build = NULL, *probe = NULL;
  QFILE_TUPLE tuple_value;

  HASH_LIST_SCAN *hash_scan;
  HASH_METHOD hash_method;
  HASH_SCAN_KEY *key, *found_key;

  int error = NO_ERROR, save_error = NO_ERROR;

  assert (thread_p != NULL);
  assert (manager != NULL);
  assert (context != NULL);
  assert (list_id != NULL);

  HASHJOIN_STATS *stats = context->stats;
  HASHJOIN_START_STATS start_stats = HASHJOIN_START_STATS_INITIALIZER;
#if HASHJOIN_PROFILE_TIME
  HASHJOIN_START_STATS profile_start_stats = HASHJOIN_START_STATS_INITIALIZER;
#endif /* HASHJOIN_PROFILE_TIME */
  assert (!thread_is_on_trace (thread_p) || stats != NULL);

  outer = &context->outer;
  inner = &context->inner;
  assert (outer->list_scan_id.status != S_CLOSED);
  assert (inner->list_scan_id.status != S_CLOSED);

  assert (outer->fill_record == NULL || outer->fill_record->tpl == NULL);

  build = context->build;
  probe = context->probe;
  assert (build != NULL);
  assert (probe != NULL);

  // *INDENT-OFF*
  probe->tuple_record = { NULL, 0 };
  build->tuple_record = { NULL, 0 };
  // *INDENT-ON*

  hash_scan = &context->hash_scan;

  hash_method = hash_scan->hash_list_scan_type;
  assert (hash_method != HASH_METH_NOT_USE);

  key = hash_scan->temp_key;
  found_key = hash_scan->temp_new_key;
  assert (key != NULL);
  assert (found_key != NULL);

  if (thread_is_on_trace (thread_p))
    {
      hjoin_trace_start (thread_p, &start_stats);
    }

  while ((scan_code = qfile_scan_list_next (thread_p, &probe->list_scan_id, &probe->tuple_record, PEEK)) == S_SUCCESS)
    {
      HJOIN_PRINT_TUPLE (probe->list_id, probe->tuple_record.tpl, HASHJOIN_PRINT_READ_KEY);

      if (manager->context_cnt == 0)	/* HASHJOIN_STATUS_SINGLE */
	{
	  HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_PROBE_FETCH);
	  error = hjoin_fetch_key (thread_p, probe, &probe->tuple_record, key, NULL /* compare_key */ ,
				   &need_skip_next);
	  HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_PROBE_FETCH);

	  if (error != NO_ERROR)
	    {
	      break;		/* error_exit */
	    }
	  else if (need_skip_next)
	    {
	      error = hjoin_outer_probe_fill_empty (thread_p, manager, context, list_id, &overflow_record);

	      if (error != NO_ERROR)
		{
		  break;	/* error_exit */
		}

	      need_skip_next = false;	/* init */
	      continue;
	    }			/* else if (need_skip_next) */
	  else
	    {
	      /* fall through */
	    }

	  HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_PROBE_HASH);
	  hash_scan->curr_hash_key = qdata_hash_scan_key (key, UINT_MAX, hash_method);
	  HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_PROBE_HASH);
	}
      else
	{
	  HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_PROBE_FETCH);
	  tuple_value = probe->tuple_record.tpl + QFILE_TUPLE_LENGTH_SIZE;
	  assert (QFILE_GET_TUPLE_VALUE_FLAG (tuple_value) == V_BOUND);
	  assert (QFILE_GET_TUPLE_VALUE_LENGTH (tuple_value) == MAX_ALIGNMENT);

	  tuple_value += QFILE_TUPLE_VALUE_HEADER_LENGTH;
	  HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_PROBE_FETCH);

	  HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_PROBE_HASH);
	  hash_scan->curr_hash_key = (UINT32) OR_GET_INT (tuple_value);
	  HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_PROBE_HASH);
	}

      any_record_added = false;

      do
	{
	  HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_PROBE_SEARCH);
	  error = hjoin_probe_key (thread_p, hash_scan, &build->list_scan_id, &build->tuple_record);
	  HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_PROBE_SEARCH);

	  if (error != NO_ERROR)
	    {
	      break;		/* error_exit */
	    }

	  if (build->tuple_record.tpl == NULL)
	    {
	      break;		/* not found */
	    }

	  if (thread_is_on_trace (thread_p))
	    {
	      stats->probe.read_keys++;	/* found */
	    }

	  if (manager->context_cnt != 0)	/* HASHJOIN_STATUS_PARTITION or HASHJOIN_STATUS_PARALLEL */
	    {
	      HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_PROBE_FETCH);
	      error = hjoin_fetch_key (thread_p, probe, &probe->tuple_record, key, NULL /* compare_key */ ,
				       &need_skip_next);
	      HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_PROBE_FETCH);

	      if (error != NO_ERROR)
		{
		  break;	/* error_exit */
		}
	      else if (need_skip_next)
		{
		  need_skip_next = false;	/* init */

		  /* impossible case */
		  assert_release_error (false);
		  save_error = er_errid ();

		  error = hjoin_outer_probe_fill_empty (thread_p, manager, context, list_id, &overflow_record);

		  if (error != NO_ERROR)
		    {
		      break;	/* error_exit */
		    }

		  error = save_error;

		  any_record_added = true;	/* meaningless */
		  break;
		}
	      else
		{
		  /* fall through */
		}
	    }

	  HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_PROBE_MATCH);
	  error = hjoin_fetch_key (thread_p, build, &build->tuple_record, found_key, key /* compare_key */ ,
				   &need_skip_next);
	  HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_PROBE_MATCH);

	  if (error != NO_ERROR)
	    {
	      break;		/* error_exit */
	    }
	  else if (need_skip_next)
	    {
	      HJOIN_PRINT_TUPLE (build->list_id, build->tuple_record.tpl, HASHJOIN_PRINT_NOT_MATCHED_KEY);

	      need_skip_next = false;	/* init */
	      continue;
	    }
	  else
	    {
	      /* fall through */
	    }

	  if (context->during_join_pred != NULL)
	    {
	      DB_LOGICAL ev_res = V_UNKNOWN;

	      HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_PROBE_MATCH);
	      do
		{
		  error =
		    fetch_val_list (thread_p, probe->regu_list_pred, context->val_descr, NULL, NULL,
				    probe->tuple_record.tpl, PEEK);
		  if (error != NO_ERROR)
		    {
		      break;	/* error_exit */
		    }

		  error =
		    fetch_val_list (thread_p, build->regu_list_pred, context->val_descr, NULL, NULL,
				    build->tuple_record.tpl, PEEK);
		  if (error != NO_ERROR)
		    {
		      break;	/* error_exit */
		    }

		  ev_res = eval_pred (thread_p, context->during_join_pred, context->val_descr, NULL);
		  if (ev_res == V_ERROR)
		    {
		      error = ER_FAILED;
		      break;	/* error_exit */
		    }
		}
	      while (false);
	      HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_PROBE_MATCH);

	      if (error != NO_ERROR)
		{
		  break;	/* error_exit */
		}

	      /* Search the next hash entry if additional conditions are not satisfied */
	      if (ev_res != V_TRUE)
		{
		  HJOIN_PRINT_TUPLE (build->list_id, build->tuple_record.tpl, HASHJOIN_PRINT_NOT_QUALIFIED_KEY);
		  assert (need_skip_next == false);
		  continue;
		}
	    }			/* if (context->during_join_pred != NULL) */

	  HJOIN_PRINT_TUPLE (build->list_id, build->tuple_record.tpl, HASHJOIN_PRINT_QUALIFIED_KEY);

	  HJOIN_PROFILE_START (thread_p, &profile_start_stats, HASHJOIN_PROFILE_PROBE_ADD);
	  error =
	    hjoin_merge_tuple_to_list_id (thread_p, list_id, &outer->tuple_record, &inner->tuple_record,
					  manager->merge_info, &overflow_record);
	  HJOIN_PROFILE_END (thread_p, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_PROBE_ADD);

	  if (error != NO_ERROR)
	    {
	      break;		/* error_exit */
	    }

	  any_record_added = true;
	}
      while (true);

      if (error != NO_ERROR)
	{
	  break;		/* error_exit */
	}

      if (!any_record_added)
	{
	  error = hjoin_outer_probe_fill_empty (thread_p, manager, context, list_id, &overflow_record);

	  if (error != NO_ERROR)
	    {
	      break;		/* error_exit */
	    }
	}			/* if (!any_record_added) */
    }				/* while (qfile_scan_list_next (probe_scan_id)) */

  if (thread_is_on_trace (thread_p))
    {
      hjoin_trace_end (thread_p, &stats->probe, &start_stats);
      stats->probe.read_rows = probe->list_id->tuple_cnt;
      stats->probe.qualified_rows = list_id->tuple_cnt;
    }

  /* qfile_close_scan is called by the caller. */

  if (scan_code == S_ERROR || error != NO_ERROR)
    {
      error = (error == NO_ERROR) ? er_errid () : error;
      goto error_exit;
    }

  ASSERT_NO_ERROR_OR_INTERRUPTED ();

cleanup:
  if (overflow_record.tpl != NULL)
    {
      db_private_free_and_init (thread_p, overflow_record.tpl);
    }

  return error;

error_exit:
  if (error == NO_ERROR || er_errid () == NO_ERROR)
    {
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
    }

  goto cleanup;
}

/*
 * hjoin_probe_key() -
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry.
 *   hash_scan(in): Hash scan structure used for hash table operations.
 *   list_scan_id(in): Scan identifier for the probe input.
 *   tuple_record(in/out): Tuple found in the hash table.
 */
int
hjoin_probe_key (THREAD_ENTRY * thread_p, HASH_LIST_SCAN * hash_scan, QFILE_LIST_SCAN_ID * list_scan_id,
		 QFILE_TUPLE_RECORD * tuple_record)
{
  HASH_SCAN_VALUE *hash_value = NULL;
  QFILE_TUPLE_SIMPLE_POS spill_pos;
  EH_SEARCH eh_search;
  QFILE_TUPLE_POSITION tuple_position;
  SCAN_CODE scan_code;

  assert (thread_p != NULL);
  assert (hash_scan != NULL);
  assert (list_scan_id != NULL);
  assert (tuple_record != NULL);

  switch (hash_scan->hash_list_scan_type)
    {
    case HASH_METH_IN_MEM:
      assert (hash_scan->memory.hash_table != NULL);

      if (tuple_record->tpl == NULL)
	{
	  hash_value =
	    (HASH_SCAN_VALUE *) mht_get_hls (hash_scan->memory.hash_table, (void *) &hash_scan->curr_hash_key,
					     (void **) &hash_scan->memory.curr_hash_entry);
	}
      else
	{
	  hash_value =
	    (HASH_SCAN_VALUE *) mht_get_next_hls (hash_scan->memory.hash_table, (void *) &hash_scan->curr_hash_key,
						  (void **) &hash_scan->memory.curr_hash_entry);
	}

      if (hash_value != NULL)
	{
	  tuple_record->tpl = hash_value->tuple;
	  tuple_record->size = QFILE_GET_TUPLE_VALUE_LENGTH (tuple_record->tpl);
	}
      else
	{
	  /* not found */
	  tuple_record->tpl = NULL;
	  tuple_record->size = 0;
	}
      break;			/* HASH_METH_IN_MEM */

    case HASH_METH_HYBRID:
      assert (hash_scan->memory.hash_table != NULL);

      if (tuple_record->tpl == NULL)
	{
	  hash_value =
	    (HASH_SCAN_VALUE *) mht_get_hls (hash_scan->memory.hash_table, (void *) &hash_scan->curr_hash_key,
					     (void **) &hash_scan->memory.curr_hash_entry);
	}
      else
	{
	  hash_value =
	    (HASH_SCAN_VALUE *) mht_get_next_hls (hash_scan->memory.hash_table,
						  (void *) &hash_scan->curr_hash_key,
						  (void **) &hash_scan->memory.curr_hash_entry);
	}

      if (hash_value != NULL)
	{
	  MAKE_TUPLE_POSTION (tuple_position, hash_value->pos, list_scan_id);
	  hjoin_debug_random_probe_reads.fetch_add (1, std::memory_order_relaxed);
	  scan_code = qfile_jump_scan_tuple_position (thread_p, list_scan_id, &tuple_position, tuple_record, PEEK);
	  if (scan_code != S_SUCCESS)
	    {
	      assert_release_error (er_errid () != NO_ERROR);
	      return er_errid ();
	    }
	}
      else
	{
	  /* not found */
	  tuple_record->tpl = NULL;
	  tuple_record->size = 0;
	}
      break;			/* HASH_METH_HYBRID */

    case HASH_METH_HASH_FILE:
      assert (hash_scan->spill.hash_table != NULL);

      /* batch-spill probe */
      if (tuple_record->tpl == NULL)
	{
	  eh_search = hls_spill_search (thread_p, hash_scan->spill.hash_table, hash_scan->spill.cursor,
					hash_scan->curr_hash_key, &spill_pos);
	}
      else
	{
	  eh_search = hls_spill_search_next (thread_p, hash_scan->spill.hash_table, hash_scan->spill.cursor,
					     &spill_pos);
	}

      if (eh_search == EH_KEY_FOUND)
	{
	  MAKE_TUPLE_POSTION (tuple_position, &spill_pos, list_scan_id);
	  hjoin_debug_random_probe_reads.fetch_add (1, std::memory_order_relaxed);
	  scan_code = qfile_jump_scan_tuple_position (thread_p, list_scan_id, &tuple_position, tuple_record, PEEK);
	  if (scan_code != S_SUCCESS)
	    {
	      assert_release_error (er_errid () != NO_ERROR);
	      return er_errid ();
	    }
	}
      else if (eh_search == EH_KEY_NOTFOUND)
	{
	  /* not found */
	  tuple_record->tpl = NULL;
	  tuple_record->size = 0;
	}
      else
	{
	  assert_release_error (er_errid () != NO_ERROR);
	  return er_errid ();
	}
      break;			/* HASH_METH_HASH_FILE */

    case HASH_METH_NOT_USE:
      /* fall through */
    default:
      /* impossible case */
      assert_release_error (false);
      return er_errid ();
    }

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;
}

/*
 * hjoin_merge_tuple_to_list_id() -
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry.
 *   list_id(in/out): List identifier to be merged.
 *   outer_record(in): Outer tuple to merge. (can be NULL).
 *   inner_record(in): Inner tuple to merge. (can be NULL).
 *   merge_info(in): Information used to merge the joined result.
 *   overflow_record(in/out): Space used for merging tuples too large to fit on a single page.
 */
int
hjoin_merge_tuple_to_list_id (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id,
			      QFILE_TUPLE_RECORD * outer_record,
			      QFILE_TUPLE_RECORD * inner_record, QFILE_LIST_MERGE_INFO * merge_info,
			      QFILE_TUPLE_RECORD * overflow_record)
{
  QFILE_TUPLE_DESCRIPTOR *tuple_descriptor;
  int max_record_size, max_unbound_size;

  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (list_id != NULL);
  assert (outer_record != NULL || inner_record != NULL);
  assert (merge_info != NULL);
  assert (overflow_record != NULL);

  max_unbound_size = QFILE_TUPLE_VALUE_HEADER_SIZE * (merge_info->ls_pos_cnt);

  max_record_size = (outer_record != NULL) ? QFILE_GET_TUPLE_LENGTH (outer_record->tpl) : max_unbound_size;
  max_record_size += (inner_record != NULL) ? QFILE_GET_TUPLE_LENGTH (inner_record->tpl) : max_unbound_size;
  max_record_size = DB_ALIGN (max_record_size, MAX_ALIGNMENT);

  if (max_record_size < QFILE_MAX_TUPLE_SIZE_IN_PAGE)
    {
      tuple_descriptor = &list_id->tpl_descr;
      tuple_descriptor->tpl_size = max_record_size;
      tuple_descriptor->tplrec1 = outer_record;
      tuple_descriptor->tplrec2 = inner_record;
      tuple_descriptor->merge_info = merge_info;

      error = qfile_generate_tuple_into_list (thread_p, list_id, T_MERGE);
    }
  else
    {
      error = hjoin_merge_tuple (thread_p, outer_record, inner_record, merge_info, overflow_record);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}

      error = qfile_add_tuple_to_list (thread_p, list_id, overflow_record->tpl);
    }

  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;

error_exit:
  assert_release_error (er_errid () != NO_ERROR);
  return er_errid ();
}

/*
 * hjoin_merge_tuple() -
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry.
 *   outer_record(in): Outer tuple to merge. (can be NULL).
 *   inner_record(in): Inner tuple to merge. (can be NULL).
 *   merge_info(in): Information used to merge the joined result.
 *   overflow_record(in/out): Space used for merging tuples too large to fit on a single page.
 */
static int
hjoin_merge_tuple (THREAD_ENTRY * thread_p, QFILE_TUPLE_RECORD * outer_record,
		   QFILE_TUPLE_RECORD * inner_record, QFILE_LIST_MERGE_INFO * merge_info,
		   QFILE_TUPLE_RECORD * overflow_record)
{
  QFILE_TUPLE_RECORD *tuple_record;
  QFILE_TUPLE outer_record_end, inner_record_end, tuple_record_end;
  QFILE_TUPLE tuple_value;
  INT32 unbound_value[2] = { 0, 0 };	/* QFILE_TUPLE_VALUE_HEADER */
  int available_size, realloc_size, offset, value_size;
  int pos_index, value_index, skip_index;

  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (outer_record != NULL || inner_record != NULL);
  assert (merge_info != NULL);
  assert (overflow_record != NULL);

  QFILE_PUT_TUPLE_VALUE_FLAG ((char *) unbound_value, V_UNBOUND);
  QFILE_PUT_TUPLE_VALUE_LENGTH ((char *) unbound_value, 0);

  outer_record_end = outer_record->tpl + QFILE_GET_TUPLE_LENGTH (outer_record->tpl);
  inner_record_end = inner_record->tpl + QFILE_GET_TUPLE_LENGTH (inner_record->tpl);

  offset = QFILE_TUPLE_LENGTH_SIZE;

  for (pos_index = 0; pos_index < merge_info->ls_pos_cnt; pos_index++)
    {
      if (merge_info->ls_outer_inner_list[pos_index] == QFILE_OUTER_LIST)
	{
	  tuple_record = outer_record;
	  tuple_record_end = outer_record_end;
	}
      else if (merge_info->ls_outer_inner_list[pos_index] == QFILE_INNER_LIST)
	{
	  tuple_record = inner_record;
	  tuple_record_end = inner_record_end;
	}
      else
	{
	  /* impossible case */
	  assert_release_error (false);
	  return er_errid ();
	}

      if (tuple_record != NULL)
	{
	  value_index = merge_info->ls_pos_list[pos_index];

	  tuple_value = tuple_record->tpl + QFILE_TUPLE_LENGTH_SIZE;
	  for (skip_index = 0; skip_index < value_index; skip_index++)
	    {
	      tuple_value += QFILE_TUPLE_VALUE_HEADER_SIZE + QFILE_GET_TUPLE_VALUE_LENGTH (tuple_value);
	    }

	  if (tuple_value >= tuple_record_end)
	    {
	      /* impossible case */
	      assert (false);
	      error = ER_TF_BUFFER_OVERFLOW;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	      return error;
	    }
	}
      else
	{
	  tuple_value = (char *) unbound_value;
	}

      value_size = QFILE_TUPLE_VALUE_HEADER_SIZE + QFILE_GET_TUPLE_VALUE_LENGTH (tuple_value);
      available_size = overflow_record->size - offset;

      if (value_size > available_size)
	{
	  realloc_size = CEIL_PTVDIV (overflow_record->size + (value_size - available_size), DB_PAGESIZE) * DB_PAGESIZE;

	  /* overflow_record is managed and cleaned up by the caller. */
	  error = qfile_reallocate_tuple (overflow_record, realloc_size);
	  if (error != NO_ERROR)
	    {
	      assert_release_error (er_errid () != NO_ERROR);
	      return er_errid ();
	    }
	}

      memcpy (overflow_record->tpl + offset, tuple_value, value_size);
      offset += value_size;
    }				/* for (pos_index < merge_info->ls_pos_cnt) */

  QFILE_PUT_TUPLE_LENGTH (overflow_record->tpl, offset);

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;
}

/*
 * hjoin_trace_start() -
 *   return: None.
 *   thread_p(in): Thread entry.
 *   start_stats(in/out): Profiling data captured at the start of the step.
 */
void
hjoin_trace_start (THREAD_ENTRY * thread_p, HASHJOIN_START_STATS * start_stats)
{
  assert (thread_p != NULL);
  assert (start_stats != NULL);

  tsc_getticks (&start_stats->tick);
  start_stats->fetches = perfmon_get_from_statistic (thread_p, PSTAT_PB_NUM_FETCHES);
  start_stats->ioreads = perfmon_get_from_statistic (thread_p, PSTAT_PB_NUM_IOREADS);
}

/*
 * hjoin_trace_end() -
 *   return: None.
 *   thread_p(in): Thread entry.
 *   stats(in/out): Profiling data to accumulate.
 *   start_stats(in): Profiling data captured at the start of the step.
 */
void
hjoin_trace_end (THREAD_ENTRY * thread_p, HASHJOIN_INPUT_STATS * stats, HASHJOIN_START_STATS * start_stats)
{
  TSC_TICKS end_tick;
  TSCTIMEVAL tv_diff;

  assert (thread_p != NULL);
  assert (stats != NULL);
  assert (start_stats != NULL);

  tsc_getticks (&end_tick);
  tsc_elapsed_time_usec (&tv_diff, end_tick, start_stats->tick);

  TSC_ADD_TIMEVAL (stats->elapsed_time, tv_diff);
  stats->fetches += perfmon_get_from_statistic (thread_p, PSTAT_PB_NUM_FETCHES) - start_stats->fetches;
  stats->ioreads += perfmon_get_from_statistic (thread_p, PSTAT_PB_NUM_IOREADS) - start_stats->ioreads;
}

#if HASHJOIN_PROFILE_TIME
/*
 * hjoin_profile_start() -
 *   return: None.
 *   thread_p(in): Thread entry.
 *   start_stats(in/out): Profiling data captured at the start of the step.
 *   step(in): Hash join profiling step to measure.
 */
void
hjoin_profile_start (THREAD_ENTRY * thread_p, HASHJOIN_START_STATS * start_stats, HASHJOIN_PROFILE_STEP step)
{
  assert (thread_p != NULL);
  assert (start_stats != NULL);

  tsc_getticks (&start_stats->tick);
  start_stats->step = step;

  switch (step)
    {
    case HASHJOIN_PROFILE_BUILD_FETCH:
    case HASHJOIN_PROFILE_BUILD_HASH:
    case HASHJOIN_PROFILE_BUILD_INSERT:
    case HASHJOIN_PROFILE_PROBE_FETCH:
    case HASHJOIN_PROFILE_PROBE_HASH:
    case HASHJOIN_PROFILE_PROBE_SEARCH:
    case HASHJOIN_PROFILE_PROBE_MATCH:
    case HASHJOIN_PROFILE_PROBE_ADD:
      /* nothing to do */
      break;

    case HASHJOIN_PROFILE_MERGE:
      start_stats->fetches = perfmon_get_from_statistic (thread_p, PSTAT_PB_NUM_FETCHES);
      start_stats->ioreads = perfmon_get_from_statistic (thread_p, PSTAT_PB_NUM_IOREADS);
      break;

    default:
      /* impossible case */
      assert (false);
      break;
    }				/* switch (step) */
}
#endif /* HASHJOIN_PROFILE_TIME */

#if HASHJOIN_PROFILE_TIME
/*
 * hjoin_profile_end() -
 *   return: None.
 *   thread_p(in): Thread entry.
 *   stats(in/out): Profiling data to accumulate.
 *   start_stats(in): Profiling data captured at the start of the step.
 *   step(in): Hash join profiling step being measured.
 */
void
hjoin_profile_end (THREAD_ENTRY * thread_p, HASHJOIN_PROFILE_STATS * stats,
		   HASHJOIN_START_STATS * start_stats, HASHJOIN_PROFILE_STEP step)
{
  TSC_TICKS end_tick;
  TSCTIMEVAL tv_diff;

  assert (thread_p != NULL);
  assert (stats != NULL);
  assert (start_stats != NULL);
  assert (start_stats->step == step);

  tsc_getticks (&end_tick);
  tsc_elapsed_time_usec (&tv_diff, end_tick, start_stats->tick);

  switch (step)
    {
    case HASHJOIN_PROFILE_BUILD_FETCH:
      TSC_ADD_TIMEVAL (stats->build.fetch, tv_diff);
      break;

    case HASHJOIN_PROFILE_BUILD_HASH:
      TSC_ADD_TIMEVAL (stats->build.hash, tv_diff);
      break;

    case HASHJOIN_PROFILE_BUILD_INSERT:
      TSC_ADD_TIMEVAL (stats->build.insert, tv_diff);
      break;

    case HASHJOIN_PROFILE_PROBE_FETCH:
      TSC_ADD_TIMEVAL (stats->probe.fetch, tv_diff);
      break;

    case HASHJOIN_PROFILE_PROBE_HASH:
      TSC_ADD_TIMEVAL (stats->probe.hash, tv_diff);
      break;

    case HASHJOIN_PROFILE_PROBE_SEARCH:
      TSC_ADD_TIMEVAL (stats->probe.search, tv_diff);
      break;

    case HASHJOIN_PROFILE_PROBE_MATCH:
      TSC_ADD_TIMEVAL (stats->probe.match, tv_diff);
      break;

    case HASHJOIN_PROFILE_PROBE_ADD:
      TSC_ADD_TIMEVAL (stats->probe.add, tv_diff);
      break;

    case HASHJOIN_PROFILE_MERGE:
      TSC_ADD_TIMEVAL (stats->merge.elapsed_time, tv_diff);
      stats->merge.fetches += perfmon_get_from_statistic (thread_p, PSTAT_PB_NUM_FETCHES) - start_stats->fetches;
      stats->merge.ioreads += perfmon_get_from_statistic (thread_p, PSTAT_PB_NUM_IOREADS) - start_stats->ioreads;
      break;

    default:
      /* impossible case */
      assert (false);
      break;
    }				/* switch (step) */
}
#endif /* HASHJOIN_PROFILE_TIME */

/*
 * hjoin_trace_merge_stats() -
 *   return: None.
 *   stats(in/out): Profiling data to be merged.
 *   context_stats(in): Profiling data per-partition.
 */
void
hjoin_trace_merge_stats (HASHJOIN_STATS * stats, HASHJOIN_STATS * context_stats, HASHJOIN_STATUS status)
{
  assert (stats != NULL);
  assert (context_stats != NULL);

  if (stats == NULL || context_stats == NULL)
    {
      /* impossible case */
      assert (false);
      return;
    }

  if (status != HASHJOIN_STATUS_PARALLEL && status != HASHJOIN_STATUS_PARALLEL_PROBE)
    {
      TSC_ADD_TIMEVAL (stats->build.elapsed_time, context_stats->build.elapsed_time);
    }
  stats->build.fetches += context_stats->build.fetches;
  stats->build.ioreads += context_stats->build.ioreads;
  stats->build.read_rows += context_stats->build.read_rows;
  stats->build.read_keys += context_stats->build.read_keys;
  stats->build.qualified_rows += context_stats->build.qualified_rows;

#if HASHJOIN_COLLISION_RATE
  stats->collision_rate = MAX (stats->collision_rate, context_stats->collision_rate);
#endif /* HASHJOIN_COLLISION_RATE */

#if HASHJOIN_PROFILE_TIME
  TSC_ADD_TIMEVAL (stats->profile.build.fetch, context_stats->profile.build.fetch);
  TSC_ADD_TIMEVAL (stats->profile.build.hash, context_stats->profile.build.hash);
  TSC_ADD_TIMEVAL (stats->profile.build.insert, context_stats->profile.build.insert);
#endif /* HASHJOIN_PROFILE_TIME */

  switch (context_stats->hash_method)
    {
    case HASH_METH_IN_MEM:
      stats->use_hash_memory = true;
      break;

    case HASH_METH_HYBRID:
      stats->use_hash_hybrid = true;
      break;

    case HASH_METH_HASH_FILE:
      stats->use_hash_file = true;
      break;

    case HASH_METH_NOT_USE:
      stats->use_hash_skip = true;
      break;

    default:
      /* impossible case */
      assert (false);
      return;
    }

  if (status != HASHJOIN_STATUS_PARALLEL && status != HASHJOIN_STATUS_PARALLEL_PROBE)
    {
      TSC_ADD_TIMEVAL (stats->probe.elapsed_time, context_stats->probe.elapsed_time);
    }
  stats->probe.fetches += context_stats->probe.fetches;
  stats->probe.ioreads += context_stats->probe.ioreads;
  stats->probe.read_rows += context_stats->probe.read_rows;
  stats->probe.read_keys += context_stats->probe.read_keys;
  stats->probe.qualified_rows += context_stats->probe.qualified_rows;

#if HASHJOIN_PROFILE_TIME
  TSC_ADD_TIMEVAL (stats->profile.probe.fetch, context_stats->profile.probe.fetch);
  TSC_ADD_TIMEVAL (stats->profile.probe.hash, context_stats->profile.probe.hash);
  TSC_ADD_TIMEVAL (stats->profile.probe.search, context_stats->profile.probe.search);
  TSC_ADD_TIMEVAL (stats->profile.probe.match, context_stats->profile.probe.match);
  TSC_ADD_TIMEVAL (stats->profile.probe.add, context_stats->profile.probe.add);
#endif /* HASHJOIN_PROFILE_TIME */
}

/*
 * hjoin_trace_get_worker_stats() -
 *   return: Parallel worker stats at index.
 *   manager(in): Hash join manager containing shared state.
 *   index(in): Parallel worker index.
 */
UINT64 *
hjoin_trace_get_worker_stats (HASHJOIN_MANAGER * manager, int index)
{
  assert (manager != NULL);
  assert (manager->num_parallel_threads > 1);
  assert (index >= 0 && index < manager->num_parallel_threads);

  if (manager->px_worker_stats == NULL)
    {
      assert (false);
      return NULL;
    }

  /* immutable */
  static const int n_stat_values = perfmon_get_number_of_statistic_values ();

  return manager->px_worker_stats + index * n_stat_values;
}

/*
 * hjoin_trace_drain_worker_stats() -
 *   return: None.
 *   thread_p(in): Thread entry.
 *   manager(in): Hash join manager containing shared state.
 */
void
hjoin_trace_drain_worker_stats (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager)
{
  UINT64 *worker_stats;
  int task_cnt, task_index;
  int stats_cnt, stats_index;

  assert (thread_p->m_px_stats != NULL);
  assert (manager != NULL);
  assert (manager->num_parallel_threads > 1);
  assert (manager->px_worker_stats != NULL);

  /* immutable */
  static const int offsets[] = {
    pstat_Metadata[PSTAT_PB_NUM_FETCHES].start_offset,
    pstat_Metadata[PSTAT_PB_NUM_IOREADS].start_offset,
    pstat_Metadata[PSTAT_PB_PAGE_FIX_ACQUIRE_TIME_10USEC].start_offset
  };

  task_cnt = manager->num_parallel_threads;
  stats_cnt = sizeof (offsets) / sizeof (offsets[0]);

  for (task_index = 0; task_index < task_cnt; task_index++)
    {
      worker_stats = hjoin_trace_get_worker_stats (manager, task_index);

      for (stats_index = 0; stats_index < stats_cnt; stats_index++)
	{
	  const int offset = offsets[stats_index];
	  thread_p->m_px_stats[offset] += worker_stats[offset];
	  worker_stats[offset] = 0;
	}
    }

  perfmon_merge_parallel_stats_to_tran_stats (thread_p);
}

#if HASHJOIN_DUMP_HASH_TABLE
/*
 * hjoin_dump_hash_table() -
 *   return: None.
 *   thread_p(in): Thread entry.
 *   hash_scan(in): Hash scan structure containing the hash table.
 *   list_id(in): List identifier used as build input.
 */
static void
hjoin_dump_hash_table (THREAD_ENTRY * thread_p, HASH_LIST_SCAN * hash_scan, QFILE_LIST_ID * list_id)
{
  assert (thread_p != NULL);
  assert (hash_scan != NULL);
  assert (list_id != NULL);

  if (list_id->tuple_cnt > DUMP_HASH_TABLE_LIMIT)
    {
      return;
    }

  switch (hash_scan->hash_list_scan_type)
    {
    case HASH_METH_IN_MEM:
    case HASH_METH_HYBRID:
      assert (hash_scan->memory.hash_table != NULL);
      mht_dump_hls (thread_p, stdout, hash_scan->memory.hash_table, 1, qdata_print_hash_scan_entry, &list_id->type_list,
		    (void *) &hash_scan->hash_list_scan_type);
      printf ("temp file : tuple count = %ld, file_size = %dK\n", list_id->tuple_cnt, list_id->page_cnt * 16);
      break;

    case HASH_METH_HASH_FILE:
      assert (hash_scan->spill.hash_table != NULL);
      printf ("batch-spill hash : tuple count = %ld, file_size = %dK\n", list_id->tuple_cnt, list_id->page_cnt * 16);
      break;

    case HASH_METH_NOT_USE:
      /* Nothing to do */
      break;

    default:
      /* impossible case */
      assert (false);
      break;
    }
}
#endif /* HASHJOIN_DUMP_HASH_TABLE */

#if !defined(NDEBUG) && HASHJOIN_DUMP_PROBE
/*
 * hjoin_print_tuple() -
 *   return: None.
 *   list_id(in): List identifier for the given tuple.
 *   tuple(in): Tuple to be printed.
 *   step(in): Step at which the tuple is printed.
 */
static void
hjoin_print_tuple (QFILE_LIST_ID * list_id, QFILE_TUPLE tuple, HASHJOIN_PRINT_STEP step)
{
  assert (list_id != NULL);
  assert (tuple != NULL);

  if (list_id->tuple_cnt > DUMP_PROBE_LIMIT)
    {
      return;
    }

  switch (step)
    {
    case HASHJOIN_PRINT_READ_KEY:
      fprintf (stdout, "\nRead Key (Probe): ");
      break;

    case HASHJOIN_PRINT_NOT_MATCHED_KEY:
      fprintf (stdout, "\nNot Matched Key (Build): ");
      break;

    case HASHJOIN_PRINT_NOT_QUALIFIED_KEY:
      fprintf (stdout, "\nNot Qualified Key (Build): ");
      break;

    case HASHJOIN_PRINT_QUALIFIED_KEY:
      fprintf (stdout, "\nQualified Key (Build): ");
      break;

    case HASHJOIN_PRINT_FILL_EMPTY_KEY:
      fprintf (stdout, "\nFill Empty Key (Probe): ");
      break;

    default:
      /* impossible case */
      assert (false);
      /* Nothing to do */
      break;
    }

  qfile_print_tuple (&list_id->type_list, tuple);
}
#endif /* !NDEBUG && HASHJOIN_DUMP_PROBE */
