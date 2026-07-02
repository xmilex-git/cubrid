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
 * px_hash_join.cpp
 */

#include "px_hash_join.hpp"
#include "px_hash_join_task_manager.hpp"

#include "error_manager.h"		/* assert_release_error, er_errid, NO_ERROR, ... */
#include "list_file.h"			/* qfile_open_list, qfile_open_list_scan, qfile_close_scan, ... */
#include "query_manager.h"		/* QMGR_TEMP_FILE (qmgr_temp_file) */
#include "memory_alloc.h"		/* db_private_alloc, db_private_free_and_init */
#include "storage_common.h"		/* OID_INITIALIZER, S_CLOSED, VPID_SET_NULL, ... */
#include "query_list.h"			/* QFILE_LIST_ID_TAPESET, qfile_list_has_new_backing */
#include "qfile_tape.hpp"		/* qfile::tapeset */
#include "qfile_chunk.hpp"		/* qfile::chunk_distributor */

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace parallel_query
{
  namespace hash_join
  {

    /*
     * build_partitions
     */

    int
    build_partitions (cubthread::entry &thread_ref, HASHJOIN_MANAGER *manager, HASHJOIN_SPLIT_INFO *split_info)
    {
      HASHJOIN_INPUT_SPLIT_INFO *outer, *inner;
      HASHJOIN_SHARED_SPLIT_INFO shared_info;
      UINT32 task_cnt, task_index;
      int error = NO_ERROR;

      assert (manager != nullptr);
      assert (split_info != nullptr);

      HASHJOIN_STATS *stats = manager->single_context.stats;
      HASHJOIN_START_STATS start_stats = HASHJOIN_START_STATS_INITIALIZER;
      assert (!thread_is_on_trace (&thread_ref) || stats != nullptr);

      outer = &split_info->outer;
      inner = &split_info->inner;

      task_cnt = manager->num_parallel_threads;

      THREAD_ENTRY *main_thread_p = thread_get_main_thread (&thread_ref);
      task_manager task_manager (manager->px_worker_manager, *main_thread_p);
      split_task *task = nullptr;

      error = hjoin_init_shared_split_info (&thread_ref, manager, &shared_info);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}

      if (thread_is_on_trace (&thread_ref))
	{
	  hjoin_trace_start (&thread_ref, &start_stats);
	}

      /* ---- outer relation split ---- */
      {
	QFILE_LIST_ID *outer_list_id = outer->fetch_info->list_id;

	if (qfile_list_has_new_backing (outer_list_id))
	  {
	    /* NEW (Tapeset): chunk_distributor + per-worker tapeset_reader. */
	    shared_info.new_tapeset = (qfile::tapeset *) QFILE_LIST_ID_TAPESET (outer_list_id);
	    if (shared_info.new_tapeset == nullptr)
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
		error = ER_FAILED;
		goto error_exit;
	      }
	    shared_info.new_dist = new qfile::chunk_distributor (shared_info.new_tapeset, task_cnt);

	    /* redesign #78 per-worker OUTPUT tape (ADR0004): allocate per-worker
	     * partition list slots so workers write without part_mutexes. */
	    shared_info.worker_count = task_cnt;
	    shared_info.worker_part_lists =
		    (QFILE_LIST_ID ***) db_private_alloc (&thread_ref, task_cnt * sizeof (QFILE_LIST_ID **));
	    if (shared_info.worker_part_lists == nullptr)
	      {
		error = er_errid ();
		goto error_exit;
	      }
	    memset (shared_info.worker_part_lists, 0, task_cnt * sizeof (QFILE_LIST_ID **));
	  }
	else
	  {
	    /* OLD: sector page scan. */
	    error = qfile_open_list_sector_scan (&thread_ref, outer_list_id, &shared_info.sector_scan);
	    if (error != NO_ERROR)
	      {
		goto error_exit;
	      }
	  }
      }

      for (task_index = 0; task_index < task_cnt; task_index++)
	{
	  task = new split_task (task_manager, manager, outer, &shared_info, task_index);
	  task_manager.push_task (task);
	}

      task_manager.join ();

      if (thread_is_on_trace (&thread_ref))
	{
	  hjoin_trace_drain_worker_stats (&thread_ref, manager);
	  hjoin_trace_end (&thread_ref, &stats->split, &start_stats);
	}

      if (task_manager.has_error ())
	{
	  goto error_exit;
	}

      /* Release outer split resources before inner run. */
      qfile_close_list_sector_scan (&thread_ref, &shared_info.sector_scan);
      delete shared_info.new_dist;
      shared_info.new_dist = nullptr;
      shared_info.new_tapeset = nullptr;

      /* redesign #78 per-worker OUTPUT tape (ADR0004): merge worker partition
       * lists into the shared part_list_id.  Sequential — no mutex needed
       * since all workers have completed. */
      if (shared_info.worker_part_lists != nullptr)
	{
	  UINT32 part_cnt = manager->context_cnt;
	  for (UINT32 wi = 0; wi < shared_info.worker_count; wi++)
	    {
	      if (shared_info.worker_part_lists[wi] == nullptr)
		{
		  continue;
		}
	      for (UINT32 pi = 0; pi < part_cnt; pi++)
		{
		  QFILE_LIST_ID *wpl = shared_info.worker_part_lists[wi][pi];
		  if (wpl == nullptr)
		    {
		      continue;
		    }
		  if (wpl->tuple_cnt > 0)
		    {
		      qfile_close_list (&thread_ref, wpl);
		      if (outer->part_list_id[pi]->tuple_cnt > 0)
			{
			  error = qfile_append_list (&thread_ref, outer->part_list_id[pi], wpl);
			  qfile_destroy_list (&thread_ref, wpl);
			}
		      else
			{
			  qfile_destroy_list (&thread_ref, outer->part_list_id[pi]);
			  qfile_copy_list_id (outer->part_list_id[pi], wpl, false, QFILE_PROHIBIT_DEPENDENT);
			}
		      if (error != NO_ERROR)
			{
			  goto error_exit;
			}
		    }
		  else
		    {
		      qfile_destroy_list (&thread_ref, wpl);
		    }
		  QFILE_FREE_AND_INIT_LIST_ID (shared_info.worker_part_lists[wi][pi]);
		}
	      db_private_free_and_init (&thread_ref, shared_info.worker_part_lists[wi]);
	    }
	  db_private_free_and_init (&thread_ref, shared_info.worker_part_lists);
	  shared_info.worker_count = 0;
	}

      if (thread_is_on_trace (&thread_ref))
	{
	  hjoin_trace_start (&thread_ref, &start_stats);
	}

      /* ---- inner relation split ---- */
      {
	QFILE_LIST_ID *inner_list_id = inner->fetch_info->list_id;

	if (qfile_list_has_new_backing (inner_list_id))
	  {
	    /* NEW (Tapeset): chunk_distributor + per-worker tapeset_reader. */
	    shared_info.new_tapeset = (qfile::tapeset *) QFILE_LIST_ID_TAPESET (inner_list_id);
	    if (shared_info.new_tapeset == nullptr)
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
		error = ER_FAILED;
		goto error_exit;
	      }
	    shared_info.new_dist = new qfile::chunk_distributor (shared_info.new_tapeset, task_cnt);

	    /* redesign #78 per-worker OUTPUT tape (ADR0004): same as outer. */
	    shared_info.worker_count = task_cnt;
	    shared_info.worker_part_lists =
		    (QFILE_LIST_ID ***) db_private_alloc (&thread_ref, task_cnt * sizeof (QFILE_LIST_ID **));
	    if (shared_info.worker_part_lists == nullptr)
	      {
		error = er_errid ();
		goto error_exit;
	      }
	    memset (shared_info.worker_part_lists, 0, task_cnt * sizeof (QFILE_LIST_ID **));
	  }
	else
	  {
	    /* OLD: sector page scan. */
	    error = qfile_open_list_sector_scan (&thread_ref, inner_list_id, &shared_info.sector_scan);
	    if (error != NO_ERROR)
	      {
		goto error_exit;
	      }
	  }
      }

      for (task_index = 0; task_index < task_cnt; task_index++)
	{
	  task = new split_task (task_manager, manager, inner, &shared_info, task_index);
	  task_manager.push_task (task);
	}

      task_manager.join ();

      if (thread_is_on_trace (&thread_ref))
	{
	  hjoin_trace_drain_worker_stats (&thread_ref, manager);
	  hjoin_trace_end (&thread_ref, &stats->split, &start_stats);
	}

      if (task_manager.has_error ())
	{
	  goto error_exit;
	}

      ASSERT_NO_ERROR_OR_INTERRUPTED ();

      /* redesign #78 per-worker OUTPUT tape (ADR0004): merge inner split worker lists. */
      if (shared_info.worker_part_lists != nullptr)
	{
	  UINT32 part_cnt = manager->context_cnt;
	  for (UINT32 wi = 0; wi < shared_info.worker_count; wi++)
	    {
	      if (shared_info.worker_part_lists[wi] == nullptr)
		{
		  continue;
		}
	      for (UINT32 pi = 0; pi < part_cnt; pi++)
		{
		  QFILE_LIST_ID *wpl = shared_info.worker_part_lists[wi][pi];
		  if (wpl == nullptr)
		    {
		      continue;
		    }
		  if (wpl->tuple_cnt > 0)
		    {
		      qfile_close_list (&thread_ref, wpl);
		      if (inner->part_list_id[pi]->tuple_cnt > 0)
			{
			  error = qfile_append_list (&thread_ref, inner->part_list_id[pi], wpl);
			  qfile_destroy_list (&thread_ref, wpl);
			}
		      else
			{
			  qfile_destroy_list (&thread_ref, inner->part_list_id[pi]);
			  qfile_copy_list_id (inner->part_list_id[pi], wpl, false, QFILE_PROHIBIT_DEPENDENT);
			}
		      if (error != NO_ERROR)
			{
			  goto error_exit;
			}
		    }
		  else
		    {
		      qfile_destroy_list (&thread_ref, wpl);
		    }
		  QFILE_FREE_AND_INIT_LIST_ID (shared_info.worker_part_lists[wi][pi]);
		}
	      db_private_free_and_init (&thread_ref, shared_info.worker_part_lists[wi]);
	    }
	  db_private_free_and_init (&thread_ref, shared_info.worker_part_lists);
	  shared_info.worker_count = 0;
	}

cleanup:
      qfile_close_list_sector_scan (&thread_ref, &shared_info.sector_scan);

      /* redesign #78 2A-3: release the NEW-input distributor (nullptr on OLD path). */
      delete shared_info.new_dist;
      shared_info.new_dist = nullptr;

      hjoin_clear_shared_split_info (&thread_ref, manager, &shared_info);

      return error;

error_exit:
      task_manager.clear_interrupt (thread_ref);

      if (error == NO_ERROR || er_errid () == NO_ERROR)
	{
	  assert_release_error (er_errid () != NO_ERROR);
	  error = er_errid ();
	}

      goto cleanup;
    }

    /*
     * execute_partitions
     */

    int
    execute_partitions (cubthread::entry &thread_ref, HASHJOIN_MANAGER *manager)
    {
      HASHJOIN_CONTEXT *current_context;
      HASHJOIN_SHARED_JOIN_INFO shared_info;
      UINT32 context_index;
      UINT32 task_cnt, task_index;

      int error = NO_ERROR;

      assert (manager != nullptr);

      HASHJOIN_STATS *stats = manager->single_context.stats;
      HASHJOIN_START_STATS start_stats = HASHJOIN_START_STATS_INITIALIZER;
#if HASHJOIN_PROFILE_TIME
      HASHJOIN_START_STATS profile_start_stats = HASHJOIN_START_STATS_INITIALIZER;
#endif /* HASHJOIN_PROFILE_TIME */
      assert (!thread_is_on_trace (&thread_ref) || stats != nullptr);

      task_cnt = manager->num_parallel_threads;

      THREAD_ENTRY *main_thread_p = thread_get_main_thread (&thread_ref);
      task_manager task_manager (manager->px_worker_manager, *main_thread_p);
      join_task *task = nullptr;

      if (thread_is_on_trace (&thread_ref))
	{
	  hjoin_trace_start (&thread_ref, &start_stats);
	}

      for (task_index = 0; task_index < task_cnt; task_index++)
	{
	  task = new join_task (task_manager, manager, manager->contexts, &shared_info, task_index);
	  task_manager.push_task (task);
	}

      task_manager.join ();

      if (thread_is_on_trace (&thread_ref))
	{
	  hjoin_trace_drain_worker_stats (&thread_ref, manager);
	  hjoin_trace_end (&thread_ref, &stats->parallel, &start_stats);

	  stats->build.range_elapsed_time.min = shared_info.build_range_time.min;
	  stats->build.range_elapsed_time.max = shared_info.build_range_time.max;
	  stats->probe.range.elapsed_time.min = shared_info.probe_range_time.min;
	  stats->probe.range.elapsed_time.max = shared_info.probe_range_time.max;
	}

      if (task_manager.has_error ())
	{
	  assert_release_error (er_errid () != NO_ERROR);
	  task_manager.clear_interrupt (thread_ref);
	  return er_errid ();
	}

      for (context_index = 0; context_index < manager->context_cnt; context_index++)
	{
	  current_context = &manager->contexts[context_index];

	  if (thread_is_on_trace (&thread_ref))
	    {
	      hjoin_trace_merge_stats (stats, current_context->stats, manager->single_context.status);
	    }

	  if (current_context->list_id == nullptr)
	    {
	      error = er_errid ();
	      if (error != NO_ERROR)
		{
		  return error;
		}
	      else
		{
		  /* list_id can be NULL when the join result is empty.
		   * In this case, it is NO_ERROR. */
		  continue;
		}
	    }

	  if (current_context->list_id->tuple_cnt == 0)
	    {
	      qfile_destroy_list (&thread_ref, current_context->list_id);
	      QFILE_FREE_AND_INIT_LIST_ID (current_context->list_id);

	      /* empty context */
	      continue;
	    }

	  HJOIN_PROFILE_START (&thread_ref, &profile_start_stats, HASHJOIN_PROFILE_MERGE);
	  error = hjoin_merge_qlist (&thread_ref, manager, current_context);
	  HJOIN_PROFILE_MERGE_END (&thread_ref, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_MERGE,
				   (manager->single_context.list_id != nullptr) ? manager->single_context.list_id->tuple_cnt : 0);

	  if (error != NO_ERROR)
	    {
	      assert_release_error (er_errid () != NO_ERROR);
	      return er_errid ();
	    }
	}

      ASSERT_NO_ERROR_OR_INTERRUPTED ();
      return NO_ERROR;
    }

    /*
     * parallel_probe
     */

    int
    init_context (cubthread::entry &thread_ref, HASHJOIN_MANAGER *manager, HASHJOIN_CONTEXT *context)
    {
      HASHJOIN_CONTEXT *single_context;
      int error = NO_ERROR;

      assert (manager != nullptr);
      assert (context != nullptr);

      single_context = &manager->single_context;

      context->outer.list_id = single_context->outer.list_id;
      context->outer.input = single_context->outer.input;
      context->outer.coerce_domains = single_context->outer.coerce_domains;
      context->outer.need_coerce_domains = single_context->outer.need_coerce_domains;
      context->outer.regu_list_pred = single_context->outer.regu_list_pred;

      context->inner.list_id = single_context->inner.list_id;
      context->inner.input = single_context->inner.input;
      context->inner.coerce_domains = single_context->inner.coerce_domains;
      context->inner.need_coerce_domains = single_context->inner.need_coerce_domains;
      context->inner.regu_list_pred = single_context->inner.regu_list_pred;

      assert (context->list_id == nullptr);

      /* Prevent faults when qfile_close_scan is called */
      context->outer.list_scan_id.status = S_CLOSED;
      context->inner.list_scan_id.status = S_CLOSED;

      switch (manager->join_type)
	{
	case JOIN_INNER:
	  context->outer.fill_record = nullptr;
	  context->inner.fill_record = nullptr;
	  break;

	case JOIN_LEFT:
	  context->outer.fill_record = &context->outer.tuple_record;
	  context->inner.fill_record = nullptr;
	  break;

	case JOIN_RIGHT:
	  context->outer.fill_record = nullptr;
	  context->inner.fill_record = &context->inner.tuple_record;
	  break;

	default:
	  /* impossible case */
	  assert_release_error (false);
	  goto error_exit;
	}

      if (single_context->build == &single_context->outer)
	{
	  /* swap_join_inputs == true */
	  context->build = &context->outer;
	  context->probe = &context->inner;
	}
      else
	{
	  /* swap_join_inputs == false */
	  context->build = &context->inner;
	  context->probe = &context->outer;
	}

      context->list_id = qfile_open_list (&thread_ref, &manager->type_list, nullptr,
					  manager->query_id, manager->qlist_flag, nullptr);
      if (context->list_id == nullptr)
	{
	  goto error_exit;
	}

      context->during_join_pred = single_context->during_join_pred;
      context->val_descr = single_context->val_descr;

      context->status = HASHJOIN_STATUS_PARALLEL_PROBE;

      ASSERT_NO_ERROR_OR_INTERRUPTED ();
      return NO_ERROR;

error_exit:
      clear_context (thread_ref, context);

      if (error == NO_ERROR || er_errid () == NO_ERROR)
	{
	  assert_release_error (er_errid () != NO_ERROR);
	  error = er_errid ();
	}

      return error;
    }

    void
    clear_context (cubthread::entry &thread_ref, HASHJOIN_CONTEXT *context)
    {
      assert (context != nullptr);

      if (context->list_id != nullptr)
	{
	  qfile_close_list (&thread_ref, context->list_id);
	  qfile_destroy_list (&thread_ref, context->list_id);
	  QFILE_FREE_AND_INIT_LIST_ID (context->list_id);
	}

      assert (context->outer.list_scan_id.curr_pgptr == nullptr);
      assert (context->inner.list_scan_id.curr_pgptr == nullptr);
    }

    int
    probe_prepare (cubthread::entry &thread_ref, HASHJOIN_MANAGER *manager)
    {
      HASHJOIN_CONTEXT *contexts = nullptr;
      HASHJOIN_STATS *context_stats = nullptr;
      UINT32 context_cnt, context_index;
      int error = NO_ERROR;

      assert (manager != nullptr);
      assert (manager->contexts == nullptr);
      assert (manager->context_cnt == 0);

      context_cnt = manager->num_parallel_threads;
      assert (context_cnt > 1);

      contexts = (HASHJOIN_CONTEXT *) db_private_alloc (&thread_ref, context_cnt * sizeof (HASHJOIN_CONTEXT));
      if (contexts == nullptr)
	{
	  goto error_exit;
	}
      memset (contexts, 0, context_cnt * sizeof (HASHJOIN_CONTEXT));

      for (context_index = 0; context_index < context_cnt; context_index++)
	{
	  error = init_context (thread_ref, manager, &contexts[context_index]);
	  if (error != NO_ERROR)
	    {
	      goto error_exit;
	    }

	  manager->context_cnt++;
	}

      manager->contexts = contexts;

      if (thread_is_on_trace (&thread_ref))
	{
	  context_stats = (HASHJOIN_STATS *) malloc (context_cnt * sizeof (HASHJOIN_STATS));
	  if (context_stats == nullptr)
	    {
	      error = ER_OUT_OF_VIRTUAL_MEMORY;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, context_cnt * sizeof (HASHJOIN_STATS));
	      goto error_exit;
	    }
	  memset (context_stats, 0, context_cnt * sizeof (HASHJOIN_STATS));

	  for (context_index = 0; context_index < context_cnt; context_index++)
	    {
	      contexts[context_index].stats = &context_stats[context_index];
	    }

	  assert (manager->stats_group != nullptr);
	  manager->stats_group->context_stats = context_stats;
	  manager->stats_group->context_cnt = context_cnt;
	}
      else
	{
	  assert (manager->stats_group == nullptr);
	}

      ASSERT_NO_ERROR_OR_INTERRUPTED ();
      return NO_ERROR;

error_exit:
      if (contexts != nullptr)
	{
	  for (context_index = 0; context_index < manager->context_cnt; context_index++)
	    {
	      clear_context (thread_ref, &contexts[context_index]);
	    }

	  db_private_free_and_init (&thread_ref, contexts);
	}

      if (thread_is_on_trace (&thread_ref))
	{
	  if (context_stats != nullptr)
	    {
	      free_and_init (context_stats);
	    }

	  assert (manager->stats_group != nullptr);
	  manager->stats_group->context_stats = nullptr;
	  manager->stats_group->context_cnt = 0;
	}
      else
	{
	  assert (context_stats == nullptr);
	  assert (manager->stats_group == nullptr);
	}

      manager->contexts = nullptr;
      manager->context_cnt = 0;

      if (error == NO_ERROR || er_errid () == NO_ERROR)
	{
	  assert_release_error (er_errid () != NO_ERROR);
	  error = er_errid ();
	}

      return error;
    }

    int
    probe_execute (cubthread::entry &thread_ref, HASHJOIN_MANAGER *manager)
    {
      HASHJOIN_CONTEXT *contexts = nullptr, *current_context;
      HASHJOIN_SHARED_PROBE_INFO shared_info;
      UINT32 context_index;
      UINT32 task_cnt, task_index;
      int error = NO_ERROR;

      assert (manager != nullptr);
      assert (manager->single_context.status == HASHJOIN_STATUS_PARALLEL_PROBE);
      assert (manager->px_worker_manager != nullptr);

      HASHJOIN_STATS *stats = manager->single_context.stats;
      HASHJOIN_START_STATS start_stats = HASHJOIN_START_STATS_INITIALIZER;
#if HASHJOIN_PROFILE_TIME
      HASHJOIN_START_STATS profile_start_stats = HASHJOIN_START_STATS_INITIALIZER;
#endif /* HASHJOIN_PROFILE_TIME */
      assert (!thread_is_on_trace (&thread_ref) || stats != nullptr);

      contexts = manager->contexts;

      task_cnt = manager->num_parallel_threads;

      THREAD_ENTRY *main_thread_p = thread_get_main_thread (&thread_ref);
      task_manager task_manager (manager->px_worker_manager, *main_thread_p);
      probe_task *task = nullptr;

      if (thread_is_on_trace (&thread_ref))
	{
	  hjoin_trace_start (&thread_ref, &start_stats);
	}

      {
	QFILE_LIST_ID *probe_list_id = manager->single_context.probe->list_id;

	if (qfile_list_has_new_backing (probe_list_id))
	  {
	    /* redesign #78 2A-3: NEW (Tapeset) probe input -> shared chunk_distributor
	     * + per-worker tapeset_reader (ADR 0003/0005/0006).  Reached only when
	     * CUBRID_WM_HASHJOIN_NEW is on (hjoin_try_parallel_probe forces SINGLE for
	     * a NEW input otherwise), so the OLD sector reader (and its backing guard)
	     * is bypassed for NEW input. */
	    shared_info.new_tapeset = (qfile::tapeset *) QFILE_LIST_ID_TAPESET (probe_list_id);
	    if (shared_info.new_tapeset == nullptr)
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
		error = ER_FAILED;
		goto error_exit;
	      }

	    shared_info.new_dist = new qfile::chunk_distributor (shared_info.new_tapeset, task_cnt);
	    if (shared_info.new_dist == nullptr)
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
			sizeof (qfile::chunk_distributor));
		error = ER_OUT_OF_VIRTUAL_MEMORY;
		goto error_exit;
	      }
	  }
	else
	  {
	    /* collect data page sectors for probe relation (OLD backing) */
	    error = qfile_open_list_sector_scan (&thread_ref, probe_list_id, &shared_info.sector_scan);
	    if (error != NO_ERROR)
	      {
		goto error_exit;
	      }
	  }
      }

      for (task_index = 0; task_index < task_cnt; task_index++)
	{
	  task = new probe_task (task_manager, manager, &contexts[task_index], &shared_info, task_index);
	  task_manager.push_task (task);
	}

      task_manager.join ();

      if (thread_is_on_trace (&thread_ref))
	{
	  hjoin_trace_drain_worker_stats (&thread_ref, manager);
	  hjoin_trace_end (&thread_ref, &stats->probe, &start_stats);

	  stats->probe.range.elapsed_time.min = shared_info.probe_range.elapsed_time.min;
	  stats->probe.range.elapsed_time.max = shared_info.probe_range.elapsed_time.max;
	  stats->probe.range.read_rows.min = shared_info.probe_range.read_rows.min;
	  stats->probe.range.read_rows.max = shared_info.probe_range.read_rows.max;
	  stats->probe.range.read_keys.min = shared_info.probe_range.read_keys.min;
	  stats->probe.range.read_keys.max = shared_info.probe_range.read_keys.max;
	  stats->probe.range.qualified_rows.min = shared_info.probe_range.qualified_rows.min;
	  stats->probe.range.qualified_rows.max = shared_info.probe_range.qualified_rows.max;
	}

      if (task_manager.has_error ())
	{
	  goto error_exit;
	}

      for (context_index = 0; context_index < manager->context_cnt; context_index++)
	{
	  current_context = &contexts[context_index];

	  if (thread_is_on_trace (&thread_ref))
	    {
	      hjoin_trace_merge_stats (stats, current_context->stats, manager->single_context.status);
	    }

	  if (current_context->list_id == nullptr)
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

	  if (current_context->list_id->tuple_cnt == 0)
	    {
	      qfile_destroy_list (&thread_ref, current_context->list_id);
	      QFILE_FREE_AND_INIT_LIST_ID (current_context->list_id);

	      /* empty context */
	      continue;
	    }

	  HJOIN_PROFILE_START (&thread_ref, &profile_start_stats, HASHJOIN_PROFILE_MERGE);
	  error = hjoin_merge_qlist (&thread_ref, manager, current_context);
	  HJOIN_PROFILE_MERGE_END (&thread_ref, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_MERGE,
				   (manager->single_context.list_id != nullptr) ? manager->single_context.list_id->tuple_cnt : 0);

	  if (error != NO_ERROR)
	    {
	      goto error_exit;
	    }
	}

      ASSERT_NO_ERROR_OR_INTERRUPTED ();

cleanup:
      qfile_close_list_sector_scan (&thread_ref, &shared_info.sector_scan);

      /* redesign #78 2A-3: release the NEW-input distributor (nullptr on OLD path). */
      delete shared_info.new_dist;
      shared_info.new_dist = nullptr;

      return error;

error_exit:
      task_manager.clear_interrupt (thread_ref);

      if (error == NO_ERROR || er_errid () == NO_ERROR)
	{
	  assert_release_error (er_errid () != NO_ERROR);
	  error = er_errid ();
	}

      goto cleanup;
    }

  } /* namespace hash_join */
} /* namespace parallel_query */
