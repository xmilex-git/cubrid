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
 * query_hash_join_fused.c - fused hash-join probe (probe pushdown into the parallel
 *                           scan of the probe input).
 *
 * This is part of the hash-join module; it shares the build/probe primitives declared
 * in query_hash_join.h (hjoin_scan_init, hjoin_build_key, hjoin_fetch_key,
 * hjoin_probe_key, hjoin_merge_tuple_to_list_id) with query_hash_join.c.  The public
 * entry points and the HJOIN_FUSED_STATE / HJOIN_FUSED_WORKER opaque handles are
 * declared in query_hash_join.h; the struct bodies below are private to this file.
 *
 * ---------------------------------------------------------------------------
 * Lifecycle (one state, one owner at a time)
 *
 *   [compile]    px_scan_checker marks the larger inner-join input buildlist with
 *                XASL_HJ_FUSED_PROBE_INPUT (candidate only; re-vetted every run).
 *   [dispatch]   qexec_hjoin_fused_dispatch (query_executor.c) reaches the marked pair
 *                first, runs the build input to completion, then calls
 *                  PREPARE  -> qexec_hjoin_fused_prepare: build the shared read-only
 *                             hash table; returns a state, or NULL to refuse.
 *                It publishes the state on probe_xasl->fused_probe and runs the probe.
 *   [scan]       each parallel-scan worker (px_scan_result_handler.cpp) binds:
 *                  ENGAGE   -> qexec_hjoin_fused_worker_open: per-worker cursor + gather
 *                             list opened with the JOIN output type list; sets engaged.
 *                  per row  -> qexec_hjoin_fused_worker_row: probe + merge straight to
 *                             the gather list (the probe-input temp list never exists).
 *                  close    -> qexec_hjoin_fused_worker_close.
 *   [finish]     ADOPT    -> qexec_hash_join sees fused_state != NULL and calls
 *                           qexec_hjoin_fused_adopt_result: the gather IS the join
 *                           output -- move it onto the join node.
 *                ABANDON  -> any refusal / error / non-engaged scan frees the state
 *                           (qexec_hjoin_fused_abandon, or qexec_clear_xasl teardown)
 *                           and the materialized path runs unchanged.
 *
 * Correctness never depends on the kill-switch: every refusal is pre-emit and falls
 * back to the materialized path.
 * ---------------------------------------------------------------------------
 */

#include "query_hash_join.h"

#include "dbtype.h"		/* db_make_null */
#include "error_manager.h"	/* er_errid, NO_ERROR, assert_release_error */
#include "fetch.h"		/* fetch_peek_dbval */
#include "list_file.h"		/* qfile_open_list, qfile_close_list */
#include "memory_alloc.h"	/* CEIL_PTVDIV */
#include "object_domain.h"	/* tp_domain_match, TP_EXACT_MATCH */
#include "object_representation.h"	/* TP_DOMAIN */
#include "perf_monitor.h"	/* perfmon_inc_stat, PSTAT_... */
#include "query_list.h"		/* JOIN_TYPE */
#include "query_manager.h"	/* QMGR_TEMP_FILE */
#include "query_opfunc.h"	/* qdata_get_valptr_type_list, qdata_copy_valptr_list_to_tuple */
#include "system_parameter.h"	/* prm_get_bigint_value, PRM_ID_... */
#include "thread_entry.hpp"	/* THREAD_ENTRY */
#include "xasl.h"		/* XASL_NODE, HASHJOIN_PROC_NODE */

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

#if defined (SERVER_MODE) && !defined (WINDOWS)
/*
 * Fused hash-join probe (hidden kill-switch hash_join_fused_probe, default ON;
 * see query_hash_join.h).
 *
 * HJOIN_FUSED_STATE is prepared on the driver thread strictly AFTER the build input
 * is complete and BEFORE the probe-input scan runs.  During the scan it is read-only
 * shared state: every scan worker owns a private HJOIN_FUSED_WORKER cursor over the
 * shared hash table and a private output (gather) list it emits merged join tuples to
 * directly (no streaming sink seam).
 */

/* HJOIN_FUSED_STATE / HJOIN_FUSED_WORKER are forward-typedef'd in query_hash_join.h;
 * their bodies are private to this translation unit. */
struct hjoin_fused_state
{
  /* immutable after qexec_hjoin_fused_prepare (read-only during the fused scan) */
  QFILE_LIST_ID *build_list_id;	/* the COMPLETE build (inner) input list */
  QFILE_LIST_MERGE_INFO *merge_info;	/* borrowed from the HASHJOIN_PROC node */
  HASH_LIST_SCAN hash_scan;	/* shared read-only hash table over build_list_id */

  QFILE_TUPLE_VALUE_TYPE_LIST output_type_list;	/* join output columns */

  int key_cnt;
  int *probe_value_indexes;	/* borrowed (merge_info->ls_outer_column or ls_inner_column,
				 * by probe side); list-column positions == non-hidden
				 * outptr entries in order */
  bool probe_is_outer;		/* which merge side the probe (scanned) input is */

  /* build-side fetch info for the exact-key compare on matched build tuples; fused
   * is gated on exactly matching key domains, so no coercion ever applies */
  HASHJOIN_INPUT_DOMAIN_INFO build_input;
  TP_DOMAIN **null_coerce_domains;	/* all-NULL, key_cnt entries */

  QUERY_ID query_id;

  /* set by any scan worker that binds to this state; read by the driver strictly
   * after the scan workers are joined (no concurrent read/write) */
  bool engaged;
};

struct hjoin_fused_worker
{
  HJOIN_FUSED_STATE *state;
  OUTPTR_LIST *outptr_list;	/* the WORKER's cloned outptr list */
  REGU_VARIABLE **key_regus;	/* probe key source regus resolved from outptr_list */
  QFILE_LIST_ID *output_list_id;	/* the worker's own gather (join-output) list */

  HASH_LIST_SCAN hash_scan;	/* per-worker cursor over the SHARED hash table */
  HASH_SCAN_KEY *probe_key;
  HASH_SCAN_KEY *found_key;

  QFILE_LIST_SCAN_ID build_scan;	/* per-worker scan over the build list (HYBRID jumps) */
  bool build_scan_opened;

  HASHJOIN_FETCH_INFO build_fetch;

  QFILE_TUPLE_RECORD probe_record;	/* reusable probe-row encode buffer */
  QFILE_TUPLE_RECORD overflow_record;
};

static int hjoin_fused_resolve_key_regus (OUTPTR_LIST * outptr_list, int key_cnt, const int *probe_value_indexes,
					  REGU_VARIABLE ** key_regus_out);

/*
 * hjoin_fused_resolve_key_regus () - Resolve the probe-key source REGU_VARIABLEs from
 *                                    the probe outptr list at the merge key column
 *                                    positions.
 *   return: Error code (NO_ERROR if every key resolved, error code otherwise).
 *   outptr_list(in): The probe input's output pointer list.
 *   key_cnt(in): Number of join keys.
 *   probe_value_indexes(in): For each key, its list-column position (counts only the
 *                            NON-HIDDEN outptr entries, in order -- exactly the
 *                            positions both tuple encoders assign).
 *   key_regus_out(out): Array of key_cnt regu pointers (caller-allocated).
 *
 * Note: list-column positions count only the NON-HIDDEN outptr entries (hidden
 *       columns are skipped by both tuple encoders), so the walk must skip them too.
 */
static int
hjoin_fused_resolve_key_regus (OUTPTR_LIST * outptr_list, int key_cnt, const int *probe_value_indexes,
			       REGU_VARIABLE ** key_regus_out)
{
  REGU_VARIABLE_LIST regu_list;
  int key_index, value_index;

  for (key_index = 0; key_index < key_cnt; key_index++)
    {
      key_regus_out[key_index] = NULL;
    }

  value_index = 0;
  for (regu_list = outptr_list->valptrp; regu_list != NULL; regu_list = regu_list->next)
    {
      if (REGU_VARIABLE_IS_FLAGED (&regu_list->value, REGU_VARIABLE_HIDDEN_COLUMN))
	{
	  continue;
	}

      for (key_index = 0; key_index < key_cnt; key_index++)
	{
	  if (probe_value_indexes[key_index] == value_index)
	    {
	      key_regus_out[key_index] = &regu_list->value;
	    }
	}

      value_index++;
    }

  for (key_index = 0; key_index < key_cnt; key_index++)
    {
      if (key_regus_out[key_index] == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
	  return ER_QPROC_INVALID_XASLNODE;
	}
    }

  return NO_ERROR;
}

/*
 * qexec_hjoin_fused_free_state() - Release everything a prepared fused state owns.
 *   return: None.
 *   thread_p(in): Thread entry (the driver that prepared the state).
 *   state(in): Fused state to free.
 */
static void
qexec_hjoin_fused_free_state (THREAD_ENTRY * thread_p, HJOIN_FUSED_STATE * state)
{
  assert (thread_p != NULL);
  assert (state != NULL);

  hjoin_scan_clear (thread_p, &state->hash_scan);

  if (state->output_type_list.domp != NULL)
    {
      db_private_free_and_init (thread_p, state->output_type_list.domp);
    }

  if (state->build_input.domains != NULL)
    {
      db_private_free_and_init (thread_p, state->build_input.domains);
    }

  if (state->null_coerce_domains != NULL)
    {
      db_private_free_and_init (thread_p, state->null_coerce_domains);
    }

  db_private_free_and_init (thread_p, state);
}

/*
 * fused_vet_probe_output() - VET 1: every probe output domain must be final.
 *   return: true if eligible.
 *   probe_type_list(in): the synthesized probe-side type list.
 *
 * The fused output adopts these compile-time domains verbatim, so a NULL or
 * still-DB_TYPE_VARIABLE domain refuses.
 */
static bool
fused_vet_probe_output (const QFILE_TUPLE_VALUE_TYPE_LIST * probe_type_list)
{
  int i;

  for (i = 0; i < probe_type_list->type_cnt; i++)
    {
      if (probe_type_list->domp[i] == NULL || TP_DOMAIN_TYPE (probe_type_list->domp[i]) == DB_TYPE_VARIABLE)
	{
	  return false;
	}
    }

  return true;
}

/*
 * fused_vet_keys() - VET 2: the byte-equivalence key gate.
 *   return: Error code (NO_ERROR unless a hard error occurs; eligibility in *ok).
 *   thread_p(in): Thread entry.
 *   probe_outptr(in): the probe input's output pointer list.
 *   key_cnt(in): number of join keys.
 *   probe_columns(in)/build_columns(in): per-key list-column positions.
 *   probe_type_list(in)/build_list_id(in): the two side type lists.
 *   ok(out): true iff every key passes the gate.
 *
 * Requires exactly-matching probe/build key domains AND each probe key sourced by a
 * DIRECT base-column attribute fetch (TYPE_ATTR_ID).
 *
 * tp_domain_match (TP_EXACT_MATCH) alone filters domain mismatch but is NOT byte
 * equivalence.  The materialized path NORMALIZES every probe value once by writing the
 * row through qdata_copy_valptr_list_to_tuple() and reading it back as a list tuple
 * (CHAR space padding, NUMERIC canonicalization, collation/codeset and any expression
 * result get re-encoded to their stored representation), then hashes that read-back
 * value.  The fused path instead hashes the IN-FLIGHT value straight from
 * fetch_peek_dbval() + pr_share_value(), with no encode/read round trip.  Only a plain
 * column fetch (TYPE_ATTR_ID) is guaranteed to carry the already-stored representation,
 * so the in-flight value is then provably identical to the materialized one.  Any
 * computed key (TYPE_INARITH / TYPE_OUTARITH / TYPE_FUNC / literal / cast) could hash
 * differently than its materialized form and would silently drop or duplicate join
 * rows, so it refuses to the materialized path.
 */
static int
fused_vet_keys (THREAD_ENTRY * thread_p, OUTPTR_LIST * probe_outptr, int key_cnt, const int *probe_columns,
		const int *build_columns, const QFILE_TUPLE_VALUE_TYPE_LIST * probe_type_list,
		const QFILE_LIST_ID * build_list_id, bool * ok)
{
  REGU_VARIABLE **key_regus;
  TP_DOMAIN *probe_domain, *build_domain;
  int key_index, pos;
  int error = NO_ERROR;

  *ok = false;

  key_regus = (REGU_VARIABLE **) db_private_alloc (thread_p, key_cnt * sizeof (REGU_VARIABLE *));
  if (key_regus == NULL)
    {
      assert_release_error (er_errid () != NO_ERROR);
      return er_errid ();
    }

  error = hjoin_fused_resolve_key_regus (probe_outptr, key_cnt, probe_columns, key_regus);
  if (error != NO_ERROR)
    {
      /* the key column could not be resolved to an outptr entry: refuse silently (no
       * vetted candidate should reach here, but a malformed shape must not abort the
       * query) */
      er_clear ();
      db_private_free_and_init (thread_p, key_regus);
      return NO_ERROR;
    }

  for (key_index = 0; key_index < key_cnt; key_index++)
    {
      if (key_regus[key_index]->type != TYPE_ATTR_ID)
	{
	  goto cleanup;
	}

      pos = probe_columns[key_index];
      if (pos < 0 || pos >= probe_type_list->type_cnt)
	{
	  goto cleanup;
	}
      probe_domain = probe_type_list->domp[pos];

      pos = build_columns[key_index];
      if (pos < 0 || pos >= build_list_id->type_list.type_cnt)
	{
	  goto cleanup;
	}
      build_domain = build_list_id->type_list.domp[pos];

      if (probe_domain == NULL || build_domain == NULL || TP_DOMAIN_TYPE (build_domain) == DB_TYPE_VARIABLE
	  || !tp_domain_match (probe_domain, build_domain, TP_EXACT_MATCH))
	{
	  goto cleanup;
	}
    }

  *ok = true;

cleanup:
  db_private_free_and_init (thread_p, key_regus);
  return NO_ERROR;
}

/*
 * fused_vet_output_positions() - VET 3: every output column position in range.
 *   return: true if eligible.
 *   merge_info(in): the join's list-merge info.
 *   probe_is_outer(in): which merge side the probe (scanned) input is.
 *   probe_type_list(in)/build_list_id(in): the two side type lists.
 *
 * The OUTER merge side is the probe input only when probe_is_outer.
 */
static bool
fused_vet_output_positions (const QFILE_LIST_MERGE_INFO * merge_info, bool probe_is_outer,
			    const QFILE_TUPLE_VALUE_TYPE_LIST * probe_type_list,
			    const QFILE_LIST_ID * build_list_id)
{
  int pos_index, pos, side_type_cnt;

  for (pos_index = 0; pos_index < merge_info->ls_pos_cnt; pos_index++)
    {
      pos = merge_info->ls_pos_list[pos_index];
      if (merge_info->ls_outer_inner_list[pos_index] == QFILE_OUTER_LIST)
	{
	  side_type_cnt = probe_is_outer ? probe_type_list->type_cnt : build_list_id->type_list.type_cnt;
	}
      else if (merge_info->ls_outer_inner_list[pos_index] == QFILE_INNER_LIST)
	{
	  side_type_cnt = probe_is_outer ? build_list_id->type_list.type_cnt : probe_type_list->type_cnt;
	}
      else
	{
	  return false;
	}

      if (pos < 0 || pos >= side_type_cnt)
	{
	  return false;
	}
    }

  return true;
}

/*
 * fused_build_hash_table() - Populate the shared hash table from the COMPLETE build
 *                            input (exactly hjoin_build's single-strategy loop).
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry (the mainblock driver).
 *   state(in/out): the prepared fused state (hash_scan already initialized).
 *   hash_method(in): the resolved IN_MEM / HYBRID method.
 *
 * Opens and closes its own scan over the build list; on any error the scan is still
 * closed before returning.
 */
static int
fused_build_hash_table (THREAD_ENTRY * thread_p, HJOIN_FUSED_STATE * state, HASH_METHOD hash_method)
{
  HASHJOIN_FETCH_INFO build_fetch;
  QFILE_TUPLE_RECORD tuple_record = { NULL, 0 };
  HASH_SCAN_KEY *key;
  SCAN_CODE scan_code;
  bool need_skip_next = false;
  int error = NO_ERROR;

  memset (&build_fetch, 0, sizeof (build_fetch));
  build_fetch.list_id = state->build_list_id;
  build_fetch.input = &state->build_input;
  build_fetch.coerce_domains = state->null_coerce_domains;
  build_fetch.need_coerce_domains = false;
  build_fetch.list_scan_id.status = S_CLOSED;

  error = qfile_open_list_scan (state->build_list_id, &build_fetch.list_scan_id);
  if (error != NO_ERROR)
    {
      return error;
    }

  key = state->hash_scan.temp_key;
  assert (key != NULL);

  while ((scan_code = qfile_scan_list_next (thread_p, &build_fetch.list_scan_id, &tuple_record, PEEK)) == S_SUCCESS)
    {
      error = hjoin_fetch_key (thread_p, &build_fetch, &tuple_record, key, NULL /* compare_key */ , &need_skip_next);
      if (error != NO_ERROR)
	{
	  goto exit;
	}
      else if (need_skip_next)
	{
	  need_skip_next = false;	/* init */
	  continue;
	}

      state->hash_scan.curr_hash_key = qdata_hash_scan_key (key, UINT_MAX, hash_method);

      error = hjoin_build_key (thread_p, &state->hash_scan, &build_fetch.list_scan_id, &tuple_record);
      if (error != NO_ERROR)
	{
	  goto exit;
	}
    }

  if (scan_code == S_ERROR)
    {
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
    }

exit:
  qfile_close_scan (thread_p, &build_fetch.list_scan_id);
  return error;
}

/*
 * qexec_hjoin_fused_prepare() - Post-build arming of the fused probe: vets the
 *                               structural/size/domain eligibility and builds the
 *                               shared hash table from the COMPLETE build input.
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry (the mainblock driver).
 *   xasl(in): The HASHJOIN_PROC node.
 *   probe_aptr(in): The client-marked probe input (either merge side).
 *   fused_state_out(out): The prepared state, or NULL on a (silent) refusal.
 *
 * Note: Every refusal leaves no error set and keeps today's materialized dispatch.
 *       The hash method must come out IN_MEM or HYBRID (a HASH_FILE spill refuses:
 *       the decision is taken here, before any probe row is consumed, so the
 *       materialize fallback is always available).
 */
int
qexec_hjoin_fused_prepare (THREAD_ENTRY * thread_p, XASL_NODE * xasl, XASL_NODE * probe_aptr,
			   HJOIN_FUSED_STATE ** fused_state_out)
{
  HASHJOIN_PROC_NODE *proc;
  QFILE_LIST_MERGE_INFO *merge_info;
  XASL_NODE *probe_xasl, *build_xasl;
  QFILE_LIST_ID *build_list_id;
  HJOIN_FUSED_STATE *state = NULL;

  QFILE_TUPLE_VALUE_TYPE_LIST probe_type_list = { NULL, 0 };
  HASH_METHOD hash_method;

  UINT64 mem_limit;
  UINT32 part_cnt;
  int *probe_columns, *build_columns;
  int key_cnt, key_index, pos_index, pos;
  bool probe_is_outer;
  bool keys_ok;

  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (xasl != NULL && xasl->type == HASHJOIN_PROC);
  assert (fused_state_out != NULL);
  assert (!thread_is_on_trace (thread_p));

  *fused_state_out = NULL;

  proc = &xasl->proc.hashjoin;
  merge_info = &proc->merge_info;

  assert (probe_aptr == proc->outer.xasl || probe_aptr == proc->inner.xasl);
  assert (merge_info->join_type == JOIN_INNER);
  assert (xasl->during_join_pred == NULL);

  probe_is_outer = (probe_aptr == proc->outer.xasl);
  probe_xasl = probe_aptr;
  build_xasl = probe_is_outer ? proc->inner.xasl : proc->outer.xasl;
  assert (probe_xasl != NULL && build_xasl != NULL);

  probe_columns = probe_is_outer ? merge_info->ls_outer_column : merge_info->ls_inner_column;
  build_columns = probe_is_outer ? merge_info->ls_inner_column : merge_info->ls_outer_column;

  if (probe_xasl->outptr_list == NULL)
    {
      return NO_ERROR;
    }

  build_list_id = build_xasl->list_id;
  if (build_list_id == NULL || build_list_id->type_list.type_cnt == 0 || build_list_id->tuple_cnt <= 0
      || build_list_id->type_list.domp == NULL)
    {
      return NO_ERROR;
    }

  key_cnt = merge_info->ls_column_cnt;
  if (key_cnt <= 0 || probe_columns == NULL || build_columns == NULL
      || merge_info->ls_pos_cnt <= 0 || merge_info->ls_pos_list == NULL || merge_info->ls_outer_inner_list == NULL)
    {
      return NO_ERROR;
    }

  /* the build side alone must guarantee the single in-memory strategy (mirror of
   * hjoin_check_partition with min == build count; the client-marked probe side is
   * decisively larger, so the materialized decision could only be SINGLE too) */
  mem_limit = prm_get_bigint_value (PRM_ID_MAX_HASH_LIST_SCAN_SIZE);
  assert (mem_limit > 0);

  part_cnt =
    CEIL_PTVDIV ((sizeof (HENTRY_HLS) + sizeof (QFILE_TUPLE_SIMPLE_POS)) * build_list_id->tuple_cnt,
		 mem_limit * PARTITION_FILL_FACTOR);
  if (part_cnt > 1)
    {
      return NO_ERROR;
    }

  /* the probe-side type list the gather WOULD have been opened with; the fused
   * output adopts these compile-time domains, so they must all be final */
  error = qdata_get_valptr_type_list (thread_p, probe_xasl->outptr_list, &probe_type_list);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  if (probe_type_list.type_cnt <= 0 || probe_type_list.domp == NULL)
    {
      goto refuse;
    }

  /* VET 1: every probe output domain must be final */
  if (!fused_vet_probe_output (&probe_type_list))
    {
      goto refuse;
    }

  /* VET 2: byte-equivalence key gate (exact-match domains + direct-attr probe keys) */
  error = fused_vet_keys (thread_p, probe_xasl->outptr_list, key_cnt, probe_columns, build_columns,
			  &probe_type_list, build_list_id, &keys_ok);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }
  if (!keys_ok)
    {
      goto refuse;
    }

  /* VET 3: every output column position must be in range on its source side */
  if (!fused_vet_output_positions (merge_info, probe_is_outer, &probe_type_list, build_list_id))
    {
      goto refuse;
    }

  state = (HJOIN_FUSED_STATE *) db_private_alloc (thread_p, sizeof (HJOIN_FUSED_STATE));
  if (state == NULL)
    {
      goto error_exit;
    }
  memset (state, 0, sizeof (HJOIN_FUSED_STATE));

  state->build_list_id = build_list_id;
  state->merge_info = merge_info;
  state->key_cnt = key_cnt;
  state->probe_value_indexes = probe_columns;
  state->probe_is_outer = probe_is_outer;
  state->query_id = build_list_id->query_id;
  state->engaged = false;

  /* join output type list (probe columns from the synthesized probe type list, build
   * columns from the complete build list; same construction as hjoin_init_manager) */
  state->output_type_list.type_cnt = merge_info->ls_pos_cnt;
  state->output_type_list.domp =
    (TP_DOMAIN **) db_private_alloc (thread_p, merge_info->ls_pos_cnt * sizeof (TP_DOMAIN *));
  if (state->output_type_list.domp == NULL)
    {
      goto error_exit;
    }

  for (pos_index = 0; pos_index < merge_info->ls_pos_cnt; pos_index++)
    {
      bool from_probe;

      pos = merge_info->ls_pos_list[pos_index];
      from_probe = (merge_info->ls_outer_inner_list[pos_index] == QFILE_OUTER_LIST) == probe_is_outer;

      state->output_type_list.domp[pos_index] =
	from_probe ? probe_type_list.domp[pos] : build_list_id->type_list.domp[pos];
    }

  /* build-side key fetch info (exact match: all-NULL coerce domains) */
  state->build_input.domains = (TP_DOMAIN **) db_private_alloc (thread_p, key_cnt * sizeof (TP_DOMAIN *));
  if (state->build_input.domains == NULL)
    {
      goto error_exit;
    }
  for (key_index = 0; key_index < key_cnt; key_index++)
    {
      state->build_input.domains[key_index] = build_list_id->type_list.domp[build_columns[key_index]];
    }
  state->build_input.value_indexes = build_columns;

  state->null_coerce_domains = (TP_DOMAIN **) db_private_alloc (thread_p, key_cnt * sizeof (TP_DOMAIN *));
  if (state->null_coerce_domains == NULL)
    {
      goto error_exit;
    }
  memset (state->null_coerce_domains, 0, key_cnt * sizeof (TP_DOMAIN *));

  /* hash table over the build list; spill to HASH_FILE refuses (fallback) */
  error = hjoin_scan_init (thread_p, &state->hash_scan, key_cnt, build_list_id);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  hash_method = state->hash_scan.hash_list_scan_type;
  if (hash_method != HASH_METH_IN_MEM && hash_method != HASH_METH_HYBRID)
    {
      goto refuse;
    }

  /* BUILD: populate the shared hash table from the complete build input */
  error = fused_build_hash_table (thread_p, state, hash_method);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  db_private_free_and_init (thread_p, probe_type_list.domp);

  *fused_state_out = state;

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;

refuse:
  /* silent pre-emit refusal: today's materialized dispatch continues */
  if (probe_type_list.domp != NULL)
    {
      db_private_free_and_init (thread_p, probe_type_list.domp);
    }
  if (state != NULL)
    {
      qexec_hjoin_fused_free_state (thread_p, state);
    }

  assert (*fused_state_out == NULL);
  return NO_ERROR;

error_exit:
  if (error == NO_ERROR || er_errid () == NO_ERROR)
    {
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
    }

  if (probe_type_list.domp != NULL)
    {
      db_private_free_and_init (thread_p, probe_type_list.domp);
    }
  if (state != NULL)
    {
      qexec_hjoin_fused_free_state (thread_p, state);
    }

  return error;
}

/*
 * qexec_hjoin_fused_abandon() - Drop a prepared fused state (refusal or error path).
 *   return: None.
 *   thread_p(in): Thread entry (the driver that prepared the state).
 *   fused_state(in): Fused state to free (may be NULL).
 */
void
qexec_hjoin_fused_abandon (THREAD_ENTRY * thread_p, HJOIN_FUSED_STATE * fused_state)
{
  if (fused_state != NULL)
    {
      qexec_hjoin_fused_free_state (thread_p, fused_state);
    }
}

/*
 * qexec_hjoin_fused_is_engaged() - True when at least one scan worker bound to the
 *                                  state (the gather output is then the JOIN output).
 *   return: bool.
 *   fused_state(in): Fused state.
 */
bool
qexec_hjoin_fused_is_engaged (HJOIN_FUSED_STATE * fused_state)
{
  assert (fused_state != NULL);

  return fused_state->engaged;
}

/*
 * qexec_hjoin_fused_output_type_list() - The join output column type list.
 *   return: QFILE_TUPLE_VALUE_TYPE_LIST *.
 *   fused_state(in): Fused state.
 */
QFILE_TUPLE_VALUE_TYPE_LIST *
qexec_hjoin_fused_output_type_list (HJOIN_FUSED_STATE * fused_state)
{
  assert (fused_state != NULL);

  return &fused_state->output_type_list;
}

/*
 * qexec_hjoin_fused_adopt_result() - Consume a fused execution: the probe-input
 *                                    gather already holds the merged join output --
 *                                    move it onto the HASHJOIN node and free the
 *                                    fused state.
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry.
 *   xasl(in): The HASHJOIN_PROC node with an armed fused_state.
 */
int
qexec_hjoin_fused_adopt_result (THREAD_ENTRY * thread_p, XASL_NODE * xasl)
{
  HJOIN_FUSED_STATE *state;
  QFILE_LIST_ID *probe_list_id;

  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (xasl != NULL && xasl->type == HASHJOIN_PROC);

  state = xasl->proc.hashjoin.fused_state;
  assert (state != NULL);
  assert (state->engaged);

  probe_list_id =
    state->probe_is_outer ? xasl->proc.hashjoin.outer.xasl->list_id : xasl->proc.hashjoin.inner.xasl->list_id;
  assert (probe_list_id != NULL);
  assert (xasl->list_id != NULL);

  /* monitor (parity with the materialized path) */
  perfmon_inc_stat (thread_p, PSTAT_QM_NUM_HASHJOINS);

  if (probe_list_id->tuple_cnt > 0)
    {
      /* the gather output IS the join output (join-typed after the worker merge) */
      assert (probe_list_id->type_list.type_cnt == state->output_type_list.type_cnt);
      assert (probe_list_id->last_pgptr == NULL);

      qfile_destroy_list (thread_p, xasl->list_id);
      qfile_copy_list_id (xasl->list_id, probe_list_id, false, QFILE_MOVE_DEPENDENT);

      /* the moved pages now belong to the join node; neutralize the source so the
       * probe input's own cleanup is a no-op */
      qfile_clear_list_id (probe_list_id);
    }
  else
    {
      /* empty join output: produce an empty, join-typed list (exactly what the
       * materialized probe would have produced) */
      QFILE_LIST_ID *empty_list_id;

      empty_list_id = qfile_open_list (thread_p, &state->output_type_list, NULL, state->query_id, QFILE_FLAG_ALL, NULL);
      if (empty_list_id == NULL)
	{
	  error = er_errid ();
	  assert_release_error (error != NO_ERROR);

	  xasl->proc.hashjoin.fused_state = NULL;
	  qexec_hjoin_fused_free_state (thread_p, state);
	  return error;
	}
      qfile_close_list (thread_p, empty_list_id);

      qfile_destroy_list (thread_p, xasl->list_id);
      qfile_copy_list_id (xasl->list_id, empty_list_id, false, QFILE_MOVE_DEPENDENT);
      QFILE_FREE_AND_INIT_LIST_ID (empty_list_id);
    }

  xasl->proc.hashjoin.fused_state = NULL;
  qexec_hjoin_fused_free_state (thread_p, state);

  ASSERT_NO_ERROR_OR_INTERRUPTED ();
  return NO_ERROR;
}

/*
 * qexec_hjoin_fused_worker_open() - Per-scan-worker probe context over the shared
 *                                   read-only hash table.
 *   return: Worker context, or NULL on error (er_set).
 *   thread_p(in): Thread entry (the SCAN WORKER; allocations live on its heap).
 *   fused_state(in): Shared fused state.
 *   outptr_list(in): The WORKER's cloned output pointer list (probe row source).
 *   output_list_id(in): The worker's own gather list (already join-typed).
 */
HJOIN_FUSED_WORKER *
qexec_hjoin_fused_worker_open (THREAD_ENTRY * thread_p, HJOIN_FUSED_STATE * fused_state, OUTPTR_LIST * outptr_list,
			       QFILE_LIST_ID * output_list_id)
{
  HJOIN_FUSED_STATE *state = fused_state;
  HJOIN_FUSED_WORKER *worker = NULL;
  int key_index;
  int error;

  assert (thread_p != NULL);
  assert (state != NULL);
  assert (outptr_list != NULL);
  assert (output_list_id != NULL);

  /* parity assert: the fused path may engage only on a vetted candidate (item C1) */
  assert (state->merge_info != NULL && state->merge_info->join_type == JOIN_INNER);
  assert (state->key_cnt > 0);
  assert (state->hash_scan.hash_list_scan_type == HASH_METH_IN_MEM
	  || state->hash_scan.hash_list_scan_type == HASH_METH_HYBRID);

  worker = (HJOIN_FUSED_WORKER *) db_private_alloc (thread_p, sizeof (HJOIN_FUSED_WORKER));
  if (worker == NULL)
    {
      return NULL;
    }
  memset (worker, 0, sizeof (HJOIN_FUSED_WORKER));

  worker->state = state;
  worker->outptr_list = outptr_list;
  worker->output_list_id = output_list_id;
  worker->build_scan.status = S_CLOSED;

  /* resolve the probe key source regus from this worker's cloned outptr list */
  worker->key_regus = (REGU_VARIABLE **) db_private_alloc (thread_p, state->key_cnt * sizeof (REGU_VARIABLE *));
  if (worker->key_regus == NULL)
    {
      goto error_exit;
    }
  error = hjoin_fused_resolve_key_regus (outptr_list, state->key_cnt, state->probe_value_indexes, worker->key_regus);
  if (error != NO_ERROR)
    {
      /* impossible for a vetted candidate */
      assert (false);
      goto error_exit;
    }

  worker->probe_key = qdata_alloc_hscan_key (thread_p, state->key_cnt, true);
  if (worker->probe_key == NULL)
    {
      goto error_exit;
    }

  worker->found_key = qdata_alloc_hscan_key (thread_p, state->key_cnt, true);
  if (worker->found_key == NULL)
    {
      goto error_exit;
    }

  /* per-worker cursor over the SHARED hash table (never owns the table) */
  worker->hash_scan.hash_list_scan_type = state->hash_scan.hash_list_scan_type;
  worker->hash_scan.memory.hash_table = state->hash_scan.memory.hash_table;
  worker->hash_scan.memory.curr_hash_entry = NULL;
  worker->hash_scan.curr_hash_key = 0;

  error = qfile_open_list_scan (state->build_list_id, &worker->build_scan);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }
  worker->build_scan_opened = true;
  worker->build_scan.is_read_only = true;

  worker->build_fetch.list_id = state->build_list_id;
  worker->build_fetch.input = &state->build_input;
  worker->build_fetch.coerce_domains = state->null_coerce_domains;
  worker->build_fetch.need_coerce_domains = false;
  worker->build_fetch.list_scan_id.status = S_CLOSED;

  /* reusable probe-row encode buffer (grown on demand by the encoder) */
  worker->probe_record.tpl = (char *) db_private_alloc (thread_p, DB_PAGESIZE);
  if (worker->probe_record.tpl == NULL)
    {
      goto error_exit;
    }
  worker->probe_record.size = DB_PAGESIZE;

  /* the gather output is now the JOIN output: the driver must adopt it */
  state->engaged = true;

  return worker;

error_exit:
  qexec_hjoin_fused_worker_close (thread_p, worker);

  assert_release_error (er_errid () != NO_ERROR);
  return NULL;
}

/*
 * qexec_hjoin_fused_worker_row() - Probe ONE in-flight scan row against the shared
 *                                  hash table and emit the merged join tuples
 *                                  directly to the worker's gather list.
 *   return: Error code (NO_ERROR if successful, error code otherwise).
 *   thread_p(in): Thread entry (the scan worker).
 *   worker_ctx(in): The worker's probe context.
 *   val_descr(in): The worker's value descriptor.
 *
 * Note: The probe row is never qfile-encoded unless its key has a hash-bucket
 *       candidate; non-matching rows (the common case) cost only the key peek,
 *       the hash and the table lookup.
 */
int
qexec_hjoin_fused_worker_row (THREAD_ENTRY * thread_p, HJOIN_FUSED_WORKER * worker_ctx, VAL_DESCR * val_descr)
{
  HJOIN_FUSED_WORKER *worker = worker_ctx;
  HJOIN_FUSED_STATE *state;
  HASH_SCAN_KEY *key;
  QFILE_TUPLE_RECORD build_record = { NULL, 0 };
  DB_VALUE *peek_value;
  bool row_encoded = false;
  bool need_skip_next = false;
  int key_index;

  int error = NO_ERROR;

  assert (thread_p != NULL);
  assert (worker != NULL);

  state = worker->state;
  key = worker->probe_key;

  /* probe key straight from the in-flight row values (PEEK semantics: shared, not
   * owned -- byte-equivalent to data_readval (copy=false) on a materialized tuple
   * because the key gate restricted every probe key to a direct base-column attr
   * fetch with an exactly matching domain) */
  for (key_index = 0; key_index < state->key_cnt; key_index++)
    {
      peek_value = NULL;
      error = fetch_peek_dbval (thread_p, worker->key_regus[key_index], val_descr, NULL, NULL, NULL, &peek_value);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}

      if (peek_value == NULL || DB_IS_NULL (peek_value))
	{
	  /* NULL join key on an inner join: the row emits nothing */
	  return NO_ERROR;
	}

      pr_clear_value (key->values[key_index]);
      pr_share_value (peek_value, key->values[key_index]);
    }

  worker->hash_scan.curr_hash_key = qdata_hash_scan_key (key, UINT_MAX, worker->hash_scan.hash_list_scan_type);

  do
    {
      error = hjoin_probe_key (thread_p, &worker->hash_scan, &worker->build_scan, &build_record);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}

      if (build_record.tpl == NULL)
	{
	  break;		/* no (more) candidates */
	}

      if (!row_encoded)
	{
	  /* first candidate: encode the in-flight row ONCE into the reusable buffer
	   * (the only place a probe row ever becomes tuple bytes) */
	  error = qdata_copy_valptr_list_to_tuple (thread_p, worker->outptr_list, val_descr, &worker->probe_record);
	  if (error != NO_ERROR)
	    {
	      goto error_exit;
	    }
	  row_encoded = true;
	}

      error = hjoin_fetch_key (thread_p, &worker->build_fetch, &build_record, worker->found_key,
			       key /* compare_key */ , &need_skip_next);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
      else if (need_skip_next)
	{
	  /* hash collision: keys differ */
	  need_skip_next = false;	/* init */
	  continue;
	}

      error = hjoin_merge_tuple_to_list_id (thread_p, worker->output_list_id,
					    state->probe_is_outer ? &worker->probe_record : &build_record,
					    state->probe_is_outer ? &build_record : &worker->probe_record,
					    state->merge_info, &worker->overflow_record);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
    }
  while (true);

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
 * qexec_hjoin_fused_worker_close() - Release a worker probe context (same worker
 *                                    thread that opened it).
 *   return: None.
 *   thread_p(in): Thread entry (the scan worker).
 *   worker_ctx(in): The worker's probe context (may be NULL).
 */
void
qexec_hjoin_fused_worker_close (THREAD_ENTRY * thread_p, HJOIN_FUSED_WORKER * worker_ctx)
{
  HJOIN_FUSED_WORKER *worker = worker_ctx;

  if (worker == NULL)
    {
      return;
    }

  if (worker->build_scan_opened)
    {
      qfile_close_scan (thread_p, &worker->build_scan);
    }

  if (worker->probe_key != NULL)
    {
      qdata_free_hscan_key (thread_p, worker->probe_key, worker->probe_key->val_count);
    }

  if (worker->found_key != NULL)
    {
      qdata_free_hscan_key (thread_p, worker->found_key, worker->found_key->val_count);
    }

  if (worker->key_regus != NULL)
    {
      db_private_free_and_init (thread_p, worker->key_regus);
    }

  if (worker->probe_record.tpl != NULL)
    {
      db_private_free_and_init (thread_p, worker->probe_record.tpl);
    }

  if (worker->overflow_record.tpl != NULL)
    {
      db_private_free_and_init (thread_p, worker->overflow_record.tpl);
    }

  /* the hash table is owned by the shared state -- never released here */
  worker->hash_scan.memory.hash_table = NULL;

  db_private_free_and_init (thread_p, worker);
}

#endif /* defined (SERVER_MODE) && !defined (WINDOWS) */
