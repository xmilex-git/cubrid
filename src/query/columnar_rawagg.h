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
 * columnar_rawagg.h - fused aggregate transition + raw hash aggregation
 *
 * Design ticket #23, decisions D3 / D7 / D8 / D9.
 *
 * The existing hash aggregation materializes the whole output pointer list
 * into DB_VALUEs for EVERY row BEFORE probing the hash table — including rows
 * that only fold into an existing group.  That materialization is what the
 * profile attributes 44.7% of Q1 to.  This module removes it structurally:
 * group keys and accumulators are raw, and a DB_VALUE appears only when a
 * finished group is emitted.
 *
 * The transition is FUSED (D7): the group lookup and the per-aggregate
 * accumulation are appended to the same RAW_PROG as steps, so one evaluation
 * per row completes argument evaluation, group lookup and accumulation.  With
 * no group-by keys the lookup step degenerates to a single fixed accumulator,
 * which is exactly the BUILDVALUE shape — the same program either way.
 *
 * There is no spill (D8): the table has a fixed budget
 * (columnar_agg_hash_size) and exceeding it raises ER_COLUMNAR_AGG_HASH_FULL.
 */

#ifndef _COLUMNAR_RAWAGG_H_
#define _COLUMNAR_RAWAGG_H_

#if !defined (SERVER_MODE) && !defined (SA_MODE)
#error Belongs to server module
#endif

#include "columnar_rawprog.h"
#include "columnar_scan.h"
#include "xasl_aggregate.hpp"

typedef struct raw_agg RAW_AGG;

/*
 * Build the fused program and the hash table.  key_roots/n_keys are the
 * group-by key expressions (n_keys == 0 for BUILDVALUE).  Returns
 * ER_COLUMNAR_UNSUPPORTED_EXPR when a key or an aggregate is outside the raw
 * coverage — the caller then keeps the existing DB_VALUE path (D13 lets the
 * two coexist until the parity gate).
 *
 * operand_override[i], when non-NULL, replaces aggregate i's own operand.
 * A GROUP BY block needs this: its aggregate operands read value-list slots
 * that the row path refills per tuple, so the expression to compile is the
 * SCAN-time regu that fills the slot, not the slot reference itself.  Pass
 * NULL (or NULL entries) to use the aggregate's own operand, which is what a
 * BUILDVALUE block wants — there the operands already point straight at the
 * scan's slots.
 */
extern int columnar_rawagg_build (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, REGU_VARIABLE ** key_roots,
				  int n_keys, AGGREGATE_TYPE * agg_list, REGU_VARIABLE ** operand_override,
				  RAW_AGG ** out);

/* consume every qualified row of the scan */
extern int columnar_rawagg_run (THREAD_ENTRY * thread_p, RAW_AGG * ra, COLUMNAR_SCAN * cs);

/* result groups (see columnar_rawagg_sort_groups for their order) */
extern int columnar_rawagg_group_count (const RAW_AGG * ra);

/*
 * Order the groups by group key, so that emission matches what the
 * sort-based group-by would have produced.
 *
 * This is not cosmetic.  The optimizer removes an `ORDER BY <group key>` at
 * COMPILE time, because the sort-based group-by sorts its input by the key
 * and its output is therefore already ordered.  A hash-based path has no such
 * property, and by the time it runs there is no ORDER BY left in the plan to
 * re-enable — so the order has to be produced here or not at all.
 *
 * desc[i] / nulls_last[i] describe key i, taken from the block's group-by
 * sort list.  After this call, group ordinal g refers to the g-th group in
 * that order.
 */
extern int columnar_rawagg_sort_groups (RAW_AGG * ra, const bool * desc, const bool * nulls_last);

/*
 * Materialize group g.
 *
 * keys_out[i] points at the DB_VALUE that key i should be written into
 * (pass NULL when there are no keys); an entry may itself be NULL to skip
 * that key.  Taking pointers rather than an array lets a caller aim the keys
 * straight at the value-list slots the group-by output reads.
 *
 * The aggregates are written into the engine's own accumulators as
 * (value, curr_cnt) — the raw path replaces the per-row TRANSITION, which is
 * where the cost is, and leaves FINALIZATION (AVG's division, the result
 * coercion) to qdata_finalize_aggregate_list () exactly as the row path does.
 * That keeps AVG bit-identical to heap instead of inheriting PostgreSQL's
 * division scale here, and keeps this seam small.
 */
extern int columnar_rawagg_load_group (RAW_AGG * ra, int g, DB_VALUE ** keys_out, AGGREGATE_TYPE * agg_list);

extern int columnar_rawagg_key_count (const RAW_AGG * ra);
extern int columnar_rawagg_agg_count (const RAW_AGG * ra);

/* rows accumulated (query statistics only) */
extern INT64 columnar_rawagg_row_count (const RAW_AGG * ra);

/*
 * The value-list slot each key expression publishes into — that is, the key
 * regu variables' vfetch_to targets, captured at build time.  Handing this
 * straight to columnar_rawagg_load_group () puts a group's key values exactly
 * where the group-by output expressions read them, which is what the per-row
 * fetch_val_list () would otherwise have done from a materialized tuple.
 */
extern DB_VALUE **columnar_rawagg_key_targets (RAW_AGG * ra);

extern void columnar_rawagg_free (RAW_AGG * ra);

#endif /* _COLUMNAR_RAWAGG_H_ */
