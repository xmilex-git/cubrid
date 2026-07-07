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
 * query_hash_join.h
 */

#ifndef _QUERY_HASH_JOIN_H_
#define _QUERY_HASH_JOIN_H_

#include "regu_var.hpp"		/* REGU_VARIABLE_LIST */

#if defined (SERVER_MODE) || defined (SA_MODE)
#include "query_hash_scan.h"	/* HASH_METHOD */
#include "system.h"		/* UINT32, UINT64 */
#include "thread_entry.hpp"	/* THREAD_ENTRY */
#include "tsc_timer.h"		/* TSC_TICKS, TSCTIMEVAL, TSC_ADD_TIMEVAL */
#include "xasl_predicate.hpp"	/* PRED_EXPR */
#endif /* defined (SERVER_MODE) || defined (SA_MODE) */

/*
 * Debug Macros
 */

#define HASHJOIN_PROFILE_TIME 0
#define HASHJOIN_COLLISION_RATE 0
#define HASHJOIN_DUMP_PARTITION 0
#define HASHJOIN_DUMP_HASH_TABLE 0
#define HASHJOIN_DUMP_BUILD 0
#define HASHJOIN_DUMP_PROBE 0

/*
 * Forward Declarations
 */

namespace parallel_query
{
  class worker_manager;
}

namespace qfile
{
  class chunk_distributor;
  class tapeset;
}

struct xasl_node;
struct tp_domain;

typedef struct xasl_node XASL_NODE;
typedef struct tp_domain TP_DOMAIN;

/*
 * Enum & Typedef Definitions
 */

typedef enum hashjoin_status
{
  HASHJOIN_STATUS_NONE = 0,
  HASHJOIN_STATUS_FILL_NULL_VALUES,
  HASHJOIN_STATUS_TRY,
  HASHJOIN_STATUS_SINGLE,
  HASHJOIN_STATUS_PARTITION,
  HASHJOIN_STATUS_PARALLEL,
  HASHJOIN_STATUS_PARALLEL_PROBE,
  /* issue #147 T1 S2: hjoin_try_partition's serial (non-px) fallback ran the
   * Grace batch state machine to completion (single_context->list_id is
   * already the final joined result) -- qexec_hash_join must NOT call
   * hjoin_execute_partitions again for this status, unlike PARTITION. */
  HASHJOIN_STATUS_GRACE_DONE,
  HASHJOIN_STATUS_END,
  HASHJOIN_STATUS_ERROR
} HASHJOIN_STATUS;

typedef enum hashjoin_merge_method
{
  HASHJOIN_MERGE_COMBINE = 0,
  HASHJOIN_MERGE_APPEND,
  HASHJOIN_MERGE_CONNECT
} HASHJOIN_MERGE_METHOD;

typedef enum hashjoin_profile_step
{
  HASHJOIN_PROFILE_NONE = 0,
  HASHJOIN_PROFILE_BUILD_FETCH,	/* hjoin_fetch_key */
  HASHJOIN_PROFILE_BUILD_HASH,	/* qdata_hash_scan_key */
  HASHJOIN_PROFILE_BUILD_INSERT,	/* hjoin_build_key */
  HASHJOIN_PROFILE_PROBE_FETCH,	/* hjoin_fetch_key */
  HASHJOIN_PROFILE_PROBE_HASH,	/* qdata_hash_scan_key */
  HASHJOIN_PROFILE_PROBE_SEARCH,	/* hjoin_probe_key */
  HASHJOIN_PROFILE_PROBE_MATCH,	/* hjoin_fetch_key */
  HASHJOIN_PROFILE_PROBE_ADD,	/* hjoin_merge_tuple_to_list_id */
  HASHJOIN_PROFILE_MERGE	/* hjoin_merge_qlist */
} HASHJOIN_PROFILE_STEP;

typedef enum hashjoin_print_step
{
  HASHJOIN_PRINT_NONE = 0,
  HASHJOIN_PRINT_READ_KEY,
  HASHJOIN_PRINT_NOT_MATCHED_KEY,
  HASHJOIN_PRINT_NOT_QUALIFIED_KEY,
  HASHJOIN_PRINT_QUALIFIED_KEY,
  HASHJOIN_PRINT_FILL_EMPTY_KEY
} HASHJOIN_PRINT_STEP;

/*
 * Struct & Typedef Definitions
 */

typedef struct hashjoin_input
{
  XASL_NODE *xasl;

  /* For evaluating during-join predicates. */
  REGU_VARIABLE_LIST regu_list_pred;
} HASHJOIN_INPUT;

typedef struct hashjoin_input_domain_info
{
  TP_DOMAIN **domains;
  int *value_indexes;
} HASHJOIN_INPUT_DOMAIN_INFO;

typedef struct hashjoin_domain_info
{
  HASHJOIN_INPUT_DOMAIN_INFO outer;
  HASHJOIN_INPUT_DOMAIN_INFO inner;

  /* Common domains of build and probe inputs. */
  TP_DOMAIN **coerce_domains;

  /* Whether to use the coerce domain. */
  bool need_coerce_domains;
} HASHJOIN_DOMAIN_INFO;

#if defined (SERVER_MODE) || defined (SA_MODE)

typedef struct hashjoin_range_time_stats
{
  TSCTIMEVAL min;
  TSCTIMEVAL max;
} HASHJOIN_RANGE_TIME_STATS;
#define HASHJOIN_RANGE_TIME_STATS_INITIALIZER { { LONG_MAX, 999999 }, { 0, 0 } }

typedef struct hashjoin_range_rows_stats
{
  UINT64 min;
  UINT64 max;
} HASHJOIN_RANGE_ROWS_STATS;
#define HASHJOIN_RANGE_ROWS_STATS_INITIALIZER { LONG_MAX, 0 }

typedef struct hashjoin_input_stats
{
  TSCTIMEVAL elapsed_time;
  UINT64 fetches;
  UINT64 ioreads;
  UINT64 read_rows;
  UINT64 read_keys;
  UINT64 qualified_rows;
} HASHJOIN_INPUT_STATS;

typedef struct hashjoin_range_stats
{
  HASHJOIN_RANGE_TIME_STATS elapsed_time;
  HASHJOIN_RANGE_ROWS_STATS read_rows;
  HASHJOIN_RANGE_ROWS_STATS read_keys;
  HASHJOIN_RANGE_ROWS_STATS qualified_rows;
} HASHJOIN_RANGE_STATS;
#define HASHJOIN_RANGE_STATS_INITIALIZER \
  { HASHJOIN_RANGE_TIME_STATS_INITIALIZER, \
    HASHJOIN_RANGE_ROWS_STATS_INITIALIZER, \
    HASHJOIN_RANGE_ROWS_STATS_INITIALIZER, \
    HASHJOIN_RANGE_ROWS_STATS_INITIALIZER }

// *INDENT-OFF*
typedef struct hashjoin_build_stats : hashjoin_input_stats
{
  HASHJOIN_RANGE_TIME_STATS range_elapsed_time;
} HASHJOIN_BUILD_STATS;
// *INDENT-ON*

// *INDENT-OFF*
typedef struct hashjoin_probe_stats : hashjoin_input_stats
{
  HASHJOIN_RANGE_STATS range;
} HASHJOIN_PROBE_STATS;
// *INDENT-ON*

#if HASHJOIN_PROFILE_TIME
typedef struct hashjoin_profile_stats
{
  struct
  {
    TSCTIMEVAL fetch;		/* hjoin_fetch_key */
    TSCTIMEVAL hash;		/* qdata_hash_scan_key */
    TSCTIMEVAL insert;		/* hjoin_build_key */
  } build;

  struct
  {
    TSCTIMEVAL fetch;		/* hjoin_fetch_key */
    TSCTIMEVAL hash;		/* qdata_hash_scan_key */
    TSCTIMEVAL search;		/* hjoin_probe_key */
    TSCTIMEVAL match;		/* hjoin_fetch_key */
    TSCTIMEVAL add;		/* hjoin_merge_tuple_to_list_id */
  } probe;

  struct
  {
    TSCTIMEVAL elapsed_time;	/* hjoin_fetch_key */
    UINT64 fetches;
    UINT64 ioreads;
    UINT64 qualified_rows;
  } merge;
} HASHJOIN_PROFILE_STATS;
#endif /* HASHJOIN_PROFILE_TIME */

typedef struct hashjoin_start_stats
{
  TSC_TICKS tick;
  UINT64 fetches;
  UINT64 ioreads;
  HASHJOIN_PROFILE_STEP step;
} HASHJOIN_START_STATS;
#define HASHJOIN_START_STATS_INITIALIZER { { 0 }, 0, 0, HASHJOIN_PROFILE_NONE }

typedef struct hashjoin_stats
{
  UINT32 num_parallel_threads;

  HASH_METHOD hash_method;
  bool use_hash_memory;
  bool use_hash_hybrid;
  bool use_hash_file;
  bool use_hash_skip;

  bool swap_join_inputs;

  double collision_rate;

  HASHJOIN_INPUT_STATS split;
  HASHJOIN_INPUT_STATS parallel;
  HASHJOIN_BUILD_STATS build;
  HASHJOIN_PROBE_STATS probe;

#if HASHJOIN_PROFILE_TIME
  HASHJOIN_INPUT_STATS merge;
  HASHJOIN_PROFILE_STATS profile;
#endif				/* HASHJOIN_PROFILE_TIME */
} HASHJOIN_STATS;

typedef struct hashjoin_stats_group
{
  HASHJOIN_STATUS status;
  HASHJOIN_STATS stats;
  HASHJOIN_STATS *context_stats;
  UINT32 context_cnt;
} HASHJOIN_STATS_GROUP;

/* HASHJOIN_FETCH_INFO */
typedef struct hashjoin_fetch_info
{
  QFILE_LIST_ID *list_id;
  QFILE_LIST_SCAN_ID list_scan_id;
  QFILE_TUPLE_RECORD tuple_record;
  QFILE_TUPLE_RECORD *fill_record;

  /* Pointers to members of HASHJOIN_DOMAIN_INFO,
   * which is a member of HASHJOIN_PROC_NODE. */
  HASHJOIN_INPUT_DOMAIN_INFO *input;
  TP_DOMAIN **coerce_domains;
  bool need_coerce_domains;

  /* Pointer to a member of HASHJOIN_INPUT. */
  REGU_VARIABLE_LIST regu_list_pred;
} HASHJOIN_FETCH_INFO;

/* HASHJOIN_INPUT_SPLIT_INFO */
typedef struct hashjoin_input_split_info
{
  HASHJOIN_FETCH_INFO *fetch_info;
  QFILE_LIST_ID **part_list_id;
} HASHJOIN_INPUT_SPLIT_INFO;

/* HASHJOIN_SPLIT_INFO */
typedef struct hashjoin_split_info
{
  HASHJOIN_INPUT_SPLIT_INFO outer;
  HASHJOIN_INPUT_SPLIT_INFO inner;
} HASHJOIN_SPLIT_INFO;

/* HASHJOIN_SHARED_SPLIT_INFO */
typedef struct hashjoin_shared_split_info
{
  // *INDENT-OFF*
  std::mutex *part_mutexes;

  /* Tapeset split input, read in parallel via chunk_distributor + per-worker
   * tapeset_reader (mirrors the probe path).  Built/destroyed per split-run
   * (outer, then inner) in build_partitions. */
  qfile::tapeset *new_tapeset;
  qfile::chunk_distributor *new_dist;

  /* Per-worker output partition lists: each worker writes its own lists without
   * part_mutexes, then the leader merges them into part_list_id.
   * worker_part_lists[worker_index][part_index].  Allocated/freed per split-run;
   * NULL on the serial pgbuf-paged path. */
  QFILE_LIST_ID ***worker_part_lists;
  UINT32 worker_count;

  hashjoin_shared_split_info ()
    : part_mutexes (nullptr)
    , new_tapeset (nullptr)
    , new_dist (nullptr)
    , worker_part_lists (nullptr)
    , worker_count (0)
  {
    //
  }
  // *INDENT-ON*
} HASHJOIN_SHARED_SPLIT_INFO;

/* HASHJOIN_SHARED_PROBE_INFO */
typedef struct hashjoin_shared_probe_info
{
  // *INDENT-OFF*
  std::mutex stats_mutex;
  HASHJOIN_RANGE_STATS probe_range;

  /* Tapeset probe input, read in parallel via chunk_distributor + per-worker
   * tapeset_reader; built on the coordinating thread and freed at cleanup. */
  qfile::tapeset *new_tapeset;
  qfile::chunk_distributor *new_dist;

  hashjoin_shared_probe_info ()
    : stats_mutex ()
    , probe_range HASHJOIN_RANGE_STATS_INITIALIZER
    , new_tapeset (nullptr)
    , new_dist (nullptr)
  {
    //
  }
  // *INDENT-ON*
} HASHJOIN_SHARED_PROBE_INFO;

/* HASHJOIN_SHARED_JOIN_INFO */
typedef struct hashjoin_shared_join_info
{
  // *INDENT-OFF*
  std::mutex scan_mutex;
  SCAN_POSITION scan_position;
  UINT32 next_index;

  std::mutex stats_mutex;
  HASHJOIN_RANGE_TIME_STATS build_range_time;
  HASHJOIN_RANGE_TIME_STATS probe_range_time;

  hashjoin_shared_join_info ()
    : scan_mutex ()
    , scan_position (S_BEFORE)
    , next_index (0)
    , stats_mutex ()
    , build_range_time HASHJOIN_RANGE_TIME_STATS_INITIALIZER
    , probe_range_time HASHJOIN_RANGE_TIME_STATS_INITIALIZER
  {
    //
  }
  // *INDENT-ON*
} HASHJOIN_SHARED_JOIN_INFO;

/* HASHJOIN_CONTEXT*/
typedef struct hashjoin_context
{
  QFILE_LIST_ID *list_id;

  HASHJOIN_FETCH_INFO outer;
  HASHJOIN_FETCH_INFO inner;

  /* Set in hjoin_init_context or hjoin_outer_fill_null_values. */
  HASHJOIN_FETCH_INFO *build;
  HASHJOIN_FETCH_INFO *probe;

  HASH_LIST_SCAN hash_scan;
  PRED_EXPR *during_join_pred;
  VAL_DESCR *val_descr;

  HASHJOIN_STATUS status;

  /* Pointer to a member of HASHJOIN_MANAGER. */
  HASHJOIN_STATS *stats;
} HASHJOIN_CONTEXT;

/* HASHJOIN_MANAGER*/
typedef struct hashjoin_manager
{
  /* Pointer to a member of HASHJOIN_PROC_NODE. */
  HASHJOIN_INPUT *outer;
  HASHJOIN_INPUT *inner;
  QFILE_LIST_MERGE_INFO *merge_info;

  /* Copy of a member of QFILE_LIST_MERGE_INFO. */
  JOIN_TYPE join_type;
  int key_cnt;

  /* Pointer to a member of XASL_NODE. */
  PRED_EXPR *during_join_pred;
  int num_parallel_threads;

  /* Pointer to a member of XASL_STATE. */
  QUERY_ID query_id;
  VAL_DESCR *val_descr;

  /* issue #149 P3: the enclosing execution's XASL_STATE, and whether this
   * join's outer runs in push-stream mode (probe rows pushed straight from
   * the outer subplan's driver, no outer materialization).  See
   * hjoin_outer_stream_push_eligible. */
  struct xasl_state *xasl_state;
  bool outer_stream_push;

  HASHJOIN_CONTEXT single_context;
  HASHJOIN_CONTEXT *contexts;
  UINT32 context_cnt;

  QFILE_TUPLE_VALUE_TYPE_LIST type_list;
  HASHJOIN_MERGE_METHOD qlist_merge_method;
  int qlist_flag;

  // *INDENT-OFF*
  parallel_query::worker_manager *px_worker_manager;
  // *INDENT-ON*
  UINT64 *px_worker_stats;

  /* From HASHJOIN_PROC_NODE */
  HASHJOIN_STATS_GROUP *stats_group;

#if HASHJOIN_DUMP_HASH_TABLE
  pthread_mutex_t dump_hash_table_mutex;
#endif				/* HASHJOIN_DUMP_HASH_TABLE */
} HASHJOIN_MANAGER;

/*
 * Macro Function Declarations
 */

#if HASHJOIN_PROFILE_TIME
#define HJOIN_PROFILE_START(thread_p, start_stats_p, step) \
   if (thread_is_on_trace ((thread_p))) \
     { \
       hjoin_profile_start ((thread_p), (start_stats_p), (step)); \
     }
#define HJOIN_PROFILE_END(thread_p, stats_p, start_stats_p, step) \
   if (thread_is_on_trace ((thread_p))) \
     { \
       hjoin_profile_end ((thread_p), (stats_p), (start_stats_p), (step)); \
     }
#define HJOIN_PROFILE_MERGE_END(thread_p, stats_p, start_stats_p, step, rows) \
   if (thread_is_on_trace ((thread_p))) \
     { \
       assert ((step) == HASHJOIN_PROFILE_MERGE); \
       hjoin_profile_end ((thread_p), (stats_p), (start_stats_p), (step)); \
       (stats_p)->merge.qualified_rows = (rows); \
     }
#else
#define HJOIN_PROFILE_START(thread_p, start_stats, step) ((void) 0)
#define HJOIN_PROFILE_END(thread_p, stats_p, start_stats_p, step) ((void) 0)
#define HJOIN_PROFILE_MERGE_END(thread_p, stats_p, start_stats_p, step, rows) ((void) 0)
#endif /* HASHJOIN_PROFILE_TIME */

#if !defined(NDEBUG) && HASHJOIN_DUMP_PROBE
#define HJOIN_PRINT_TUPLE(list_id, tuple, step) \
  hjoin_print_tuple ((list_id), (tuple), (step))
#else
#define HJOIN_PRINT_TUPLE(list_id, tuple, step) ((void) 0)
#endif /* !NDEBUG && HASHJOIN_DUMP_PROBE */

/*
 * Function Declarations
 */

int qexec_hash_join (THREAD_ENTRY * thread_p, XASL_NODE * xasl, QUERY_ID query_id, VAL_DESCR * val_descr,
		     struct xasl_state * xasl_state);

/* issue #149 P3: true when this HASHJOIN_PROC's plan-time-detached outer
 * (XASL_HASHJOIN_OUTER_STREAMED) can actually be push-streamed at runtime:
 * JOIN_LEFT (probe side is always the physical outer -- RIGHT swaps outer
 * into the build side, which needs real materialized sizes) and the outer is
 * a narrow-shape BUILDLIST (no sort/group/agg/analytic/connect-by/topn --
 * every feature whose semantics depend on the materialized list).  The
 * executor consults this to decide whether to pre-materialize the detached
 * outer (fallback) or leave it to the join's probe phase. */
bool hjoin_outer_stream_push_eligible (const XASL_NODE * hashjoin_xasl);

/* issue #147 T1 S2 gate 1: process-wide count of hjoin_probe_key's random-read
 * branches (HYBRID/HASH_FILE -> qfile_jump_scan_tuple_position). Meaningful
 * only when scoped to a query run known to have taken the Grace path (see
 * hjoin_execute_grace) -- must stay 0 for such a run. */
long hjoin_debug_random_probe_read_count (void);

/* issue #147 T1 S4: process-wide count of mid-build/mid-reload nbatch
 * doublings actually performed by hjoin_execute_grace (see
 * hjoin_grace_maybe_grow). Zero for any run whose upfront nbatch estimate
 * held; nonzero means the estimate under-shot (skew or per-tuple-size
 * variance) and the real-memory-tracked overflow check kicked in. */
long hjoin_debug_grace_nbatch_grow_count (void);

/* issue #147 T1 S4: cumulative over-hash_mem bytes accepted once the
 * give-up heuristic (hjoin_grace_maybe_grow) has latched grow_enabled=false
 * for a join (repeated doubling attempts stopped helping -- e.g. one
 * dominant key). Zero means growth either never triggered or, when it did,
 * kept freeing memory each time. */
long hjoin_debug_grace_skew_overflow_bytes (void);

/* issue #149 P2: process-wide count of hjoin_execute_grace invocations that
 * confirmed probe=outer (XASL_HASHJOIN_OUTER_STREAMED, JOIN_LEFT) and
 * actually pulled outer row-by-row instead of self-materializing it. */
long hjoin_debug_outer_streamed_count (void);

/* issue #149 P2: process-wide count of hjoin_execute_grace invocations where
 * outer was flagged XASL_HASHJOIN_OUTER_STREAMED but had to be
 * self-materialized anyway -- either because it was confirmed build side
 * (JOIN_RIGHT) or its XASL shape didn't qualify for the narrow streaming
 * path (CTE/aggregate/scan_ptr chain). Zero regression either way (same
 * result as the pre-P2 fully-materialized path), just no W1 benefit. */
long hjoin_debug_outer_fallback_materialize_count (void);

/* Hash Join Execution */
int hjoin_execute (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context);
/* issue #147 S6: per-partition PARALLEL entry point (join_task::execute) --
 * forces the Grace IN_MEM reload instead of hjoin_execute's old auto-tier
 * select for one claimed partition context. See query_hash_join.c. */
int hjoin_execute_grace_px (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context,
			    UINT32 worker_id);
int hjoin_merge_qlist (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager, HASHJOIN_CONTEXT * context);

/* Hash Join Shared Split Info */
int hjoin_init_shared_split_info (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager,
				  HASHJOIN_SHARED_SPLIT_INFO * shared_info);
void hjoin_clear_shared_split_info (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager,
				    HASHJOIN_SHARED_SPLIT_INFO * shared_info);

/* Hash List Scan */
/* issue #147 S5-lite: use_grace (NULL-able; NULL for the "skip hash table"
 * list_id == NULL callers, e.g. px probe workers) is set true when the
 * IN_MEM tier's byte estimate said it should fit but the layer-2 accountant's
 * live reserve rejected it -- the caller must re-route into Grace instead of
 * accepting the HYBRID/HASH_FILE degrade this function would otherwise pick
 * (hash_scan is left cleared/unusable in that case). */
int hjoin_scan_init (THREAD_ENTRY * thread_p, HASH_LIST_SCAN * hash_scan, int key_cnt, QFILE_LIST_ID * list_id,
		     bool * use_grace);
void hjoin_scan_clear (THREAD_ENTRY * thread_p, HASH_LIST_SCAN * hash_scan);

/* Hash Join Partitioning */
int hjoin_fetch_key (THREAD_ENTRY * thread_p, HASHJOIN_FETCH_INFO * fetch_info, QFILE_TUPLE_RECORD * tuple_record,
		     HASH_SCAN_KEY * key, HASH_SCAN_KEY * compare_key, bool * need_skip_next);
void hjoin_update_tuple_hash_key (THREAD_ENTRY * thread_p, QFILE_TUPLE_RECORD * tuple_record, UINT32 hash_key);
int hjoin_probe_key (THREAD_ENTRY * thread_p, HASH_LIST_SCAN * hash_scan, QFILE_LIST_SCAN_ID * list_scan_id,
		     QFILE_TUPLE_RECORD * tuple_record);
int hjoin_merge_tuple_to_list_id (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id,
				  QFILE_TUPLE_RECORD * outer_record, QFILE_TUPLE_RECORD * inner_record,
				  QFILE_LIST_MERGE_INFO * merge_info, QFILE_TUPLE_RECORD * overflow_record);

void hjoin_trace_start (THREAD_ENTRY * thread_p, HASHJOIN_START_STATS * start_stats);
void hjoin_trace_end (THREAD_ENTRY * thread_p, HASHJOIN_INPUT_STATS * stats, HASHJOIN_START_STATS * start_stats);
void hjoin_trace_merge_stats (HASHJOIN_STATS * stats, HASHJOIN_STATS * context_stats, HASHJOIN_STATUS status);

UINT64 *hjoin_trace_get_worker_stats (HASHJOIN_MANAGER * manager, int index);
void hjoin_trace_drain_worker_stats (THREAD_ENTRY * thread_p, HASHJOIN_MANAGER * manager);

#if HASHJOIN_PROFILE_TIME
void hjoin_profile_start (THREAD_ENTRY * thread_p, HASHJOIN_START_STATS * start_stats, HASHJOIN_PROFILE_STEP step);
void hjoin_profile_end (THREAD_ENTRY * thread_p, HASHJOIN_PROFILE_STATS * stats, HASHJOIN_START_STATS * start_stats,
			HASHJOIN_PROFILE_STEP step);
#endif /* HASHJOIN_PROFILE_TIME */

#endif /* defined (SERVER_MODE) || defined (SA_MODE) */

#endif /* _QUERY_HASH_JOIN_H_ */
