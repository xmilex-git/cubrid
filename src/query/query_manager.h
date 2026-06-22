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
 * Query manager (Server Side)
 */

#ifndef _QUERY_MANAGER_H_
#define _QUERY_MANAGER_H_

#ident "$Id$"

#if !defined (SERVER_MODE) && !defined (SA_MODE)
#error Belongs to server module
#endif /* !defined (SERVER_MODE) && !defined (SA_MODE) */

#include "dbtype_def.h"
#include "file_manager.h"
#include "list_file.h"
#include "storage_common.h"
#include "thread_compat.hpp"

// forward definitions
struct xasl_cache_ent;

/* Phase 1 temp-page accessor split (Axis A). Replaces the legacy
 * qmgr_free_old_page_and_init / qmgr_free_old_page_simple_fix_and_init macros.
 * Free is backing-dispatch (Phase 1: pgbuf_unfix for every QMGR_TEMP_FILE backing). */
#define qmgr_temp_page_free_readonly_and_init(thread_p, page_p, tfile_vfidp) \
  do \
    { \
      qmgr_temp_page_free_readonly ((thread_p), (page_p), (tfile_vfidp)); \
      (page_p) = NULL; \
    } \
  while (0)

/* Latchless (pgbuf_simple_fix) read-only temp-page release. */
#define qmgr_temp_page_free_readonly_simple_and_init(thread_p, page_p, tfile_vfidp) \
  do \
    { \
      qmgr_temp_page_free_readonly_simple ((thread_p), (page_p), (tfile_vfidp)); \
      (page_p) = NULL; \
    } \
  while (0)

#define NULL_PAGEID_IN_PROGRESS -2

typedef enum
{
  TEMP_FILE_MEMBUF_NONE = -1,
  TEMP_FILE_MEMBUF_NORMAL,
  TEMP_FILE_MEMBUF_KEY_BUFFER,

  TEMP_FILE_MEMBUF_NUM_TYPES
} QMGR_TEMP_FILE_MEMBUF_TYPE;

typedef enum
{
  QMGR_TRAN_NULL,		/* Null transaction: a transaction not issued a query */
  QMGR_TRAN_RUNNING,		/* Running transaction */
  QMGR_TRAN_DELAYED_START,	/* Suspended transaction: waiting for all the waiting transactions to be served */
  QMGR_TRAN_WAITING,		/* Suspended transaction: waiting for a query file page to be freed. */
  QMGR_TRAN_RESUME_TO_DEALLOCATE,	/* Transaction has been resumed to deallocate all query pages. Transaction will
					 * have to restart the query */
  QMGR_TRAN_RESUME_DUE_DEADLOCK,	/* Transaction has been resumed to deallocate all query pages. The transaction
					 * was involved in a deadlock. Transaction will have to restart the query. Note
					 * that the transaction is not aborted. */
  QMGR_TRAN_TERMINATED,		/* Terminated transaction */
  QMGR_TRAN_DBLINK_ABORTED	/* A dblink'ed transaction is aborted */
} QMGR_TRAN_STATUS;

typedef struct qmgr_temp_file QMGR_TEMP_FILE;
struct qmgr_temp_file
{
  QMGR_TEMP_FILE *next;
  QMGR_TEMP_FILE *prev;
  FILE_TYPE temp_file_type;
  VFID temp_vfid;
  int membuf_last;
  PAGE_PTR *membuf;
  int membuf_npages;
  QMGR_TEMP_FILE_MEMBUF_TYPE membuf_type;
  bool preserved;		/* if temp file is preserved */
  bool tde_encrypted;		/* whether the file of temp_vfid has to be encrypted when flushing (TDE) */
  /* P2-B: provably single-owner (plan-wide non-parallel, non-result, non-preserved, non-holdable)
   * list. When true, the disk pages of this temp file are routed through file_io as per-call copy
   * buffers with ZERO page-buffer fixes; the membuf(NULL_VOLID) tier is unchanged. Default false. */
  bool private_spill;
  /* P4 (track 6): SHARED parallel hash-join partition accumulator. Routes disk pages through the
   * SAME file_io copy-buffer path as private_spill (ZERO page-buffer fix), but the list is written
   * concurrently by px workers -- file_io is safe ONLY because every disk read/write happens under
   * the owner's HASHJOIN_SHARED_SPLIT_INFO::part_mutexes[part_id] (writes, during the split phase)
   * or after the split barrier (reads, each consumer allocates its own copy buffer). Kept DISTINCT
   * from private_spill so the sector-scan guard rejects private but permits shared. Default false. */
  bool shared_spill;
  int spill_npages;		/* number of file_io-direct disk pages allocated (numerable nth cursor) */
};

/*
 * Arguments to pass to the routine used to wait for the next available page
 * for streaming queries.
 */
typedef struct qmgr_wait_args QMGR_WAIT_ARGS;
struct qmgr_wait_args
{
  QUERY_ID query_id;
  VPID vpid;
  VPID next_vpid;
  QMGR_TEMP_FILE *tfile_vfidp;
};

typedef enum
{
  OTHERS,
  M_QUERY,
  UNION_QUERY,
  VALUE_QUERY,
  GROUPBY_QUERY,
  ORDERBY_QUERY,
  DISTINCT_QUERY,
  ANALYTIC_QUERY
} QMGR_QUERY_TYPE;

typedef enum
{
  QUERY_IN_PROGRESS,
  QUERY_COMPLETED,		/* execution completed */
  QUERY_CLOSED,			/* cursor closed or aborted */
} QMGR_QUERY_STATUS;

typedef struct qmgr_query_entry QMGR_QUERY_ENTRY;
struct qmgr_query_entry
{
  QUERY_ID query_id;		/* unique query identifier */
  XASL_ID xasl_id;		/* XASL tree storage identifier */
  xasl_cache_ent *xasl_ent;	/* XASL cache entry for this query */
  QFILE_LIST_ID *list_id;	/* result list file identifier */
  QFILE_LIST_CACHE_ENTRY *list_ent;	/* list cache entry for this query */
  QMGR_QUERY_ENTRY *next;
  QMGR_TEMP_FILE *temp_vfid;	/* head of per query temp file VFID */
  int num_tmp;			/* number of tmpfiles allocated */
  int total_count;		/* total number of file pages alloc'd for the entire query */
  char *er_msg;			/* pointer to error message string of last error */
  int errid;			/* errid for last error of query */
  QMGR_QUERY_STATUS query_status;
  QUERY_FLAG query_flag;
  bool is_holdable;		/* true if this query should be available */
  bool includes_tde_class;	/* true if this query include some tde class. It is from xasl node */
  unsigned int alloc_no;	/* incremented on every retrieval from the free list (qmgr_allocate_query_entry()) */
};

extern QMGR_QUERY_ENTRY *qmgr_get_query_entry (THREAD_ENTRY * thread_p, QUERY_ID query_id, int trans_ind);
extern int qmgr_allocate_tran_entries (THREAD_ENTRY * thread_p, int trans_cnt);
extern void qmgr_dump (void);
extern int qmgr_initialize (THREAD_ENTRY * thread_p);
extern void qmgr_finalize (THREAD_ENTRY * thread_p);
extern void qmgr_clear_trans_wakeup (THREAD_ENTRY * thread_p, int tran_index, bool tran_died, bool is_abort);
#if defined(ENABLE_UNUSED_FUNCTION)
extern QMGR_TRAN_STATUS qmgr_get_tran_status (THREAD_ENTRY * thread_p, int tran_index);
extern void qmgr_set_tran_status (THREAD_ENTRY * thread_p, int tran_index, QMGR_TRAN_STATUS trans_status);
extern int qmgr_get_query_error_with_entry (QMGR_QUERY_ENTRY * query_entryp);
#endif /* ENABLE_UNUSED_FUNCTION */
extern void qmgr_add_modified_class (THREAD_ENTRY * thread_p, const OID * class_oid);
extern PAGE_PTR qmgr_get_old_page (THREAD_ENTRY * thread_p, VPID * vpidp, QMGR_TEMP_FILE * tfile_vfidp);
extern void qmgr_free_old_page (THREAD_ENTRY * thread_p, PAGE_PTR page_ptr, QMGR_TEMP_FILE * tfile_vfidp);
extern PAGE_PTR qmgr_get_old_page_read_only (THREAD_ENTRY * thread_p, VPID * vpidp, QMGR_TEMP_FILE * tfile_vfidp);
extern PAGE_PTR qmgr_get_old_page_simple_fix (THREAD_ENTRY * thread_p, VPID * vpidp, QMGR_TEMP_FILE * tfile_vfidp);
extern void qmgr_free_old_page_simple_fix (THREAD_ENTRY * thread_p, PAGE_PTR page_ptr, QMGR_TEMP_FILE * tfile_vfidp);
extern void qmgr_set_dirty_page (THREAD_ENTRY * thread_p, PAGE_PTR page_ptr, int free_page, LOG_DATA_ADDR * addrp,
				 QMGR_TEMP_FILE * tfile_vfidp);
extern PAGE_PTR qmgr_get_new_page (THREAD_ENTRY * thread_p, VPID * vpidp, QMGR_TEMP_FILE * tfile_vfidp);

/* ---- Phase 1 temp-page accessor split (Axis A: read-only vs write-back) ----
 * The legacy qmgr_get_old_page / _read_only / _simple_fix / qmgr_free_old_page /
 * _simple_fix / qmgr_get_new_page / qmgr_set_dirty_page symbols above are kept as
 * thin shims (retire after Phase 6); all call sites route through the families below.
 * Behavior is preserved: Phase 1 still uses pgbuf and leaves the membuf branch intact. */

/* QMGR_TEMP_FIX_MODE preserves each read-only call site's ORIGINAL fix mode so a
 * WRITE-latch reader is never silently downgraded to a READ-latch (the PERF_PAGE_TEMP
 * zero-delta gate cannot detect a latch downgrade; only per-site classification can). */
typedef enum
{
  QMGR_TEMP_FIX_WRITE_LATCH = 0,	/* legacy qmgr_get_old_page: pgbuf_fix PGBUF_LATCH_WRITE */
  QMGR_TEMP_FIX_READ_LATCH,	/* legacy qmgr_get_old_page_read_only: pgbuf_fix PGBUF_LATCH_READ */
  QMGR_TEMP_FIX_SIMPLE		/* legacy qmgr_get_old_page_simple_fix: pgbuf_simple_fix (latchless) */
} QMGR_TEMP_FIX_MODE;

/* Type-distinct write-back handle: the compile-time guard that only write-back callers
 * (page mutation + writeback) can mark a temp page dirty. Read-only callers receive a
 * bare PAGE_PTR and therefore cannot reach qmgr_temp_page_writeback. */
typedef struct qmgr_temp_wpage QMGR_TEMP_WPAGE;
struct qmgr_temp_wpage
{
  PAGE_PTR page_p;		/* writable temp page; NULL when unavailable */
};

extern PAGE_PTR qmgr_temp_page_get_readonly (THREAD_ENTRY * thread_p, VPID * vpidp, QMGR_TEMP_FILE * tfile_vfidp,
					     QMGR_TEMP_FIX_MODE fix_mode);
extern void qmgr_temp_page_free_readonly (THREAD_ENTRY * thread_p, PAGE_PTR page_ptr, QMGR_TEMP_FILE * tfile_vfidp);
extern void qmgr_temp_page_free_readonly_simple (THREAD_ENTRY * thread_p, PAGE_PTR page_ptr,
						 QMGR_TEMP_FILE * tfile_vfidp);
extern QMGR_TEMP_WPAGE qmgr_temp_page_get_writable (THREAD_ENTRY * thread_p, VPID * vpidp, QMGR_TEMP_FILE * tfile_vfidp);
extern QMGR_TEMP_WPAGE qmgr_temp_page_get_new_writable (THREAD_ENTRY * thread_p, VPID * vpidp,
							QMGR_TEMP_FILE * tfile_vfidp);
extern void qmgr_temp_page_writeback (THREAD_ENTRY * thread_p, QMGR_TEMP_WPAGE * wpage, int free_page,
				      LOG_DATA_ADDR * addrp, QMGR_TEMP_FILE * tfile_vfidp);
extern void qmgr_temp_page_free_writable (THREAD_ENTRY * thread_p, QMGR_TEMP_WPAGE * wpage,
					  QMGR_TEMP_FILE * tfile_vfidp);
extern QMGR_TEMP_FILE *qmgr_create_new_temp_file (THREAD_ENTRY * thread_p, QUERY_ID query_id,
						  QMGR_TEMP_FILE_MEMBUF_TYPE membuf_type);
extern QMGR_TEMP_FILE *qmgr_create_result_file (THREAD_ENTRY * thread_p, QUERY_ID query_id);
extern int qmgr_free_list_temp_file (THREAD_ENTRY * thread_p, QUERY_ID query_id, QMGR_TEMP_FILE * tfile_vfidp);
extern int qmgr_free_temp_file_list (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_vfidp, QUERY_ID query_id,
				     bool is_error);

#if defined (SERVER_MODE)
extern bool qmgr_is_query_interrupted (THREAD_ENTRY * thread_p, QUERY_ID query_id);
#endif /* SERVER_MODE */

extern void qmgr_set_query_error (THREAD_ENTRY * thread_p, QUERY_ID query_id);
extern void qmgr_setup_empty_list_file (char *page_buf);
extern int qmgr_get_temp_file_membuf_pages (QMGR_TEMP_FILE * temp_file_p);
/* P2-B: true iff the query is a holdable cursor (track 7); side-effect-free lookup used by the
 * private-spill gate to exclude results that outlive the producing statement. */
extern bool qmgr_is_query_holdable (THREAD_ENTRY * thread_p, QUERY_ID query_id);
extern int qmgr_get_sql_id (THREAD_ENTRY * thread_p, char **sql_id_buf, char *query, size_t sql_len);
extern struct drand48_data *qmgr_get_rand_buf (THREAD_ENTRY * thread_p);
extern QUERY_ID qmgr_get_current_query_id (THREAD_ENTRY * thread_p);
extern char *qmgr_get_query_sql_user_text (THREAD_ENTRY * thread_p, QUERY_ID query_id, int tran_index);
extern QMGR_TRAN_STATUS qmgr_check_dblink_trans (THREAD_ENTRY * thread_p, bool is_abort);
extern int qmgr_dblink_find_conn_handle (THREAD_ENTRY * thread_p, char *conn_url, char *user_name, char *password,
					 bool set_participant);
extern int qmgr_dblink_add_conn_handle (THREAD_ENTRY * thread_p, int conn_handle, char *conn_url, char *user_name,
					char *password, bool set_participant);
extern DBLINK_CONN_ENTRY *qmgr_dblink_get_conn_entry (THREAD_ENTRY * thread_p);
extern void qmgr_dblink_clear_conn_entry (THREAD_ENTRY * thread_p);

/* ---- Phase 3: temp_query_mem_cap admission-time enforcement (Principle P2: admission-only) ----
 * Per-query temp in-memory budget computed by the admission gate below. */
typedef struct qmgr_temp_mem_budget QMGR_TEMP_MEM_BUDGET;
struct qmgr_temp_mem_budget
{
  UINT64 per_worker_bytes;	/* membuf + sort + hash in-memory budget for one parallel participant */
  UINT64 total_bytes;		/* per_worker_bytes * admitted worker count */
};

/* qmgr_admission_apply_mem_cap () - clamp a query's parallel degree to temp_query_mem_cap at
 * ADMISSION time (before worker acquisition and barrier init). Returns true and increments
 * PSTAT_QM_TEMP_MEM_CAP_DEGRADES iff the parallel degree was lowered. No-op (returns false and
 * leaves *parallel_workers_inout unchanged) when temp_query_mem_cap == 0 (default) or the
 * requested degree is already serial. budget_out is optional (may be NULL). */
extern bool qmgr_admission_apply_mem_cap (THREAD_ENTRY * thread_p, QMGR_TEMP_MEM_BUDGET * budget_out,
					  int *parallel_workers_inout);
#endif /* _QUERY_MANAGER_H_ */
