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
 * List files shared
 */

#ifndef _QUERY_LIST_H_
#define _QUERY_LIST_H_

#ident "$Id$"

#ifdef __cplusplus
#include <atomic>
#include "thread_compat.hpp"
#endif

#include "storage_common.h"
#include "object_domain.h"
#include "error_code.h"

#include <assert.h>

typedef enum
{
  NO_JOIN = -1,
  JOIN_INNER = 0,
  JOIN_LEFT,
  JOIN_RIGHT,
  JOIN_OUTER,
  JOIN_CSELECT
} JOIN_TYPE;

#define IS_OUTER_JOIN_TYPE(t) ((t) == JOIN_LEFT || (t) == JOIN_RIGHT || (t) == JOIN_OUTER)

/* PAGE CONSTANTS */

/* aligned size of the field */
#define QFILE_PAGE_HEADER_SIZE          32

/* offset values to access fields */
#define QFILE_TUPLE_COUNT_OFFSET        0
#define QFILE_PREV_PAGE_ID_OFFSET       4
#define QFILE_NEXT_PAGE_ID_OFFSET       8
#define QFILE_LAST_TUPLE_OFFSET         12
#define QFILE_OVERFLOW_PAGE_ID_OFFSET   16
#define QFILE_PREV_VOL_ID_OFFSET        20
#define QFILE_NEXT_VOL_ID_OFFSET        22
#define QFILE_OVERFLOW_VOL_ID_OFFSET    24
#define QFILE_RESERVED_OFFSET		26

/* Invalid offset value to the page */
#define QFILE_NULL_PAGE_OFFSET          -1

/*
 *       		READERS/WRITERS FOR PAGE FIELDS
 */

#define QFILE_GET_TUPLE_COUNT(ptr) \
  OR_GET_INT ((ptr) + QFILE_TUPLE_COUNT_OFFSET)

#define QFILE_GET_PREV_PAGE_ID(ptr) \
  (PAGEID) OR_GET_INT ((ptr) + QFILE_PREV_PAGE_ID_OFFSET)

#define QFILE_GET_NEXT_PAGE_ID(ptr) \
  (PAGEID) OR_GET_INT ((ptr) + QFILE_NEXT_PAGE_ID_OFFSET)

#define QFILE_GET_LAST_TUPLE_OFFSET(ptr) \
  (PAGEID) OR_GET_INT ((ptr) + QFILE_LAST_TUPLE_OFFSET)

#define QFILE_GET_OVERFLOW_PAGE_ID(ptr) \
  (PAGEID) OR_GET_INT ((ptr) + QFILE_OVERFLOW_PAGE_ID_OFFSET)

#define QFILE_GET_PREV_VOLUME_ID(ptr) \
  (VOLID) OR_GET_SHORT ((ptr) + QFILE_PREV_VOL_ID_OFFSET)

#define QFILE_GET_NEXT_VOLUME_ID(ptr) \
  (VOLID) OR_GET_SHORT ((ptr) + QFILE_NEXT_VOL_ID_OFFSET)

#define QFILE_GET_OVERFLOW_VOLUME_ID(ptr) \
  (VOLID) OR_GET_SHORT ((ptr) + QFILE_OVERFLOW_VOL_ID_OFFSET)

/*
 * Don't change the order of reading VPID's member in 'GET_XXX_VPID' series.
 * It is arranged for synchronization of async query execution.
 */

#define QFILE_GET_PREV_VPID(des,ptr) \
  do \
    { \
      (des)->pageid = (PAGEID) OR_GET_INT ((ptr) + QFILE_PREV_PAGE_ID_OFFSET); \
      (des)->volid = (VOLID) OR_GET_SHORT ((ptr) + QFILE_PREV_VOL_ID_OFFSET); \
    } \
  while (0)

#define QFILE_GET_NEXT_VPID(des,ptr) \
  do \
    { \
      (des)->pageid = (PAGEID) OR_GET_INT ((ptr) + QFILE_NEXT_PAGE_ID_OFFSET); \
      (des)->volid = (VOLID) OR_GET_SHORT ((ptr) + QFILE_NEXT_VOL_ID_OFFSET); \
    } \
  while (0)

#define QFILE_GET_OVERFLOW_VPID(des,ptr) \
  do \
    { \
      (des)->pageid = (PAGEID) OR_GET_INT ((ptr) + QFILE_OVERFLOW_PAGE_ID_OFFSET); \
      (des)->volid = (VOLID) OR_GET_SHORT ((ptr) + QFILE_OVERFLOW_VOL_ID_OFFSET); \
    } \
  while (0)

#define QFILE_PUT_TUPLE_COUNT(ptr,val) \
   OR_PUT_INT ((ptr) + QFILE_TUPLE_COUNT_OFFSET, (val))

#define QFILE_PUT_PREV_PAGE_ID(ptr,val) \
   OR_PUT_INT ((ptr) + QFILE_PREV_PAGE_ID_OFFSET, (val))

#define QFILE_PUT_NEXT_PAGE_ID(ptr,val) \
   OR_PUT_INT ((ptr) + QFILE_NEXT_PAGE_ID_OFFSET, (val))

#define QFILE_PUT_LAST_TUPLE_OFFSET(ptr,val) \
   OR_PUT_INT ((ptr) + QFILE_LAST_TUPLE_OFFSET, (val))

#define QFILE_PUT_OVERFLOW_PAGE_ID(ptr,val) \
   OR_PUT_INT ((ptr) + QFILE_OVERFLOW_PAGE_ID_OFFSET, (val))

#define QFILE_PUT_PREV_VOLUME_ID(ptr,val) \
   OR_PUT_SHORT ((ptr) + QFILE_PREV_VOL_ID_OFFSET, (val))

#define QFILE_PUT_NEXT_VOLUME_ID(ptr,val) \
   OR_PUT_SHORT ((ptr) + QFILE_NEXT_VOL_ID_OFFSET, (val))

#define QFILE_PUT_OVERFLOW_VOLUME_ID(ptr,val) \
   OR_PUT_SHORT ((ptr) + QFILE_OVERFLOW_VOL_ID_OFFSET, (val))

/*
 * Don't change the order of writing VPID's member in 'PUT_XXX_VPID' series.
 * It is arranged for synchronization of async query execution.
 */

#define QFILE_PUT_PREV_VPID(ptr,vpid) \
  do \
    { \
      OR_PUT_SHORT ((ptr) + QFILE_PREV_VOL_ID_OFFSET, (vpid)->volid); \
      OR_PUT_INT ((ptr) + QFILE_PREV_PAGE_ID_OFFSET, (vpid)->pageid); \
    } \
  while (0)

#define QFILE_PUT_NEXT_VPID(ptr,vpid) \
  do \
    { \
      OR_PUT_SHORT ((ptr) + QFILE_NEXT_VOL_ID_OFFSET, (vpid)->volid); \
      OR_PUT_INT ((ptr) + QFILE_NEXT_PAGE_ID_OFFSET, (vpid)->pageid); \
    } \
  while (0)

#define QFILE_PUT_OVERFLOW_VPID(ptr,vpid) \
  do \
    { \
      OR_PUT_SHORT ((ptr) + QFILE_OVERFLOW_VOL_ID_OFFSET, (vpid)->volid); \
      OR_PUT_INT ((ptr) + QFILE_OVERFLOW_PAGE_ID_OFFSET, (vpid)->pageid); \
    } \
  while (0)

#define QFILE_PUT_PREV_VPID_NULL(ptr) \
  do \
    { \
      OR_PUT_SHORT ((ptr) + QFILE_PREV_VOL_ID_OFFSET, NULL_VOLID); \
      OR_PUT_INT ((ptr) + QFILE_PREV_PAGE_ID_OFFSET, NULL_PAGEID); \
    } \
  while (0)

#define QFILE_PUT_NEXT_VPID_NULL(ptr) \
  do \
    { \
      OR_PUT_SHORT ((ptr) + QFILE_NEXT_VOL_ID_OFFSET, NULL_VOLID); \
      OR_PUT_INT ((ptr) + QFILE_NEXT_PAGE_ID_OFFSET, NULL_PAGEID); \
    } \
  while (0)

#define QFILE_PUT_OVERFLOW_VPID_NULL(ptr) \
  do \
    { \
      OR_PUT_SHORT ((ptr) + QFILE_OVERFLOW_VOL_ID_OFFSET, NULL_VOLID); \
      OR_PUT_INT ((ptr) + QFILE_OVERFLOW_PAGE_ID_OFFSET, NULL_PAGEID); \
    } \
  while (0)

#define QFILE_COPY_VPID(ptr1, ptr2) \
  do \
    { \
      (ptr1)->pageid = (ptr2)->pageid; \
      (ptr1)->volid  = (ptr2)->volid; \
    } \
  while (0)

/* OVERFLOW PAGE CONSTANTS */

#define QFILE_OVERFLOW_TUPLE_PAGE_SIZE_OFFSET   12

#define QFILE_GET_OVERFLOW_TUPLE_PAGE_SIZE(ptr) \
  (int) OR_GET_INT ((ptr) + QFILE_OVERFLOW_TUPLE_PAGE_SIZE_OFFSET)

#define QFILE_PUT_OVERFLOW_TUPLE_PAGE_SIZE(ptr,val) \
  OR_PUT_INT ((ptr) + QFILE_OVERFLOW_TUPLE_PAGE_SIZE_OFFSET, (val))

/* QFILE_TUPLE CONSTANTS */

#define QFILE_MAX_TUPLE_SIZE_IN_PAGE  (DB_PAGESIZE - QFILE_PAGE_HEADER_SIZE)

/* Each tuple start is aligned with MAX_ALIGNMENT
 * Each tuple value header is aligned with MAX_ALIGNMENT,
 * Each tuple value is aligned with MAX_ALIGNMENT
 */

#define QFILE_TUPLE_LENGTH_SIZE                 8
#define QFILE_TUPLE_LENGTH_OFFSET               0
#define QFILE_TUPLE_PREV_LENGTH_OFFSET          4

#define QFILE_TUPLE_VALUE_HEADER_LENGTH         8
#define QFILE_TUPLE_VALUE_HEADER_SIZE           8
#define QFILE_TUPLE_VALUE_FLAG_SIZE             4
#define QFILE_TUPLE_VALUE_LENGTH_SIZE           4

#define QFILE_TUPLE_VALUE_FLAG_OFFSET           0
#define QFILE_TUPLE_VALUE_LENGTH_OFFSET         4

/* READERS/WRITERS FOR QFILE_TUPLE FIELDS */

#define QFILE_GET_TUPLE_LENGTH(tpl) \
  OR_GET_INT ((tpl) + QFILE_TUPLE_LENGTH_OFFSET)

#define QFILE_GET_PREV_TUPLE_LENGTH(tpl) \
  OR_GET_INT ((tpl) + QFILE_TUPLE_PREV_LENGTH_OFFSET)

#define QFILE_PUT_TUPLE_LENGTH(tpl,val) \
  OR_PUT_INT ((tpl) + QFILE_TUPLE_LENGTH_OFFSET,val)

#define QFILE_PUT_PREV_TUPLE_LENGTH(tpl,val) \
  OR_PUT_INT ((tpl) + QFILE_TUPLE_PREV_LENGTH_OFFSET,val)

#define QFILE_GET_TUPLE_VALUE_FLAG(ptr) \
  (QFILE_TUPLE_VALUE_FLAG) OR_GET_INT ((ptr) + QFILE_TUPLE_VALUE_FLAG_OFFSET)

#define QFILE_GET_TUPLE_VALUE_LENGTH(ptr) \
  (int) OR_GET_INT ((ptr) + QFILE_TUPLE_VALUE_LENGTH_OFFSET)

#define QFILE_PUT_TUPLE_VALUE_FLAG(ptr,val) \
  OR_PUT_INT ((ptr) + QFILE_TUPLE_VALUE_FLAG_OFFSET, (val))

#define QFILE_PUT_TUPLE_VALUE_LENGTH(ptr,val) \
  OR_PUT_INT ((ptr) + QFILE_TUPLE_VALUE_LENGTH_OFFSET, (val))

#define QFILE_GET_TUPLE_VALUE_HEADER_POSITION(tpl,ind,valp) \
  do \
    { \
      int _k; \
      (valp) = (char*) (tpl) + QFILE_TUPLE_LENGTH_SIZE; \
      for (_k = 0; _k < (ind); _k++) \
        { \
          (valp) += QFILE_TUPLE_VALUE_HEADER_SIZE + QFILE_GET_TUPLE_VALUE_LENGTH ((valp)); \
        } \
    } \
  while (0)

/* Special flag set in the TUPLE_CNT field to indicate an overflow page */
#define QFILE_OVERFLOW_TUPLE_COUNT_FLAG -2

/*
 *       		    QFILE_TUPLE FORMAT DEFINITIONS
 */

typedef char *QFILE_TUPLE;	/* list file tuple */

/* tuple record descriptor */
typedef struct qfile_tuple_record QFILE_TUPLE_RECORD;
struct qfile_tuple_record
{
  char *tpl;			/* tuple pointer */
  int size;			/* area _allocated_ for tuple pointer */
};

typedef enum
{
  V_BOUND = 1,
  V_UNBOUND
} QFILE_TUPLE_VALUE_FLAG;

/* tuple value header */
typedef struct qfile_tuple_value_header QFILE_TUPLE_VALUE_HEADER;
struct qfile_tuple_value_header
{
  QFILE_TUPLE_VALUE_FLAG val_flag;	/* V_BOUND/V_UNBOUND? */
  int val_len;			/* length of tuple value */
};

/* Type list structure */
typedef struct qfile_tuple_value_type_list QFILE_TUPLE_VALUE_TYPE_LIST;
struct qfile_tuple_value_type_list
{
  TP_DOMAIN **domp;		/* array of column domains */
  int type_cnt;			/* number of data types */
};

/* tuple value position descriptor */
typedef struct qfile_tuple_value_position QFILE_TUPLE_VALUE_POSITION;
struct qfile_tuple_value_position
{
  TP_DOMAIN *dom;		/* value domain */
  TP_DOMAIN *original_domain;	/* original domain */
  int pos_no;			/* value position number */
};

/*
 *                          SCAN FETCH MODE
 */

typedef enum
{
  QPROC_NO_SINGLE_INNER = 0,	/* 0 or n qualified rows */
  QPROC_SINGLE_INNER,		/* 0 or 1 qualified row - currently, not used */
  QPROC_SINGLE_OUTER,		/* 1 NULL row or 1 qualified row */
  QPROC_NO_SINGLE_OUTER		/* 1 NULL row or n qualified rows */
} QPROC_SINGLE_FETCH;

/* List File Merge Information */
typedef struct qfile_list_merge_info QFILE_LIST_MERGE_INFO;
struct qfile_list_merge_info
{
  JOIN_TYPE join_type;		/* inner, left, right or outer */
  QPROC_SINGLE_FETCH single_fetch;	/* merge in single fetch mode */
  int ls_column_cnt;		/* join columns count */
  int ls_pos_cnt;		/* tuple value fetch count */
  int *ls_outer_column;		/* outer list join columns number */
  int *ls_outer_unique;		/* outer column values unique?
				 * currently, not used */
  int *ls_inner_column;		/* inner list join columns number */
  int *ls_inner_unique;		/* inner column values unique?
				 * currently, not used */
  int *ls_outer_inner_list;	/* outer/inner list indicators */
  int *ls_pos_list;		/* tuple value positions */
};

typedef enum
{
  T_UNKNOWN,			/* uninitialized: not used */
  T_SINGLE_BOUND_ITEM,		/* called by qfile_add_item_to_list() */
  T_NORMAL,			/* normal case */
  T_SORTKEY,			/* called by ls_sort_put_next() */
  T_MERGE			/* called by xs_add_mergetuple() */
} QFILE_TUPLE_TYPE;

/* tuple descriptor */
typedef struct qfile_tuple_descriptor QFILE_TUPLE_DESCRIPTOR;
struct qfile_tuple_descriptor
{
  /* T_SINGLE_BOUND_ITEM */
  char *item;			/* pointer of item (i.e, single bound field tuple) */
  int item_size;		/* item size */

  /* T_NORMAL */
  int tpl_size;			/* tuple size */
  int f_cnt;			/* number of field */
  DB_VALUE **f_valp;		/* pointer of field value pointer array */

  /* T_SORTKEY */
  void *sortkey_info;		/* casted pointer of (SORTKEY_INFO *) */
  void *sort_rec;		/* casted pointer of (SORT_REC *) */

  /* T_MERGE */
  QFILE_TUPLE_RECORD *tplrec1;	/* first tuple */
  QFILE_TUPLE_RECORD *tplrec2;	/* second tuple */
  QFILE_LIST_MERGE_INFO *merge_info;	/* tuple merge info */
};

/*
 *       	      SORTING RELATED DEFINITIONS
 */

typedef enum
{
  SORT_TEMP = 0,
  SORT_GROUPBY,
  SORT_ORDERBY,
  SORT_DISTINCT,
  SORT_LIMIT
} SORT_TYPE;

typedef enum
{
  S_ASC = 1,
  S_DESC
} SORT_ORDER;

typedef enum
{
  S_NULLS_FIRST = 1,
  S_NULLS_LAST
} SORT_NULLS;

typedef struct sort_list SORT_LIST;
struct sort_list
{
  UINT64 del_id;		/* for latch-free freelist */
  struct sort_list *local_next;	/* for latch-free freelist */
  struct sort_list *next;	/* Next sort item */
  QFILE_TUPLE_VALUE_POSITION pos_descr;	/* Value position descriptor */
  SORT_ORDER s_order;		/* Ascending/Descending Order */
  SORT_NULLS s_nulls;		/* NULLS as First/Last position */
};				/* Sort item list */

/*
 *       		     LIST FILE DEFINITIONS
 */

typedef struct qfile_list_id QFILE_LIST_ID;

/* Migration backing-kind tag (redesign G008, issue #73; Phase 2 MIGRATE).
 * During expand->migrate the OLD backing (qfile_connect_list / raw-fd / pgbuf
 * sector) and the NEW backing (Tapeset / per-worker private file / offset
 * arithmetic) coexist, but a SINGLE list is produced wholly by one path --
 * never a mix (operator-atomic switch).  This permanently avoids the
 * mixed-backing scan that produced garbled results and the +209%/crash dead
 * end (evidence FAIL-03 / FAIL-06).  The tag is the migration-limited dispatch
 * discriminator; it is deleted at contract (Phase 3) with the OLD variants.
 * NONE = not yet committed to a backing (a freshly cleared list). */
enum qfile_backing_kind
{
  QFILE_BACKING_NONE = 0,
  QFILE_BACKING_OLD = 1,	/* connect_list / raw-fd / pgbuf-sector */
  QFILE_BACKING_NEW = 2		/* Tapeset / per-worker private file / offset */
};
typedef enum qfile_backing_kind QFILE_BACKING_KIND;
struct qfile_list_id
{
  QFILE_TUPLE_VALUE_TYPE_LIST type_list;	/* data type of each column */
  SORT_LIST *sort_list;		/* sort info of each column */
  INT64 tuple_cnt;		/* total number of tuples in the file */
  int page_cnt;			/* total number of pages in the list file */
  VPID first_vpid_;		/* first real page identifier (access via QFILE_LIST_ID_FIRST_VPID) */
  VPID last_vpid_;		/* last real page identifier (access via QFILE_LIST_ID_LAST_VPID) */
  PAGE_PTR last_pgptr;		/* last page pointer */
  int last_offset;		/* mark current end of last page */
  int lasttpl_len;		/* length of the last tuple file identifier NOTE: A tuple can be larger than one page
				 * therefore, this field must be int instead of a short value */
  QUERY_ID query_id;		/* Associated Query Id */
  VFID temp_vfid;		/* temp file id; duplicated from tfile_vfid */
  struct qmgr_temp_file *tfile_vfid_;	/* Create a tmp file per list (access via QFILE_LIST_ID_TFILE_VFID) */
  QFILE_TUPLE_DESCRIPTOR tpl_descr;	/* tuple descriptor */
  bool is_domain_resolved;	/* domains for host var is resolved or not */
  bool is_result_cached;	/* for subquery result cache */
  QFILE_LIST_ID *dependent_list_id_;	/* Linked as dependent by qfile_connect_list; cleared together. (access via QFILE_LIST_ID_DEPENDENT) */
  /* Phase1 1A scan contract (redesign G005, issue #70).  Ordered Tape vector
   * (Tapeset) that replaces cross-file next_vpid linkage as the connection
   * structure (SSOT #75 §3.2 B1, ADR 0002).  NULL = legacy single-backing list
   * (no behavior change).  Opaque (qfile::tapeset *); access via the accessor
   * macro below.  Not serialized (transient runtime structure). */
  void *tapeset_;		/* (access via QFILE_LIST_ID_TAPESET) */
  bool owns_tapeset_;		/* this list_id owns/free the tapeset (access via QFILE_LIST_ID_OWNS_TAPESET) */
  bool new_contains_overflow_;	/* NEW Tapeset has ADR0006 overflow tuple pages; page-parallel list scan must avoid it. */
  /* Migration backing-kind tag (redesign G008, issue #73).  QFILE_BACKING_NONE
   * on a cleared list; set OLD/NEW when a producer commits a backing.  Access
   * via QFILE_LIST_ID_BACKING_KIND.  Not serialized (transient runtime tag). */
  QFILE_BACKING_KIND backing_kind_;
  /* Phase2 2A-1 producer hook (redesign #78).  While a NEW-backed list is being
   * PRODUCED, producer_writer_ holds the qfile::tape_writer that completed pages
   * are appended to, and producer_page_ is the single reusable in-memory page
   * being filled (== last_pgptr).  Both NULL for OLD production and after freeze.
   * Opaque; access via the accessor macros.  Not serialized (transient). */
  void *producer_writer_;	/* qfile::tape_writer * (QFILE_LIST_ID_PRODUCER_WRITER) */
  void *producer_page_;		/* scratch page being filled (QFILE_LIST_ID_PRODUCER_PAGE) */
};

#define QFILE_CLEAR_LIST_ID(list_id) \
  do \
    { \
      (list_id)->type_list.type_cnt = 0; \
      (list_id)->type_list.domp = NULL; \
      (list_id)->sort_list = NULL; \
      (list_id)->tuple_cnt = 0; \
      (list_id)->page_cnt = 0; \
      (list_id)->first_vpid_.pageid = NULL_PAGEID; \
      (list_id)->first_vpid_.volid  = NULL_VOLID; \
      (list_id)->last_vpid_.pageid = NULL_PAGEID; \
      (list_id)->last_vpid_.volid  = NULL_VOLID; \
      (list_id)->last_pgptr = NULL; \
      (list_id)->last_offset = QFILE_NULL_PAGE_OFFSET; \
      (list_id)->lasttpl_len = 0; \
      (list_id)->query_id = 0; \
      (list_id)->temp_vfid.fileid = NULL_PAGEID; \
      (list_id)->temp_vfid.volid = NULL_VOLID; \
      (list_id)->tfile_vfid_ = NULL; \
      (list_id)->tpl_descr.item = NULL; \
      (list_id)->tpl_descr.item_size = 0; \
      (list_id)->tpl_descr.tpl_size = 0; \
      (list_id)->tpl_descr.f_cnt = 0; \
      (list_id)->tpl_descr.f_valp = NULL; \
      (list_id)->tpl_descr.sortkey_info = NULL; \
      (list_id)->tpl_descr.sort_rec = NULL; \
      (list_id)->tpl_descr.tplrec1 = NULL; \
      (list_id)->tpl_descr.tplrec2 = NULL; \
      (list_id)->tpl_descr.merge_info = NULL; \
      (list_id)->is_domain_resolved = false; \
      (list_id)->is_result_cached = false; \
      (list_id)->dependent_list_id_ = NULL; \
      (list_id)->tapeset_ = NULL; \
      (list_id)->owns_tapeset_ = false; \
      (list_id)->backing_kind_ = QFILE_BACKING_NONE; \
      (list_id)->new_contains_overflow_ = false; \
      (list_id)->producer_writer_ = NULL; \
      (list_id)->producer_page_ = NULL; \
    } \
  while (0)

/*
 * QFILE_LIST_ID accessor shim  (Phase1 1A-0 — redesign G004, issue #69).
 *
 * Route EVERY access to the connection-identity (first_vpid/last_vpid),
 * backing (tfile_vfid) and dependency-chain (dependent_list_id) fields through
 * these accessors so the compiler enumerates every consumer of the F1
 * (qfile_copy_list_id / qfile_clear_list_id ownership) and F3 (Phase3 symbol
 * sweep) surfaces.  No behavior change: each accessor expands to the original
 * lvalue, so reads, writes and address-of (&...) all keep working unchanged.
 *
 * The trailing-underscore raw fields (first_vpid_/last_vpid_/tfile_vfid_/
 * dependent_list_id_) MUST NOT be accessed directly outside of these macros and
 * QFILE_CLEAR_LIST_ID (the canonical initializer).  Adding a new direct field
 * access is a compile error by design.
 */
#define QFILE_LIST_ID_FIRST_VPID(list_id)  ((list_id)->first_vpid_)
#define QFILE_LIST_ID_LAST_VPID(list_id)   ((list_id)->last_vpid_)
#define QFILE_LIST_ID_TFILE_VFID(list_id)  ((list_id)->tfile_vfid_)
#define QFILE_LIST_ID_DEPENDENT(list_id)   ((list_id)->dependent_list_id_)
#define QFILE_LIST_ID_TAPESET(list_id)     ((list_id)->tapeset_)
#define QFILE_LIST_ID_OWNS_TAPESET(list_id) ((list_id)->owns_tapeset_)
#define QFILE_LIST_ID_BACKING_KIND(list_id) ((list_id)->backing_kind_)
#define QFILE_LIST_ID_PRODUCER_WRITER(list_id) ((list_id)->producer_writer_)
#define QFILE_LIST_ID_PRODUCER_PAGE(list_id)   ((list_id)->producer_page_)
#define QFILE_LIST_ID_NEW_CONTAINS_OVERFLOW(list_id) ((list_id)->new_contains_overflow_)

/*
 * No-mixed-backing invariant (redesign G008, issue #73; SSOT #75 §5.5 (7) /
 * §6).  A list "has OLD backing" when it is physically backed the legacy way
 * (a real first-page VPID, or an old temp-file handle); it "has NEW backing"
 * when it carries a Tapeset.  The migration invariant: a single list never
 * holds BOTH at once -- producers switch backing operator-atomically, so the
 * mixed-backing scan (FAIL-03/06) can never recur.  qfile_list_is_mixed_backing
 * returns true on a violation; producers assert it via
 * qfile_check_no_mixed_backing (debug-only).
 */
static inline bool
qfile_list_has_old_backing (const QFILE_LIST_ID * list_id)
{
  return list_id != NULL
	 && (!VPID_ISNULL (&QFILE_LIST_ID_FIRST_VPID (list_id)) || QFILE_LIST_ID_TFILE_VFID (list_id) != NULL);
}

static inline bool
qfile_list_has_new_backing (const QFILE_LIST_ID * list_id)
{
  return list_id != NULL && QFILE_LIST_ID_TAPESET (list_id) != NULL;
}

static inline bool
qfile_list_is_mixed_backing (const QFILE_LIST_ID * list_id)
{
  return qfile_list_has_old_backing (list_id) && qfile_list_has_new_backing (list_id);
}

static inline void
qfile_check_no_mixed_backing (const QFILE_LIST_ID * list_id)
{
  /* Debug-only.  Never fires today: real lists are still all OLD and the NEW
   * backing is additive and disconnected.  Becomes load-bearing once Phase 2
   * producers commit a NEW backing -- a single list staying one kind keeps the
   * violation count at 0 (SSOT #75 §6 (7)). */
  assert (!qfile_list_is_mixed_backing (list_id));
}

/*
 * Backing-kind ENTRY guard (SSOT #75 round-3 (d)/(e); ADR 0002/0003/0005).
 *
 * Generalizes the no-mixed-backing invariant from a per-list debug assert to a
 * production-hard check at every backing-SENSITIVE consume boundary: an OLD
 * mechanism (qfile_connect_list / qfile_append_list / qfile_open_list_sector_scan
 * + sector_page_iterator input) must reject a NEW (Tapeset) list, and a NEW
 * mechanism (chunk_distributor / tapeset_scan) must reject an OLD list.  The
 * entry boundary is the cheapest place (one check per operator open) to stop the
 * worst failure class (silent wrong result, FAIL-03/06) before any garbage read,
 * so it is hardened in release builds (er_set + error), unlike the per-tuple /
 * per-page checks which stay debug-only asserts.  combine_two_list is EXEMPT
 * (scan-based, not a VPID-header bypass -- IR-8).
 *
 * qfile_backing_mechanism_violation is the pure (er_set-free) detector so a
 * bootless unit test can exercise it; qfile_backing_guard (list_file.c) is the
 * production-hard wrapper that er_sets, bumps the A~E counter and returns an
 * error code.
 */
static inline bool
qfile_backing_mechanism_violation (const QFILE_LIST_ID * list_id, QFILE_BACKING_KIND mechanism)
{
  if (list_id == NULL)
    {
      return false;
    }
  if (mechanism == QFILE_BACKING_OLD)
    {
      return qfile_list_has_new_backing (list_id);
    }
  if (mechanism == QFILE_BACKING_NEW)
    {
      return qfile_list_has_old_backing (list_id);
    }
  return false;
}

#if defined(__cplusplus)
extern "C"
{
#endif
/* Production-hard entry guard: returns NO_ERROR or an error code (er_set on
 * violation; an OLD mechanism touching a NEW list also bumps the A~E counter). */
extern int qfile_backing_guard (const QFILE_LIST_ID * list_id, QFILE_BACKING_KIND mechanism, const char *file,
				int line);

/* A~E runtime counter: "a NEW-backed list was touched by an OLD scan-bypass
 * path" (evidence §H-3 inventory A~E).  MUST read 0 on a migrated NEW operator
 * (SSOT #75 §6).  Process-wide; reset is for tests. */
extern void qfile_ae_record_old_touch (void);
extern long qfile_ae_old_touch_count (void);
extern void qfile_ae_reset_old_touch_count (void);

/* Sibling counter: a list was actually converted to NEW(Tapeset) backing
 * (qfile_list_make_new_backed()).  Process-wide; reset is for tests. Both
 * counters are also exposed via `cubrid statdump` (PSTAT_QF_NEW_BACKED_CREATE /
 * PSTAT_QF_OLD_TOUCH_ON_NEW, redesign #78/#92). */
extern void qfile_new_backed_record_create (void);
extern long qfile_new_backed_create_count (void);
extern void qfile_new_backed_reset_create_count (void);

/* Release-hard backstop for the store-to-DB TAPE-misuse invariant (#105).  Kept
 * out of the inline (same philosophy as qfile_backing_guard above): raises
 * ER_QPROC_UNKNOWN_CRSPOS so a TAPE coord that reaches
 * qfile_tuple_position_store_to_db -- which QFILE_TUPLE_POSITION_DB cannot
 * represent -- fails loudly in release instead of punning into a bogus VPID. */
extern void qfile_tuple_position_report_tape_misuse (void);
#if defined(__cplusplus)
}
#endif

/* Call-site macros carrying ARG_FILE_LINE (error_manager.h must be in scope). */
#define QFILE_GUARD_OLD_MECHANISM(list_id) qfile_backing_guard ((list_id), QFILE_BACKING_OLD, ARG_FILE_LINE)
#define QFILE_GUARD_NEW_MECHANISM(list_id) qfile_backing_guard ((list_id), QFILE_BACKING_NEW, ARG_FILE_LINE)

/* Tuple position coordinate type */
enum qfile_tuple_position_coordinate_type
{
  QFILE_TUPLE_POSITION_COORD_VPID = 0,
  QFILE_TUPLE_POSITION_COORD_RAW_FD = 1,
  /* Tape-relative coordinate (redesign G005, issue #70 / ADR 0002): a tuple is
   * addressed by (tape_idx, tape_page_offset, tape_byte_offset) + tplno, by
   * pure offset arithmetic — no VPID, no raw-fd segment.  Intra-query only
   * (never stored to QFILE_TUPLE_POSITION_DB; CONNECT BY does not serialize it). */
  QFILE_TUPLE_POSITION_COORD_TAPE = 2
};
typedef enum qfile_tuple_position_coordinate_type QFILE_TUPLE_POSITION_COORDINATE_TYPE;

/* Tuple position structure */
typedef struct qfile_tuple_position QFILE_TUPLE_POSITION;
struct qfile_tuple_position
{
  SCAN_STATUS status;		/* Scan status */
  SCAN_POSITION position;	/* Scan position */
  QFILE_TUPLE_POSITION_COORDINATE_TYPE coord_type;	/* Coordinate discriminator */
  union
  {
    struct
    {
      VPID vpid;		/* Real tuple page identifier */
      int offset;		/* Tuple offset inside the page */
      int vpid_reserved;	/* Keep coordinate width fixed */
    };
    struct
    {
      UINT64 raw_fd_segment_id;	/* Raw-fd segment identifier */
      INT32 page_index;		/* Page index inside the raw-fd segment */
      INT32 tuple_offset;	/* Tuple offset inside the raw-fd page */
    };
    struct
    {
      INT32 tape_idx;		/* Tape index within the Tapeset (COORD_TAPE) */
      INT32 tape_page_offset;	/* Logical page index within the Tape */
      INT32 tape_byte_offset;	/* Tuple byte offset inside the page */
    };
  };
  QFILE_TUPLE tpl;		/* Tuple pointer inside the page */
  int tplno;			/* Tuple number inside the page */
};
typedef struct qfile_tuple_position_db QFILE_TUPLE_POSITION_DB;
struct qfile_tuple_position_db
{
  SCAN_STATUS status;		/* Scan status */
  SCAN_POSITION position;	/* Scan position */
  QFILE_TUPLE_POSITION_COORDINATE_TYPE coord_type;	/* Coordinate discriminator */
  union
  {
    struct
    {
      VPID vpid;		/* Real tuple page identifier */
      int offset;		/* Tuple offset inside the page */
      int vpid_reserved;	/* Keep coordinate width fixed */
    };
    struct
    {
      UINT64 raw_fd_segment_id;	/* Raw-fd segment identifier */
      INT32 page_index;		/* Page index inside the raw-fd segment */
      INT32 tuple_offset;	/* Tuple offset inside the raw-fd page */
    };
  };
  int tplno;			/* Tuple number inside the page */
};


#define QFILE_TUPLE_POSITION_DB_BIT_SIZE (sizeof (QFILE_TUPLE_POSITION_DB) * 8)

static inline bool
qfile_tuple_position_is_raw_fd (const QFILE_TUPLE_POSITION * tuple_position_p)
{
  return tuple_position_p != NULL && tuple_position_p->coord_type == QFILE_TUPLE_POSITION_COORD_RAW_FD;
}

static inline void
qfile_tuple_position_set_vpid (QFILE_TUPLE_POSITION * tuple_position_p, const VPID * vpid_p, int offset)
{
  tuple_position_p->coord_type = QFILE_TUPLE_POSITION_COORD_VPID;
  tuple_position_p->vpid = *vpid_p;
  tuple_position_p->offset = offset;
  tuple_position_p->vpid_reserved = 0;
}

static inline void
qfile_tuple_position_set_raw_fd (QFILE_TUPLE_POSITION * tuple_position_p, UINT64 raw_fd_segment_id, INT32 page_index,
				 INT32 tuple_offset)
{
  tuple_position_p->coord_type = QFILE_TUPLE_POSITION_COORD_RAW_FD;
  tuple_position_p->raw_fd_segment_id = raw_fd_segment_id;
  tuple_position_p->page_index = page_index;
  tuple_position_p->tuple_offset = tuple_offset;
}

static inline bool
qfile_tuple_position_is_tape (const QFILE_TUPLE_POSITION * tuple_position_p)
{
  return tuple_position_p != NULL && tuple_position_p->coord_type == QFILE_TUPLE_POSITION_COORD_TAPE;
}

static inline void
qfile_tuple_position_set_tape (QFILE_TUPLE_POSITION * tuple_position_p, INT32 tape_idx, INT32 tape_page_offset,
			       INT32 tape_byte_offset)
{
  tuple_position_p->coord_type = QFILE_TUPLE_POSITION_COORD_TAPE;
  tuple_position_p->tape_idx = tape_idx;
  tuple_position_p->tape_page_offset = tape_page_offset;
  tuple_position_p->tape_byte_offset = tape_byte_offset;
}

static inline void
qfile_tuple_position_copy_coord (QFILE_TUPLE_POSITION * dst_p, const QFILE_TUPLE_POSITION * src_p)
{
  dst_p->coord_type = src_p->coord_type;
  if (qfile_tuple_position_is_raw_fd (src_p))
    {
      qfile_tuple_position_set_raw_fd (dst_p, src_p->raw_fd_segment_id, src_p->page_index, src_p->tuple_offset);
    }
  else if (qfile_tuple_position_is_tape (src_p))
    {
      qfile_tuple_position_set_tape (dst_p, src_p->tape_idx, src_p->tape_page_offset, src_p->tape_byte_offset);
    }
  else
    {
      qfile_tuple_position_set_vpid (dst_p, &src_p->vpid, src_p->offset);
    }
}

static inline bool
qfile_tuple_position_db_is_raw_fd (const QFILE_TUPLE_POSITION_DB * tuple_position_p)
{
  return tuple_position_p != NULL && tuple_position_p->coord_type == QFILE_TUPLE_POSITION_COORD_RAW_FD;
}

static inline int
qfile_tuple_position_store_to_db (QFILE_TUPLE_POSITION_DB * stored_p, const QFILE_TUPLE_POSITION * src_p)
{
  /* QFILE_TUPLE_POSITION_DB has no TAPE variant (COORD_TAPE is intra-query only, see the
   * enum comment) -- a TAPE-coord src_p would otherwise fall through to the VPID branch
   * below and get its tape_idx/tape_page_offset punned into vpid.pageid/volid (#85, #105).
   * The part-1 CONNECT BY guard (#105) keeps a NEW(Tapeset) list from ever reaching the
   * parent-pos recalc, so this never fires in normal operation.  It is the release-hard
   * backstop: debug aborts on the assert; release raises ER_QPROC_UNKNOWN_CRSPOS and
   * refuses the punning store (returns error) instead of silently persisting a bogus VPID. */
  if (qfile_tuple_position_is_tape (src_p))
    {
      assert (false);
      qfile_tuple_position_report_tape_misuse ();
      return ER_FAILED;
    }
  stored_p->status = src_p->status;
  stored_p->position = src_p->position;
  if (qfile_tuple_position_is_raw_fd (src_p))
    {
      stored_p->coord_type = QFILE_TUPLE_POSITION_COORD_RAW_FD;
      stored_p->raw_fd_segment_id = src_p->raw_fd_segment_id;
      stored_p->page_index = src_p->page_index;
      stored_p->tuple_offset = src_p->tuple_offset;
    }
  else
    {
      stored_p->coord_type = QFILE_TUPLE_POSITION_COORD_VPID;
      stored_p->vpid = src_p->vpid;
      stored_p->offset = src_p->offset;
      stored_p->vpid_reserved = 0;
    }
  stored_p->tplno = src_p->tplno;
  return NO_ERROR;
}


static inline void
qfile_tuple_position_restore_from_stored (QFILE_TUPLE_POSITION * dst_p, const QFILE_TUPLE_POSITION_DB * stored_p)
{
  dst_p->status = stored_p->status;
  dst_p->position = stored_p->position;
  if (qfile_tuple_position_db_is_raw_fd (stored_p))
    {
      qfile_tuple_position_set_raw_fd (dst_p, stored_p->raw_fd_segment_id, stored_p->page_index,
				       stored_p->tuple_offset);
    }
  else
    {
      qfile_tuple_position_set_vpid (dst_p, &stored_p->vpid, stored_p->offset);
    }
  dst_p->tpl = NULL;
  dst_p->tplno = stored_p->tplno;
}

#define QFILE_OUTER_LIST  0	/* outer list file indicator */
#define QFILE_INNER_LIST  1	/* inner list file indicator */

/* List File Scan Identifier */
typedef struct qfile_list_scan_id QFILE_LIST_SCAN_ID;
struct qfile_list_scan_id
{
  SCAN_STATUS status;		/* Scan Status */
  SCAN_POSITION position;	/* Scan Position */
  VPID curr_vpid;		/* current real page identifier */
  PAGE_PTR curr_pgptr;		/* current page pointer */
  QFILE_TUPLE curr_tpl;		/* current tuple pointer */
  bool keep_page_on_finish;	/* flag; when set, does not free page when scan ends */
  bool is_read_only;		/* flag; when set, does not latch write */
  int curr_offset;		/* current page offset */
  int curr_tplno;		/* current tuple number */
  QFILE_TUPLE_RECORD tplrec;	/* used for overflow tuple peeking */
  QFILE_LIST_ID list_id;	/* list file identifier */
  /* Phase1 1A scan contract (redesign G005, issue #70).  When the scanned
   * list_id carries a Tapeset (QFILE_LIST_ID_TAPESET != NULL), this holds the
   * offset-arithmetic multi-Tape scan driver (qfile::tapeset_scan *).  NULL =
   * legacy single-backing scan (no behavior change). */
  void *tapeset_scan_;
};

/* list file flag; denoting type and/or operation of the list file */
enum
{
  QFILE_FLAG_RESULT_FILE = 0x0001,
  QFILE_FLAG_UNION = 0x0010,
  QFILE_FLAG_INTERSECT = 0x0020,
  QFILE_FLAG_DIFFERENCE = 0x0040,
  QFILE_FLAG_ALL = 0x0100,
  QFILE_FLAG_DISTINCT = 0x0200,
  QFILE_FLAG_USE_KEY_BUFFER = 0x0400,
  QFILE_FLAG_PRIVATE_SPILL = 0x0800
};

#define QFILE_SET_FLAG(var, flag)          ((var) |= (flag))
#define QFILE_CLEAR_FLAG(var, flag)        ((var) &= (flag))
#define QFILE_IS_FLAG_SET(var, flag)       ((var) & (flag))
#define QFILE_IS_FLAG_SET_BOTH(var, flag1, flag2) (((var) & (flag1)) && ((var) & (flag2)))

#ifdef __cplusplus
/* Sector-based data page info for QFILE_LIST_ID.
 * membuf_tfile: membuf exists only in the first list_id (not in dependent_list_id).
 * sectors/tfiles: parallel arrays, one entry per disk sector across all dependent list_ids. */
typedef struct qfile_list_sector_info QFILE_LIST_SECTOR_INFO;
struct qfile_list_sector_info
{
  // *INDENT-OFF*
  struct qmgr_temp_file *membuf_tfile;	/* tfile owning membuf pages (NULL = none) */
  struct file_partial_sector *sectors;	/* data page sectors (FTAB excluded) */
  void **tfiles;			/* parallel array: tfile per sector */
  int sector_cnt;

  qfile_list_sector_info ()
    : membuf_tfile (NULL)
    , sectors (NULL)
    , tfiles (NULL)
    , sector_cnt (0)
  {
    //
  }

  // *INDENT-ON*
};
#endif /*  __cplusplus */

#ifdef __cplusplus
/* Sector-based parallel page scan distribution state.
 * Wraps QFILE_LIST_SECTOR_INFO with the atomic cursors workers use to coordinate. */
typedef struct qfile_list_sector_scan_info QFILE_LIST_SECTOR_SCAN_INFO;
struct qfile_list_sector_scan_info
{
  // *INDENT-OFF*
  QFILE_LIST_SECTOR_INFO sector_info;	/* sector layout (from qfile_collect_list_sector_info) */
  std::atomic<bool> membuf_claimed;	/* atomic flag: one worker claims all membuf pages */
  std::atomic<int> next_sector_index;	/* atomic cursor for sector distribution */

  qfile_list_sector_scan_info ()
    : sector_info ()
    , membuf_claimed (false)
    , next_sector_index (0)
  {
    //
  }
  // *INDENT-ON*
};
#endif /*  __cplusplus */

#ifdef __cplusplus
/*
 * sector_page_iterator
 *
 * Per-thread sector-based page iterator over a QFILE_LIST_ID's data pages.
 * Phase 1: one worker (the CAS winner of membuf_claimed) iterates the
 *          membuf region sequentially.
 * Phase 2: all workers split disk pages by atomically claiming sectors
 *          via next_sector_index and walking each sector's bitmap.
 */
// *INDENT-OFF*
class sector_page_iterator
{
  public:
    sector_page_iterator ();

    PAGE_PTR get_next_page (THREAD_ENTRY *thread_p, QFILE_LIST_SECTOR_SCAN_INFO &sector_scan);

    inline struct qmgr_temp_file *get_current_tfile () const { return m_current_tfile; }
    inline VPID get_current_vpid () const { return m_last_vpid; }

  private:
    int m_membuf_index;		/* -1 = initial, >=0 = CAS winner iterating membuf, -2 = not winner */
    int m_sector_index;
    UINT64 m_current_bitmap;
    VSID m_current_vsid;
    VPID m_last_vpid;		/* VPID of the last returned page */
    struct qmgr_temp_file *m_current_tfile;
};
// *INDENT-ON*
#endif /* __cplusplus */

/* SORTING RELATED DEFINITIONS */

/* Sorted list identifier */
typedef struct qfile_sorted_list_id QFILE_SORTED_LIST_ID;
struct qfile_sorted_list_id
{
  QFILE_LIST_ID *list_id;	/* List File identifier */
  int sorted;			/* Has file already been sorted? */
};

/* Sorting Scan Identifier */
typedef struct qfile_sort_scan_id QFILE_SORT_SCAN_ID;
struct qfile_sort_scan_id
{
  QFILE_LIST_SCAN_ID *s_id;	/* Scan Identifier */
  QFILE_TUPLE_RECORD tplrec;	/* Tuple Descriptor used for sorting */
};


typedef enum
{
  SYNC_EXEC = 0,
  DEFAULT_EXEC_MODE = SYNC_EXEC
} QUERY_EXEC_MODE;

enum
{
  NOT_FROM_RESULT_CACHE = 0x1 << 0,
  RESULT_CACHE_REQUIRED = 0x1 << 1,
  RESULT_CACHE_INHIBITED = 0x1 << 2,
  RESULT_HOLDABLE = 0x1 << 3,
  DONT_COLLECT_EXEC_STATS = 0x1 << 4,
  MRO_CANDIDATE = 0x1 << 5,
  MRO_IS_USED = 0x1 << 6,
  SORT_LIMIT_CANDIDATE = 0x1 << 7,
  SORT_LIMIT_USED = 0x1 << 8,
  XASL_TRACE_TEXT = 0x1 << 9,
  XASL_TRACE_JSON = 0x1 << 10,
  TRIGGER_IS_INVOLVED = 0x1 << 11,
  RETURN_GENERATED_KEYS = 0x1 << 12,
  XASL_CACHE_PINNED_REFERENCE = 0x1 << 13,
  EXECUTE_QUERY_WITHOUT_DATA_BUFFERS = 0x1 << 14,
  EXECUTE_QUERY_WITH_COMMIT = 0x1 << 15,
  TRAN_AUTO_COMMIT = 0x1 << 16,
  LIKE_RECOMPILE_CANDIDATE = 0x1 << 17
};

#define DO_NOT_COLLECT_EXEC_STATS(flag)    ((flag) & DONT_COLLECT_EXEC_STATS)

#define IS_XASL_TRACE_TEXT(flag)    (((flag) & XASL_TRACE_TEXT) != 0)
#define IS_XASL_TRACE_JSON(flag)    (((flag) & XASL_TRACE_JSON) != 0)

#define IS_TRIGGER_INVOLVED(flag)   (((flag) & TRIGGER_IS_INVOLVED) != 0)

#define IS_XASL_CACHE_PINNED_REFERENCE(flag)   (((flag) & XASL_CACHE_PINNED_REFERENCE) != 0)
#define IS_QUERY_EXECUTED_WITHOUT_DATA_BUFFERS(flag)   (((flag) & EXECUTE_QUERY_WITHOUT_DATA_BUFFERS) != 0)
#define IS_QUERY_EXECUTE_WITH_COMMIT(flag)   (((flag) & EXECUTE_QUERY_WITH_COMMIT) != 0)
#define IS_TRAN_AUTO_COMMIT(flag)   (((flag) & TRAN_AUTO_COMMIT) != 0)

typedef int QUERY_FLAG;

#endif /* _QUERY_LIST_H_ */
