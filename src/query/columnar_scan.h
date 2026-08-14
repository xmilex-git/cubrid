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
 * columnar_scan.h - columnar storage block executor (read path)
 *
 * Self-contained block executor for ACCESS_METHOD_COLUMNAR specs.  Reads the
 * stripe directory snapshot, applies MVCCID stripe visibility, decompresses
 * chunks, evaluates the WHERE predicate as vectorized kernels over the raw
 * column arrays (uint64 bitmaps, Kleene 3-valued logic), skips chunk groups
 * through the stripe-footer min/max entries, and decodes surviving rows
 * directly into the val_list slots the access spec's TYPE_ATTR_ID regu
 * variables point at.  scan_manager, HEAP_CACHE_ATTRINFO and the per-row
 * fetch/deform path are never touched.
 */

#ifndef _COLUMNAR_SCAN_H_
#define _COLUMNAR_SCAN_H_

#if !defined (SERVER_MODE) && !defined (SA_MODE)
#error Belongs to server module
#endif

#include "storage_common.h"
#include "thread_compat.hpp"
#include "xasl.h"

/* opaque scan handle */
typedef struct columnar_scan COLUMNAR_SCAN;

/* one qualified chunk group exposed to the block-aggregation loop (#21 Tier 2):
 * n_rows rows, qualified rows flagged in bitmap (n_words UINT64 words) */
typedef struct columnar_chunk_view COLUMNAR_CHUNK_VIEW;
struct columnar_chunk_view
{
  int n_rows;
  int n_words;
  const UINT64 *bitmap;
};

/* raw array of one column inside the current chunk (#21 Tier 2a): the
 * accumulate kernels read values straight from data without any DB_VALUE */
typedef struct columnar_raw_col COLUMNAR_RAW_COL;
struct columnar_raw_col
{
  const char *data;		/* decompressed column array */
  const char *exists;		/* NULL bitmap (1 = value present) */
  int stride;			/* bytes per value; < 0 = variable width */
  DB_TYPE type;
};

/* open a columnar block scan over the given access spec; builds the column
 * bindings and the vectorized filter plan.  Returns
 * ER_COLUMNAR_UNSUPPORTED_EXPR when the WHERE predicate contains a node the
 * vectorized filter does not cover (zero-fallback contract). */
extern int columnar_scan_open (THREAD_ENTRY * thread_p, COLUMNAR_SCAN ** cs_out, ACCESS_SPEC_TYPE * spec,
			       struct val_descr *vd);

/* advance to the next qualified row; fills every bound val_list slot */
extern SCAN_CODE columnar_scan_next (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs);

/* -------- chunk-level API for the BUILDVALUE block-aggregation loop --------
 * Do not mix with columnar_scan_next () on the same scan: both consume the
 * chunk cursor. */

/* position on the next chunk group that has at least one qualified row */
extern SCAN_CODE columnar_scan_next_chunk (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, COLUMNAR_CHUNK_VIEW * view);

/* decode row (chunk-relative) of the current chunk into every bound slot */
extern void columnar_scan_decode_row (COLUMNAR_SCAN * cs, int row);

/* raw array of the binding whose val_list slot is slot_addr, for the current
 * chunk; false when no binding owns that slot.  Pointers are valid until the
 * next columnar_scan_next_chunk () call. */
extern bool columnar_scan_raw_column (COLUMNAR_SCAN * cs, const DB_VALUE * slot_addr, COLUMNAR_RAW_COL * raw);

/* true (with the directory total row count) when the scan reads no column and
 * has no filter, so a COUNT(*)-style consumer needs no page reads at all */
extern bool columnar_scan_total_rows (COLUMNAR_SCAN * cs, INT64 * total);

/* -------- raw accumulate kernels (#21 Tier 2a) --------
 * Accumulate the qualified non-NULL rows of one raw column; return the number
 * of accumulated rows.  Row order matches the interpreted per-row path. */
extern int columnar_raw_sum_int64 (const COLUMNAR_RAW_COL * raw, const UINT64 * bitmap, int n_rows, INT64 * sum_out,
				   bool * overflow);
extern int columnar_raw_sum_double (const COLUMNAR_RAW_COL * raw, const UINT64 * bitmap, int n_rows, double *sum_out);
extern int columnar_raw_count (const COLUMNAR_RAW_COL * raw, const UINT64 * bitmap, int n_rows);

/* snapshot runtime counters into the access spec's col_scan_stats */
extern void columnar_scan_stats (COLUMNAR_SCAN * cs, COL_SCAN_STATS * out);

/* dump the compiled filter plan (COLUMNAR_COMPILE trace section) */
extern void columnar_scan_dump_plan (COLUMNAR_SCAN * cs, FILE * fp);

extern void columnar_scan_close (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs);

#endif /* _COLUMNAR_SCAN_H_ */
