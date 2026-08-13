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

/* open a columnar block scan over the given access spec; builds the column
 * bindings and the vectorized filter plan.  Returns
 * ER_COLUMNAR_UNSUPPORTED_EXPR when the WHERE predicate contains a node the
 * vectorized filter does not cover (zero-fallback contract). */
extern int columnar_scan_open (THREAD_ENTRY * thread_p, COLUMNAR_SCAN ** cs_out, ACCESS_SPEC_TYPE * spec,
			       struct val_descr *vd);

/* advance to the next qualified row; fills every bound val_list slot */
extern SCAN_CODE columnar_scan_next (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs);

/* snapshot runtime counters into the access spec's col_scan_stats */
extern void columnar_scan_stats (COLUMNAR_SCAN * cs, COL_SCAN_STATS * out);

/* dump the compiled filter plan (COLUMNAR_COMPILE trace section) */
extern void columnar_scan_dump_plan (COLUMNAR_SCAN * cs, FILE * fp);

extern void columnar_scan_close (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs);

#endif /* _COLUMNAR_SCAN_H_ */
