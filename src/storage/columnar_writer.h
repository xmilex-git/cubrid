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
 * columnar_writer.h - columnar storage write path
 *
 * Per-transaction, per-class write state that buffers rows and flushes
 * them as stripes.  Hooks into log_commit_local / log_abort_local to
 * flush or discard pending data.  Recovery indices RVCOL_PAGE_DATA and
 * RVCOL_META_DIR_ENTRY handle crash recovery.
 */

#ifndef _COLUMNAR_WRITER_H_
#define _COLUMNAR_WRITER_H_

#if !defined (SERVER_MODE) && !defined (SA_MODE)
#error Belongs to server module
#endif

#include "columnar_file.h"
#include "dbtype_def.h"
#include "oid.h"
#include "storage_common.h"
#include "thread_compat.hpp"

/* ========================================================================== */
/* Column buffer — one per column in a write state                            */
/* ========================================================================== */
/*
 * Hot-path field order (INSERT touches every column every row):
 *
 *   bytes  0..7   data          — always: memcpy target
 *   bytes  8..11  data_size     — always: write offset
 *   bytes 12..15  value_size    — always: fixed/var dispatch
 *   bytes 16..23  exists        — always: set bit
 *   bytes 24..27  exists_size   — always: byte index
 *   bytes 28..31  type          — serialization dispatch
 *   bytes 32..35  data_alloc    — cold: only on grow
 *   bytes 36..39  exists_alloc  — cold: only on grow
 *
 * 40 bytes total.  Hot fields (0..31) fit in half a cache line.
 * Two consecutive COL_BUFFERs (80B) span just over one cache line;
 * column-at-a-time access has good spatial locality.
 */
typedef struct columnar_col_buffer COLUMNAR_COL_BUFFER;
struct columnar_col_buffer
{
  /* --- hot (per-row) --- */
  char *data;			/* value bytes: fixed stride or variable stream */
  int data_size;		/* bytes used */
  int value_size;		/* per-value byte count; -1 = variable */
  char *exists;			/* exists bitmap: 1 bit per row, 0 = NULL */
  int exists_size;		/* bytes used in exists */
  DB_TYPE type;			/* column type */
  /* --- cold (realloc only) --- */
  int data_alloc;		/* bytes allocated for data */
  int exists_alloc;		/* bytes allocated for exists */
};

/* ========================================================================== */
/* Write state savepoint marker                                               */
/* ========================================================================== */
typedef struct columnar_savept COLUMNAR_SAVEPT;
struct columnar_savept
{
  COLUMNAR_SAVEPT *prev;
  char *savept_name;		/* savepoint name (strdup) */
  int rows_at_savepoint;	/* current_rows when savepoint was pushed */
};

/* ========================================================================== */
/* Write state — one per (transaction, class) pair                            */
/* ========================================================================== */
/*
 * Hot-path field order — first cache line holds everything the INSERT
 * fast path reads/writes:
 *
 *   bytes  0..7   next            — linked-list lookup
 *   bytes  8..15  class_oid       — OID comparison in lookup
 *   bytes 16..23  columns         — per-row column-buffer access
 *   bytes 24..27  n_columns       — per-row loop bound
 *   bytes 28..31  current_rows    — per-row increment + threshold
 *   bytes 32..35  stripe_max_rows — threshold check
 *   bytes 36..39  chunk_max_rows  — chunk boundary check
 *         40..63  (next struct members — flush-only, cold)
 *
 * 40 bytes of hot fields ≤ 64-byte cache line.
 */
typedef struct columnar_write_state COLUMNAR_WRITE_STATE;
struct columnar_write_state
{
  /* ---- cache line 0: INSERT hot path ---- */
  COLUMNAR_WRITE_STATE *next;	/* linked list per tran_index */
  OID class_oid;		/* 8 bytes */
  COLUMNAR_COL_BUFFER *columns;	/* [n_columns] */
  int n_columns;
  int current_rows;		/* rows buffered (unflushed) for current stripe */
  int stripe_max_rows;
  int chunk_max_rows;

  /* ---- cache line 1+: flush / management (cold) ---- */
  HFID hfid;			/* 12 bytes (4-byte aligned) */
  int n_chunk_groups;		/* completed chunk groups in current stripe */

  char *stripe_data;		/* serialized chunk data accumulator */
  int stripe_data_size;
  int stripe_data_alloc;

  COLUMNAR_CHUNK_DESC *chunk_descs;	/* [n_columns * n_chunk_groups] */
  int n_chunk_descs;
  int chunk_descs_alloc;

  COLUMNAR_SAVEPT *savept_stack;
};

/* ========================================================================== */
/* Public API                                                                 */
/* ========================================================================== */

/* per-value on-disk byte count of a fixed-width column; -1 = variable width,
 * -2 = type not storable in columnar format (shared with the read path) */
struct tp_domain;
extern int columnar_value_disk_size (DB_TYPE type, struct tp_domain * domain);

/* Boot-time init / final */
extern void columnar_writer_init (int max_tran_indices);
extern void columnar_writer_final (void);

/* INSERT entry point: buffer a row into the write state */
extern int columnar_insert_row (THREAD_ENTRY * thread_p, const OID * class_oid, const HFID * hfid, RECDES * recdes,
				OID * out_oid);

/* Commit / abort hooks — called from log_commit_local / log_abort_local */
extern int columnar_flush_all_write_states (THREAD_ENTRY * thread_p);
extern void columnar_discard_all_write_states (THREAD_ENTRY * thread_p);

/* Savepoint hooks */
extern void columnar_on_savepoint (THREAD_ENTRY * thread_p, const char *savept_name);
extern void columnar_on_partial_abort (THREAD_ENTRY * thread_p, const char *savept_name);

/* Same-txn scan trigger: flush pending data for a class so scan can see it */
extern int columnar_flush_for_scan (THREAD_ENTRY * thread_p, const OID * class_oid);

/* Check whether there are pending writes for a class in this txn */
extern bool columnar_has_pending_writes (THREAD_ENTRY * thread_p, const OID * class_oid);

/* Recovery functions */
extern int columnar_rv_page_data_redo (THREAD_ENTRY * thread_p, LOG_RCV * rcv);
extern int columnar_rv_dir_entry_redo (THREAD_ENTRY * thread_p, LOG_RCV * rcv);
extern int columnar_rv_dir_entry_undo (THREAD_ENTRY * thread_p, LOG_RCV * rcv);

#endif /* _COLUMNAR_WRITER_H_ */
