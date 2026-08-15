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
 * columnar_rawprog.h - RAW_PROG: DB_VALUE-free compiled evaluation for the
 *                      columnar block executor
 *
 * Design ticket #23, decisions D1 / D2 / D4 / D7.
 *
 * RAW_PROG is to the columnar block what EXPR_PROG is to the row engine, with
 * one difference that changes everything downstream: its cells are 16-byte
 * UNTAGGED unions, not DB_VALUE pointers.  There is no runtime type tag
 * because a columnar column's domain is fixed at compile time, so the choice
 * of kernel *is* the type (the PostgreSQL Datum principle).  Strings are
 * pointer+length views straight into the decompressed chunk buffer, so they
 * cost no copy.  NUMERIC is the single by-reference case: a cell holds a
 * pointer to step-owned scratch, allocated once per program, never per row.
 *
 * The compiler reads the XASL regu tree directly and emits its own kernels;
 * expr_compile.c is not touched (D2), and no step ever bridges to a DB_VALUE
 * (D4).  A regu node outside the kernel coverage fails compilation, which
 * raises ER_COLUMNAR_UNSUPPORTED_EXPR at the block executor — the raw
 * coverage *is* the columnar support boundary (D5).
 */

#ifndef _COLUMNAR_RAWPROG_H_
#define _COLUMNAR_RAWPROG_H_

#if !defined (SERVER_MODE) && !defined (SA_MODE)
#error Belongs to server module
#endif

#include "columnar_numeric.h"
#include "columnar_scan.h"
#include "dbtype_def.h"
#include "regu_var.hpp"
#include "storage_common.h"
#include "thread_compat.hpp"

/* ========================================================================== */
/* Cells (D1)                                                                 */
/* ========================================================================== */

/* what a cell holds; fixed per cell at compile time, never inspected per row */
typedef enum
{
  RAW_T_NONE = 0,
  RAW_T_I64,			/* SHORT/INT/BIGINT/DATE/TIME/TIMESTAMP/DATETIME */
  RAW_T_DBL,			/* FLOAT/DOUBLE/MONETARY */
  RAW_T_NUM,			/* NUMERIC — cell points at step-owned scratch */
  RAW_T_STR			/* CHAR/VARCHAR — pointer+length into the chunk */
} RAW_TYPE;

typedef union raw_cell RAW_CELL;
union raw_cell
{
  INT64 i64;
  double dbl;
  COL_NUMVAR *num;		/* step-owned scratch, never per-row allocated */
  struct
  {
    const char *ptr;		/* into the decompressed chunk buffer (zero copy) */
    int len;
  } str;
};

/* ========================================================================== */
/* Programs                                                                   */
/* ========================================================================== */

typedef struct raw_prog RAW_PROG;
typedef struct raw_step RAW_STEP;
typedef struct raw_eval_ctx RAW_EVAL_CTX;

struct raw_eval_ctx
{
  RAW_PROG *prog;
  COLUMNAR_SCAN *cs;
  int row;			/* row index inside the current chunk */
  void *user;			/* consumer state for appended steps (raw hash agg) */
};

typedef int (*RAW_KERNEL_FN) (RAW_STEP * step, RAW_EVAL_CTX * ctx);

struct raw_step
{
  RAW_KERNEL_FN kernel;

  int a1, a2;			/* argument cell indices (-1 = unused) */
  int out;			/* result cell index */

  /* leaf steps: which scan binding to read, and its layout.  The three chunk
   * pointers are refreshed once per chunk group by
   * columnar_rawprog_new_chunk (), so the row loop chases no indirection. */
  bool is_leaf;
  int bind_idx;
  int stride;			/* bytes per value; < 0 = variable-width stream */
  const char *data;
  const char *exists;
  const int *offsets;		/* variable-width: per-row byte offset */

  /* RAW_T_NUM result storage owned by the program.  Recorded as an index at
   * compile time (the array still moves) and bound to a pointer once. */
  int scratch_idx;
  COL_NUMVAR *scratch;

  int aux;			/* kernel-specific parameter (target scale, ...) */
};

struct raw_prog
{
  RAW_STEP *steps;
  int n_steps;

  /* Constants need no step at all: compilation happens once per execution, so
   * literals and bound host variables are converted straight into their cell
   * and simply never rewritten. */
  RAW_CELL *cells;
  bool *nulls;			/* per-cell null flag, separate from the value (D1) */
  RAW_TYPE *cell_types;
  int n_cells;

  COL_NUMVAR *scratch;		/* one slot per NUMERIC-producing step */
  int n_scratch;

  int *root_cells;		/* cell index of each compiled root, in root order */
  int n_roots;
};

/* ========================================================================== */
/* API                                                                        */
/* ========================================================================== */

/*
 * Compile an array of root regu variables against an open columnar scan.
 * Every leaf must resolve to a column of that scan or to a compile-time
 * constant; anything else fails.  Returns NO_ERROR with *prog_out set, or
 * ER_COLUMNAR_UNSUPPORTED_EXPR (with *prog_out NULL) when the tree is outside
 * the raw coverage.
 */
extern int columnar_rawprog_compile (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, REGU_VARIABLE ** roots,
				     int n_roots, RAW_PROG ** prog_out);

/*
 * Append a step to an already-compiled program.  This is how the fused
 * aggregate transition (#23 D7) puts its group lookup and its per-aggregate
 * accumulation INTO the program, so one evaluation per row completes argument
 * evaluation, group lookup and accumulation.  Appended steps read their
 * consumer state through ctx->user and produce no cell of their own.
 */
extern int columnar_rawprog_append_step (RAW_PROG * prog, RAW_KERNEL_FN kernel, int a1, int a2, int aux,
					 RAW_STEP ** step_out);

/* evaluate every step with ctx->user set (fused aggregation) */
extern int columnar_rawprog_eval_user (RAW_PROG * prog, COLUMNAR_SCAN * cs, int row, void *user);

/* rebind the leaf steps to the chunk the scan is currently positioned on;
 * call once per chunk group, before the row loop */
extern void columnar_rawprog_new_chunk (RAW_PROG * prog, COLUMNAR_SCAN * cs);

/* evaluate every step for one row of the current chunk */
extern int columnar_rawprog_eval (RAW_PROG * prog, COLUMNAR_SCAN * cs, int row);

/* the cell / null flag / type of the i-th root after an eval */
extern const RAW_CELL *columnar_rawprog_cell (const RAW_PROG * prog, int root_idx);
extern bool columnar_rawprog_is_null (const RAW_PROG * prog, int root_idx);
extern RAW_TYPE columnar_rawprog_type (const RAW_PROG * prog, int root_idx);

/* materialize a cell into a DB_VALUE — output time only, never in the row loop */
extern int columnar_rawprog_to_dbvalue (RAW_TYPE type, const RAW_CELL * cell, bool is_null, TP_DOMAIN * domain,
					DB_VALUE * out);

extern void columnar_rawprog_free (RAW_PROG * prog);

/* one line per step (COLUMNAR_COMPILE trace section) */
extern void columnar_rawprog_dump (const RAW_PROG * prog, FILE * fp, int indent);

#endif /* _COLUMNAR_RAWPROG_H_ */
