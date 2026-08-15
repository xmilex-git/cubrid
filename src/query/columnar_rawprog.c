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
 * columnar_rawprog.c - RAW_PROG compiler and kernels (#23 D1/D2/D4)
 *
 * The compiler walks the XASL regu tree itself and emits kernels that read
 * and write 16-byte untagged cells.  Nothing here constructs, reads or writes
 * a DB_VALUE in the row loop; the only DB_VALUE contact is at compile time
 * (constants) and at materialization time (columnar_rawprog_to_dbvalue),
 * which the consumers call once per output row or group, never per input row.
 *
 * Coverage note (D5): a regu node this compiler does not understand is not
 * bridged to the interpreted path — compilation fails, and the block executor
 * turns that into ER_COLUMNAR_UNSUPPORTED_EXPR.
 */

#ident "$Id$"

#include "columnar_rawprog.h"

#include "dbtype.h"
#include "error_manager.h"
#include "language_support.h"
#include "object_domain.h"
#include "object_primitive.h"
#include "object_representation.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#define RAW_UNSUPPORTED(what) \
  (er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_COLUMNAR_UNSUPPORTED_EXPR, 1, (what)), ER_COLUMNAR_UNSUPPORTED_EXPR)

/* ========================================================================== */
/* Compile-time builder state                                                 */
/* ========================================================================== */

typedef struct raw_builder RAW_BUILDER;
struct raw_builder
{
  COLUMNAR_SCAN *cs;
  RAW_PROG *prog;
  int steps_alloc;
  int cells_alloc;
  int scratch_alloc;

  /* constant NUMERIC cells hold a scratch index until the array stops moving */
  int *const_num_cells;
  int *const_num_scratch;
  int n_const_num;
  int const_num_alloc;
};

static int raw_compile_regu (RAW_BUILDER * bd, REGU_VARIABLE * regu, int *cell_out, RAW_TYPE * type_out);

/* ========================================================================== */
/* Builder helpers                                                            */
/* ========================================================================== */

static int
raw_new_cell (RAW_BUILDER * bd, RAW_TYPE type, int *idx_out)
{
  RAW_PROG *p = bd->prog;

  if (p->n_cells == bd->cells_alloc)
    {
      int na = (bd->cells_alloc == 0) ? 16 : bd->cells_alloc * 2;
      RAW_CELL *nc = (RAW_CELL *) realloc (p->cells, (size_t) na * sizeof (RAW_CELL));
      bool *nn = NULL;
      RAW_TYPE *nt = NULL;

      if (nc != NULL)
	{
	  p->cells = nc;
	  nn = (bool *) realloc (p->nulls, (size_t) na * sizeof (bool));
	}
      if (nn != NULL)
	{
	  p->nulls = nn;
	  nt = (RAW_TYPE *) realloc (p->cell_types, (size_t) na * sizeof (RAW_TYPE));
	}
      if (nt == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) na * sizeof (RAW_CELL));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      p->cell_types = nt;
      bd->cells_alloc = na;
    }

  memset (&p->cells[p->n_cells], 0, sizeof (RAW_CELL));
  p->nulls[p->n_cells] = false;
  p->cell_types[p->n_cells] = type;
  *idx_out = p->n_cells++;
  return NO_ERROR;
}

/*
 * NUMERIC results live in program-owned scratch.  The array can move while
 * later steps are compiled, so a step records the scratch INDEX and
 * raw_bind_scratch () turns it into a pointer once compilation is done.
 */
static int
raw_new_scratch (RAW_BUILDER * bd, int *idx_out)
{
  RAW_PROG *p = bd->prog;

  if (p->n_scratch == bd->scratch_alloc)
    {
      int na = (bd->scratch_alloc == 0) ? 8 : bd->scratch_alloc * 2;
      COL_NUMVAR *ns = (COL_NUMVAR *) realloc (p->scratch, (size_t) na * sizeof (COL_NUMVAR));

      if (ns == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) na * sizeof (COL_NUMVAR));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      p->scratch = ns;
      bd->scratch_alloc = na;
    }
  memset (&p->scratch[p->n_scratch], 0, sizeof (COL_NUMVAR));
  *idx_out = p->n_scratch++;
  return NO_ERROR;
}

static int
raw_new_step (RAW_BUILDER * bd, RAW_KERNEL_FN kernel, int a1, int a2, int out, RAW_STEP ** step_out)
{
  RAW_PROG *p = bd->prog;
  RAW_STEP *s;

  if (p->n_steps == bd->steps_alloc)
    {
      int na = (bd->steps_alloc == 0) ? 16 : bd->steps_alloc * 2;
      RAW_STEP *nsp = (RAW_STEP *) realloc (p->steps, (size_t) na * sizeof (RAW_STEP));

      if (nsp == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) na * sizeof (RAW_STEP));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      p->steps = nsp;
      bd->steps_alloc = na;
    }

  s = &p->steps[p->n_steps++];
  memset (s, 0, sizeof (*s));
  s->kernel = kernel;
  s->a1 = a1;
  s->a2 = a2;
  s->out = out;
  s->bind_idx = -1;
  s->scratch_idx = -1;
  s->aux = -1;
  *step_out = s;
  return NO_ERROR;
}

/* remember a constant NUMERIC cell so its scratch index can be turned into a
 * pointer once compilation is done */
static int
raw_note_const_num (RAW_BUILDER * bd, int cell, int scratch_idx)
{
  if (bd->n_const_num == bd->const_num_alloc)
    {
      int na = (bd->const_num_alloc == 0) ? 8 : bd->const_num_alloc * 2;
      int *nc = (int *) realloc (bd->const_num_cells, (size_t) na * sizeof (int));
      int *ns = NULL;

      if (nc != NULL)
	{
	  bd->const_num_cells = nc;
	  ns = (int *) realloc (bd->const_num_scratch, (size_t) na * sizeof (int));
	}
      if (ns == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) na * sizeof (int));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      bd->const_num_scratch = ns;
      bd->const_num_alloc = na;
    }
  bd->const_num_cells[bd->n_const_num] = cell;
  bd->const_num_scratch[bd->n_const_num] = scratch_idx;
  bd->n_const_num++;
  return NO_ERROR;
}

/* ========================================================================== */
/* Type mapping                                                               */
/* ========================================================================== */

static RAW_TYPE
raw_type_of (DB_TYPE t)
{
  switch (t)
    {
    case DB_TYPE_SHORT:
    case DB_TYPE_INTEGER:
    case DB_TYPE_BIGINT:
    case DB_TYPE_DATE:
    case DB_TYPE_TIME:
    case DB_TYPE_TIMESTAMP:
    case DB_TYPE_DATETIME:
      return RAW_T_I64;
    case DB_TYPE_FLOAT:
    case DB_TYPE_DOUBLE:
    case DB_TYPE_MONETARY:
      return RAW_T_DBL;
    case DB_TYPE_NUMERIC:
      return RAW_T_NUM;
    case DB_TYPE_CHAR:
    case DB_TYPE_VARCHAR:
      return RAW_T_STR;
    default:
      return RAW_T_NONE;
    }
}

/* ========================================================================== */
/* Leaf kernels: raw column array -> cell                                     */
/* ========================================================================== */

#define RAW_EXISTS(ex, r)  (((ex)[(r) >> 3] >> ((r) & 7)) & 1)

#define RAW_DEF_LEAF_I64(NAME, CTYPE)                                         \
  static int                                                                  \
  NAME (RAW_STEP * s, RAW_EVAL_CTX * ctx)                                     \
  {                                                                           \
    RAW_PROG *p = ctx->prog;                                                  \
    int r = ctx->row;                                                         \
    if (!RAW_EXISTS (s->exists, r))                                           \
      {                                                                       \
        p->nulls[s->out] = true;                                              \
        return NO_ERROR;                                                      \
      }                                                                       \
    {                                                                         \
      CTYPE v;                                                                \
      memcpy (&v, s->data + (size_t) r * s->stride, sizeof (v));              \
      p->cells[s->out].i64 = (INT64) v;                                       \
    }                                                                         \
    p->nulls[s->out] = false;                                                 \
    return NO_ERROR;                                                          \
  }

/* *INDENT-OFF* */
RAW_DEF_LEAF_I64 (raw_leaf_i16, short)
RAW_DEF_LEAF_I64 (raw_leaf_i32, int)
RAW_DEF_LEAF_I64 (raw_leaf_i64, INT64)
RAW_DEF_LEAF_I64 (raw_leaf_u32, unsigned int)
/* *INDENT-ON* */

/* DATETIME: (date << 32) | time, matching the vectorized filter's encoding */
static int
raw_leaf_dt (RAW_STEP * s, RAW_EVAL_CTX * ctx)
{
  RAW_PROG *p = ctx->prog;
  int r = ctx->row;
  unsigned int d, t;

  if (!RAW_EXISTS (s->exists, r))
    {
      p->nulls[s->out] = true;
      return NO_ERROR;
    }
  memcpy (&d, s->data + (size_t) r * s->stride, sizeof (d));
  memcpy (&t, s->data + (size_t) r * s->stride + sizeof (d), sizeof (t));
  p->cells[s->out].i64 = ((INT64) d << 32) | (INT64) t;
  p->nulls[s->out] = false;
  return NO_ERROR;
}

static int
raw_leaf_f32 (RAW_STEP * s, RAW_EVAL_CTX * ctx)
{
  RAW_PROG *p = ctx->prog;
  int r = ctx->row;
  float v;

  if (!RAW_EXISTS (s->exists, r))
    {
      p->nulls[s->out] = true;
      return NO_ERROR;
    }
  memcpy (&v, s->data + (size_t) r * s->stride, sizeof (v));
  p->cells[s->out].dbl = (double) v;
  p->nulls[s->out] = false;
  return NO_ERROR;
}

/* DOUBLE, and MONETARY whose amount is the leading double */
static int
raw_leaf_f64 (RAW_STEP * s, RAW_EVAL_CTX * ctx)
{
  RAW_PROG *p = ctx->prog;
  int r = ctx->row;

  if (!RAW_EXISTS (s->exists, r))
    {
      p->nulls[s->out] = true;
      return NO_ERROR;
    }
  memcpy (&p->cells[s->out].dbl, s->data + (size_t) r * s->stride, sizeof (double));
  p->nulls[s->out] = false;
  return NO_ERROR;
}

/*
 * NUMERIC leaf: the stored bytes are already the arithmetic representation
 * (#23 D11), so this is a header read plus a digit memcpy into step scratch —
 * no decimal conversion, no allocation.
 */
static int
raw_leaf_num (RAW_STEP * s, RAW_EVAL_CTX * ctx)
{
  RAW_PROG *p = ctx->prog;
  int r = ctx->row;
  COL_NUMREF ref;

  if (!RAW_EXISTS (s->exists, r))
    {
      p->nulls[s->out] = true;
      return NO_ERROR;
    }
  columnar_num_ref (s->data + s->offsets[r] + (int) sizeof (int), &ref);
  columnar_num_from_ref (&ref, s->scratch);
  p->cells[s->out].num = s->scratch;
  p->nulls[s->out] = false;
  return NO_ERROR;
}

/* CHAR(n): fixed stride, no length prefix */
static int
raw_leaf_charn (RAW_STEP * s, RAW_EVAL_CTX * ctx)
{
  RAW_PROG *p = ctx->prog;
  int r = ctx->row;

  if (!RAW_EXISTS (s->exists, r))
    {
      p->nulls[s->out] = true;
      return NO_ERROR;
    }
  p->cells[s->out].str.ptr = s->data + (size_t) r * s->stride;
  p->cells[s->out].str.len = s->stride;
  p->nulls[s->out] = false;
  return NO_ERROR;
}

/* VARCHAR: 4-byte length prefix in the variable-width stream */
static int
raw_leaf_varchar (RAW_STEP * s, RAW_EVAL_CTX * ctx)
{
  RAW_PROG *p = ctx->prog;
  int r = ctx->row;
  int off, len;

  if (!RAW_EXISTS (s->exists, r))
    {
      p->nulls[s->out] = true;
      return NO_ERROR;
    }
  off = s->offsets[r];
  memcpy (&len, s->data + off, sizeof (int));
  p->cells[s->out].str.ptr = s->data + off + (int) sizeof (int);
  p->cells[s->out].str.len = len;
  p->nulls[s->out] = false;
  return NO_ERROR;
}

/* ========================================================================== */
/* Arithmetic kernels                                                         */
/* ========================================================================== */

#define RAW_BINARY_NULL_GUARD(P, S)                                           \
  if ((P)->nulls[(S)->a1] || (P)->nulls[(S)->a2])                             \
    {                                                                         \
      (P)->nulls[(S)->out] = true;                                            \
      return NO_ERROR;                                                        \
    }                                                                         \
  (P)->nulls[(S)->out] = false

#define RAW_DEF_ARITH_I64(NAME, EXPR)                                         \
  static int                                                                  \
  NAME (RAW_STEP * s, RAW_EVAL_CTX * ctx)                                     \
  {                                                                           \
    RAW_PROG *p = ctx->prog;                                                  \
    INT64 x, y;                                                               \
    RAW_BINARY_NULL_GUARD (p, s);                                             \
    x = p->cells[s->a1].i64;                                                  \
    y = p->cells[s->a2].i64;                                                  \
    EXPR;                                                                     \
    return NO_ERROR;                                                          \
  }

/* *INDENT-OFF* */
RAW_DEF_ARITH_I64 (raw_add_i64, {
  INT64 z = (INT64) ((UINT64) x + (UINT64) y);
  if (OR_CHECK_ADD_OVERFLOW (x, y, z))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_ADDITION, 0);
      return ER_QPROC_OVERFLOW_ADDITION;
    }
  p->cells[s->out].i64 = z;
})
RAW_DEF_ARITH_I64 (raw_sub_i64, {
  INT64 z = (INT64) ((UINT64) x - (UINT64) y);
  if (OR_CHECK_SUB_UNDERFLOW (x, y, z))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_SUBTRACTION, 0);
      return ER_QPROC_OVERFLOW_SUBTRACTION;
    }
  p->cells[s->out].i64 = z;
})
RAW_DEF_ARITH_I64 (raw_mul_i64, {
  INT64 z = (INT64) ((UINT64) x * (UINT64) y);
  if (OR_CHECK_MULT_OVERFLOW (x, y, z))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_MULTIPLICATION, 0);
      return ER_QPROC_OVERFLOW_MULTIPLICATION;
    }
  p->cells[s->out].i64 = z;
})
RAW_DEF_ARITH_I64 (raw_div_i64, {
  if (y == 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_ZERO_DIVIDE, 0);
      return ER_QPROC_ZERO_DIVIDE;
    }
  p->cells[s->out].i64 = x / y;
})
/* *INDENT-ON* */

#define RAW_DEF_ARITH_DBL(NAME, OP)                                           \
  static int                                                                  \
  NAME (RAW_STEP * s, RAW_EVAL_CTX * ctx)                                     \
  {                                                                           \
    RAW_PROG *p = ctx->prog;                                                  \
    RAW_BINARY_NULL_GUARD (p, s);                                             \
    p->cells[s->out].dbl = p->cells[s->a1].dbl OP p->cells[s->a2].dbl;        \
    return NO_ERROR;                                                          \
  }

/* *INDENT-OFF* */
RAW_DEF_ARITH_DBL (raw_add_dbl, +)
RAW_DEF_ARITH_DBL (raw_sub_dbl, -)
RAW_DEF_ARITH_DBL (raw_mul_dbl, *)
/* *INDENT-ON* */

static int
raw_div_dbl (RAW_STEP * s, RAW_EVAL_CTX * ctx)
{
  RAW_PROG *p = ctx->prog;
  double y;

  RAW_BINARY_NULL_GUARD (p, s);
  y = p->cells[s->a2].dbl;
  if (y == 0.0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_ZERO_DIVIDE, 0);
      return ER_QPROC_ZERO_DIVIDE;
    }
  p->cells[s->out].dbl = p->cells[s->a1].dbl / y;
  return NO_ERROR;
}

#define RAW_DEF_ARITH_NUM(NAME, FN)                                           \
  static int                                                                  \
  NAME (RAW_STEP * s, RAW_EVAL_CTX * ctx)                                     \
  {                                                                           \
    RAW_PROG *p = ctx->prog;                                                  \
    COL_NUMREF a, b;                                                          \
    int error;                                                                \
    RAW_BINARY_NULL_GUARD (p, s);                                             \
    columnar_num_ref_var (p->cells[s->a1].num, &a);                           \
    columnar_num_ref_var (p->cells[s->a2].num, &b);                           \
    error = FN (&a, &b, s->scratch);                                          \
    if (error != NO_ERROR)                                                    \
      {                                                                       \
        return error;                                                         \
      }                                                                       \
    p->cells[s->out].num = s->scratch;                                        \
    return NO_ERROR;                                                          \
  }

/* *INDENT-OFF* */
RAW_DEF_ARITH_NUM (raw_add_num, columnar_num_add)
RAW_DEF_ARITH_NUM (raw_sub_num, columnar_num_sub)
RAW_DEF_ARITH_NUM (raw_mul_num, columnar_num_mul)
RAW_DEF_ARITH_NUM (raw_div_num, columnar_num_div)
/* *INDENT-ON* */

/* ========================================================================== */
/* Coercion kernels (compile-time inserted, never per-row dispatched)         */
/* ========================================================================== */

static int
raw_cast_i64_to_dbl (RAW_STEP * s, RAW_EVAL_CTX * ctx)
{
  RAW_PROG *p = ctx->prog;

  if (p->nulls[s->a1])
    {
      p->nulls[s->out] = true;
      return NO_ERROR;
    }
  p->cells[s->out].dbl = (double) p->cells[s->a1].i64;
  p->nulls[s->out] = false;
  return NO_ERROR;
}

/* INT -> NUMERIC at the scale recorded in step->aux */
static int
raw_cast_i64_to_num (RAW_STEP * s, RAW_EVAL_CTX * ctx)
{
  RAW_PROG *p = ctx->prog;
  int error;

  if (p->nulls[s->a1])
    {
      p->nulls[s->out] = true;
      return NO_ERROR;
    }
  error = columnar_num_set_int64 (s->scratch, p->cells[s->a1].i64, s->aux);
  if (error != NO_ERROR)
    {
      return error;
    }
  p->cells[s->out].num = s->scratch;
  p->nulls[s->out] = false;
  return NO_ERROR;
}

static int
raw_cast_num_to_dbl (RAW_STEP * s, RAW_EVAL_CTX * ctx)
{
  RAW_PROG *p = ctx->prog;
  COL_NUMREF a;

  if (p->nulls[s->a1])
    {
      p->nulls[s->out] = true;
      return NO_ERROR;
    }
  columnar_num_ref_var (p->cells[s->a1].num, &a);
  p->cells[s->out].dbl = columnar_num_to_double (&a);
  p->nulls[s->out] = false;
  return NO_ERROR;
}

/* ========================================================================== */
/* Compilation                                                                */
/* ========================================================================== */

/*
 * raw_compile_leaf () - a column of the scan becomes one leaf step whose chunk
 *   pointers are rebound per chunk group.
 */
static int
raw_compile_leaf (RAW_BUILDER * bd, int bind_idx, int *cell_out, RAW_TYPE * type_out)
{
  COLUMNAR_BIND_VIEW view;
  RAW_KERNEL_FN kernel = NULL;
  RAW_TYPE type;
  RAW_STEP *step;
  int cell, scratch_idx = -1, error;

  columnar_scan_bind_view (bd->cs, bind_idx, &view);
  type = raw_type_of (view.type);
  if (type == RAW_T_NONE)
    {
      return RAW_UNSUPPORTED ("column type not covered by the raw program");
    }

  switch (view.type)
    {
    case DB_TYPE_SHORT:
      kernel = raw_leaf_i16;
      break;
    case DB_TYPE_INTEGER:
      kernel = raw_leaf_i32;
      break;
    case DB_TYPE_BIGINT:
      kernel = raw_leaf_i64;
      break;
    case DB_TYPE_DATE:
    case DB_TYPE_TIME:
    case DB_TYPE_TIMESTAMP:
      kernel = raw_leaf_u32;
      break;
    case DB_TYPE_DATETIME:
      kernel = raw_leaf_dt;
      break;
    case DB_TYPE_FLOAT:
      kernel = raw_leaf_f32;
      break;
    case DB_TYPE_DOUBLE:
    case DB_TYPE_MONETARY:
      kernel = raw_leaf_f64;
      break;
    case DB_TYPE_NUMERIC:
      kernel = raw_leaf_num;
      break;
    case DB_TYPE_CHAR:
      kernel = raw_leaf_charn;
      break;
    case DB_TYPE_VARCHAR:
      kernel = raw_leaf_varchar;
      break;
    default:
      return RAW_UNSUPPORTED ("column type not covered by the raw program");
    }

  error = raw_new_cell (bd, type, &cell);
  if (error != NO_ERROR)
    {
      return error;
    }
  if (type == RAW_T_NUM)
    {
      error = raw_new_scratch (bd, &scratch_idx);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }
  error = raw_new_step (bd, kernel, -1, -1, cell, &step);
  if (error != NO_ERROR)
    {
      return error;
    }
  step->is_leaf = true;
  step->bind_idx = bind_idx;
  step->stride = view.stride;
  step->scratch_idx = scratch_idx;

  *cell_out = cell;
  *type_out = type;
  return NO_ERROR;
}

/*
 * raw_compile_const () - a compile-time-known DB_VALUE becomes a pre-filled
 *   cell with no step; compilation runs once per execution, so a literal or a
 *   bound host variable never needs to be re-read.
 */
static int
raw_compile_const (RAW_BUILDER * bd, DB_VALUE * val, int *cell_out, RAW_TYPE * type_out)
{
  DB_TYPE t;
  RAW_TYPE type;
  int cell, error;

  if (val == NULL)
    {
      return RAW_UNSUPPORTED ("null constant operand");
    }

  t = DB_VALUE_DOMAIN_TYPE (val);
  type = raw_type_of (t);
  if (type == RAW_T_NONE)
    {
      return RAW_UNSUPPORTED ("constant type not covered by the raw program");
    }

  error = raw_new_cell (bd, type, &cell);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (DB_IS_NULL (val))
    {
      bd->prog->nulls[cell] = true;
      *cell_out = cell;
      *type_out = type;
      return NO_ERROR;
    }

  switch (type)
    {
    case RAW_T_I64:
      switch (t)
	{
	case DB_TYPE_SHORT:
	  bd->prog->cells[cell].i64 = db_get_short (val);
	  break;
	case DB_TYPE_INTEGER:
	  bd->prog->cells[cell].i64 = db_get_int (val);
	  break;
	case DB_TYPE_BIGINT:
	  bd->prog->cells[cell].i64 = db_get_bigint (val);
	  break;
	case DB_TYPE_DATE:
	  bd->prog->cells[cell].i64 = (INT64) (*db_get_date (val));
	  break;
	case DB_TYPE_TIME:
	  bd->prog->cells[cell].i64 = (INT64) (*db_get_time (val));
	  break;
	case DB_TYPE_TIMESTAMP:
	  bd->prog->cells[cell].i64 = (INT64) (*db_get_timestamp (val));
	  break;
	default:
	  {
	    DB_DATETIME *dt = db_get_datetime (val);
	    bd->prog->cells[cell].i64 = ((INT64) dt->date << 32) | (INT64) dt->time;
	  }
	  break;
	}
      break;

    case RAW_T_DBL:
      switch (t)
	{
	case DB_TYPE_FLOAT:
	  bd->prog->cells[cell].dbl = (double) db_get_float (val);
	  break;
	case DB_TYPE_DOUBLE:
	  bd->prog->cells[cell].dbl = db_get_double (val);
	  break;
	default:
	  bd->prog->cells[cell].dbl = db_get_monetary (val)->amount;
	  break;
	}
      break;

    case RAW_T_NUM:
      {
	int scratch_idx;

	error = raw_new_scratch (bd, &scratch_idx);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
	error = columnar_num_from_dbvalue (val, &bd->prog->scratch[scratch_idx]);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
	error = raw_note_const_num (bd, cell, scratch_idx);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
      }
      break;

    default:			/* RAW_T_STR */
      bd->prog->cells[cell].str.ptr = db_get_string (val);
      bd->prog->cells[cell].str.len = db_get_string_size (val);
      break;
    }

  *cell_out = cell;
  *type_out = type;
  return NO_ERROR;
}

/*
 * raw_coerce () - insert a coercion step so both operands of an arithmetic
 *   node share one cell type.  Widening only; anything else fails compilation.
 */
static int
raw_coerce (RAW_BUILDER * bd, int in_cell, RAW_TYPE from, RAW_TYPE to, int target_scale, int *cell_out)
{
  RAW_KERNEL_FN kernel;
  RAW_STEP *step;
  int cell, scratch_idx = -1, error;

  if (from == to)
    {
      *cell_out = in_cell;
      return NO_ERROR;
    }

  if (from == RAW_T_I64 && to == RAW_T_DBL)
    {
      kernel = raw_cast_i64_to_dbl;
    }
  else if (from == RAW_T_I64 && to == RAW_T_NUM)
    {
      kernel = raw_cast_i64_to_num;
    }
  else if (from == RAW_T_NUM && to == RAW_T_DBL)
    {
      kernel = raw_cast_num_to_dbl;
    }
  else
    {
      return RAW_UNSUPPORTED ("operand coercion not covered by the raw program");
    }

  error = raw_new_cell (bd, to, &cell);
  if (error != NO_ERROR)
    {
      return error;
    }
  if (to == RAW_T_NUM)
    {
      error = raw_new_scratch (bd, &scratch_idx);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }
  error = raw_new_step (bd, kernel, in_cell, -1, cell, &step);
  if (error != NO_ERROR)
    {
      return error;
    }
  step->scratch_idx = scratch_idx;
  step->aux = target_scale;

  *cell_out = cell;
  return NO_ERROR;
}

static int
raw_compile_arith (RAW_BUILDER * bd, ARITH_TYPE * arith, int *cell_out, RAW_TYPE * type_out)
{
  RAW_TYPE lt, rt, rest;
  RAW_KERNEL_FN kernel = NULL;
  RAW_STEP *step;
  int lc, rc, cell, scratch_idx = -1, error;
  DB_TYPE res_dbtype;

  if (arith->leftptr == NULL || arith->rightptr == NULL)
    {
      return RAW_UNSUPPORTED ("unary arithmetic not covered by the raw program");
    }

  error = raw_compile_regu (bd, arith->leftptr, &lc, &lt);
  if (error != NO_ERROR)
    {
      return error;
    }
  error = raw_compile_regu (bd, arith->rightptr, &rc, &rt);
  if (error != NO_ERROR)
    {
      return error;
    }

  res_dbtype = (arith->domain != NULL) ? TP_DOMAIN_TYPE (arith->domain) : DB_TYPE_NULL;
  rest = raw_type_of (res_dbtype);
  if (rest == RAW_T_NONE || rest == RAW_T_STR)
    {
      return RAW_UNSUPPORTED ("arithmetic result type not covered by the raw program");
    }
  /*
   * The planner's declared result domain decides the cell type; both operands
   * are widened to it.  A widening the raw path cannot express (NUMERIC or
   * DOUBLE feeding an integer result, a string operand) fails compilation
   * rather than quietly computing in some other type — under zero fallback
   * (D4) declining is the safe answer, silently changing the result type is
   * not.
   */
  {
    int scale = (arith->domain != NULL) ? arith->domain->scale : 0;

    error = raw_coerce (bd, lc, lt, rest, scale, &lc);
    if (error != NO_ERROR)
      {
	return error;
      }
    error = raw_coerce (bd, rc, rt, rest, scale, &rc);
    if (error != NO_ERROR)
      {
	return error;
      }
  }

  switch (arith->opcode)
    {
    case T_ADD:
      kernel = (rest == RAW_T_I64) ? raw_add_i64 : ((rest == RAW_T_DBL) ? raw_add_dbl : raw_add_num);
      break;
    case T_SUB:
      kernel = (rest == RAW_T_I64) ? raw_sub_i64 : ((rest == RAW_T_DBL) ? raw_sub_dbl : raw_sub_num);
      break;
    case T_MUL:
      kernel = (rest == RAW_T_I64) ? raw_mul_i64 : ((rest == RAW_T_DBL) ? raw_mul_dbl : raw_mul_num);
      break;
    case T_DIV:
      kernel = (rest == RAW_T_I64) ? raw_div_i64 : ((rest == RAW_T_DBL) ? raw_div_dbl : raw_div_num);
      break;
    default:
      return RAW_UNSUPPORTED ("operator not covered by the raw program");
    }

  error = raw_new_cell (bd, rest, &cell);
  if (error != NO_ERROR)
    {
      return error;
    }
  if (rest == RAW_T_NUM)
    {
      error = raw_new_scratch (bd, &scratch_idx);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }
  error = raw_new_step (bd, kernel, lc, rc, cell, &step);
  if (error != NO_ERROR)
    {
      return error;
    }
  step->scratch_idx = scratch_idx;

  *cell_out = cell;
  *type_out = rest;
  return NO_ERROR;
}

static int
raw_compile_regu (RAW_BUILDER * bd, REGU_VARIABLE * regu, int *cell_out, RAW_TYPE * type_out)
{
  int bind_idx;

  if (regu == NULL)
    {
      return RAW_UNSUPPORTED ("empty regu node");
    }

  bind_idx = columnar_scan_bind_regu (bd->cs, regu);
  if (bind_idx >= 0)
    {
      return raw_compile_leaf (bd, bind_idx, cell_out, type_out);
    }

  switch (regu->type)
    {
    case TYPE_DBVAL:
      return raw_compile_const (bd, &regu->value.dbval, cell_out, type_out);
    case TYPE_CONSTANT:
      /* a TYPE_CONSTANT that is not one of this scan's columns is an outer
       * reference; its value is fixed for the execution the program is
       * compiled for */
      return raw_compile_const (bd, regu->value.dbvalptr, cell_out, type_out);
    case TYPE_INARITH:
    case TYPE_OUTARITH:
      return raw_compile_arith (bd, regu->value.arithptr, cell_out, type_out);
    default:
      return RAW_UNSUPPORTED ("regu node not covered by the raw program");
    }
}

/*
 * raw_bind_scratch () - turn the scratch INDICES recorded during compilation
 *   into pointers.  Deferred because the scratch array is reallocated while
 *   later steps are compiled.
 */
static void
raw_bind_scratch (RAW_BUILDER * bd)
{
  RAW_PROG *prog = bd->prog;
  int i;

  for (i = 0; i < prog->n_steps; i++)
    {
      RAW_STEP *s = &prog->steps[i];

      if (s->scratch_idx >= 0)
	{
	  assert (s->scratch_idx < prog->n_scratch);
	  s->scratch = &prog->scratch[s->scratch_idx];
	}
    }
  for (i = 0; i < bd->n_const_num; i++)
    {
      prog->cells[bd->const_num_cells[i]].num = &prog->scratch[bd->const_num_scratch[i]];
    }
}

int
columnar_rawprog_compile (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, REGU_VARIABLE ** roots, int n_roots,
			  RAW_PROG ** prog_out)
{
  RAW_BUILDER bd;
  RAW_PROG *prog;
  int i, error = NO_ERROR;

  (void) thread_p;
  *prog_out = NULL;

  prog = (RAW_PROG *) calloc (1, sizeof (RAW_PROG));
  if (prog == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (RAW_PROG));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  prog->root_cells = (int *) malloc ((size_t) (n_roots > 0 ? n_roots : 1) * sizeof (int));
  if (prog->root_cells == NULL)
    {
      free (prog);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) n_roots * sizeof (int));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  memset (&bd, 0, sizeof (bd));
  bd.cs = cs;
  bd.prog = prog;

  for (i = 0; i < n_roots; i++)
    {
      RAW_TYPE t;
      int cell;

      error = raw_compile_regu (&bd, roots[i], &cell, &t);
      if (error != NO_ERROR)
	{
	  free (bd.const_num_cells);
	  free (bd.const_num_scratch);
	  columnar_rawprog_free (prog);
	  return error;
	}
      prog->root_cells[i] = cell;
    }
  prog->n_roots = n_roots;

  raw_bind_scratch (&bd);
  free (bd.const_num_cells);
  free (bd.const_num_scratch);
  *prog_out = prog;
  return NO_ERROR;
}

/* ========================================================================== */
/* Evaluation                                                                 */
/* ========================================================================== */

void
columnar_rawprog_new_chunk (RAW_PROG * prog, COLUMNAR_SCAN * cs)
{
  int i;

  for (i = 0; i < prog->n_steps; i++)
    {
      RAW_STEP *s = &prog->steps[i];
      COLUMNAR_BIND_VIEW view;

      if (!s->is_leaf)
	{
	  continue;
	}
      columnar_scan_bind_view (cs, s->bind_idx, &view);
      s->data = view.data;
      s->exists = view.exists;
      s->offsets = view.offsets;
    }
}

int
columnar_rawprog_append_step (RAW_PROG * prog, RAW_KERNEL_FN kernel, int a1, int a2, int aux, RAW_STEP ** step_out)
{
  RAW_STEP *ns = (RAW_STEP *) realloc (prog->steps, (size_t) (prog->n_steps + 1) * sizeof (RAW_STEP));
  RAW_STEP *s;
  int i;

  if (ns == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      (size_t) (prog->n_steps + 1) * sizeof (RAW_STEP));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  prog->steps = ns;

  /* realloc moved the array, so the bound scratch pointers must be re-taken */
  for (i = 0; i < prog->n_steps; i++)
    {
      if (prog->steps[i].scratch_idx >= 0)
	{
	  prog->steps[i].scratch = &prog->scratch[prog->steps[i].scratch_idx];
	}
    }

  s = &prog->steps[prog->n_steps++];
  memset (s, 0, sizeof (*s));
  s->kernel = kernel;
  s->a1 = a1;
  s->a2 = a2;
  s->out = -1;
  s->bind_idx = -1;
  s->scratch_idx = -1;
  s->aux = aux;
  *step_out = s;
  return NO_ERROR;
}

int
columnar_rawprog_eval_user (RAW_PROG * prog, COLUMNAR_SCAN * cs, int row, void *user)
{
  RAW_EVAL_CTX ctx;
  int i, error;

  ctx.prog = prog;
  ctx.cs = cs;
  ctx.row = row;
  ctx.user = user;

  for (i = 0; i < prog->n_steps; i++)
    {
      error = prog->steps[i].kernel (&prog->steps[i], &ctx);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }
  return NO_ERROR;
}

int
columnar_rawprog_eval (RAW_PROG * prog, COLUMNAR_SCAN * cs, int row)
{
  RAW_EVAL_CTX ctx;
  int i, error;

  ctx.prog = prog;
  ctx.cs = cs;
  ctx.row = row;
  ctx.user = NULL;

  for (i = 0; i < prog->n_steps; i++)
    {
      error = prog->steps[i].kernel (&prog->steps[i], &ctx);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }
  return NO_ERROR;
}

const RAW_CELL *
columnar_rawprog_cell (const RAW_PROG * prog, int root_idx)
{
  return &prog->cells[prog->root_cells[root_idx]];
}

bool
columnar_rawprog_is_null (const RAW_PROG * prog, int root_idx)
{
  return prog->nulls[prog->root_cells[root_idx]];
}

RAW_TYPE
columnar_rawprog_type (const RAW_PROG * prog, int root_idx)
{
  return prog->cell_types[prog->root_cells[root_idx]];
}

/* ========================================================================== */
/* Materialization (output time only)                                         */
/* ========================================================================== */

int
columnar_rawprog_to_dbvalue (RAW_TYPE type, const RAW_CELL * cell, bool is_null, TP_DOMAIN * domain, DB_VALUE * out)
{
  DB_TYPE dt = (domain != NULL) ? TP_DOMAIN_TYPE (domain) : DB_TYPE_NULL;

  if (is_null)
    {
      db_make_null (out);
      return NO_ERROR;
    }

  switch (type)
    {
    case RAW_T_I64:
      switch (dt)
	{
	case DB_TYPE_SHORT:
	  db_make_short (out, (short) cell->i64);
	  break;
	case DB_TYPE_INTEGER:
	  db_make_int (out, (int) cell->i64);
	  break;
	case DB_TYPE_DATE:
	  {
	    DB_DATE d = (DB_DATE) cell->i64;
	    db_value_put_encoded_date (out, &d);
	  }
	  break;
	case DB_TYPE_TIME:
	  {
	    DB_TIME t = (DB_TIME) cell->i64;
	    db_value_put_encoded_time (out, &t);
	  }
	  break;
	case DB_TYPE_TIMESTAMP:
	  db_make_timestamp (out, (DB_TIMESTAMP) cell->i64);
	  break;
	case DB_TYPE_DATETIME:
	  {
	    DB_DATETIME dtv;
	    dtv.date = (unsigned int) (cell->i64 >> 32);
	    dtv.time = (unsigned int) (cell->i64 & 0xffffffffLL);
	    db_make_datetime (out, &dtv);
	  }
	  break;
	default:
	  db_make_bigint (out, cell->i64);
	  break;
	}
      break;

    case RAW_T_DBL:
      switch (dt)
	{
	case DB_TYPE_FLOAT:
	  db_make_float (out, (float) cell->dbl);
	  break;
	case DB_TYPE_MONETARY:
	  db_make_monetary (out, DB_CURRENCY_DEFAULT, cell->dbl);
	  break;
	default:
	  db_make_double (out, cell->dbl);
	  break;
	}
      break;

    case RAW_T_NUM:
      {
	/*
	 * The value carries PostgreSQL's result scale (D12).  When the target
	 * really is NUMERIC its own (precision, scale) pair applies, so the
	 * divergence is confined to what PostgreSQL's intermediate scale rules
	 * change about the rounded result (ADR 0002).  When it is NOT numeric,
	 * a coherent full-width numeric is built here and the coercion tail
	 * below converts it -- mixing a foreign domain's precision with this
	 * value's scale would produce a pair that describes no real value.
	 */
	int prec, scale;
	int error;

	if (domain != NULL && dt == DB_TYPE_NUMERIC)
	  {
	    prec = (domain->precision > 0) ? domain->precision : DB_MAX_NUMERIC_PRECISION;
	    scale = domain->scale;
	  }
	else
	  {
	    prec = DB_MAX_NUMERIC_PRECISION;
	    scale = cell->num->dscale;
	    if (scale > DB_MAX_NUMERIC_PRECISION)
	      {
		scale = DB_MAX_NUMERIC_PRECISION;
	      }
	  }
	error = columnar_num_to_dbvalue (cell->num, prec, scale, out);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
      }
      break;

    default:			/* RAW_T_STR */
      if (dt == DB_TYPE_CHAR)
	{
	  db_make_char (out, (domain != NULL) ? domain->precision : cell->str.len, cell->str.ptr, cell->str.len,
			(domain != NULL) ? TP_DOMAIN_CODESET (domain) : LANG_SYS_CODESET,
			(domain != NULL) ? TP_DOMAIN_COLLATION (domain) : LANG_SYS_COLLATION);
	}
      else
	{
	  db_make_varchar (out, (domain != NULL) ? domain->precision : cell->str.len, cell->str.ptr, cell->str.len,
			   (domain != NULL) ? TP_DOMAIN_CODESET (domain) : LANG_SYS_CODESET,
			   (domain != NULL) ? TP_DOMAIN_COLLATION (domain) : LANG_SYS_COLLATION);
	}
      break;
    }

  /*
   * Last line of defence: the raw cell types are coarser than the engine's
   * (RAW_T_I64 covers every integer and date/time type, RAW_T_NUM every
   * NUMERIC), and an aggregate's accumulator domain need not match its
   * operand's type at all.  Handing the tuple writer a value whose type
   * disagrees with the domain it will be sized by is memory corruption, so
   * any residual mismatch is coerced through the engine's own conversion.
   */
  if (domain != NULL && dt != DB_TYPE_NULL && DB_VALUE_DOMAIN_TYPE (out) != dt)
    {
      DB_VALUE tmp;

      tmp = *out;
      db_make_null (out);
      if (tp_value_coerce (&tmp, out, domain) != DOMAIN_COMPATIBLE)
	{
	  pr_clear_value (&tmp);
	  db_make_null (out);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IT_DATA_OVERFLOW, 1, "numeric");
	  return ER_IT_DATA_OVERFLOW;
	}
      pr_clear_value (&tmp);
    }

  return NO_ERROR;
}

/* ========================================================================== */
/* Teardown / trace                                                           */
/* ========================================================================== */

void
columnar_rawprog_free (RAW_PROG * prog)
{
  if (prog == NULL)
    {
      return;
    }
  free (prog->steps);
  free (prog->cells);
  free (prog->nulls);
  free (prog->cell_types);
  free (prog->scratch);
  free (prog->root_cells);
  free (prog);
}

static const char *
raw_kernel_name (RAW_KERNEL_FN k)
{
  if (k == raw_leaf_i16)
    return "leaf_i16";
  if (k == raw_leaf_i32)
    return "leaf_i32";
  if (k == raw_leaf_i64)
    return "leaf_i64";
  if (k == raw_leaf_u32)
    return "leaf_u32";
  if (k == raw_leaf_dt)
    return "leaf_datetime";
  if (k == raw_leaf_f32)
    return "leaf_f32";
  if (k == raw_leaf_f64)
    return "leaf_f64";
  if (k == raw_leaf_num)
    return "leaf_numeric";
  if (k == raw_leaf_charn)
    return "leaf_char";
  if (k == raw_leaf_varchar)
    return "leaf_varchar";
  if (k == raw_add_i64)
    return "add_i64";
  if (k == raw_sub_i64)
    return "sub_i64";
  if (k == raw_mul_i64)
    return "mul_i64";
  if (k == raw_div_i64)
    return "div_i64";
  if (k == raw_add_dbl)
    return "add_dbl";
  if (k == raw_sub_dbl)
    return "sub_dbl";
  if (k == raw_mul_dbl)
    return "mul_dbl";
  if (k == raw_div_dbl)
    return "div_dbl";
  if (k == raw_add_num)
    return "add_num";
  if (k == raw_sub_num)
    return "sub_num";
  if (k == raw_mul_num)
    return "mul_num";
  if (k == raw_div_num)
    return "div_num";
  if (k == raw_cast_i64_to_dbl)
    return "cast_i64_dbl";
  if (k == raw_cast_i64_to_num)
    return "cast_i64_num";
  if (k == raw_cast_num_to_dbl)
    return "cast_num_dbl";
  return "?";
}

void
columnar_rawprog_dump (const RAW_PROG * prog, FILE * fp, int indent)
{
  int i;

  fprintf (fp, "%*sRAW_PROG: %d steps, %d cells, %d numeric scratch, %d roots\n", indent, "", prog->n_steps,
	   prog->n_cells, prog->n_scratch, prog->n_roots);
  for (i = 0; i < prog->n_steps; i++)
    {
      const RAW_STEP *s = &prog->steps[i];

      fprintf (fp, "%*s[%2d] %-14s a1=%d a2=%d -> c%d", indent + 2, "", i, raw_kernel_name (s->kernel), s->a1, s->a2,
	       s->out);
      if (s->is_leaf)
	{
	  fprintf (fp, " bind=%d stride=%d", s->bind_idx, s->stride);
	}
      fprintf (fp, "\n");
    }
  for (i = 0; i < prog->n_roots; i++)
    {
      fprintf (fp, "%*sroot[%d] -> c%d (type %d)\n", indent + 2, "", i, prog->root_cells[i],
	       (int) prog->cell_types[prog->root_cells[i]]);
    }
}
