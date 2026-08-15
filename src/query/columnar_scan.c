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
 * columnar_scan.c - columnar storage block executor (read path)
 *
 * Execution model (design ticket #12):
 *   - stripe directory snapshot taken once at open (single metapage latch),
 *     MVCCID visibility decided per stripe from the directory entry
 *   - same-transaction reads flush the pending write state first, so the
 *     transaction's own stripes are naturally visible
 *   - the WHERE predicate is compiled once at open into a tree of vectorized
 *     filter kernels; each chunk group is evaluated raw-array -> uint64
 *     bitmap with Kleene 3-valued logic (no DB_VALUE per row)
 *   - chunk groups provably empty under the top-level AND leaves are skipped
 *     through the stripe footer min/max entries before any data page read
 *   - surviving rows are decoded straight into the val_list slots that the
 *     access spec's TYPE_ATTR_ID regu variables point at (vfetch_to), through
 *     a per-column decode function chosen once at open.  Everything above the
 *     slots (projection, expressions, aggregation - including the compiled
 *     step programs of expr_compile.c) runs unchanged.
 *   - zero fallback: a predicate node outside the kernel coverage raises
 *     ER_COLUMNAR_UNSUPPORTED_EXPR at open (first execution) time
 */

#ident "$Id$"

#include "columnar_scan.h"

#include "columnar_file.h"
#include "columnar_numeric.h"
#include "columnar_writer.h"
#include "dbtype.h"
#include "error_manager.h"
#include "fetch.h"
#include "heap_file.h"
#include "language_support.h"
#include "log_impl.h"
#include "mvcc.h"
#include "object_domain.h"
#include "object_primitive.h"
#include "page_buffer.h"
#include "query_evaluator.h"
#include "regu_var.hpp"
#include "set_object.h"
#include "xasl_predicate.hpp"

#include "lz4.h"
#if defined (HAVE_ZSTD)
#include "zstd.h"
#endif

#include <assert.h>
#include <string.h>

/* ========================================================================== */
/* Filter plan                                                                */
/* ========================================================================== */

typedef enum
{
  COL_F_AND,
  COL_F_OR,
  COL_F_NOT,
  COL_F_CMP,			/* column op constant */
  COL_F_CMPCOL,			/* column op column */
  COL_F_ISNULL,			/* column IS NULL */
  COL_F_IN,			/* column IN (constant list) */
  COL_F_LIKE			/* column LIKE constant pattern */
} COL_FILTER_KIND;

/* type-specialized kernel class, chosen once at filter build */
typedef enum
{
  COL_KRN_NONE = 0,
  COL_KRN_I16,			/* SHORT */
  COL_KRN_I32,			/* INTEGER */
  COL_KRN_I64,			/* BIGINT */
  COL_KRN_U32,			/* DATE / TIME / TIMESTAMP */
  COL_KRN_DT,			/* DATETIME: (date << 32) | time */
  COL_KRN_F32,			/* FLOAT */
  COL_KRN_F64,			/* DOUBLE / MONETARY (leading double, runtime stride) */
  COL_KRN_NUM,			/* NUMERIC: PG base-10000 variable-width stream */
  COL_KRN_CHARN,		/* CHAR(n): fixed n-byte memcmp (binary collation) */
  COL_KRN_VARCHAR		/* VARCHAR: length-prefixed stream (binary collation) */
} COL_KERNEL;

typedef struct col_filter_node COL_FILTER_NODE;
struct col_filter_node
{
  COL_FILTER_KIND kind;
  COL_FILTER_NODE *left;	/* AND/OR/NOT child */
  COL_FILTER_NODE *right;	/* AND/OR child */

  /* leaf */
  int bind_idx;			/* left operand column (binding index) */
  int bind_idx2;		/* COL_F_CMPCOL right column */
  REL_OP op;			/* R_EQ .. R_LE */
  COL_KERNEL kernel;
  bool const_is_null;		/* constant operand is SQL NULL -> all-UNKNOWN */

  /* prepared constant (coerced to the column domain at build) */
  DB_VALUE cval;		/* owned; cleared at close */
  INT64 ci64;			/* canonical integer form */
  double cdbl;			/* canonical double form */
  COL_NUMVAR cnum;		/* NUMERIC, coerced to the column domain */
  char *cstr;			/* CHAR padded to n / VARCHAR bytes / LIKE pattern */
  int cstr_len;

  /* IN list */
  INT64 *in_i64;		/* canonical integer elements */
  double *in_dbl;		/* canonical double elements */
  char *in_str;			/* concatenated fixed-width CHAR elements */
  COL_NUMVAR *in_num;		/* NUMERIC elements */
  int n_in;
  bool in_has_null;		/* IN list contains NULL -> UNKNOWN when no match */

  /* min/max skip capability (COL_F_CMP under top-level AND only) */
  bool skip_capable;
};

/* ========================================================================== */
/* Column binding                                                             */
/* ========================================================================== */

typedef struct col_binding COL_BINDING;

typedef void (*COL_DECODE_FN) (const COL_BINDING * b, int row, DB_VALUE * out);

struct col_binding
{
  /* --- hot: touched for every decoded row --- */
  DB_VALUE *slot;		/* val_list slot (regu vfetch_to) */
  COL_DECODE_FN decode;		/* chosen once at open */
  const char *chunk_data;	/* decompressed data of the current chunk */
  const char *chunk_exists;	/* exists bitmap of the current chunk */
  int value_size;		/* stride; -1 = variable */
  int *var_offsets;		/* variable width: per-row byte offset (-1 = NULL) */

  /* --- cold: chunk-load / open time --- */
  int col_idx;			/* storage column position */
  ATTR_ID attr_id;
  TP_DOMAIN *domain;
  DB_TYPE type;
  bool used_in_filter;
  bool stamped;			/* slot type tag stamped once at open (#21 Tier 3d):
				 * decode writes only the value bytes + is_null */

  char *data_buf;		/* owned decompress target */
  int data_buf_alloc;
  char *exists_buf;		/* owned exists copy */
  int exists_buf_alloc;
  char *comp_buf;		/* owned compressed-read scratch */
  int comp_buf_alloc;
  int var_offsets_alloc;	/* rows */
};

/* ========================================================================== */
/* Scan state                                                                 */
/* ========================================================================== */

struct columnar_scan
{
  /* ---- cache line 0: per-row hot path ---- */
  int row_in_chunk;		/* cursor within current chunk */
  int chunk_rows;		/* rows in current chunk */
  UINT64 *bitmap;		/* qualified-row bitmap of current chunk */
  COL_BINDING *bindings;
  int n_bindings;
  int n_words;			/* bitmap words for current chunk */
  bool count_only;		/* no bindings, no filter: emit empty rows */

  /* ---- warm: chunk / stripe cursor ---- */
  int chunk_idx;		/* current chunk group in stripe */
  int n_chunks;			/* chunk groups in current stripe */
  int stripe_idx;		/* current stripe in directory snapshot */
  int n_stripes;
  int chunk_row_count;		/* rows per full chunk group (footer) */
  int stripe_rows;		/* rows in current stripe */
  int count_rows_left;		/* count_only: rows left to emit in stripe */
  bool stripe_opened;		/* footer of dir[stripe_idx] loaded */

  /* ---- cold ---- */
  COLUMNAR_STRIPE_DIR_ENTRY *dir;	/* directory snapshot (visible entries) */
  COLUMNAR_CHUNK_DESC *descs;	/* footer descs of current stripe */
  int descs_alloc;		/* bytes */
  int footer_n_columns;

  VPID *stripe_vpids;		/* stripe page map snapshot (from stripe page 0) */
  int stripe_vpids_count;	/* page_count from the map header */
  int stripe_vpids_alloc;	/* allocated entries */

  COL_FILTER_NODE *filter;	/* NULL = no WHERE */
  UINT64 **bm_pool;		/* eval scratch: 2 bitmaps per tree depth */
  int bm_pool_n;		/* number of bitmaps in pool */
  int bm_words_alloc;		/* words per pool bitmap */

  HFID hfid;
  OID class_oid;
  struct val_descr *vd;

  COL_SCAN_STATS stats;
};

#define COL_EXISTS_BIT(ex, r)  (((ex)[(r) >> 3] >> ((r) & 7)) & 1)
#define COL_BM_SET(bm, r)      ((bm)[(r) >> 6] |= (1ULL << ((r) & 63)))
#define COL_BM_GET(bm, r)      (((bm)[(r) >> 6] >> ((r) & 63)) & 1)

/* ========================================================================== */
/* Forward declarations                                                       */
/* ========================================================================== */
static int col_build_bindings (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, ACCESS_SPEC_TYPE * spec);
static int col_build_filter (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, const PRED_EXPR * pred,
			     COL_FILTER_NODE ** out, bool under_and);
static int col_build_comp (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, const COMP_EVAL_TERM * comp,
			   COL_FILTER_NODE ** out, bool under_and);
static int col_build_comp_norm (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, REGU_VARIABLE * col_regu,
				REGU_VARIABLE * const_regu, REL_OP op, int lbind, COL_FILTER_NODE ** out,
				bool under_and);
static void col_free_filter (COL_FILTER_NODE * f);
static int col_filter_depth (const COL_FILTER_NODE * f);
static void col_eval_filter (COLUMNAR_SCAN * cs, const COL_FILTER_NODE * f, int n_rows, int depth,
			     UINT64 * t_bm, UINT64 * n_bm);
static int col_snapshot_directory (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs);
static int col_read_range (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, const COLUMNAR_STRIPE_DIR_ENTRY * ent,
			   INT64 offset, int length, char *dst);
static int col_open_stripe (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs);
static int col_load_chunk (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, bool * skipped);
static bool col_chunk_skippable (const COLUMNAR_SCAN * cs, const COL_FILTER_NODE * f, int chunk);
static int col_unsupported (const char *what);

/* ========================================================================== */
/* Error helpers                                                              */
/* ========================================================================== */
static int
col_unsupported (const char *what)
{
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_COLUMNAR_UNSUPPORTED_EXPR, 1, what);
  return ER_COLUMNAR_UNSUPPORTED_EXPR;
}

static bool
col_is_binary_collation (int coll_id)
{
  return (coll_id == LANG_COLL_ISO_BINARY || coll_id == LANG_COLL_UTF8_BINARY
	  || coll_id == LANG_COLL_EUCKR_BINARY || coll_id == LANG_COLL_BINARY);
}

/* ========================================================================== */
/* Decode functions (per-type; chosen once at open)                           */
/* ========================================================================== */

#define COL_CELL(b, row)  ((b)->chunk_data + (size_t) (row) * (b)->value_size)

static void
col_decode_int (const COL_BINDING * b, int row, DB_VALUE * out)
{
  int v;
  memcpy (&v, COL_CELL (b, row), sizeof (int));
  db_make_int (out, v);
}

static void
col_decode_short (const COL_BINDING * b, int row, DB_VALUE * out)
{
  short v;
  memcpy (&v, COL_CELL (b, row), sizeof (short));
  db_make_short (out, v);
}

static void
col_decode_bigint (const COL_BINDING * b, int row, DB_VALUE * out)
{
  INT64 v;
  memcpy (&v, COL_CELL (b, row), sizeof (INT64));
  db_make_bigint (out, v);
}

static void
col_decode_float (const COL_BINDING * b, int row, DB_VALUE * out)
{
  float v;
  memcpy (&v, COL_CELL (b, row), sizeof (float));
  db_make_float (out, v);
}

static void
col_decode_double (const COL_BINDING * b, int row, DB_VALUE * out)
{
  double v;
  memcpy (&v, COL_CELL (b, row), sizeof (double));
  db_make_double (out, v);
}

static void
col_decode_monetary (const COL_BINDING * b, int row, DB_VALUE * out)
{
  const char *cell = COL_CELL (b, row);
  double amount;
  int type;
  memcpy (&amount, cell, sizeof (double));
  memcpy (&type, cell + sizeof (double), sizeof (int));
  db_make_monetary (out, (DB_CURRENCY) type, amount);
}

static void
col_decode_date (const COL_BINDING * b, int row, DB_VALUE * out)
{
  DB_DATE v;
  memcpy (&v, COL_CELL (b, row), sizeof (DB_DATE));
  db_value_put_encoded_date (out, &v);
}

static void
col_decode_time (const COL_BINDING * b, int row, DB_VALUE * out)
{
  DB_TIME v;
  memcpy (&v, COL_CELL (b, row), sizeof (DB_TIME));
  db_value_put_encoded_time (out, &v);
}

static void
col_decode_timestamp (const COL_BINDING * b, int row, DB_VALUE * out)
{
  DB_TIMESTAMP v;
  memcpy (&v, COL_CELL (b, row), sizeof (DB_TIMESTAMP));
  db_make_timestamp (out, v);
}

static void
col_decode_datetime (const COL_BINDING * b, int row, DB_VALUE * out)
{
  const char *cell = COL_CELL (b, row);
  DB_DATETIME dt;
  memcpy (&dt.date, cell, sizeof (unsigned int));
  memcpy (&dt.time, cell + sizeof (unsigned int), sizeof (unsigned int));
  db_make_datetime (out, &dt);
}

/* NUMERIC is stored in the PG base-10000 stream (#23 D11), so materializing a
 * DB_VALUE converts back into the engine representation.  Only the legacy
 * per-row path needs this; the raw path computes on the stored digits. */
static void
col_decode_numeric (const COL_BINDING * b, int row, DB_VALUE * out)
{
  COL_NUMREF ref;
  COL_NUMVAR nv;
  int off = b->var_offsets[row];

  assert (off >= 0);
  columnar_num_ref (b->chunk_data + off + (int) sizeof (int), &ref);
  columnar_num_from_ref (&ref, &nv);
  if (columnar_num_to_dbvalue (&nv, b->domain->precision, b->domain->scale, out) != NO_ERROR)
    {
      db_make_null (out);
    }
}

static void
col_decode_char (const COL_BINDING * b, int row, DB_VALUE * out)
{
  /* peek into the chunk buffer; the tuple/aggregate machinery copies the
   * bytes before the next chunk is loaded */
  db_make_char (out, b->domain->precision, COL_CELL (b, row), b->value_size,
		TP_DOMAIN_CODESET (b->domain), TP_DOMAIN_COLLATION (b->domain));
}

static void
col_decode_bit (const COL_BINDING * b, int row, DB_VALUE * out)
{
  db_make_bit (out, b->domain->precision, COL_CELL (b, row), b->domain->precision);
}

static void
col_decode_varchar (const COL_BINDING * b, int row, DB_VALUE * out)
{
  int off = b->var_offsets[row];
  int len;
  assert (off >= 0);
  memcpy (&len, b->chunk_data + off, sizeof (int));
  db_make_varchar (out, b->domain->precision, b->chunk_data + off + (int) sizeof (int), len,
		   TP_DOMAIN_CODESET (b->domain), TP_DOMAIN_COLLATION (b->domain));
}

static void
col_decode_varbit (const COL_BINDING * b, int row, DB_VALUE * out)
{
  int off = b->var_offsets[row];
  int len;
  assert (off >= 0);
  memcpy (&len, b->chunk_data + off, sizeof (int));
  db_make_varbit (out, b->domain->precision, b->chunk_data + off + (int) sizeof (int), len * 8);
}

static COL_DECODE_FN
col_select_decode_fn (DB_TYPE type)
{
  switch (type)
    {
    case DB_TYPE_INTEGER:
      return col_decode_int;
    case DB_TYPE_SHORT:
      return col_decode_short;
    case DB_TYPE_BIGINT:
      return col_decode_bigint;
    case DB_TYPE_FLOAT:
      return col_decode_float;
    case DB_TYPE_DOUBLE:
      return col_decode_double;
    case DB_TYPE_MONETARY:
      return col_decode_monetary;
    case DB_TYPE_DATE:
      return col_decode_date;
    case DB_TYPE_TIME:
      return col_decode_time;
    case DB_TYPE_TIMESTAMP:
      return col_decode_timestamp;
    case DB_TYPE_DATETIME:
      return col_decode_datetime;
    case DB_TYPE_NUMERIC:
      return col_decode_numeric;
    case DB_TYPE_CHAR:
      return col_decode_char;
    case DB_TYPE_BIT:
      return col_decode_bit;
    case DB_TYPE_VARCHAR:
      return col_decode_varchar;
    case DB_TYPE_VARBIT:
      return col_decode_varbit;
    default:
      return NULL;
    }
}

/* ========================================================================== */
/* Batch-type-stamped decode (#21 Tier 3d)                                    */
/* ========================================================================== */
/*
 * For fixed-width types whose DB_VALUE header never changes across rows, the
 * slot's type tag / need_clear are stamped ONCE at scan open (db_make_* with a
 * zero value); the per-row decode then writes only the value bytes and the
 * is_null flag.  NULL rows flip is_null without touching the type tag, so the
 * stamp survives the whole scan.  Types whose header varies per value
 * (MONETARY currency, NUMERIC precision/scale, CHAR/VARCHAR pointers) keep
 * the full db_make_* decode.
 */

#define COL_DEF_FAST_DECODE(NAME, DBTYPE, FIELD, CTYPE)                       \
  static void                                                                 \
  NAME (const COL_BINDING * b, int row, DB_VALUE * out)                       \
  {                                                                           \
    assert (out->domain.general_info.type == (DBTYPE) && !out->need_clear);   \
    memcpy (&out->data.FIELD, COL_CELL (b, row), sizeof (CTYPE));             \
    out->domain.general_info.is_null = 0;                                     \
  }

/* *INDENT-OFF* */
COL_DEF_FAST_DECODE (col_decode_int_fast, DB_TYPE_INTEGER, i, int)
COL_DEF_FAST_DECODE (col_decode_short_fast, DB_TYPE_SHORT, sh, short)
COL_DEF_FAST_DECODE (col_decode_bigint_fast, DB_TYPE_BIGINT, bigint, INT64)
COL_DEF_FAST_DECODE (col_decode_float_fast, DB_TYPE_FLOAT, f, float)
COL_DEF_FAST_DECODE (col_decode_double_fast, DB_TYPE_DOUBLE, d, double)
COL_DEF_FAST_DECODE (col_decode_date_fast, DB_TYPE_DATE, date, DB_DATE)
COL_DEF_FAST_DECODE (col_decode_time_fast, DB_TYPE_TIME, time, DB_TIME)
COL_DEF_FAST_DECODE (col_decode_timestamp_fast, DB_TYPE_TIMESTAMP, utime, DB_TIMESTAMP)
/* *INDENT-ON* */

static void
col_decode_datetime_fast (const COL_BINDING * b, int row, DB_VALUE * out)
{
  const char *cell = COL_CELL (b, row);
  assert (out->domain.general_info.type == DB_TYPE_DATETIME && !out->need_clear);
  memcpy (&out->data.datetime.date, cell, sizeof (unsigned int));
  memcpy (&out->data.datetime.time, cell + sizeof (unsigned int), sizeof (unsigned int));
  out->domain.general_info.is_null = 0;
}

/* stamp the slot header and switch the binding to the fast decode; no-op for
 * types whose header varies per value */
static void
col_stamp_slot (COL_BINDING * b)
{
  COL_DECODE_FN fast = NULL;

  switch (b->type)
    {
    case DB_TYPE_INTEGER:
      db_make_int (b->slot, 0);
      fast = col_decode_int_fast;
      break;
    case DB_TYPE_SHORT:
      db_make_short (b->slot, 0);
      fast = col_decode_short_fast;
      break;
    case DB_TYPE_BIGINT:
      db_make_bigint (b->slot, 0);
      fast = col_decode_bigint_fast;
      break;
    case DB_TYPE_FLOAT:
      db_make_float (b->slot, 0.0f);
      fast = col_decode_float_fast;
      break;
    case DB_TYPE_DOUBLE:
      db_make_double (b->slot, 0.0);
      fast = col_decode_double_fast;
      break;
    case DB_TYPE_DATE:
      {
	DB_DATE d = 0;
	db_value_put_encoded_date (b->slot, &d);
	fast = col_decode_date_fast;
      }
      break;
    case DB_TYPE_TIME:
      {
	DB_TIME t = 0;
	db_value_put_encoded_time (b->slot, &t);
	fast = col_decode_time_fast;
      }
      break;
    case DB_TYPE_TIMESTAMP:
      db_make_timestamp (b->slot, 0);
      fast = col_decode_timestamp_fast;
      break;
    case DB_TYPE_DATETIME:
      {
	DB_DATETIME dt = { 0, 0 };
	db_make_datetime (b->slot, &dt);
	fast = col_decode_datetime_fast;
      }
      break;
    default:
      return;
    }

  b->slot->domain.general_info.is_null = 1;	/* no row decoded yet */
  b->decode = fast;
  b->stamped = true;
}

/* NULL row: a stamped slot only flips is_null (the type tag must survive) */
static void
col_slot_set_null (COL_BINDING * b)
{
  if (b->stamped)
    {
      b->slot->domain.general_info.is_null = 1;
    }
  else
    {
      db_make_null (b->slot);
    }
}

/* ========================================================================== */
/* Compare kernels: raw array op constant -> bitmap                           */
/* ========================================================================== */
/*
 * Each kernel walks the raw column array with a compile-time element type,
 * so the row loop body is branch-free on type and the compiler can
 * vectorize.  NULL rows never set a bit (t_bm is the definite-TRUE bitmap).
 */

#define COL_DEF_CMP_KERNEL(NAME, CTYPE, LOAD)                                 \
  static void                                                                 \
  NAME (const char *data, int stride, const char *ex, int n, REL_OP op,       \
        CTYPE cval, UINT64 *t_bm)                                             \
  {                                                                           \
    int r;                                                                    \
    switch (op)                                                               \
      {                                                                       \
      case R_EQ:                                                              \
        for (r = 0; r < n; r++)                                               \
          { CTYPE v; LOAD; if (COL_EXISTS_BIT (ex, r) && v == cval) COL_BM_SET (t_bm, r); } \
        break;                                                                \
      case R_NE:                                                              \
        for (r = 0; r < n; r++)                                               \
          { CTYPE v; LOAD; if (COL_EXISTS_BIT (ex, r) && v != cval) COL_BM_SET (t_bm, r); } \
        break;                                                                \
      case R_GT:                                                              \
        for (r = 0; r < n; r++)                                               \
          { CTYPE v; LOAD; if (COL_EXISTS_BIT (ex, r) && v > cval) COL_BM_SET (t_bm, r); }  \
        break;                                                                \
      case R_GE:                                                              \
        for (r = 0; r < n; r++)                                               \
          { CTYPE v; LOAD; if (COL_EXISTS_BIT (ex, r) && v >= cval) COL_BM_SET (t_bm, r); } \
        break;                                                                \
      case R_LT:                                                              \
        for (r = 0; r < n; r++)                                               \
          { CTYPE v; LOAD; if (COL_EXISTS_BIT (ex, r) && v < cval) COL_BM_SET (t_bm, r); }  \
        break;                                                                \
      case R_LE:                                                              \
        for (r = 0; r < n; r++)                                               \
          { CTYPE v; LOAD; if (COL_EXISTS_BIT (ex, r) && v <= cval) COL_BM_SET (t_bm, r); } \
        break;                                                                \
      default:                                                                \
        assert (false);                                                       \
        break;                                                                \
      }                                                                       \
  }

/* *INDENT-OFF* */
COL_DEF_CMP_KERNEL (col_cmp_i16, INT64, { short t; memcpy (&t, data + (size_t) r * stride, sizeof (t)); v = t; })
COL_DEF_CMP_KERNEL (col_cmp_i32, INT64, { int t; memcpy (&t, data + (size_t) r * stride, sizeof (t)); v = t; })
COL_DEF_CMP_KERNEL (col_cmp_i64, INT64, { memcpy (&v, data + (size_t) r * stride, sizeof (v)); })
COL_DEF_CMP_KERNEL (col_cmp_u32, INT64, { unsigned int t; memcpy (&t, data + (size_t) r * stride, sizeof (t)); v = (INT64) t; })
static void
col_cmp_dt (const char *data, int stride, const char *ex, int n, REL_OP op, INT64 cval, UINT64 *t_bm)
{
  int r;
  switch (op)
    {
    case R_EQ:
      for (r = 0; r < n; r++)
	{ unsigned int d2, t2; INT64 v; memcpy (&d2, data + (size_t) r * stride, 4); memcpy (&t2, data + (size_t) r * stride + 4, 4); v = ((INT64) d2 << 32) | (INT64) t2; if (COL_EXISTS_BIT (ex, r) && v == cval) COL_BM_SET (t_bm, r); }
      break;
    case R_NE:
      for (r = 0; r < n; r++)
	{ unsigned int d2, t2; INT64 v; memcpy (&d2, data + (size_t) r * stride, 4); memcpy (&t2, data + (size_t) r * stride + 4, 4); v = ((INT64) d2 << 32) | (INT64) t2; if (COL_EXISTS_BIT (ex, r) && v != cval) COL_BM_SET (t_bm, r); }
      break;
    case R_GT:
      for (r = 0; r < n; r++)
	{ unsigned int d2, t2; INT64 v; memcpy (&d2, data + (size_t) r * stride, 4); memcpy (&t2, data + (size_t) r * stride + 4, 4); v = ((INT64) d2 << 32) | (INT64) t2; if (COL_EXISTS_BIT (ex, r) && v > cval) COL_BM_SET (t_bm, r); }
      break;
    case R_GE:
      for (r = 0; r < n; r++)
	{ unsigned int d2, t2; INT64 v; memcpy (&d2, data + (size_t) r * stride, 4); memcpy (&t2, data + (size_t) r * stride + 4, 4); v = ((INT64) d2 << 32) | (INT64) t2; if (COL_EXISTS_BIT (ex, r) && v >= cval) COL_BM_SET (t_bm, r); }
      break;
    case R_LT:
      for (r = 0; r < n; r++)
	{ unsigned int d2, t2; INT64 v; memcpy (&d2, data + (size_t) r * stride, 4); memcpy (&t2, data + (size_t) r * stride + 4, 4); v = ((INT64) d2 << 32) | (INT64) t2; if (COL_EXISTS_BIT (ex, r) && v < cval) COL_BM_SET (t_bm, r); }
      break;
    case R_LE:
      for (r = 0; r < n; r++)
	{ unsigned int d2, t2; INT64 v; memcpy (&d2, data + (size_t) r * stride, 4); memcpy (&t2, data + (size_t) r * stride + 4, 4); v = ((INT64) d2 << 32) | (INT64) t2; if (COL_EXISTS_BIT (ex, r) && v <= cval) COL_BM_SET (t_bm, r); }
      break;
    default:
      assert (false);
      break;
    }
}
static void
col_cmp_f32 (const char *data, int stride, const char *ex, int n, REL_OP op, double cval, UINT64 *t_bm)
{
  int r;
  switch (op)
    {
    case R_EQ:
      for (r = 0; r < n; r++)
	{ float t; double v; memcpy (&t, data + (size_t) r * stride, sizeof (t)); v = (double) t; if (COL_EXISTS_BIT (ex, r) && v == cval) COL_BM_SET (t_bm, r); }
      break;
    case R_NE:
      for (r = 0; r < n; r++)
	{ float t; double v; memcpy (&t, data + (size_t) r * stride, sizeof (t)); v = (double) t; if (COL_EXISTS_BIT (ex, r) && v != cval) COL_BM_SET (t_bm, r); }
      break;
    case R_GT:
      for (r = 0; r < n; r++)
	{ float t; double v; memcpy (&t, data + (size_t) r * stride, sizeof (t)); v = (double) t; if (COL_EXISTS_BIT (ex, r) && v > cval) COL_BM_SET (t_bm, r); }
      break;
    case R_GE:
      for (r = 0; r < n; r++)
	{ float t; double v; memcpy (&t, data + (size_t) r * stride, sizeof (t)); v = (double) t; if (COL_EXISTS_BIT (ex, r) && v >= cval) COL_BM_SET (t_bm, r); }
      break;
    case R_LT:
      for (r = 0; r < n; r++)
	{ float t; double v; memcpy (&t, data + (size_t) r * stride, sizeof (t)); v = (double) t; if (COL_EXISTS_BIT (ex, r) && v < cval) COL_BM_SET (t_bm, r); }
      break;
    case R_LE:
      for (r = 0; r < n; r++)
	{ float t; double v; memcpy (&t, data + (size_t) r * stride, sizeof (t)); v = (double) t; if (COL_EXISTS_BIT (ex, r) && v <= cval) COL_BM_SET (t_bm, r); }
      break;
    default:
      assert (false);
      break;
    }
}
COL_DEF_CMP_KERNEL (col_cmp_f64, double, { memcpy (&v, data + (size_t) r * stride, sizeof (v)); })
/* *INDENT-ON* */

/* view the NUMERIC of a row inside the variable-width stream */
#define COL_NUM_AT(data, offsets, r, ref) \
  columnar_num_ref ((data) + (offsets)[r] + (int) sizeof (int), (ref))

static bool
col_rel_match (REL_OP op, int c)
{
  switch (op)
    {
    case R_EQ:
      return c == 0;
    case R_NE:
      return c != 0;
    case R_GT:
      return c > 0;
    case R_GE:
      return c >= 0;
    case R_LT:
      return c < 0;
    case R_LE:
      return c <= 0;
    default:
      assert (false);
      return false;
    }
}

static void
col_cmp_num (const char *data, const int *offsets, const char *ex, int n, REL_OP op, const COL_NUMVAR * cval,
	     UINT64 * t_bm)
{
  COL_NUMREF cref;
  int r;

  columnar_num_ref_var (cval, &cref);
  for (r = 0; r < n; r++)
    {
      COL_NUMREF v;

      if (!COL_EXISTS_BIT (ex, r))
	{
	  continue;
	}
      COL_NUM_AT (data, offsets, r, &v);
      if (col_rel_match (op, columnar_num_cmp (&v, &cref)))
	{
	  COL_BM_SET (t_bm, r);
	}
    }
}

static void
col_cmp_charn (const char *data, int stride, const char *ex, int n, REL_OP op, const char *cval, UINT64 * t_bm)
{
  int r;
  for (r = 0; r < n; r++)
    {
      if (COL_EXISTS_BIT (ex, r) && col_rel_match (op, memcmp (data + (size_t) r * stride, cval, stride)))
	{
	  COL_BM_SET (t_bm, r);
	}
    }
}

/* binary-collation compare of two byte strings with length tie-break */
static int
col_str_cmp (const char *a, int alen, const char *b, int blen)
{
  int m = (alen < blen) ? alen : blen;
  int c = (m > 0) ? memcmp (a, b, m) : 0;
  if (c != 0)
    {
      return c;
    }
  return (alen > blen) ? 1 : ((alen < blen) ? -1 : 0);
}

static void
col_cmp_varchar (const char *data, const int *offsets, const char *ex, int n, REL_OP op, const char *cval,
		 int cval_len, UINT64 * t_bm)
{
  int r, len;
  for (r = 0; r < n; r++)
    {
      if (!COL_EXISTS_BIT (ex, r))
	{
	  continue;
	}
      memcpy (&len, data + offsets[r], sizeof (int));
      if (col_rel_match (op, col_str_cmp (data + offsets[r] + (int) sizeof (int), len, cval, cval_len)))
	{
	  COL_BM_SET (t_bm, r);
	}
    }
}

/* column op column: canonical INT64/double load per row */
static INT64
col_load_i64 (COL_KERNEL k, const char *cell)
{
  switch (k)
    {
    case COL_KRN_I16:
      {
	short t;
	memcpy (&t, cell, sizeof (t));
	return t;
      }
    case COL_KRN_I32:
      {
	int t;
	memcpy (&t, cell, sizeof (t));
	return t;
      }
    case COL_KRN_I64:
      {
	INT64 t;
	memcpy (&t, cell, sizeof (t));
	return t;
      }
    case COL_KRN_U32:
      {
	unsigned int t;
	memcpy (&t, cell, sizeof (t));
	return (INT64) t;
      }
    case COL_KRN_DT:
    default:
      {
	unsigned int d, t2;
	memcpy (&d, cell, 4);
	memcpy (&t2, cell + 4, 4);
	return ((INT64) d << 32) | (INT64) t2;
      }
    }
}

static double
col_load_dbl (COL_KERNEL k, const char *cell)
{
  if (k == COL_KRN_F32)
    {
      float t;
      memcpy (&t, cell, sizeof (t));
      return (double) t;
    }
  else
    {
      double t;
      memcpy (&t, cell, sizeof (t));
      return t;
    }
}

/* ========================================================================== */
/* LIKE byte matcher (binary collation)                                       */
/* ========================================================================== */

/* general %/_ wildcard matcher over raw bytes (greedy with backtrack) */
static bool
col_like_match (const char *s, int slen, const char *p, int plen)
{
  int si = 0, pi = 0, star_p = -1, star_s = 0;

  while (si < slen)
    {
      if (pi < plen && (p[pi] == s[si] || p[pi] == '_'))
	{
	  si++;
	  pi++;
	}
      else if (pi < plen && p[pi] == '%')
	{
	  star_p = pi++;
	  star_s = si;
	}
      else if (star_p >= 0)
	{
	  pi = star_p + 1;
	  si = ++star_s;
	}
      else
	{
	  return false;
	}
    }
  while (pi < plen && p[pi] == '%')
    {
      pi++;
    }
  return pi == plen;
}

static void
col_like_charn (const char *data, int stride, const char *ex, int n, const char *pat, int plen, UINT64 * t_bm)
{
  int r;
  for (r = 0; r < n; r++)
    {
      if (COL_EXISTS_BIT (ex, r) && col_like_match (data + (size_t) r * stride, stride, pat, plen))
	{
	  COL_BM_SET (t_bm, r);
	}
    }
}

static void
col_like_varchar (const char *data, const int *offsets, const char *ex, int n, const char *pat, int plen,
		  UINT64 * t_bm)
{
  int r, len;
  for (r = 0; r < n; r++)
    {
      if (!COL_EXISTS_BIT (ex, r))
	{
	  continue;
	}
      memcpy (&len, data + offsets[r], sizeof (int));
      if (col_like_match (data + offsets[r] + (int) sizeof (int), len, pat, plen))
	{
	  COL_BM_SET (t_bm, r);
	}
    }
}

/* ========================================================================== */
/* Filter build                                                               */
/* ========================================================================== */

static COL_KERNEL
col_select_kernel (DB_TYPE type)
{
  switch (type)
    {
    case DB_TYPE_SHORT:
      return COL_KRN_I16;
    case DB_TYPE_INTEGER:
      return COL_KRN_I32;
    case DB_TYPE_BIGINT:
      return COL_KRN_I64;
    case DB_TYPE_DATE:
    case DB_TYPE_TIME:
    case DB_TYPE_TIMESTAMP:
      return COL_KRN_U32;
    case DB_TYPE_DATETIME:
      return COL_KRN_DT;
    case DB_TYPE_FLOAT:
      return COL_KRN_F32;
    case DB_TYPE_DOUBLE:
    case DB_TYPE_MONETARY:
      return COL_KRN_F64;
    case DB_TYPE_NUMERIC:
      return COL_KRN_NUM;
    case DB_TYPE_CHAR:
      return COL_KRN_CHARN;
    case DB_TYPE_VARCHAR:
      return COL_KRN_VARCHAR;
    default:
      return COL_KRN_NONE;
    }
}

/* canonical integer form of a coerced constant */
static INT64
col_const_to_i64 (const DB_VALUE * v, DB_TYPE type)
{
  switch (type)
    {
    case DB_TYPE_SHORT:
      return (INT64) db_get_short (v);
    case DB_TYPE_INTEGER:
      return (INT64) db_get_int (v);
    case DB_TYPE_BIGINT:
      return (INT64) db_get_bigint (v);
    case DB_TYPE_DATE:
      return (INT64) (*db_get_date (v));
    case DB_TYPE_TIME:
      return (INT64) (*db_get_time (v));
    case DB_TYPE_TIMESTAMP:
      return (INT64) (*db_get_timestamp (v));
    case DB_TYPE_DATETIME:
      {
	DB_DATETIME *dt = db_get_datetime (v);
	return ((INT64) dt->date << 32) | (INT64) dt->time;
      }
    default:
      assert (false);
      return 0;
    }
}

static double
col_const_to_dbl (const DB_VALUE * v, DB_TYPE type)
{
  switch (type)
    {
    case DB_TYPE_FLOAT:
      return (double) db_get_float (v);
    case DB_TYPE_DOUBLE:
      return db_get_double (v);
    case DB_TYPE_MONETARY:
      return db_get_monetary (v)->amount;
    default:
      assert (false);
      return 0.0;
    }
}

/* find the binding whose val_list slot a TYPE_CONSTANT regu points at */
/*
 * col_find_binding () - find the binding index whose val_list slot matches
 *   the regu variable's output.  The WHERE predicate's column references
 *   arrive as TYPE_ATTR_ID (pointing at vfetch_to), while the projection
 *   list's use TYPE_CONSTANT (pointing at dbvalptr).  Both point at the
 *   same val_list DB_VALUE slot that the binding owns.
 */
static int
col_find_binding (const COLUMNAR_SCAN * cs, const REGU_VARIABLE * regu)
{
  int i;
  DB_VALUE *target = NULL;

  if (regu == NULL)
    {
      return -1;
    }

  switch (regu->type)
    {
    case TYPE_CONSTANT:
      target = regu->value.dbvalptr;
      break;
    case TYPE_ATTR_ID:
      /* pred regu and cls_regu_list regu may have different vfetch_to
       * pointers but share the same attr_descr.id; try vfetch_to first,
       * fall back to attr id match */
      target = regu->vfetch_to;
      break;
    default:
      return -1;
    }

  /* try pointer match first (same val_list slot) */
  if (target != NULL)
    {
      for (i = 0; i < cs->n_bindings; i++)
	{
	  if (cs->bindings[i].slot == target)
	    {
	      return i;
	    }
	}
    }

  /* pointer didn't match; for TYPE_ATTR_ID fall back to attr id */
  if (regu->type == TYPE_ATTR_ID)
    {
      ATTR_ID aid = regu->value.attr_descr.id;
      for (i = 0; i < cs->n_bindings; i++)
	{
	  if (cs->bindings[i].attr_id == aid)
	    {
	      return i;
	    }
	}

    }

  return -1;
}

/* fetch a constant operand's current value (literal, host variable, outer
 * correlated value); once per open, never per row */
static int
col_fetch_const (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, REGU_VARIABLE * regu, DB_VALUE ** peek)
{
  switch (regu->type)
    {
    case TYPE_DBVAL:
    case TYPE_POS_VALUE:
    case TYPE_CONSTANT:
      return fetch_peek_dbval (thread_p, regu, cs->vd, NULL, NULL, NULL, peek);
    default:
      return col_unsupported ("non-constant predicate operand");
    }
}

static COL_FILTER_NODE *
col_alloc_fnode (COL_FILTER_KIND kind)
{
  COL_FILTER_NODE *f = (COL_FILTER_NODE *) calloc (1, sizeof (COL_FILTER_NODE));
  if (f == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (COL_FILTER_NODE));
      return NULL;
    }
  f->kind = kind;
  f->bind_idx = -1;
  f->bind_idx2 = -1;
  db_make_null (&f->cval);
  return f;
}

static void
col_free_filter (COL_FILTER_NODE * f)
{
  if (f == NULL)
    {
      return;
    }
  col_free_filter (f->left);
  col_free_filter (f->right);
  pr_clear_value (&f->cval);
  free (f->cstr);
  free (f->in_i64);
  free (f->in_dbl);
  free (f->in_str);
  free (f->in_num);
  free (f);
}

static int
col_filter_depth (const COL_FILTER_NODE * f)
{
  int dl, dr;
  if (f == NULL)
    {
      return 0;
    }
  dl = col_filter_depth (f->left);
  dr = col_filter_depth (f->right);
  return 1 + ((dl > dr) ? dl : dr);
}

/* prepare the constant side of a CMP leaf: coerce to the column domain and
 * precompute the canonical kernel form */
static int
col_prepare_const (COLUMNAR_SCAN * cs, COL_FILTER_NODE * f, const DB_VALUE * val)
{
  COL_BINDING *b = &cs->bindings[f->bind_idx];
  TP_DOMAIN_STATUS dstatus;

  if (DB_IS_NULL (val))
    {
      f->const_is_null = true;
      return NO_ERROR;
    }

  db_make_null (&f->cval);
  dstatus = tp_value_coerce (val, &f->cval, b->domain);
  if (dstatus != DOMAIN_COMPATIBLE)
    {
      return col_unsupported ("predicate constant not coercible to column domain");
    }

  switch (f->kernel)
    {
    case COL_KRN_I16:
    case COL_KRN_I32:
    case COL_KRN_I64:
    case COL_KRN_U32:
    case COL_KRN_DT:
      f->ci64 = col_const_to_i64 (&f->cval, b->type);
      break;
    case COL_KRN_F32:
    case COL_KRN_F64:
      f->cdbl = col_const_to_dbl (&f->cval, b->type);
      break;
    case COL_KRN_NUM:
      {
	int error = columnar_num_from_dbvalue (&f->cval, &f->cnum);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
      }
      break;
    case COL_KRN_CHARN:
      {
	int n = b->value_size;
	const char *s = db_get_string (&f->cval);
	int len = db_get_string_size (&f->cval);
	if (len > n)
	  {
	    len = n;
	  }
	f->cstr = (char *) malloc (n);
	if (f->cstr == NULL)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) n);
	    return ER_OUT_OF_VIRTUAL_MEMORY;
	  }
	if (s != NULL && len > 0)
	  {
	    memcpy (f->cstr, s, len);
	  }
	if (len < n)
	  {
	    memset (f->cstr + len, ' ', n - len);
	  }
	f->cstr_len = n;
      }
      break;
    case COL_KRN_VARCHAR:
      {
	const char *s = db_get_string (&f->cval);
	int len = db_get_string_size (&f->cval);
	f->cstr = (char *) malloc (len > 0 ? len : 1);
	if (f->cstr == NULL)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) (len > 0 ? len : 1));
	    return ER_OUT_OF_VIRTUAL_MEMORY;
	  }
	if (s != NULL && len > 0)
	  {
	    memcpy (f->cstr, s, len);
	  }
	f->cstr_len = len;
      }
      break;
    default:
      return col_unsupported ("column type not covered by vectorized filter");
    }
  return NO_ERROR;
}

/* normalized comparison leaf: col_regu op const_regu */
static int
col_build_comp_norm (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, REGU_VARIABLE * col_regu,
		     REGU_VARIABLE * const_regu, REL_OP op, int lbind, COL_FILTER_NODE ** out, bool under_and)
{
  COL_BINDING *b = &cs->bindings[lbind];
  COL_FILTER_NODE *f;
  DB_VALUE *peek = NULL;
  int error;

  (void) col_regu;

  f = col_alloc_fnode (COL_F_CMP);
  if (f == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  f->bind_idx = lbind;
  f->op = op;
  f->kernel = col_select_kernel (b->type);
  if (f->kernel == COL_KRN_NONE)
    {
      col_free_filter (f);
      return col_unsupported ("column type not covered by vectorized filter");
    }
  if ((b->type == DB_TYPE_CHAR || b->type == DB_TYPE_VARCHAR)
      && !col_is_binary_collation (TP_DOMAIN_COLLATION (b->domain)))
    {
      col_free_filter (f);
      return col_unsupported ("non-binary collation string comparison");
    }

  error = col_fetch_const (thread_p, cs, const_regu, &peek);
  if (error != NO_ERROR)
    {
      col_free_filter (f);
      return error;
    }
  error = col_prepare_const (cs, f, peek);
  if (error != NO_ERROR)
    {
      col_free_filter (f);
      return error;
    }

  b->used_in_filter = true;
  f->skip_capable = under_and;
  *out = f;
  return NO_ERROR;
}

/* comparison term: column op constant / constant op column / column op column */
static int
col_build_comp (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, const COMP_EVAL_TERM * comp, COL_FILTER_NODE ** out,
		bool under_and)
{
  int lbind, rbind;
  COL_FILTER_NODE *f;

  if (comp->rel_op == R_NULL)
    {
      lbind = col_find_binding (cs, comp->lhs);
      if (lbind < 0)
	{
	  return col_unsupported ("IS NULL on non-column operand");
	}
      f = col_alloc_fnode (COL_F_ISNULL);
      if (f == NULL)
	{
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      f->bind_idx = lbind;
      cs->bindings[lbind].used_in_filter = true;
      *out = f;
      return NO_ERROR;
    }

  if (comp->rel_op != R_EQ && comp->rel_op != R_NE && comp->rel_op != R_GT && comp->rel_op != R_GE
      && comp->rel_op != R_LT && comp->rel_op != R_LE)
    {
      return col_unsupported ("comparison operator not covered by vectorized filter");
    }

  lbind = col_find_binding (cs, comp->lhs);
  rbind = col_find_binding (cs, comp->rhs);

  if (lbind >= 0 && rbind >= 0)
    {
      /* column op column */
      COL_BINDING *b1 = &cs->bindings[lbind];
      COL_BINDING *b2 = &cs->bindings[rbind];
      COL_KERNEL k = col_select_kernel (b1->type);

      /* NUMERIC is variable-width but still comparable digit-wise, so it is
       * the one variable-width kernel column-op-column covers */
      if (b1->type != b2->type || k == COL_KRN_NONE || k == COL_KRN_VARCHAR
	  || (k != COL_KRN_NUM && (b1->value_size <= 0 || b1->value_size != b2->value_size)))
	{
	  return col_unsupported ("column-op-column on unsupported/mismatched types");
	}
      if (b1->type == DB_TYPE_CHAR && !col_is_binary_collation (TP_DOMAIN_COLLATION (b1->domain)))
	{
	  return col_unsupported ("non-binary collation string comparison");
	}
      f = col_alloc_fnode (COL_F_CMPCOL);
      if (f == NULL)
	{
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      f->bind_idx = lbind;
      f->bind_idx2 = rbind;
      f->op = comp->rel_op;
      f->kernel = k;
      b1->used_in_filter = true;
      b2->used_in_filter = true;
      *out = f;
      return NO_ERROR;
    }

  if (lbind >= 0)
    {
      return col_build_comp_norm (thread_p, cs, comp->lhs, comp->rhs, comp->rel_op, lbind, out, under_and);
    }
  if (rbind >= 0)
    {
      /* const op col -> col reversed-op const */
      REL_OP rev;
      switch (comp->rel_op)
	{
	case R_GT:
	  rev = R_LT;
	  break;
	case R_GE:
	  rev = R_LE;
	  break;
	case R_LT:
	  rev = R_GT;
	  break;
	case R_LE:
	  rev = R_GE;
	  break;
	default:
	  rev = comp->rel_op;
	  break;
	}
      return col_build_comp_norm (thread_p, cs, comp->rhs, comp->lhs, rev, rbind, out, under_and);
    }

  return col_unsupported ("predicate references no columnar column");
}

/* IN (constant list) leaf */
static int
col_build_in (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, const ALSM_EVAL_TERM * alsm, COL_FILTER_NODE ** out)
{
  int lbind = col_find_binding (cs, alsm->elem);
  COL_BINDING *b;
  COL_FILTER_NODE *f;
  DB_VALUE *set_val = NULL;
  DB_SET *set;
  int n, i, error;

  if (alsm->rel_op != R_EQ || alsm->eq_flag != F_SOME)
    {
      return col_unsupported ("quantified comparison other than IN");
    }
  if (lbind < 0)
    {
      return col_unsupported ("IN on non-column operand");
    }
  b = &cs->bindings[lbind];

  error = col_fetch_const (thread_p, cs, alsm->elemset, &set_val);
  if (error != NO_ERROR)
    {
      return error;
    }
  if (DB_IS_NULL (set_val) || !TP_IS_SET_TYPE (DB_VALUE_DOMAIN_TYPE (set_val)))
    {
      return col_unsupported ("IN list is not a constant set");
    }
  set = db_get_set (set_val);
  n = db_set_size (set);
  if (n <= 0)
    {
      return col_unsupported ("empty IN list");
    }

  if ((b->type == DB_TYPE_CHAR || b->type == DB_TYPE_VARCHAR)
      && !col_is_binary_collation (TP_DOMAIN_COLLATION (b->domain)))
    {
      return col_unsupported ("non-binary collation string comparison");
    }

  f = col_alloc_fnode (COL_F_IN);
  if (f == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  f->bind_idx = lbind;
  f->kernel = col_select_kernel (b->type);

  switch (f->kernel)
    {
    case COL_KRN_I16:
    case COL_KRN_I32:
    case COL_KRN_I64:
    case COL_KRN_U32:
    case COL_KRN_DT:
      f->in_i64 = (INT64 *) malloc (n * sizeof (INT64));
      error = (f->in_i64 == NULL) ? ER_OUT_OF_VIRTUAL_MEMORY : NO_ERROR;
      break;
    case COL_KRN_F32:
    case COL_KRN_F64:
      f->in_dbl = (double *) malloc (n * sizeof (double));
      error = (f->in_dbl == NULL) ? ER_OUT_OF_VIRTUAL_MEMORY : NO_ERROR;
      break;
    case COL_KRN_NUM:
      f->in_num = (COL_NUMVAR *) malloc ((size_t) n * sizeof (COL_NUMVAR));
      error = (f->in_num == NULL) ? ER_OUT_OF_VIRTUAL_MEMORY : NO_ERROR;
      break;
    case COL_KRN_CHARN:
      f->in_str = (char *) malloc ((size_t) n * b->value_size);
      error = (f->in_str == NULL) ? ER_OUT_OF_VIRTUAL_MEMORY : NO_ERROR;
      break;
    default:
      col_free_filter (f);
      return col_unsupported ("IN element type not covered");
    }
  if (error != NO_ERROR)
    {
      col_free_filter (f);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) n * 24);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  for (i = 0; i < n; i++)
    {
      DB_VALUE elem, coerced;
      TP_DOMAIN_STATUS dstatus;

      if (db_set_get (set, i, &elem) != NO_ERROR)
	{
	  col_free_filter (f);
	  return ER_FAILED;
	}
      if (DB_IS_NULL (&elem))
	{
	  f->in_has_null = true;
	  pr_clear_value (&elem);
	  continue;
	}
      db_make_null (&coerced);
      dstatus = tp_value_coerce (&elem, &coerced, b->domain);
      pr_clear_value (&elem);
      if (dstatus != DOMAIN_COMPATIBLE)
	{
	  pr_clear_value (&coerced);
	  col_free_filter (f);
	  return col_unsupported ("IN element not coercible to column domain");
	}

      switch (f->kernel)
	{
	case COL_KRN_I16:
	case COL_KRN_I32:
	case COL_KRN_I64:
	case COL_KRN_U32:
	case COL_KRN_DT:
	  f->in_i64[f->n_in++] = col_const_to_i64 (&coerced, b->type);
	  break;
	case COL_KRN_F32:
	case COL_KRN_F64:
	  f->in_dbl[f->n_in++] = col_const_to_dbl (&coerced, b->type);
	  break;
	case COL_KRN_NUM:
	  if (columnar_num_from_dbvalue (&coerced, &f->in_num[f->n_in]) != NO_ERROR)
	    {
	      pr_clear_value (&coerced);
	      col_free_filter (f);
	      return ER_IT_DATA_OVERFLOW;
	    }
	  f->n_in++;
	  break;
	case COL_KRN_CHARN:
	  {
	    const char *s = db_get_string (&coerced);
	    int len = db_get_string_size (&coerced);
	    char *dst = f->in_str + (size_t) f->n_in * b->value_size;
	    if (len > b->value_size)
	      {
		len = b->value_size;
	      }
	    if (s != NULL && len > 0)
	      {
		memcpy (dst, s, len);
	      }
	    if (len < b->value_size)
	      {
		memset (dst + len, ' ', b->value_size - len);
	      }
	    f->n_in++;
	  }
	  break;
	default:
	  break;
	}
      pr_clear_value (&coerced);
    }

  b->used_in_filter = true;
  *out = f;
  return NO_ERROR;
}

/* LIKE leaf */
static int
col_build_like (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, const LIKE_EVAL_TERM * like, COL_FILTER_NODE ** out)
{
  int lbind = col_find_binding (cs, like->src);
  COL_BINDING *b;
  COL_FILTER_NODE *f;
  DB_VALUE *pat = NULL;
  const char *s;
  int len, error;

  if (lbind < 0)
    {
      return col_unsupported ("LIKE on non-column operand");
    }
  b = &cs->bindings[lbind];
  if (b->type != DB_TYPE_CHAR && b->type != DB_TYPE_VARCHAR)
    {
      return col_unsupported ("LIKE on non-string column");
    }
  if (!col_is_binary_collation (TP_DOMAIN_COLLATION (b->domain)))
    {
      return col_unsupported ("LIKE on non-binary collation");
    }
  if (like->esc_char != NULL)
    {
      DB_VALUE *esc = NULL;
      if (col_fetch_const (thread_p, cs, like->esc_char, &esc) != NO_ERROR || esc == NULL || !DB_IS_NULL (esc))
	{
	  return col_unsupported ("LIKE with ESCAPE");
	}
    }

  error = col_fetch_const (thread_p, cs, like->pattern, &pat);
  if (error != NO_ERROR)
    {
      return error;
    }
  if (DB_IS_NULL (pat) || !TP_IS_CHAR_TYPE (DB_VALUE_DOMAIN_TYPE (pat)))
    {
      return col_unsupported ("LIKE pattern is not a constant string");
    }

  f = col_alloc_fnode (COL_F_LIKE);
  if (f == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  f->bind_idx = lbind;
  f->kernel = (b->type == DB_TYPE_CHAR) ? COL_KRN_CHARN : COL_KRN_VARCHAR;

  s = db_get_string (pat);
  len = db_get_string_size (pat);
  f->cstr = (char *) malloc (len > 0 ? len : 1);
  if (f->cstr == NULL)
    {
      col_free_filter (f);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) (len > 0 ? len : 1));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  if (s != NULL && len > 0)
    {
      memcpy (f->cstr, s, len);
    }
  f->cstr_len = len;

  /* CHAR(n) values are space padded on disk; a tail-anchored pattern must
   * match the padded width, so extend it with spaces (patterns ending in '%'
   * absorb the padding by themselves) */
  if (b->type == DB_TYPE_CHAR && (len == 0 || f->cstr[len - 1] != '%') && len < b->value_size)
    {
      char *padded = (char *) malloc (b->value_size);
      if (padded == NULL)
	{
	  col_free_filter (f);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) b->value_size);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      memcpy (padded, f->cstr, len);
      memset (padded + len, ' ', b->value_size - len);
      free (f->cstr);
      f->cstr = padded;
      f->cstr_len = b->value_size;
    }

  b->used_in_filter = true;
  *out = f;
  return NO_ERROR;
}

/*
 * col_build_filter () - compile a PRED_EXPR subtree into a filter node.
 *   under_and stays true only while every ancestor is an AND (min/max skip
 *   eligibility).
 */
static int
col_build_filter (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, const PRED_EXPR * pred, COL_FILTER_NODE ** out,
		  bool under_and)
{
  int error;

  *out = NULL;
  if (pred == NULL)
    {
      return NO_ERROR;
    }

  switch (pred->type)
    {
    case T_PRED:
      {
	COL_FILTER_NODE *f;
	COL_FILTER_KIND kind;

	if (pred->pe.m_pred.bool_op == B_AND)
	  {
	    kind = COL_F_AND;
	  }
	else if (pred->pe.m_pred.bool_op == B_OR)
	  {
	    kind = COL_F_OR;
	  }
	else
	  {
	    return col_unsupported ("boolean operator not covered (XOR/IS)");
	  }

	f = col_alloc_fnode (kind);
	if (f == NULL)
	  {
	    return ER_OUT_OF_VIRTUAL_MEMORY;
	  }
	error = col_build_filter (thread_p, cs, pred->pe.m_pred.lhs, &f->left, under_and && kind == COL_F_AND);
	if (error == NO_ERROR)
	  {
	    error = col_build_filter (thread_p, cs, pred->pe.m_pred.rhs, &f->right, under_and && kind == COL_F_AND);
	  }
	if (error != NO_ERROR)
	  {
	    col_free_filter (f);
	    return error;
	  }
	*out = f;
	return NO_ERROR;
      }

    case T_NOT_TERM:
      {
	COL_FILTER_NODE *f = col_alloc_fnode (COL_F_NOT);
	if (f == NULL)
	  {
	    return ER_OUT_OF_VIRTUAL_MEMORY;
	  }
	error = col_build_filter (thread_p, cs, pred->pe.m_not_term, &f->left, false);
	if (error != NO_ERROR)
	  {
	    col_free_filter (f);
	    return error;
	  }
	*out = f;
	return NO_ERROR;
      }

    case T_EVAL_TERM:
      switch (pred->pe.m_eval_term.et_type)
	{
	case T_COMP_EVAL_TERM:
	  return col_build_comp (thread_p, cs, &pred->pe.m_eval_term.et.et_comp, out, under_and);
	case T_ALSM_EVAL_TERM:
	  return col_build_in (thread_p, cs, &pred->pe.m_eval_term.et.et_alsm, out);
	case T_LIKE_EVAL_TERM:
	  return col_build_like (thread_p, cs, &pred->pe.m_eval_term.et.et_like, out);
	default:
	  return col_unsupported ("predicate term not covered (RLIKE)");
	}

    default:
      return col_unsupported ("predicate node not covered");
    }
}

/* ========================================================================== */
/* Filter evaluation (per chunk)                                              */
/* ========================================================================== */

/* set n_bm bits where the column value is NULL */
static void
col_null_from_exists (const char *ex, int n_rows, UINT64 * n_bm)
{
  int r;
  for (r = 0; r < n_rows; r++)
    {
      if (!COL_EXISTS_BIT (ex, r))
	{
	  COL_BM_SET (n_bm, r);
	}
    }
}

static void
col_eval_leaf_cmp (COLUMNAR_SCAN * cs, const COL_FILTER_NODE * f, int n_rows, UINT64 * t_bm, UINT64 * n_bm)
{
  const COL_BINDING *b = &cs->bindings[f->bind_idx];

  if (f->const_is_null)
    {
      int r;
      for (r = 0; r < n_rows; r++)
	{
	  COL_BM_SET (n_bm, r);
	}
      return;
    }

  switch (f->kernel)
    {
    case COL_KRN_I16:
      col_cmp_i16 (b->chunk_data, b->value_size, b->chunk_exists, n_rows, f->op, f->ci64, t_bm);
      break;
    case COL_KRN_I32:
      col_cmp_i32 (b->chunk_data, b->value_size, b->chunk_exists, n_rows, f->op, f->ci64, t_bm);
      break;
    case COL_KRN_I64:
      col_cmp_i64 (b->chunk_data, b->value_size, b->chunk_exists, n_rows, f->op, f->ci64, t_bm);
      break;
    case COL_KRN_U32:
      col_cmp_u32 (b->chunk_data, b->value_size, b->chunk_exists, n_rows, f->op, f->ci64, t_bm);
      break;
    case COL_KRN_DT:
      col_cmp_dt (b->chunk_data, b->value_size, b->chunk_exists, n_rows, f->op, f->ci64, t_bm);
      break;
    case COL_KRN_F32:
      col_cmp_f32 (b->chunk_data, b->value_size, b->chunk_exists, n_rows, f->op, f->cdbl, t_bm);
      break;
    case COL_KRN_F64:
      col_cmp_f64 (b->chunk_data, b->value_size, b->chunk_exists, n_rows, f->op, f->cdbl, t_bm);
      break;
    case COL_KRN_NUM:
      col_cmp_num (b->chunk_data, b->var_offsets, b->chunk_exists, n_rows, f->op, &f->cnum, t_bm);
      break;
    case COL_KRN_CHARN:
      col_cmp_charn (b->chunk_data, b->value_size, b->chunk_exists, n_rows, f->op, f->cstr, t_bm);
      break;
    case COL_KRN_VARCHAR:
      col_cmp_varchar (b->chunk_data, b->var_offsets, b->chunk_exists, n_rows, f->op, f->cstr, f->cstr_len, t_bm);
      break;
    default:
      assert (false);
      break;
    }
  col_null_from_exists (b->chunk_exists, n_rows, n_bm);
}

static void
col_eval_leaf_cmpcol (COLUMNAR_SCAN * cs, const COL_FILTER_NODE * f, int n_rows, UINT64 * t_bm, UINT64 * n_bm)
{
  const COL_BINDING *b1 = &cs->bindings[f->bind_idx];
  const COL_BINDING *b2 = &cs->bindings[f->bind_idx2];
  int r;

  for (r = 0; r < n_rows; r++)
    {
      int c;
      if (!COL_EXISTS_BIT (b1->chunk_exists, r) || !COL_EXISTS_BIT (b2->chunk_exists, r))
	{
	  continue;
	}
      switch (f->kernel)
	{
	case COL_KRN_I16:
	case COL_KRN_I32:
	case COL_KRN_I64:
	case COL_KRN_U32:
	case COL_KRN_DT:
	  {
	    INT64 v1 = col_load_i64 (f->kernel, b1->chunk_data + (size_t) r * b1->value_size);
	    INT64 v2 = col_load_i64 (f->kernel, b2->chunk_data + (size_t) r * b2->value_size);
	    c = (v1 < v2) ? -1 : ((v1 > v2) ? 1 : 0);
	  }
	  break;
	case COL_KRN_F32:
	case COL_KRN_F64:
	  {
	    double v1 = col_load_dbl (f->kernel, b1->chunk_data + (size_t) r * b1->value_size);
	    double v2 = col_load_dbl (f->kernel, b2->chunk_data + (size_t) r * b2->value_size);
	    c = (v1 < v2) ? -1 : ((v1 > v2) ? 1 : 0);
	  }
	  break;
	case COL_KRN_NUM:
	  {
	    COL_NUMREF v1, v2;
	    COL_NUM_AT (b1->chunk_data, b1->var_offsets, r, &v1);
	    COL_NUM_AT (b2->chunk_data, b2->var_offsets, r, &v2);
	    c = columnar_num_cmp (&v1, &v2);
	  }
	  break;
	case COL_KRN_CHARN:
	  c = memcmp (b1->chunk_data + (size_t) r * b1->value_size, b2->chunk_data + (size_t) r * b2->value_size,
		      b1->value_size);
	  break;
	default:
	  assert (false);
	  c = 0;
	  break;
	}
      if (col_rel_match (f->op, c))
	{
	  COL_BM_SET (t_bm, r);
	}
    }

  col_null_from_exists (b1->chunk_exists, n_rows, n_bm);
  col_null_from_exists (b2->chunk_exists, n_rows, n_bm);
}

static void
col_eval_leaf_in (COLUMNAR_SCAN * cs, const COL_FILTER_NODE * f, int n_rows, UINT64 * t_bm, UINT64 * n_bm)
{
  const COL_BINDING *b = &cs->bindings[f->bind_idx];
  int r, k;

  for (r = 0; r < n_rows; r++)
    {
      bool match = false;
      const char *cell;

      if (!COL_EXISTS_BIT (b->chunk_exists, r))
	{
	  COL_BM_SET (n_bm, r);
	  continue;
	}
      cell = (b->value_size > 0) ? b->chunk_data + (size_t) r * b->value_size : NULL;
      switch (f->kernel)
	{
	case COL_KRN_I16:
	case COL_KRN_I32:
	case COL_KRN_I64:
	case COL_KRN_U32:
	case COL_KRN_DT:
	  {
	    INT64 v = col_load_i64 (f->kernel, cell);
	    for (k = 0; k < f->n_in; k++)
	      {
		if (f->in_i64[k] == v)
		  {
		    match = true;
		    break;
		  }
	      }
	  }
	  break;
	case COL_KRN_F32:
	case COL_KRN_F64:
	  {
	    double v = col_load_dbl (f->kernel, cell);
	    for (k = 0; k < f->n_in; k++)
	      {
		if (f->in_dbl[k] == v)
		  {
		    match = true;
		    break;
		  }
	      }
	  }
	  break;
	case COL_KRN_NUM:
	  {
	    COL_NUMREF v;
	    COL_NUM_AT (b->chunk_data, b->var_offsets, r, &v);
	    for (k = 0; k < f->n_in; k++)
	      {
		COL_NUMREF e;
		columnar_num_ref_var (&f->in_num[k], &e);
		if (columnar_num_cmp (&v, &e) == 0)
		  {
		    match = true;
		    break;
		  }
	      }
	  }
	  break;
	case COL_KRN_CHARN:
	  for (k = 0; k < f->n_in; k++)
	    {
	      if (memcmp (cell, f->in_str + (size_t) k * b->value_size, b->value_size) == 0)
		{
		  match = true;
		  break;
		}
	    }
	  break;
	default:
	  assert (false);
	  break;
	}
      if (match)
	{
	  COL_BM_SET (t_bm, r);
	}
      else if (f->in_has_null)
	{
	  COL_BM_SET (n_bm, r);
	}
    }
}

/*
 * col_eval_filter () - evaluate a filter subtree over the current chunk into
 *   (t_bm, n_bm): definite-TRUE and UNKNOWN bitmaps (Kleene 3-valued logic).
 */
static void
col_eval_filter (COLUMNAR_SCAN * cs, const COL_FILTER_NODE * f, int n_rows, int depth, UINT64 * t_bm, UINT64 * n_bm)
{
  int n_words = (n_rows + 63) / 64;
  int w;

  memset (t_bm, 0, n_words * sizeof (UINT64));
  memset (n_bm, 0, n_words * sizeof (UINT64));

  switch (f->kind)
    {
    case COL_F_AND:
    case COL_F_OR:
      {
	UINT64 *t2 = cs->bm_pool[depth * 2];
	UINT64 *n2 = cs->bm_pool[depth * 2 + 1];

	col_eval_filter (cs, f->left, n_rows, depth + 1, t_bm, n_bm);
	col_eval_filter (cs, f->right, n_rows, depth + 1, t2, n2);

	if (f->kind == COL_F_AND)
	  {
	    for (w = 0; w < n_words; w++)
	      {
		UINT64 f1 = ~t_bm[w] & ~n_bm[w];	/* definite FALSE, side 1 */
		UINT64 f2 = ~t2[w] & ~n2[w];
		UINT64 t = t_bm[w] & t2[w];
		t_bm[w] = t;
		n_bm[w] = ~t & ~(f1 | f2);	/* UNKNOWN unless any side is FALSE */
	      }
	  }
	else
	  {
	    for (w = 0; w < n_words; w++)
	      {
		UINT64 t = t_bm[w] | t2[w];
		n_bm[w] = (n_bm[w] | n2[w]) & ~t;
		t_bm[w] = t;
	      }
	  }
	/* clear tail bits beyond n_rows (the ~ operations set them) */
	if (n_rows & 63)
	  {
	    UINT64 tail_mask = (1ULL << (n_rows & 63)) - 1;
	    t_bm[n_words - 1] &= tail_mask;
	    n_bm[n_words - 1] &= tail_mask;
	  }
      }
      break;

    case COL_F_NOT:
      col_eval_filter (cs, f->left, n_rows, depth + 1, t_bm, n_bm);
      for (w = 0; w < n_words; w++)
	{
	  t_bm[w] = ~t_bm[w] & ~n_bm[w];	/* NOT: true <- definite false */
	}
      if (n_rows & 63)
	{
	  t_bm[n_words - 1] &= (1ULL << (n_rows & 63)) - 1;
	}
      break;

    case COL_F_CMP:
      col_eval_leaf_cmp (cs, f, n_rows, t_bm, n_bm);
      break;
    case COL_F_CMPCOL:
      col_eval_leaf_cmpcol (cs, f, n_rows, t_bm, n_bm);
      break;
    case COL_F_IN:
      col_eval_leaf_in (cs, f, n_rows, t_bm, n_bm);
      break;
    case COL_F_LIKE:
      {
	const COL_BINDING *b = &cs->bindings[f->bind_idx];
	if (f->kernel == COL_KRN_CHARN)
	  {
	    col_like_charn (b->chunk_data, b->value_size, b->chunk_exists, n_rows, f->cstr, f->cstr_len, t_bm);
	  }
	else
	  {
	    col_like_varchar (b->chunk_data, b->var_offsets, b->chunk_exists, n_rows, f->cstr, f->cstr_len, t_bm);
	  }
	col_null_from_exists (b->chunk_exists, n_rows, n_bm);
      }
      break;
    case COL_F_ISNULL:
      col_null_from_exists (cs->bindings[f->bind_idx].chunk_exists, n_rows, t_bm);
      break;
    default:
      assert (false);
      break;
    }
}

/* ========================================================================== */
/* Min/max chunk skip                                                         */
/* ========================================================================== */

static bool
col_chunk_skippable_leaf (const COLUMNAR_SCAN * cs, const COL_FILTER_NODE * f, int chunk)
{
  const COL_BINDING *b;
  const COLUMNAR_CHUNK_DESC *d;

  if (f->kind != COL_F_CMP || !f->skip_capable || f->const_is_null)
    {
      return false;
    }
  b = &cs->bindings[f->bind_idx];
  d = &cs->descs[chunk * cs->footer_n_columns + b->col_idx];

  if (d->minmax_kind == COLUMNAR_MINMAX_INT64)
    {
      INT64 mn = d->min_val, mx = d->max_val, c = f->ci64;
      switch (f->op)
	{
	case R_EQ:
	  return (c < mn || c > mx);
	case R_LT:
	  return (mn >= c);
	case R_LE:
	  return (mn > c);
	case R_GT:
	  return (mx <= c);
	case R_GE:
	  return (mx < c);
	case R_NE:
	  return (mn == c && mx == c);
	default:
	  return false;
	}
    }
  else if (d->minmax_kind == COLUMNAR_MINMAX_DOUBLE)
    {
      double mn, mx, c = f->cdbl;
      memcpy (&mn, &d->min_val, sizeof (double));
      memcpy (&mx, &d->max_val, sizeof (double));
      switch (f->op)
	{
	case R_EQ:
	  return (c < mn || c > mx);
	case R_LT:
	  return (mn >= c);
	case R_LE:
	  return (mn > c);
	case R_GT:
	  return (mx <= c);
	case R_GE:
	  return (mx < c);
	case R_NE:
	  return (mn == c && mx == c);
	default:
	  return false;
	}
    }
  else if (d->minmax_kind == COLUMNAR_MINMAX_NUMERIC)
    {
      /* bounds are exact unscaled integers at d->minmax_dscale; the skip
       * applies only when the constant is exactly representable there too */
      INT64 mn = d->min_val, mx = d->max_val, c;
      COL_NUMREF cref;

      columnar_num_ref_var (&f->cnum, &cref);
      if (!columnar_num_unscaled_int64 (&cref, d->minmax_dscale, &c))
	{
	  return false;
	}
      switch (f->op)
	{
	case R_EQ:
	  return (c < mn || c > mx);
	case R_LT:
	  return (mn >= c);
	case R_LE:
	  return (mn > c);
	case R_GT:
	  return (mx <= c);
	case R_GE:
	  return (mx < c);
	case R_NE:
	  return (mn == c && mx == c);
	default:
	  return false;
	}
    }
  return false;
}

/* true when the chunk group is provably empty under an AND-reachable leaf */
static bool
col_chunk_skippable (const COLUMNAR_SCAN * cs, const COL_FILTER_NODE * f, int chunk)
{
  if (f == NULL)
    {
      return false;
    }
  switch (f->kind)
    {
    case COL_F_AND:
      return col_chunk_skippable (cs, f->left, chunk) || col_chunk_skippable (cs, f->right, chunk);
    case COL_F_CMP:
      return col_chunk_skippable_leaf (cs, f, chunk);
    default:
      return false;
    }
}

/* ========================================================================== */
/* Directory snapshot + stripe/chunk IO                                       */
/* ========================================================================== */

static int
col_snapshot_directory (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs)
{
  VPID meta_vpid;
  MVCC_SNAPSHOT *snapshot;
  MVCCID my_mvccid = MVCCID_NULL;
  LOG_TDES *tdes = LOG_FIND_CURRENT_TDES (thread_p);
  int i, n_visible = 0, total_entries = 0, dir_alloc = 0, error;

  if (tdes != NULL)
    {
      my_mvccid = tdes->mvccinfo.id;
    }
  snapshot = logtb_get_mvcc_snapshot (thread_p);

  /* walk the metapage chain, accumulating visible directory entries */
  meta_vpid.volid = cs->hfid.vfid.volid;
  meta_vpid.pageid = cs->hfid.hpgid;

  while (!VPID_ISNULL (&meta_vpid))
    {
      PAGE_PTR pgptr;
      const COLUMNAR_METAPAGE_HEADER *hdr;

      pgptr = pgbuf_fix (thread_p, &meta_vpid, OLD_PAGE, PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH);
      if (pgptr == NULL)
	{
	  ASSERT_ERROR_AND_SET (error);
	  return error;
	}

      hdr = (const COLUMNAR_METAPAGE_HEADER *) pgptr;
      if (hdr->magic != COLUMNAR_METAPAGE_MAGIC)
	{
	  pgbuf_unfix_and_init (thread_p, pgptr);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	  return ER_FAILED;
	}

      if (hdr->entry_count > 0)
	{
	  if (n_visible + hdr->entry_count > dir_alloc)
	    {
	      COLUMNAR_STRIPE_DIR_ENTRY *nd;
	      dir_alloc = n_visible + hdr->entry_count;
	      nd = (COLUMNAR_STRIPE_DIR_ENTRY *) realloc (cs->dir, dir_alloc * sizeof (COLUMNAR_STRIPE_DIR_ENTRY));
	      if (nd == NULL)
		{
		  pgbuf_unfix_and_init (thread_p, pgptr);
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
			  dir_alloc * sizeof (COLUMNAR_STRIPE_DIR_ENTRY));
		  return ER_OUT_OF_VIRTUAL_MEMORY;
		}
	      cs->dir = nd;
	    }

	  for (i = 0; i < hdr->entry_count; i++)
	    {
	      const COLUMNAR_STRIPE_DIR_ENTRY *ent =
		(const COLUMNAR_STRIPE_DIR_ENTRY *) ((const char *) pgptr + sizeof (COLUMNAR_METAPAGE_HEADER)
						      + i * sizeof (COLUMNAR_STRIPE_DIR_ENTRY));
	      bool visible;

	      if (!MVCCID_IS_VALID (ent->insert_mvccid))
		{
		  visible = true;	/* legacy stripe without id: committed by definition */
		}
	      else if (ent->insert_mvccid == my_mvccid)
		{
		  visible = true;	/* own write */
		}
	      else if (snapshot != NULL)
		{
		  visible = !snapshot->m_active_mvccs.is_active (ent->insert_mvccid);
		}
	      else
		{
		  visible = true;
		}

	      if (visible)
		{
		  cs->dir[n_visible++] = *ent;
		}
	      else
		{
		  cs->stats.stripes_skipped_mvcc++;
		}
	    }
	}

      total_entries += hdr->entry_count;
      meta_vpid = hdr->next_metapage;
      pgbuf_unfix_and_init (thread_p, pgptr);
    }

  cs->stats.stripes_total = total_entries;
  cs->n_stripes = n_visible;
  return NO_ERROR;
}

/* copy [offset, offset+length) of the stripe's data byte stream.  Offset 0
 * is the first byte of stripe page 1 — page 0 holds the stripe page map,
 * which col_open_stripe snapshots into cs->stripe_vpids before any range
 * read.  Every page lookup is an O(1) index into that map, so
 * non-contiguous file_alloc placements cost nothing on the hot path. */
static int
col_read_range (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, const COLUMNAR_STRIPE_DIR_ENTRY * ent, INT64 offset,
		int length, char *dst)
{
  INT64 pos = offset;
  int copied = 0;

  while (copied < length)
    {
      VPID vpid;
      PAGE_PTR pgptr;
      int page_idx = 1 + (int) (pos / DB_PAGESIZE);	/* +1: skip the map page */
      int in_page = (int) (pos % DB_PAGESIZE);
      int chunk = DB_PAGESIZE - in_page;
      int error;

      if (chunk > length - copied)
	{
	  chunk = length - copied;
	}
      if (page_idx >= ent->page_count || page_idx >= cs->stripe_vpids_count)
	{
	  assert (false);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	  return ER_FAILED;
	}

      vpid = cs->stripe_vpids[page_idx];

      pgptr = pgbuf_fix (thread_p, &vpid, OLD_PAGE, PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH);
      if (pgptr == NULL)
	{
	  ASSERT_ERROR_AND_SET (error);
	  return error;
	}
      memcpy (dst + copied, (const char *) pgptr + in_page, chunk);
      pgbuf_unfix_and_init (thread_p, pgptr);

      copied += chunk;
      pos += chunk;
    }
  return NO_ERROR;
}

static int
col_ensure_buf (char **buf, int *alloc, int needed)
{
  if (needed <= *alloc)
    {
      return NO_ERROR;
    }
  {
    char *nb = (char *) realloc (*buf, needed);
    if (nb == NULL)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) needed);
	return ER_OUT_OF_VIRTUAL_MEMORY;
      }
    *buf = nb;
    *alloc = needed;
  }
  return NO_ERROR;
}

/*
 * col_open_stripe () - snapshot the stripe page map from page 0, then read +
 *   validate the footer of dir[stripe_idx], reset the chunk cursor, and grow
 *   the bitmap pool for this stripe's chunk size
 */
static int
col_open_stripe (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs)
{
  const COLUMNAR_STRIPE_DIR_ENTRY *ent = &cs->dir[cs->stripe_idx];
  COLUMNAR_STRIPE_FOOTER_HEADER fhdr;
  INT64 footer_start;
  int descs_size, error, need_words, i;

  /* load the stripe page map from page 0 (dir entry's start_vpid).  One page
   * fix per stripe; afterwards every page lookup is an array index. */
  cs->stripe_vpids_count = 0;
  {
    PAGE_PTR pgptr;
    const COLUMNAR_STRIPE_PAGE_MAP_HEADER *mhdr;

    pgptr = pgbuf_fix (thread_p, &ent->start_vpid, OLD_PAGE, PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH);
    if (pgptr == NULL)
      {
	ASSERT_ERROR_AND_SET (error);
	return error;
      }
    mhdr = (const COLUMNAR_STRIPE_PAGE_MAP_HEADER *) pgptr;
    if (mhdr->magic != COLUMNAR_STRIPE_MAP_MAGIC || mhdr->version != COLUMNAR_STRIPE_MAP_VERSION
	|| mhdr->page_count != ent->page_count || mhdr->page_count > COLUMNAR_STRIPE_PAGE_MAP_CAPACITY)
      {
	pgbuf_unfix_and_init (thread_p, pgptr);
	assert (false);
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	return ER_FAILED;
      }
    if (mhdr->page_count > cs->stripe_vpids_alloc)
      {
	VPID *nv = (VPID *) realloc (cs->stripe_vpids, mhdr->page_count * sizeof (VPID));
	if (nv == NULL)
	  {
	    pgbuf_unfix_and_init (thread_p, pgptr);
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
		    mhdr->page_count * sizeof (VPID));
	    return ER_OUT_OF_VIRTUAL_MEMORY;
	  }
	cs->stripe_vpids = nv;
	cs->stripe_vpids_alloc = mhdr->page_count;
      }
    memcpy (cs->stripe_vpids, (const char *) pgptr + sizeof (COLUMNAR_STRIPE_PAGE_MAP_HEADER),
	    mhdr->page_count * sizeof (VPID));
    cs->stripe_vpids_count = mhdr->page_count;
    pgbuf_unfix_and_init (thread_p, pgptr);
  }

  /* footer byte offset in the data stream (stream offset 0 = stripe page 1) */
  footer_start = (INT64) (ent->footer_page_idx - 1) * DB_PAGESIZE + ent->footer_offset;

  error = col_read_range (thread_p, cs, ent, footer_start, (int) sizeof (fhdr), (char *) &fhdr);
  if (error != NO_ERROR)
    {
      return error;
    }
  if (fhdr.magic != COLUMNAR_FOOTER_MAGIC)
    {
      assert (false);		/* not a footer at all: a real corruption */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
      return ER_FAILED;
    }
  if (fhdr.version != COLUMNAR_FOOTER_VERSION)
    {
      /*
       * A stripe written by an older build.  This is an EXPECTED condition
       * once the on-disk format changes (#23 D11 moved NUMERIC to a different
       * representation and bumped the footer version), so it must be a clean
       * error the caller can act on -- asserting here aborts the server, and
       * an abnormal death is far worse than a failed query (see #27).
       */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_COLUMNAR_NOT_SUPPORTED, 1,
	      "table was written by an incompatible older build; drop and reload it");
      return ER_COLUMNAR_NOT_SUPPORTED;
    }

  descs_size = fhdr.n_columns * fhdr.n_chunk_groups * (int) sizeof (COLUMNAR_CHUNK_DESC);
  if (descs_size > cs->descs_alloc)
    {
      COLUMNAR_CHUNK_DESC *nd = (COLUMNAR_CHUNK_DESC *) realloc (cs->descs, descs_size);
      if (nd == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) descs_size);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      cs->descs = nd;
      cs->descs_alloc = descs_size;
    }
  error = col_read_range (thread_p, cs, ent, footer_start + (int) sizeof (fhdr), descs_size, (char *) cs->descs);
  if (error != NO_ERROR)
    {
      return error;
    }

  cs->footer_n_columns = fhdr.n_columns;
  cs->n_chunks = fhdr.n_chunk_groups;
  cs->chunk_row_count = (fhdr.chunk_row_count > 0) ? fhdr.chunk_row_count : COLUMNAR_DEFAULT_CHUNK_ROW_COUNT;
  cs->stripe_rows = ent->row_count;
  cs->chunk_idx = -1;
  cs->chunk_rows = 0;
  cs->row_in_chunk = 0;
  cs->stats.stripes_read++;
  cs->stats.chunk_groups_total += cs->n_chunks;

  /* (re)size the bitmap pool for this stripe's chunk row count */
  need_words = (cs->chunk_row_count + 63) / 64;
  if (need_words > cs->bm_words_alloc)
    {
      for (i = 0; i < cs->bm_pool_n; i++)
	{
	  UINT64 *nb = (UINT64 *) realloc (cs->bm_pool[i], need_words * sizeof (UINT64));
	  if (nb == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, need_words * sizeof (UINT64));
	      return ER_OUT_OF_VIRTUAL_MEMORY;
	    }
	  cs->bm_pool[i] = nb;
	}
      cs->bm_words_alloc = need_words;
    }
  return NO_ERROR;
}

/* load one binding's chunk: exists bitmap + (decompressed) data + var offsets */
static int
col_load_binding_chunk (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, COL_BINDING * b, int chunk_rows)
{
  const COLUMNAR_STRIPE_DIR_ENTRY *ent = &cs->dir[cs->stripe_idx];
  const COLUMNAR_CHUNK_DESC *d = &cs->descs[cs->chunk_idx * cs->footer_n_columns + b->col_idx];
  int error;

  /* exists bitmap (always uncompressed) */
  error = col_ensure_buf (&b->exists_buf, &b->exists_buf_alloc, d->exists_length > 0 ? d->exists_length : 1);
  if (error != NO_ERROR)
    {
      return error;
    }
  if (d->exists_length > 0)
    {
      error = col_read_range (thread_p, cs, ent, d->exists_offset, d->exists_length, b->exists_buf);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }
  /* rows beyond exists_length have their bits missing: zero-fill the tail */
  {
    int needed_bytes = (chunk_rows + 7) / 8;
    if (needed_bytes > d->exists_length)
      {
	error = col_ensure_buf (&b->exists_buf, &b->exists_buf_alloc, needed_bytes);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
	memset (b->exists_buf + d->exists_length, 0, needed_bytes - d->exists_length);
      }
  }
  b->chunk_exists = b->exists_buf;

  /* data */
  error = col_ensure_buf (&b->data_buf, &b->data_buf_alloc, d->decompressed_length > 0 ? d->decompressed_length : 1);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (d->compression == COLUMNAR_COMPRESS_NONE)
    {
      if (d->data_length > 0)
	{
	  error = col_read_range (thread_p, cs, ent, d->data_offset, d->data_length, b->data_buf);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }
	}
    }
  else
    {
      error = col_ensure_buf (&b->comp_buf, &b->comp_buf_alloc, d->data_length > 0 ? d->data_length : 1);
      if (error != NO_ERROR)
	{
	  return error;
	}
      if (d->data_length > 0)
	{
	  error = col_read_range (thread_p, cs, ent, d->data_offset, d->data_length, b->comp_buf);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }
	}

      if (d->data_length == 0 && d->decompressed_length == 0)
	{
	  /* empty chunk (e.g. variable-width column where every row is NULL):
	   * nothing to decompress, data_buf stays at 0 bytes used */
	}
      else if (d->compression == COLUMNAR_COMPRESS_LZ4)
	{
	  int out_len = LZ4_decompress_safe (b->comp_buf, b->data_buf, d->data_length, d->decompressed_length);
	  if (out_len != d->decompressed_length)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	      return ER_FAILED;
	    }
	}
#if defined (HAVE_ZSTD)
      else if (d->compression == COLUMNAR_COMPRESS_ZSTD)
	{
	  size_t out_len = ZSTD_decompress (b->data_buf, d->decompressed_length, b->comp_buf, d->data_length);
	  if (ZSTD_isError (out_len) || (int) out_len != d->decompressed_length)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	      return ER_FAILED;
	    }
	}
#endif
      else
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	  return ER_FAILED;	/* unknown codec (or zstd data without HAVE_ZSTD) */
	}
    }
  b->chunk_data = b->data_buf;

  /* variable width: build the per-row offset table (NULL rows own no bytes) */
  if (b->value_size < 0)
    {
      int r, off = 0, len;

      if (chunk_rows > b->var_offsets_alloc)
	{
	  int *no = (int *) realloc (b->var_offsets, chunk_rows * sizeof (int));
	  if (no == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, chunk_rows * sizeof (int));
	      return ER_OUT_OF_VIRTUAL_MEMORY;
	    }
	  b->var_offsets = no;
	  b->var_offsets_alloc = chunk_rows;
	}
      for (r = 0; r < chunk_rows; r++)
	{
	  if (COL_EXISTS_BIT (b->chunk_exists, r))
	    {
	      b->var_offsets[r] = off;
	      memcpy (&len, b->chunk_data + off, sizeof (int));
	      off += (int) sizeof (int) + len;
	    }
	  else
	    {
	      b->var_offsets[r] = -1;
	    }
	}
    }
  return NO_ERROR;
}

/*
 * col_load_chunk () - position on the next chunk group: min/max skip check,
 *   filter columns load, vectorized filter, remaining columns load.
 *   skipped=true when the chunk yields no row (min/max or empty bitmap).
 */
static int
col_load_chunk (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, bool * skipped)
{
  int chunk_rows, i, w, error;
  UINT64 any;

  *skipped = true;

  chunk_rows = cs->chunk_row_count;
  if (cs->chunk_idx == cs->n_chunks - 1)
    {
      chunk_rows = cs->stripe_rows - cs->chunk_idx * cs->chunk_row_count;
    }
  if (chunk_rows <= 0)
    {
      return NO_ERROR;
    }
  cs->chunk_rows = chunk_rows;
  cs->n_words = (chunk_rows + 63) / 64;

  /* min/max skip before any page read */
  if (cs->filter != NULL && col_chunk_skippable (cs, cs->filter, cs->chunk_idx))
    {
      cs->stats.chunk_groups_skipped++;
      return NO_ERROR;
    }

  /* filter columns first */
  for (i = 0; i < cs->n_bindings; i++)
    {
      if (cs->bindings[i].used_in_filter)
	{
	  error = col_load_binding_chunk (thread_p, cs, &cs->bindings[i], chunk_rows);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }
	}
    }

  if (cs->filter != NULL)
    {
      col_eval_filter (cs, cs->filter, chunk_rows, 0, cs->bitmap, cs->bm_pool[cs->bm_pool_n - 1]);
    }
  else
    {
      /* no WHERE: all rows qualify */
      memset (cs->bitmap, 0xff, cs->n_words * sizeof (UINT64));
      if (chunk_rows & 63)
	{
	  cs->bitmap[cs->n_words - 1] = (1ULL << (chunk_rows & 63)) - 1;
	}
    }

  any = 0;
  for (w = 0; w < cs->n_words; w++)
    {
      any |= cs->bitmap[w];
      cs->stats.rows_decoded += __builtin_popcountll (cs->bitmap[w]);
    }
  if (any == 0)
    {
      return NO_ERROR;		/* nothing survived; do not load the rest */
    }

  /* remaining output columns */
  for (i = 0; i < cs->n_bindings; i++)
    {
      if (!cs->bindings[i].used_in_filter)
	{
	  error = col_load_binding_chunk (thread_p, cs, &cs->bindings[i], chunk_rows);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }
	}
    }

  cs->row_in_chunk = 0;
  *skipped = false;
  return NO_ERROR;
}

/* ========================================================================== */
/* Binding construction                                                       */
/* ========================================================================== */

static int
col_count_regu_list (REGU_VARIABLE_LIST list)
{
  int n = 0;
  for (; list != NULL; list = list->next)
    {
      n++;
    }
  return n;
}

/*
 * col_build_bindings () - one binding per TYPE_ATTR_ID regu in the spec's
 *   pred/rest regu lists; the attr id -> storage column position and domain
 *   mapping comes from the class representation (attrinfo used once at open,
 *   never per row - the same order the write path serialized columns in).
 */
static int
col_build_bindings (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, ACCESS_SPEC_TYPE * spec)
{
  HEAP_CACHE_ATTRINFO attr_info;
  REGU_VARIABLE_LIST lists[2];
  int n_total, li, error, i;
  bool attr_inited = false;

  lists[0] = spec->s.cls_node.cls_regu_list_pred;
  lists[1] = spec->s.cls_node.cls_regu_list_rest;

  n_total = col_count_regu_list (lists[0]) + col_count_regu_list (lists[1]);
  cs->n_bindings = 0;
  if (n_total == 0)
    {
      return NO_ERROR;
    }

  cs->bindings = (COL_BINDING *) calloc (n_total, sizeof (COL_BINDING));
  if (cs->bindings == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, n_total * sizeof (COL_BINDING));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  error = heap_attrinfo_start (thread_p, &cs->class_oid, -1, NULL, &attr_info);
  if (error != NO_ERROR)
    {
      return error;
    }
  attr_inited = true;

  for (li = 0; li < 2; li++)
    {
      REGU_VARIABLE_LIST node;
      for (node = lists[li]; node != NULL; node = node->next)
	{
	  REGU_VARIABLE *regu = &node->value;
	  COL_BINDING *b;
	  int col_idx = -1;

	  if (regu->type != TYPE_ATTR_ID)
	    {
	      error = col_unsupported ("non-instance-attribute in columnar scan list");
	      goto end;
	    }

	  for (i = 0; i < attr_info.num_values; i++)
	    {
	      if (attr_info.values[i].attrid == regu->value.attr_descr.id)
		{
		  col_idx = i;
		  break;
		}
	    }
	  if (col_idx < 0 || attr_info.values[col_idx].last_attrepr == NULL)
	    {
	      error = col_unsupported ("attribute not found in class representation");
	      goto end;
	    }

	  b = &cs->bindings[cs->n_bindings];
	  b->slot = regu->vfetch_to;
	  b->attr_id = regu->value.attr_descr.id;
	  b->col_idx = col_idx;
	  b->domain = attr_info.values[col_idx].last_attrepr->domain;
	  b->type = TP_DOMAIN_TYPE (b->domain);
	  b->value_size = columnar_value_disk_size (b->type, b->domain);
	  b->decode = col_select_decode_fn (b->type);
	  if (b->slot == NULL || b->decode == NULL || b->value_size == -2)
	    {
	      error = col_unsupported ("column type not decodable from columnar format");
	      goto end;
	    }
	  cs->n_bindings++;
	}
    }

end:
  if (attr_inited)
    {
      heap_attrinfo_end (thread_p, &attr_info);
    }
  return error;
}

/* ========================================================================== */
/* Public API                                                                 */
/* ========================================================================== */

int
columnar_scan_open (THREAD_ENTRY * thread_p, COLUMNAR_SCAN ** cs_out, ACCESS_SPEC_TYPE * spec, struct val_descr *vd)
{
  COLUMNAR_SCAN *cs;
  int error, i, depth, n_bm;

  *cs_out = NULL;

  assert (spec != NULL && spec->type == TARGET_CLASS && spec->access == ACCESS_METHOD_COLUMNAR);

  cs = (COLUMNAR_SCAN *) calloc (1, sizeof (COLUMNAR_SCAN));
  if (cs == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (COLUMNAR_SCAN));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  cs->hfid = spec->s.cls_node.hfid;
  cs->class_oid = spec->s.cls_node.cls_oid;
  cs->vd = vd;
  cs->stripe_idx = -1;
  cs->stripe_opened = false;

  /* same-transaction reads: flush pending buffered rows first */
  if (columnar_has_pending_writes (thread_p, &cs->class_oid))
    {
      error = columnar_flush_for_scan (thread_p, &cs->class_oid);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
    }

  error = col_build_bindings (thread_p, cs, spec);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  /* the scan owns the bound slots for its lifetime; fixed-width slots are
   * type-stamped once here so the per-row decode writes only value bytes */
  for (i = 0; i < cs->n_bindings; i++)
    {
      pr_clear_value (cs->bindings[i].slot);
      db_make_null (cs->bindings[i].slot);
      col_stamp_slot (&cs->bindings[i]);
    }

  error = col_build_filter (thread_p, cs, spec->where_pred, &cs->filter, true);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  cs->count_only = (cs->n_bindings == 0 && cs->filter == NULL);

  /* bitmap pool: 2 scratch per tree depth + main bitmap + top-level null bm */
  depth = col_filter_depth (cs->filter);
  n_bm = 2 * (depth + 1) + 2;
  cs->bm_pool = (UINT64 **) calloc (n_bm, sizeof (UINT64 *));
  if (cs->bm_pool == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, n_bm * sizeof (UINT64 *));
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      goto error_exit;
    }
  cs->bm_pool_n = n_bm;
  cs->bm_words_alloc = 0;
  /* the main bitmap aliases the second-to-last pool slot after first stripe
   * open resizes the pool; set it lazily in col_open_stripe-driven flow */

  error = col_snapshot_directory (thread_p, cs);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  *cs_out = cs;
  return NO_ERROR;

error_exit:
  columnar_scan_close (thread_p, cs);
  return error;
}

/*
 * col_advance_chunk () - position on the next chunk group with at least one
 *   qualified row (col_load_chunk reports empty ones as skipped)
 */
static SCAN_CODE
col_advance_chunk (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs)
{
  int error;

  for (;;)
    {
      if (cs->stripe_opened && cs->chunk_idx < cs->n_chunks - 1)
	{
	  bool skipped;
	  cs->chunk_idx++;
	  /* the main bitmap is the last pool slot (sized in col_open_stripe) */
	  cs->bitmap = cs->bm_pool[cs->bm_pool_n - 2];
	  error = col_load_chunk (thread_p, cs, &skipped);
	  if (error != NO_ERROR)
	    {
	      return S_ERROR;
	    }
	  if (skipped)
	    {
	      cs->chunk_rows = 0;
	      continue;
	    }
	  return S_SUCCESS;
	}

      /* next stripe */
      cs->stripe_idx++;
      if (cs->stripe_idx >= cs->n_stripes)
	{
	  return S_END;
	}
      if (cs->dir[cs->stripe_idx].row_count <= 0)
	{
	  cs->stripe_opened = false;
	  continue;
	}
      error = col_open_stripe (thread_p, cs);
      if (error != NO_ERROR)
	{
	  return S_ERROR;
	}
      cs->stripe_opened = true;
    }
}

SCAN_CODE
columnar_scan_next (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs)
{
  SCAN_CODE sc;

  for (;;)
    {
      /* count-only fast path: no pages touched at all */
      if (cs->count_only)
	{
	  if (cs->count_rows_left > 0)
	    {
	      cs->count_rows_left--;
	      cs->stats.rows_output++;
	      return S_SUCCESS;
	    }
	  cs->stripe_idx++;
	  if (cs->stripe_idx >= cs->n_stripes)
	    {
	      return S_END;
	    }
	  cs->count_rows_left = cs->dir[cs->stripe_idx].row_count;
	  cs->stats.stripes_read++;
	  continue;
	}

      /* emit the next set bit of the current chunk */
      if (cs->stripe_opened && cs->chunk_idx >= 0 && cs->row_in_chunk < cs->chunk_rows)
	{
	  int r = cs->row_in_chunk;

	  while (r < cs->chunk_rows)
	    {
	      UINT64 word = cs->bitmap[r >> 6] >> (r & 63);
	      if (word == 0)
		{
		  r = ((r >> 6) + 1) << 6;	/* skip to next word */
		  continue;
		}
	      r += __builtin_ctzll (word);
	      break;
	    }

	  if (r < cs->chunk_rows)
	    {
	      cs->row_in_chunk = r + 1;
	      columnar_scan_decode_row (cs, r);
	      cs->stats.rows_output++;
	      return S_SUCCESS;
	    }
	  /* chunk exhausted */
	}

      sc = col_advance_chunk (thread_p, cs);
      if (sc != S_SUCCESS)
	{
	  return sc;
	}
    }
}

/* ========================================================================== */
/* Chunk-level API (#21 Tier 2: BUILDVALUE block aggregation)                 */
/* ========================================================================== */

SCAN_CODE
columnar_scan_next_chunk (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, COLUMNAR_CHUNK_VIEW * view)
{
  SCAN_CODE sc;
  int w;

  assert (!cs->count_only);

  sc = col_advance_chunk (thread_p, cs);
  if (sc != S_SUCCESS)
    {
      return sc;
    }

  view->n_rows = cs->chunk_rows;
  view->n_words = cs->n_words;
  view->bitmap = cs->bitmap;

  /* every qualified row of the chunk is consumed by the block loop */
  for (w = 0; w < cs->n_words; w++)
    {
      cs->stats.rows_output += __builtin_popcountll (cs->bitmap[w]);
    }
  return S_SUCCESS;
}

void
columnar_scan_decode_row (COLUMNAR_SCAN * cs, int row)
{
  int i;

  for (i = 0; i < cs->n_bindings; i++)
    {
      COL_BINDING *b = &cs->bindings[i];
      if (COL_EXISTS_BIT (b->chunk_exists, row))
	{
	  b->decode (b, row, b->slot);
	}
      else
	{
	  col_slot_set_null (b);
	}
    }
}

int
columnar_scan_bind_regu (const COLUMNAR_SCAN * cs, const REGU_VARIABLE * regu)
{
  return col_find_binding (cs, regu);
}

void
columnar_scan_bind_view (const COLUMNAR_SCAN * cs, int bind_idx, COLUMNAR_BIND_VIEW * out)
{
  const COL_BINDING *b = &cs->bindings[bind_idx];

  assert (bind_idx >= 0 && bind_idx < cs->n_bindings);
  out->data = b->chunk_data;
  out->exists = b->chunk_exists;
  out->offsets = b->var_offsets;
  out->stride = b->value_size;
  out->type = b->type;
  out->domain = b->domain;
}

bool
columnar_scan_raw_column (COLUMNAR_SCAN * cs, const DB_VALUE * slot_addr, COLUMNAR_RAW_COL * raw)
{
  int i;

  for (i = 0; i < cs->n_bindings; i++)
    {
      COL_BINDING *b = &cs->bindings[i];
      if (b->slot == slot_addr)
	{
	  raw->data = b->chunk_data;
	  raw->exists = b->chunk_exists;
	  raw->stride = b->value_size;
	  raw->type = b->type;
	  return true;
	}
    }
  return false;
}

bool
columnar_scan_total_rows (COLUMNAR_SCAN * cs, INT64 * total)
{
  INT64 sum = 0;
  int i;

  if (!cs->count_only)
    {
      return false;
    }
  for (i = 0; i < cs->n_stripes; i++)
    {
      sum += cs->dir[i].row_count;
    }
  *total = sum;
  return true;
}

/* ========================================================================== */
/* Raw accumulate kernels (#21 Tier 2a)                                       */
/* ========================================================================== */
/*
 * Each kernel walks the qualified (bitmap) AND non-NULL (exists) rows of one
 * raw column array and accumulates without any DB_VALUE, step program or
 * function dispatch.  Row order matches the interpreted path, so sequential
 * overflow/rounding behavior is identical.  Returns the number of accumulated
 * rows; the caller merges the subtotal into the aggregate accumulator.
 */

int
columnar_raw_sum_int64 (const COLUMNAR_RAW_COL * raw, const UINT64 * bitmap, int n_rows, INT64 * sum_out,
			bool * overflow)
{
  INT64 sum = 0;
  int n_words = (n_rows + 63) / 64;
  int w, cnt = 0;

  *overflow = false;
  for (w = 0; w < n_words; w++)
    {
      UINT64 bits = bitmap[w];
      while (bits)
	{
	  int r = (w << 6) + __builtin_ctzll (bits);
	  bits &= bits - 1;
	  if (!COL_EXISTS_BIT (raw->exists, r))
	    {
	      continue;
	    }
	  {
	    INT64 v;
	    switch (raw->type)
	      {
	      case DB_TYPE_SHORT:
		{
		  short t;
		  memcpy (&t, raw->data + (size_t) r * raw->stride, sizeof (t));
		  v = t;
		}
		break;
	      case DB_TYPE_INTEGER:
		{
		  int t;
		  memcpy (&t, raw->data + (size_t) r * raw->stride, sizeof (t));
		  v = t;
		}
		break;
	      default:		/* DB_TYPE_BIGINT */
		memcpy (&v, raw->data + (size_t) r * raw->stride, sizeof (v));
		break;
	      }
	    if (__builtin_add_overflow (sum, v, &sum))
	      {
		*overflow = true;
		return cnt;
	      }
	  }
	  cnt++;
	}
    }
  *sum_out = sum;
  return cnt;
}

int
columnar_raw_sum_double (const COLUMNAR_RAW_COL * raw, const UINT64 * bitmap, int n_rows, double *sum_out)
{
  double sum = 0.0;
  int n_words = (n_rows + 63) / 64;
  int w, cnt = 0;

  for (w = 0; w < n_words; w++)
    {
      UINT64 bits = bitmap[w];
      while (bits)
	{
	  int r = (w << 6) + __builtin_ctzll (bits);
	  bits &= bits - 1;
	  if (!COL_EXISTS_BIT (raw->exists, r))
	    {
	      continue;
	    }
	  if (raw->type == DB_TYPE_FLOAT)
	    {
	      float t;
	      memcpy (&t, raw->data + (size_t) r * raw->stride, sizeof (t));
	      sum += (double) t;
	    }
	  else			/* DB_TYPE_DOUBLE */
	    {
	      double t;
	      memcpy (&t, raw->data + (size_t) r * raw->stride, sizeof (t));
	      sum += t;
	    }
	  cnt++;
	}
    }
  *sum_out = sum;
  return cnt;
}

int
columnar_raw_count (const COLUMNAR_RAW_COL * raw, const UINT64 * bitmap, int n_rows)
{
  int n_words = (n_rows + 63) / 64;
  int w, cnt = 0;

  for (w = 0; w < n_words; w++)
    {
      UINT64 bits = bitmap[w];
      while (bits)
	{
	  int r = (w << 6) + __builtin_ctzll (bits);
	  bits &= bits - 1;
	  if (COL_EXISTS_BIT (raw->exists, r))
	    {
	      cnt++;
	    }
	}
    }
  return cnt;
}

void
columnar_scan_stats (COLUMNAR_SCAN * cs, COL_SCAN_STATS * out)
{
  if (cs != NULL && out != NULL)
    {
      *out = cs->stats;
    }
}

static void
col_dump_filter (const COL_FILTER_NODE * f, FILE * fp, int indent)
{
  static const char *rel_names[] = { "NONE", "=", "<>", ">", ">=", "<", "<=" };
  const char *rel;

  if (f == NULL)
    {
      return;
    }
  rel = (f->op >= R_EQ && f->op <= R_LE) ? rel_names[f->op] : "?";

  fprintf (fp, "%*s", indent, "");
  switch (f->kind)
    {
    case COL_F_AND:
      fprintf (fp, "AND\n");
      break;
    case COL_F_OR:
      fprintf (fp, "OR\n");
      break;
    case COL_F_NOT:
      fprintf (fp, "NOT\n");
      break;
    case COL_F_CMP:
      fprintf (fp, "CMP col[%d] %s const (kernel %d%s%s)\n", f->bind_idx, rel, (int) f->kernel,
	       f->skip_capable ? ", minmax-skip" : "", f->const_is_null ? ", const NULL" : "");
      break;
    case COL_F_CMPCOL:
      fprintf (fp, "CMPCOL col[%d] %s col[%d] (kernel %d)\n", f->bind_idx, rel, f->bind_idx2, (int) f->kernel);
      break;
    case COL_F_ISNULL:
      fprintf (fp, "ISNULL col[%d]\n", f->bind_idx);
      break;
    case COL_F_IN:
      fprintf (fp, "IN col[%d] (%d elems%s, kernel %d)\n", f->bind_idx, f->n_in,
	       f->in_has_null ? " + NULL" : "", (int) f->kernel);
      break;
    case COL_F_LIKE:
      fprintf (fp, "LIKE col[%d] pattern_len=%d (kernel %d)\n", f->bind_idx, f->cstr_len, (int) f->kernel);
      break;
    }
  col_dump_filter (f->left, fp, indent + 2);
  col_dump_filter (f->right, fp, indent + 2);
}

void
columnar_scan_dump_plan (COLUMNAR_SCAN * cs, FILE * fp)
{
  int i;

  fprintf (fp, "COLUMNAR_COMPILE (class %d|%d|%d)\n", cs->class_oid.volid, cs->class_oid.pageid,
	   cs->class_oid.slotid);
  fprintf (fp, "  bindings: %d\n", cs->n_bindings);
  for (i = 0; i < cs->n_bindings; i++)
    {
      const COL_BINDING *b = &cs->bindings[i];
      fprintf (fp, "    col[%d]: attr %d storage_pos %d type %d stride %d%s\n", i, (int) b->attr_id, b->col_idx,
	       (int) b->type, b->value_size, b->used_in_filter ? " [filter]" : "");
    }
  if (cs->filter != NULL)
    {
      fprintf (fp, "  filter:\n");
      col_dump_filter (cs->filter, fp, 4);
    }
  else
    {
      fprintf (fp, "  filter: none%s\n", cs->count_only ? " (count-only: no page reads)" : "");
    }
}

void
columnar_scan_close (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs)
{
  int i;

  (void) thread_p;

  if (cs == NULL)
    {
      return;
    }
  if (cs->bindings != NULL)
    {
      for (i = 0; i < cs->n_bindings; i++)
	{
	  COL_BINDING *b = &cs->bindings[i];
	  /* slots hold peeked (non-owning) values; reset to NULL for safety */
	  if (b->slot != NULL)
	    {
	      db_make_null (b->slot);
	    }
	  free (b->data_buf);
	  free (b->exists_buf);
	  free (b->comp_buf);
	  free (b->var_offsets);
	}
      free (cs->bindings);
    }
  col_free_filter (cs->filter);
  if (cs->bm_pool != NULL)
    {
      for (i = 0; i < cs->bm_pool_n; i++)
	{
	  free (cs->bm_pool[i]);
	}
      free (cs->bm_pool);
    }
  free (cs->dir);
  free (cs->descs);
  free (cs->stripe_vpids);
  free (cs);
}
