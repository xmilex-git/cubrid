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
 * columnar_numeric.h - columnar NUMERIC: PostgreSQL base-10000 representation
 *
 * Design ticket #23, decisions D6 / D11 / D12.
 *
 * columnar stores and computes NUMERIC in PostgreSQL's (and therefore
 * Citus's) natural format: sign + weight + display scale + a base-10000
 * digit array.  This replaces the engine's 17-byte base-256 sign-magnitude
 * buffer *inside the columnar block executor only*; heap keeps its own
 * representation and semantics untouched.
 *
 * Consequences that are intentional (ADR 0002):
 *   - the on-disk column moves from a fixed 17-byte stride to a
 *     variable-width stream, so the arithmetic representation *is* the disk
 *     representation and the read hot loop performs no conversion at all;
 *   - division and AVG follow PostgreSQL's result-scale rules
 *     (select_div_scale), so they may differ from the heap engine in the
 *     number of fractional digits produced;
 *   - the sign travels inside the value, which is what removes the
 *     sign-loss defect the old fixed-width path had (the sign used to live
 *     outside the 17-byte buffer and was never serialized).
 *
 * Only the columnar block executor may use this module.
 */

#ifndef _COLUMNAR_NUMERIC_H_
#define _COLUMNAR_NUMERIC_H_

#if !defined (SERVER_MODE) && !defined (SA_MODE)
#error Belongs to server module
#endif

#include "dbtype_def.h"
#include "system.h"

#include <stdbool.h>

/* ========================================================================== */
/* Representation                                                             */
/* ========================================================================== */

#define COL_NUM_NBASE       10000
#define COL_NUM_HALF_NBASE  5000
#define COL_NUM_DEC_DIGITS  4	/* decimal digits per base-10000 digit */

#define COL_NUM_POS  0
#define COL_NUM_NEG  1

typedef INT16 COL_NUM_DIGIT;

/*
 * Working buffer size.  DB_MAX_NUMERIC_PRECISION is 40 decimal digits, so a
 * stored value never exceeds 10 base-10000 digits before the point plus its
 * scale.  Intermediates grow: multiplication doubles the digit count and
 * division adds up to COL_NUM_MAX_DISPLAY_SCALE fractional digits plus guard
 * digits.  128 digits = 512 decimal digits covers every intermediate the
 * bounded operand precision can produce, so every variable is a fixed-size
 * struct and no step ever allocates (D1: step-owned scratch).
 */
#define COL_NUM_MAX_NDIGITS  128

/* PostgreSQL's NUMERIC_MIN_SIG_DIGITS / display-scale bounds for division.
 * The maximum is clamped far below PostgreSQL's 1000 so that a quotient
 * always fits COL_NUM_MAX_NDIGITS. */
#define COL_NUM_MIN_SIG_DIGITS      16
#define COL_NUM_MIN_DISPLAY_SCALE   0
#define COL_NUM_MAX_DISPLAY_SCALE   100

typedef struct col_numvar COL_NUMVAR;
struct col_numvar
{
  int ndigits;			/* digits in use */
  int weight;			/* base-10000 weight of digits[0] */
  int sign;			/* COL_NUM_POS / COL_NUM_NEG */
  int dscale;			/* display scale (decimal digits after point) */
  COL_NUM_DIGIT digits[COL_NUM_MAX_NDIGITS];
};

/*
 * Read-only view over a serialized value sitting in a chunk buffer.  The
 * digit array is referenced in place — decoding a stored NUMERIC costs no
 * copy and no conversion (D11).
 */
typedef struct col_numref COL_NUMREF;
struct col_numref
{
  int ndigits;
  int weight;
  int sign;
  int dscale;
  const COL_NUM_DIGIT *digits;
};

/* ========================================================================== */
/* On-disk format                                                             */
/* ========================================================================== */
/*
 * Serialized payload, little-endian host order like every other columnar
 * column:
 *
 *   INT16 ndigits
 *   INT16 weight
 *   INT16 sign
 *   INT16 dscale
 *   INT16 digits[ndigits]
 *
 * The payload is written into the variable-width stream behind the usual
 * 4-byte length prefix, so the reader's per-row offset table and every other
 * variable-width mechanism apply unchanged.  All payload offsets are even,
 * so the digit array is naturally aligned.
 */
#define COL_NUM_HEADER_BYTES  (4 * (int) sizeof (INT16))

#define COL_NUM_DISK_SIZE(ndigits)  (COL_NUM_HEADER_BYTES + (ndigits) * (int) sizeof (COL_NUM_DIGIT))

/* ========================================================================== */
/* Serialization                                                              */
/* ========================================================================== */

/* bytes columnar_num_serialize () will write for v */
extern int columnar_num_serialized_size (const COL_NUMVAR * v);

/* write v at dst; returns the number of bytes written */
extern int columnar_num_serialize (const COL_NUMVAR * v, char *dst);

/* view a serialized payload in place (no copy) */
extern void columnar_num_ref (const char *src, COL_NUMREF * out);

/* materialize a view into a working variable */
extern void columnar_num_from_ref (const COL_NUMREF * src, COL_NUMVAR * out);

/* ========================================================================== */
/* Conversion to / from the engine representation                             */
/* ========================================================================== */

/* DB_VALUE of type DB_TYPE_NUMERIC -> working variable.  Exact. */
extern int columnar_num_from_dbvalue (const DB_VALUE * val, COL_NUMVAR * out);

/*
 * Working variable -> DB_VALUE of type DB_TYPE_NUMERIC with the given
 * precision and scale.  Rounds half-up to scale (PostgreSQL semantics) and
 * raises ER_IT_DATA_OVERFLOW when the rounded value does not fit precision.
 */
extern int columnar_num_to_dbvalue (const COL_NUMVAR * v, int precision, int scale, DB_VALUE * out);

/*
 * As above, but marks the result a FLOATING-SCALE numeric when float_numeric
 * is true — precision and scale then live in the DB_VALUE's numeric header
 * rather than its domain.  That is the representation the engine's aggregate
 * accumulators use for a NUMERIC SUM/AVG (see numeric_sum_state_result ()),
 * and handing those consumers a fixed-scale value instead makes them read a
 * scale of zero.
 */
extern int columnar_num_to_dbvalue_ex (const COL_NUMVAR * v, int precision, int scale, bool float_numeric,
				       DB_VALUE * out);

/* decimal digits of the unscaled integer (the value's natural precision) */
extern int columnar_num_decimal_digits (const COL_NUMVAR * v);

/* exact double approximation (min/max bookkeeping, coercion to DOUBLE) */
extern double columnar_num_to_double (const COL_NUMREF * v);

/*
 * Unscaled integer of v at the given display scale, i.e. v * 10^dscale.
 * Returns false when the value is not exactly representable at that scale
 * or does not fit an INT64 — used by the chunk min/max skip, which must be
 * exact to stay sound.
 */
extern bool columnar_num_unscaled_int64 (const COL_NUMREF * v, int dscale, INT64 * out);

/* ========================================================================== */
/* Comparison and arithmetic (PostgreSQL numeric.c semantics — D12)           */
/* ========================================================================== */

extern int columnar_num_cmp (const COL_NUMREF * a, const COL_NUMREF * b);
extern bool columnar_num_is_zero (const COL_NUMREF * a);

extern void columnar_num_set_zero (COL_NUMVAR * v);
extern int columnar_num_set_int64 (COL_NUMVAR * v, INT64 iv, int dscale);

extern int columnar_num_add (const COL_NUMREF * a, const COL_NUMREF * b, COL_NUMVAR * res);
extern int columnar_num_sub (const COL_NUMREF * a, const COL_NUMREF * b, COL_NUMVAR * res);
extern int columnar_num_mul (const COL_NUMREF * a, const COL_NUMREF * b, COL_NUMVAR * res);
extern int columnar_num_div (const COL_NUMREF * a, const COL_NUMREF * b, COL_NUMVAR * res);

/* divide with an explicit result scale (AVG: sum / count at the sum's scale) */
extern int columnar_num_div_rscale (const COL_NUMREF * a, const COL_NUMREF * b, int rscale, COL_NUMVAR * res);

/* round in place to the given display scale */
extern void columnar_num_round (COL_NUMVAR * v, int rscale);

/* view a working variable without copying */
extern void columnar_num_ref_var (const COL_NUMVAR * v, COL_NUMREF * out);

#endif /* _COLUMNAR_NUMERIC_H_ */
