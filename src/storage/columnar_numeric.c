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
 * columnar_numeric.c - columnar NUMERIC: PostgreSQL base-10000 representation
 *
 * Design ticket #23, decisions D6 / D11 / D12.  The algorithms below are a
 * port of PostgreSQL's src/backend/utils/adt/numeric.c (add_var, sub_var,
 * mul_var, div_var, cmp_var, round_var, trunc_var, strip_var,
 * select_div_scale) onto fixed-size digit buffers: the operand precision is
 * bounded by DB_MAX_NUMERIC_PRECISION, so every intermediate fits
 * COL_NUM_MAX_NDIGITS and no operation allocates.
 */

#ident "$Id$"

#include "columnar_numeric.h"

#include "dbtype.h"
#include "error_manager.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>

#define COL_NUM_MUL_GUARD_DIGITS  2
#define COL_NUM_DIV_GUARD_DIGITS  4

#define COL_NUM_MAX(a, b)  ((a) > (b) ? (a) : (b))
#define COL_NUM_MIN(a, b)  ((a) < (b) ? (a) : (b))

/* 10^0 .. 10^19 — the largest power of ten that fits UINT64 */
static const UINT64 col_num_pow10_u64[20] = {
  UINT64_C (1), UINT64_C (10), UINT64_C (100), UINT64_C (1000),
  UINT64_C (10000), UINT64_C (100000), UINT64_C (1000000), UINT64_C (10000000),
  UINT64_C (100000000), UINT64_C (1000000000), UINT64_C (10000000000), UINT64_C (100000000000),
  UINT64_C (1000000000000), UINT64_C (10000000000000), UINT64_C (100000000000000),
  UINT64_C (1000000000000000), UINT64_C (10000000000000000), UINT64_C (100000000000000000),
  UINT64_C (1000000000000000000), UINT64_C (10000000000000000000)
};

/* divisors used when rounding/truncating inside the last base-10000 digit */
static const int col_num_round_powers[COL_NUM_DEC_DIGITS] = { 0, 1000, 100, 10 };

static int col_num_overflow (void);
static void col_num_strip (COL_NUMVAR * v);
static int col_num_cmp_abs (const COL_NUMREF * a, const COL_NUMREF * b);
static int col_num_add_abs (const COL_NUMREF * a, const COL_NUMREF * b, COL_NUMVAR * res);
static int col_num_sub_abs (const COL_NUMREF * a, const COL_NUMREF * b, COL_NUMVAR * res);
static int col_num_add_signed (const COL_NUMREF * a, const COL_NUMREF * b, int bsign, COL_NUMVAR * res);
static int col_num_select_div_scale (const COL_NUMREF * a, const COL_NUMREF * b);

/* ========================================================================== */
/* Errors                                                                     */
/* ========================================================================== */

static int
col_num_overflow (void)
{
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IT_DATA_OVERFLOW, 1, "numeric");
  return ER_IT_DATA_OVERFLOW;
}

/* ========================================================================== */
/* Small helpers                                                              */
/* ========================================================================== */

void
columnar_num_set_zero (COL_NUMVAR * v)
{
  v->ndigits = 0;
  v->weight = 0;
  v->sign = COL_NUM_POS;
  v->dscale = 0;
}

void
columnar_num_ref_var (const COL_NUMVAR * v, COL_NUMREF * out)
{
  out->ndigits = v->ndigits;
  out->weight = v->weight;
  out->sign = v->sign;
  out->dscale = v->dscale;
  out->digits = v->digits;
}

bool
columnar_num_is_zero (const COL_NUMREF * a)
{
  return a->ndigits == 0;
}

/*
 * col_num_strip () - PostgreSQL strip_var (): drop leading and trailing zero
 *   digits, normalizing the sign and weight of a zero.  Leading digits are
 *   moved rather than re-based, since the digit array is inline.
 */
static void
col_num_strip (COL_NUMVAR * v)
{
  int lead = 0;

  while (lead < v->ndigits && v->digits[lead] == 0)
    {
      lead++;
    }
  if (lead > 0)
    {
      v->ndigits -= lead;
      v->weight -= lead;
      if (v->ndigits > 0)
	{
	  memmove (v->digits, v->digits + lead, (size_t) v->ndigits * sizeof (COL_NUM_DIGIT));
	}
    }

  while (v->ndigits > 0 && v->digits[v->ndigits - 1] == 0)
    {
      v->ndigits--;
    }

  if (v->ndigits == 0)
    {
      v->sign = COL_NUM_POS;
      v->weight = 0;
    }
}

/*
 * columnar_num_round () - PostgreSQL round_var (): round half-away-from-zero
 *   to rscale fractional decimal digits, in place.
 */
void
columnar_num_round (COL_NUMVAR * v, int rscale)
{
  COL_NUM_DIGIT *digits = v->digits;
  int di, ndigits, carry;

  v->dscale = rscale;

  di = (v->weight + 1) * COL_NUM_DEC_DIGITS + rscale;

  if (di < 0)
    {
      v->ndigits = 0;
      v->weight = 0;
      v->sign = COL_NUM_POS;
      return;
    }

  ndigits = (di + COL_NUM_DEC_DIGITS - 1) / COL_NUM_DEC_DIGITS;
  di %= COL_NUM_DEC_DIGITS;

  if (ndigits > v->ndigits || (ndigits == v->ndigits && di == 0))
    {
      return;			/* nothing to drop */
    }

  v->ndigits = ndigits;

  if (di == 0)
    {
      carry = (digits[ndigits] >= COL_NUM_NBASE / 2) ? 1 : 0;
    }
  else
    {
      int extra, pow10 = col_num_round_powers[di];

      ndigits--;
      extra = digits[ndigits] % pow10;
      digits[ndigits] = (COL_NUM_DIGIT) (digits[ndigits] - extra);
      carry = 0;
      if (extra >= pow10 / 2)
	{
	  pow10 += digits[ndigits];
	  if (pow10 >= COL_NUM_NBASE)
	    {
	      pow10 -= COL_NUM_NBASE;
	      carry = 1;
	    }
	  digits[ndigits] = (COL_NUM_DIGIT) pow10;
	}
    }

  while (carry != 0 && ndigits > 0)
    {
      ndigits--;
      carry += digits[ndigits];
      if (carry >= COL_NUM_NBASE)
	{
	  digits[ndigits] = (COL_NUM_DIGIT) (carry - COL_NUM_NBASE);
	  carry = 1;
	}
      else
	{
	  digits[ndigits] = (COL_NUM_DIGIT) carry;
	  carry = 0;
	}
    }

  if (carry != 0)
    {
      /* carried out of the most significant digit: prepend it.  PostgreSQL
       * keeps a spare slot before digits[0]; the inline array shifts right
       * instead (at most one digit, and only on a full-carry round). */
      assert (carry == 1);
      if (v->ndigits + 1 > COL_NUM_MAX_NDIGITS)
	{
	  v->ndigits--;		/* drop the least significant digit; it is zero
				 * after a full carry-out */
	}
      memmove (v->digits + 1, v->digits, (size_t) v->ndigits * sizeof (COL_NUM_DIGIT));
      v->digits[0] = 1;
      v->ndigits++;
      v->weight++;
    }

  col_num_strip (v);
}

/* ========================================================================== */
/* Serialization                                                              */
/* ========================================================================== */

int
columnar_num_serialized_size (const COL_NUMVAR * v)
{
  return COL_NUM_DISK_SIZE (v->ndigits);
}

int
columnar_num_serialize (const COL_NUMVAR * v, char *dst)
{
  INT16 hdr[4];

  assert (v->ndigits >= 0 && v->ndigits <= COL_NUM_MAX_NDIGITS);

  hdr[0] = (INT16) v->ndigits;
  hdr[1] = (INT16) v->weight;
  hdr[2] = (INT16) v->sign;
  hdr[3] = (INT16) v->dscale;
  memcpy (dst, hdr, sizeof (hdr));
  if (v->ndigits > 0)
    {
      memcpy (dst + COL_NUM_HEADER_BYTES, v->digits, (size_t) v->ndigits * sizeof (COL_NUM_DIGIT));
    }
  return COL_NUM_DISK_SIZE (v->ndigits);
}

void
columnar_num_ref (const char *src, COL_NUMREF * out)
{
  INT16 hdr[4];

  memcpy (hdr, src, sizeof (hdr));
  out->ndigits = hdr[0];
  out->weight = hdr[1];
  out->sign = hdr[2];
  out->dscale = hdr[3];
  out->digits = (const COL_NUM_DIGIT *) (src + COL_NUM_HEADER_BYTES);
}

void
columnar_num_from_ref (const COL_NUMREF * src, COL_NUMVAR * out)
{
  assert (src->ndigits >= 0 && src->ndigits <= COL_NUM_MAX_NDIGITS);
  out->ndigits = src->ndigits;
  out->weight = src->weight;
  out->sign = src->sign;
  out->dscale = src->dscale;
  if (src->ndigits > 0)
    {
      memcpy (out->digits, src->digits, (size_t) src->ndigits * sizeof (COL_NUM_DIGIT));
    }
}

/* ========================================================================== */
/* Bignum helpers over 32-bit limbs (engine representation bridge)            */
/* ========================================================================== */
/*
 * Limbs are base 2^32, least significant first.  Only the conversion between
 * the engine's 17-byte big-endian magnitude and the base-10000 digit array
 * needs them; the arithmetic itself never leaves base 10000.
 */
#define COL_BN_LIMBS  10

static bool
col_bn_is_zero (const UINT32 * w, int n)
{
  int i;
  for (i = 0; i < n; i++)
    {
      if (w[i] != 0)
	{
	  return false;
	}
    }
  return true;
}

/* w = w * m + add; false on limb overflow */
static bool
col_bn_mul_add_small (UINT32 * w, int n, UINT32 m, UINT32 add)
{
  UINT64 carry = add;
  int i;

  for (i = 0; i < n; i++)
    {
      UINT64 cur = (UINT64) w[i] * m + carry;
      w[i] = (UINT32) cur;
      carry = cur >> 32;
    }
  return carry == 0;
}

/* w = w / d, returns the remainder */
static UINT32
col_bn_divmod_small (UINT32 * w, int n, UINT32 d)
{
  UINT64 rem = 0;
  int i;

  for (i = n - 1; i >= 0; i--)
    {
      UINT64 cur = (rem << 32) | w[i];
      w[i] = (UINT32) (cur / d);
      rem = cur % d;
    }
  return (UINT32) rem;
}

/* ========================================================================== */
/* Engine representation bridge                                               */
/* ========================================================================== */

int
columnar_num_from_dbvalue (const DB_VALUE * val, COL_NUMVAR * out)
{
  const unsigned char *mag;
  UINT32 w[COL_BN_LIMBS];
  COL_NUM_DIGIT rev[COL_NUM_MAX_NDIGITS];
  int scale, pad, n = 0, i;

  assert (DB_VALUE_DOMAIN_TYPE (val) == DB_TYPE_NUMERIC);

  scale = val->domain.numeric_info.scale;
  mag = (const unsigned char *) db_get_numeric (val);

  /* 17 big-endian magnitude bytes -> little-endian 32-bit limbs */
  memset (w, 0, sizeof (w));
  for (i = 0; i < DB_NUMERIC_BUF_SIZE; i++)
    {
      int limb = (DB_NUMERIC_BUF_SIZE - 1 - i) >> 2;
      int shift = ((DB_NUMERIC_BUF_SIZE - 1 - i) & 3) * 8;
      w[limb] |= ((UINT32) mag[i]) << shift;
    }

  /*
   * The value is magnitude * 10^-scale.  Scale the magnitude up so that the
   * total decimal scale becomes a multiple of DEC_DIGITS; then the base-10000
   * digits line up with whole base-10000 weights.
   */
  pad = (COL_NUM_DEC_DIGITS - (scale % COL_NUM_DEC_DIGITS)) % COL_NUM_DEC_DIGITS;
  if (pad > 0)
    {
      if (!col_bn_mul_add_small (w, COL_BN_LIMBS, (UINT32) col_num_pow10_u64[pad], 0))
	{
	  return col_num_overflow ();
	}
    }

  while (!col_bn_is_zero (w, COL_BN_LIMBS))
    {
      if (n >= COL_NUM_MAX_NDIGITS)
	{
	  return col_num_overflow ();
	}
      rev[n++] = (COL_NUM_DIGIT) col_bn_divmod_small (w, COL_BN_LIMBS, COL_NUM_NBASE);
    }

  /* rev[] is least significant first; the variable wants most significant first */
  for (i = 0; i < n; i++)
    {
      out->digits[i] = rev[n - 1 - i];
    }
  out->ndigits = n;
  out->weight = n - 1 - (scale + pad) / COL_NUM_DEC_DIGITS;
  out->sign = val->domain.numeric_info.is_value_negative ? COL_NUM_NEG : COL_NUM_POS;
  out->dscale = scale;

  col_num_strip (out);
  return NO_ERROR;
}

int
columnar_num_decimal_digits (const COL_NUMVAR * v)
{
  int digits;

  if (v->ndigits == 0)
    {
      return 1;
    }
  /* full base-10000 digits below the leading one, plus the leading digit's own
   * decimal width, plus whatever the display scale adds beyond them */
  digits = (v->weight) * COL_NUM_DEC_DIGITS;
  if (v->digits[0] >= 1000)
    {
      digits += 4;
    }
  else if (v->digits[0] >= 100)
    {
      digits += 3;
    }
  else if (v->digits[0] >= 10)
    {
      digits += 2;
    }
  else
    {
      digits += 1;
    }
  if (digits < 1)
    {
      digits = 1;
    }
  digits += v->dscale;
  return (digits > DB_MAX_NUMERIC_PRECISION) ? DB_MAX_NUMERIC_PRECISION : digits;
}

int
columnar_num_to_dbvalue (const COL_NUMVAR * v, int precision, int scale, DB_VALUE * out)
{
  return columnar_num_to_dbvalue_ex (v, precision, scale, false, out);
}

int
columnar_num_to_dbvalue_ex (const COL_NUMVAR * v, int precision, int scale, bool float_numeric, DB_VALUE * out)
{
  COL_NUMVAR r;
  UINT32 w[COL_BN_LIMBS];
  unsigned char mag[DB_NUMERIC_BUF_SIZE];
  int shift, i;

  r = *v;
  /* unconditional: rounding to the target scale is what makes the "no nonzero
   * digit below dscale" invariant the byte extraction below relies on hold */
  columnar_num_round (&r, scale);

  if (r.ndigits == 0)
    {
      memset (mag, 0, sizeof (mag));
      db_make_numeric (out, (DB_C_NUMERIC) mag, precision, scale, DB_NUMERIC_BUF_SIZE, false, float_numeric);
      return NO_ERROR;
    }

  /*
   * Unscaled integer = digits * 10^shift with
   *   shift = DEC_DIGITS * (weight - ndigits + 1) + scale
   * (negative shift only ever drops digits rounding already zeroed).
   */
  shift = COL_NUM_DEC_DIGITS * (r.weight - r.ndigits + 1) + scale;

  memset (w, 0, sizeof (w));
  for (i = 0; i < r.ndigits; i++)
    {
      if (!col_bn_mul_add_small (w, COL_BN_LIMBS, COL_NUM_NBASE, (UINT32) r.digits[i]))
	{
	  return col_num_overflow ();
	}
    }

  while (shift > 0)
    {
      int step = COL_NUM_MIN (shift, 9);
      if (!col_bn_mul_add_small (w, COL_BN_LIMBS, (UINT32) col_num_pow10_u64[step], 0))
	{
	  return col_num_overflow ();
	}
      shift -= step;
    }
  while (shift < 0)
    {
      int step = COL_NUM_MIN (-shift, 9);
      col_bn_divmod_small (w, COL_BN_LIMBS, (UINT32) col_num_pow10_u64[step]);
      shift += step;
    }

  /* must fit the declared precision: |unscaled| < 10^precision */
  {
    UINT32 lim[COL_BN_LIMBS];
    int p = precision;

    memset (lim, 0, sizeof (lim));
    lim[0] = 1;
    while (p > 0)
      {
	int step = COL_NUM_MIN (p, 9);
	if (!col_bn_mul_add_small (lim, COL_BN_LIMBS, (UINT32) col_num_pow10_u64[step], 0))
	  {
	    break;		/* 10^precision exceeds the limb buffer: no value can reach it */
	  }
	p -= step;
      }
    if (p == 0)
      {
	for (i = COL_BN_LIMBS - 1; i >= 0; i--)
	  {
	    if (w[i] != lim[i])
	      {
		if (w[i] > lim[i])
		  {
		    return col_num_overflow ();
		  }
		break;
	      }
	    if (i == 0)
	      {
		return col_num_overflow ();	/* exactly 10^precision */
	      }
	  }
      }
  }

  /* limbs -> 17 big-endian magnitude bytes */
  for (i = 0; i < DB_NUMERIC_BUF_SIZE; i++)
    {
      int limb = (DB_NUMERIC_BUF_SIZE - 1 - i) >> 2;
      int sh = ((DB_NUMERIC_BUF_SIZE - 1 - i) & 3) * 8;
      mag[i] = (unsigned char) ((limb < COL_BN_LIMBS) ? ((w[limb] >> sh) & 0xff) : 0);
    }
  /* anything above the 17th byte would not fit the engine buffer */
  for (i = (DB_NUMERIC_BUF_SIZE + 3) / 4; i < COL_BN_LIMBS; i++)
    {
      if (w[i] != 0)
	{
	  return col_num_overflow ();
	}
    }
  if ((w[4] >> 8) != 0)
    {
      return col_num_overflow ();	/* byte 17 is the low byte of limb 4 */
    }

  db_make_numeric (out, (DB_C_NUMERIC) mag, precision, scale, DB_NUMERIC_BUF_SIZE,
		   r.sign == COL_NUM_NEG, float_numeric);
  return NO_ERROR;
}

double
columnar_num_to_double (const COL_NUMREF * v)
{
  double acc = 0.0;
  int i;

  for (i = 0; i < v->ndigits; i++)
    {
      acc = acc * COL_NUM_NBASE + v->digits[i];
    }
  /* acc currently holds digits as an integer with weight ndigits-1 */
  {
    int exp10 = COL_NUM_DEC_DIGITS * (v->weight - v->ndigits + 1);
    while (exp10 > 0)
      {
	int step = COL_NUM_MIN (exp10, 19);
	acc *= (double) col_num_pow10_u64[step];
	exp10 -= step;
      }
    while (exp10 < 0)
      {
	int step = COL_NUM_MIN (-exp10, 19);
	acc /= (double) col_num_pow10_u64[step];
	exp10 += step;
      }
  }
  return (v->sign == COL_NUM_NEG) ? -acc : acc;
}

bool
columnar_num_unscaled_int64 (const COL_NUMREF * v, int dscale, INT64 * out)
{
  UINT64 acc = 0;
  int shift, i;

  if (v->ndigits == 0)
    {
      *out = 0;
      return true;
    }

  shift = COL_NUM_DEC_DIGITS * (v->weight - v->ndigits + 1) + dscale;

  for (i = 0; i < v->ndigits; i++)
    {
      if (acc > (UINT64_MAX - (UINT64) v->digits[i]) / COL_NUM_NBASE)
	{
	  return false;
	}
      acc = acc * COL_NUM_NBASE + (UINT64) v->digits[i];
    }
  while (shift > 0)
    {
      int step = COL_NUM_MIN (shift, 19);
      UINT64 mul = col_num_pow10_u64[step];
      if (acc != 0 && acc > UINT64_MAX / mul)
	{
	  return false;
	}
      acc *= mul;
      shift -= step;
    }
  /*
   * A negative shift means the digit array carries decimals below the
   * requested scale.  strip_var () only drops whole base-10000 digits, so
   * this is the common case (0.12 is stored as digit 1200 at weight -1);
   * it stays exact as long as those low decimals are zero.
   */
  while (shift < 0)
    {
      int step = COL_NUM_MIN (-shift, 19);
      UINT64 div = col_num_pow10_u64[step];
      if (acc % div != 0)
	{
	  return false;
	}
      acc /= div;
      shift += step;
    }

  /* acc is the unsigned magnitude, so the negative range reaches one further
   * than the positive one: -2^63 is representable, +2^63 is not */
  if (v->sign == COL_NUM_NEG)
    {
      if (acc > (UINT64) INT64_MAX + 1)
	{
	  return false;
	}
      *out = (acc == (UINT64) INT64_MAX + 1) ? INT64_MIN : -(INT64) acc;
    }
  else
    {
      if (acc > (UINT64) INT64_MAX)
	{
	  return false;
	}
      *out = (INT64) acc;
    }
  return true;
}

int
columnar_num_set_int64 (COL_NUMVAR * v, INT64 iv, int dscale)
{
  UINT64 mag;
  COL_NUM_DIGIT rev[COL_NUM_MAX_NDIGITS];
  int n = 0, pad, i;

  columnar_num_set_zero (v);
  v->dscale = dscale;
  if (iv == 0)
    {
      return NO_ERROR;
    }

  v->sign = (iv < 0) ? COL_NUM_NEG : COL_NUM_POS;
  mag = (iv < 0) ? (UINT64) (-(iv + 1)) + 1 : (UINT64) iv;

  pad = (COL_NUM_DEC_DIGITS - (dscale % COL_NUM_DEC_DIGITS)) % COL_NUM_DEC_DIGITS;
  for (i = 0; i < pad; i++)
    {
      if (mag > UINT64_MAX / 10)
	{
	  return col_num_overflow ();
	}
      mag *= 10;
    }

  while (mag != 0)
    {
      if (n >= COL_NUM_MAX_NDIGITS)
	{
	  return col_num_overflow ();
	}
      rev[n++] = (COL_NUM_DIGIT) (mag % COL_NUM_NBASE);
      mag /= COL_NUM_NBASE;
    }
  for (i = 0; i < n; i++)
    {
      v->digits[i] = rev[n - 1 - i];
    }
  v->ndigits = n;
  v->weight = n - 1 - (dscale + pad) / COL_NUM_DEC_DIGITS;
  col_num_strip (v);
  return NO_ERROR;
}

/* ========================================================================== */
/* Comparison — PostgreSQL cmp_var () / cmp_abs ()                            */
/* ========================================================================== */

static int
col_num_cmp_abs (const COL_NUMREF * a, const COL_NUMREF * b)
{
  int i1 = 0, i2 = 0;
  int w1 = a->weight, w2 = b->weight;

  while (w1 > w2 && i1 < a->ndigits)
    {
      if (a->digits[i1++] != 0)
	{
	  return 1;
	}
      w1--;
    }
  while (w2 > w1 && i2 < b->ndigits)
    {
      if (b->digits[i2++] != 0)
	{
	  return -1;
	}
      w2--;
    }

  if (w1 == w2)
    {
      while (i1 < a->ndigits && i2 < b->ndigits)
	{
	  int stat = a->digits[i1++] - b->digits[i2++];
	  if (stat != 0)
	    {
	      return (stat > 0) ? 1 : -1;
	    }
	}
    }

  while (i1 < a->ndigits)
    {
      if (a->digits[i1++] != 0)
	{
	  return 1;
	}
    }
  while (i2 < b->ndigits)
    {
      if (b->digits[i2++] != 0)
	{
	  return -1;
	}
    }
  return 0;
}

int
columnar_num_cmp (const COL_NUMREF * a, const COL_NUMREF * b)
{
  if (a->ndigits == 0)
    {
      if (b->ndigits == 0)
	{
	  return 0;
	}
      return (b->sign == COL_NUM_NEG) ? 1 : -1;
    }
  if (b->ndigits == 0)
    {
      return (a->sign == COL_NUM_POS) ? 1 : -1;
    }
  if (a->sign == COL_NUM_POS)
    {
      if (b->sign == COL_NUM_NEG)
	{
	  return 1;
	}
      return col_num_cmp_abs (a, b);
    }
  if (b->sign == COL_NUM_POS)
    {
      return -1;
    }
  return -col_num_cmp_abs (a, b);
}

/* ========================================================================== */
/* Addition / subtraction — PostgreSQL add_var () / sub_var ()                */
/* ========================================================================== */

static int
col_num_add_abs (const COL_NUMREF * a, const COL_NUMREF * b, COL_NUMVAR * res)
{
  int res_weight, res_rscale, res_ndigits, rscale1, rscale2;
  int i, i1, i2, carry = 0;

  res_weight = COL_NUM_MAX (a->weight, b->weight) + 1;
  rscale1 = a->ndigits - a->weight - 1;
  rscale2 = b->ndigits - b->weight - 1;
  res_rscale = COL_NUM_MAX (rscale1, rscale2);

  res_ndigits = res_rscale + res_weight + 1;
  if (res_ndigits <= 0)
    {
      res_ndigits = 1;
    }
  if (res_ndigits > COL_NUM_MAX_NDIGITS)
    {
      return col_num_overflow ();
    }

  i1 = res_rscale + a->weight + 1;
  i2 = res_rscale + b->weight + 1;
  for (i = res_ndigits - 1; i >= 0; i--)
    {
      i1--;
      i2--;
      if (i1 >= 0 && i1 < a->ndigits)
	{
	  carry += a->digits[i1];
	}
      if (i2 >= 0 && i2 < b->ndigits)
	{
	  carry += b->digits[i2];
	}
      if (carry >= COL_NUM_NBASE)
	{
	  res->digits[i] = (COL_NUM_DIGIT) (carry - COL_NUM_NBASE);
	  carry = 1;
	}
      else
	{
	  res->digits[i] = (COL_NUM_DIGIT) carry;
	  carry = 0;
	}
    }
  assert (carry == 0);

  res->ndigits = res_ndigits;
  res->weight = res_weight;
  res->dscale = COL_NUM_MAX (a->dscale, b->dscale);
  col_num_strip (res);
  return NO_ERROR;
}

/* |a| - |b|; the caller guarantees |a| >= |b| */
static int
col_num_sub_abs (const COL_NUMREF * a, const COL_NUMREF * b, COL_NUMVAR * res)
{
  int res_weight, res_rscale, res_ndigits, rscale1, rscale2;
  int i, i1, i2, borrow = 0;

  res_weight = a->weight;
  rscale1 = a->ndigits - a->weight - 1;
  rscale2 = b->ndigits - b->weight - 1;
  res_rscale = COL_NUM_MAX (rscale1, rscale2);

  res_ndigits = res_rscale + res_weight + 1;
  if (res_ndigits <= 0)
    {
      res_ndigits = 1;
    }
  if (res_ndigits > COL_NUM_MAX_NDIGITS)
    {
      return col_num_overflow ();
    }

  i1 = res_rscale + a->weight + 1;
  i2 = res_rscale + b->weight + 1;
  for (i = res_ndigits - 1; i >= 0; i--)
    {
      i1--;
      i2--;
      if (i1 >= 0 && i1 < a->ndigits)
	{
	  borrow += a->digits[i1];
	}
      if (i2 >= 0 && i2 < b->ndigits)
	{
	  borrow -= b->digits[i2];
	}
      if (borrow < 0)
	{
	  res->digits[i] = (COL_NUM_DIGIT) (borrow + COL_NUM_NBASE);
	  borrow = -1;
	}
      else
	{
	  res->digits[i] = (COL_NUM_DIGIT) borrow;
	  borrow = 0;
	}
    }
  assert (borrow == 0);

  res->ndigits = res_ndigits;
  res->weight = res_weight;
  res->dscale = COL_NUM_MAX (a->dscale, b->dscale);
  col_num_strip (res);
  return NO_ERROR;
}

/* a + (b with sign forced to bsign) */
static int
col_num_add_signed (const COL_NUMREF * a, const COL_NUMREF * b, int bsign, COL_NUMVAR * res)
{
  COL_NUMREF bb = *b;
  int error;

  bb.sign = bsign;

  if (a->sign == bb.sign)
    {
      error = col_num_add_abs (a, &bb, res);
      if (error == NO_ERROR)
	{
	  res->sign = (res->ndigits == 0) ? COL_NUM_POS : a->sign;
	}
      return error;
    }

  switch (col_num_cmp_abs (a, &bb))
    {
    case 0:
      columnar_num_set_zero (res);
      res->dscale = COL_NUM_MAX (a->dscale, bb.dscale);
      return NO_ERROR;
    case 1:
      error = col_num_sub_abs (a, &bb, res);
      if (error == NO_ERROR)
	{
	  res->sign = (res->ndigits == 0) ? COL_NUM_POS : a->sign;
	}
      return error;
    default:
      error = col_num_sub_abs (&bb, a, res);
      if (error == NO_ERROR)
	{
	  res->sign = (res->ndigits == 0) ? COL_NUM_POS : bb.sign;
	}
      return error;
    }
}

int
columnar_num_add (const COL_NUMREF * a, const COL_NUMREF * b, COL_NUMVAR * res)
{
  return col_num_add_signed (a, b, b->sign, res);
}

int
columnar_num_sub (const COL_NUMREF * a, const COL_NUMREF * b, COL_NUMVAR * res)
{
  return col_num_add_signed (a, b, (b->sign == COL_NUM_POS) ? COL_NUM_NEG : COL_NUM_POS, res);
}

/* ========================================================================== */
/* Multiplication — PostgreSQL mul_var ()                                     */
/* ========================================================================== */

int
columnar_num_mul (const COL_NUMREF * a, const COL_NUMREF * b, COL_NUMVAR * res)
{
  int rscale = a->dscale + b->dscale;
  int res_ndigits, res_weight, res_sign, maxdigits;
  int dig[COL_NUM_MAX_NDIGITS];
  int i, i1, i2, carry, newdig;

  if (a->ndigits == 0 || b->ndigits == 0)
    {
      columnar_num_set_zero (res);
      res->dscale = rscale;
      return NO_ERROR;
    }

  res_sign = (a->sign == b->sign) ? COL_NUM_POS : COL_NUM_NEG;
  res_weight = a->weight + b->weight + 2;

  res_ndigits = a->ndigits + b->ndigits + 1;
  maxdigits = res_weight + 1 + (rscale + COL_NUM_DEC_DIGITS - 1) / COL_NUM_DEC_DIGITS + COL_NUM_MUL_GUARD_DIGITS;
  res_ndigits = COL_NUM_MIN (res_ndigits, maxdigits);

  if (res_ndigits < 3)
    {
      columnar_num_set_zero (res);
      res->dscale = rscale;
      return NO_ERROR;
    }
  if (res_ndigits > COL_NUM_MAX_NDIGITS)
    {
      return col_num_overflow ();
    }

  /*
   * Accumulate into 32-bit slots.  The operand digit count is bounded by
   * COL_NUM_MAX_NDIGITS, and 128 * 9999 * 9999 is far below INT_MAX, so
   * PostgreSQL's intermediate renormalization pass cannot be needed here.
   */
  memset (dig, 0, (size_t) res_ndigits * sizeof (int));
  for (i1 = a->ndigits - 1; i1 >= 0; i1--)
    {
      int adigit = a->digits[i1];

      if (adigit == 0)
	{
	  continue;
	}
      i2 = COL_NUM_MIN (b->ndigits - 1, res_ndigits - i1 - 3);
      i = i1 + i2 + 2;
      for (; i2 >= 0; i2--)
	{
	  dig[i--] += adigit * b->digits[i2];
	}
    }

  carry = 0;
  for (i = res_ndigits - 1; i >= 0; i--)
    {
      newdig = dig[i] + carry;
      if (newdig >= COL_NUM_NBASE)
	{
	  carry = newdig / COL_NUM_NBASE;
	  newdig -= carry * COL_NUM_NBASE;
	}
      else
	{
	  carry = 0;
	}
      res->digits[i] = (COL_NUM_DIGIT) newdig;
    }
  assert (carry == 0);

  res->ndigits = res_ndigits;
  res->weight = res_weight;
  res->sign = res_sign;
  columnar_num_round (res, rscale);
  return NO_ERROR;
}

/* ========================================================================== */
/* Division — PostgreSQL div_var () with select_div_scale ()                  */
/* ========================================================================== */

static int
col_num_select_div_scale (const COL_NUMREF * a, const COL_NUMREF * b)
{
  int weight1 = a->weight, weight2 = b->weight;
  int firstdigit1 = 0, firstdigit2 = 0;
  int qweight, rscale, i;

  for (i = 0; i < a->ndigits; i++)
    {
      firstdigit1 = a->digits[i];
      if (firstdigit1 != 0)
	{
	  break;
	}
      weight1--;
    }
  for (i = 0; i < b->ndigits; i++)
    {
      firstdigit2 = b->digits[i];
      if (firstdigit2 != 0)
	{
	  break;
	}
      weight2--;
    }

  qweight = weight1 - weight2;
  if (firstdigit1 <= firstdigit2)
    {
      qweight--;
    }

  rscale = COL_NUM_MIN_SIG_DIGITS - qweight * COL_NUM_DEC_DIGITS;
  rscale = COL_NUM_MAX (rscale, a->dscale);
  rscale = COL_NUM_MAX (rscale, b->dscale);
  rscale = COL_NUM_MAX (rscale, COL_NUM_MIN_DISPLAY_SCALE);
  rscale = COL_NUM_MIN (rscale, COL_NUM_MAX_DISPLAY_SCALE);
  return rscale;
}

int
columnar_num_div_rscale (const COL_NUMREF * a, const COL_NUMREF * b, int rscale, COL_NUMVAR * res)
{
  /* working dividend (div_ndigits + 1 entries) and divisor (var2ndigits + 1) */
  int dividend[2 * COL_NUM_MAX_NDIGITS + 4];
  int divisor[COL_NUM_MAX_NDIGITS + 2];
  int res_sign, res_weight, res_ndigits, div_ndigits;
  int divisor1, divisor2, carry, borrow;
  int i, j;

  if (b->ndigits == 0 || b->digits[0] == 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_ZERO_DIVIDE, 0);
      return ER_QPROC_ZERO_DIVIDE;
    }
  if (a->ndigits == 0)
    {
      columnar_num_set_zero (res);
      res->dscale = rscale;
      return NO_ERROR;
    }

  res_sign = (a->sign == b->sign) ? COL_NUM_POS : COL_NUM_NEG;
  res_weight = a->weight - b->weight;
  res_ndigits = res_weight + 1 + (rscale + COL_NUM_DEC_DIGITS - 1) / COL_NUM_DEC_DIGITS;
  res_ndigits = COL_NUM_MAX (res_ndigits, 1);
  res_ndigits++;		/* one extra digit so the final rounding is correct */

  if (res_ndigits > COL_NUM_MAX_NDIGITS)
    {
      return col_num_overflow ();
    }

  div_ndigits = res_ndigits + b->ndigits;
  div_ndigits = COL_NUM_MAX (div_ndigits, a->ndigits);
  if (div_ndigits + 1 > (int) (sizeof (dividend) / sizeof (dividend[0])))
    {
      return col_num_overflow ();
    }

  memset (dividend, 0, (size_t) (div_ndigits + 1) * sizeof (int));
  memset (divisor, 0, (size_t) (b->ndigits + 1) * sizeof (int));
  for (i = 0; i < a->ndigits; i++)
    {
      dividend[i + 1] = a->digits[i];
    }
  for (i = 0; i < b->ndigits; i++)
    {
      divisor[i + 1] = b->digits[i];
    }

  if (b->ndigits == 1)
    {
      divisor1 = divisor[1];
      carry = 0;
      for (i = 0; i < res_ndigits; i++)
	{
	  carry = carry * COL_NUM_NBASE + dividend[i + 1];
	  res->digits[i] = (COL_NUM_DIGIT) (carry / divisor1);
	  carry = carry % divisor1;
	}
    }
  else
    {
      /* Knuth 4.3.1 D, exactly as PostgreSQL implements it */
      if (divisor[1] < COL_NUM_HALF_NBASE)
	{
	  int d = COL_NUM_NBASE / (divisor[1] + 1);

	  carry = 0;
	  for (i = b->ndigits; i > 0; i--)
	    {
	      carry += divisor[i] * d;
	      divisor[i] = carry % COL_NUM_NBASE;
	      carry = carry / COL_NUM_NBASE;
	    }
	  assert (carry == 0);
	  carry = 0;
	  for (i = div_ndigits; i >= 0; i--)
	    {
	      carry += dividend[i] * d;
	      dividend[i] = carry % COL_NUM_NBASE;
	      carry = carry / COL_NUM_NBASE;
	    }
	  assert (carry == 0);
	  assert (divisor[1] >= COL_NUM_HALF_NBASE);
	}

      divisor1 = divisor[1];
      divisor2 = divisor[2];

      for (j = 0; j < res_ndigits; j++)
	{
	  int next2digits = dividend[j] * COL_NUM_NBASE + dividend[j + 1];
	  int qhat;

	  if (next2digits == 0)
	    {
	      res->digits[j] = 0;
	      continue;
	    }

	  if (dividend[j] == divisor1)
	    {
	      qhat = COL_NUM_NBASE - 1;
	    }
	  else
	    {
	      qhat = next2digits / divisor1;
	    }

	  while (divisor2 * qhat > (next2digits - qhat * divisor1) * COL_NUM_NBASE + dividend[j + 2])
	    {
	      qhat--;
	    }

	  if (qhat > 0)
	    {
	      carry = 0;
	      borrow = 0;
	      for (i = b->ndigits; i > 0; i--)
		{
		  carry += divisor[i] * qhat;
		  borrow -= carry % COL_NUM_NBASE;
		  carry = carry / COL_NUM_NBASE;
		  dividend[j + i] += borrow;
		  if (dividend[j + i] < 0)
		    {
		      dividend[j + i] += COL_NUM_NBASE;
		      borrow = -1;
		    }
		  else
		    {
		      borrow = 0;
		    }
		}
	      borrow -= carry;
	      dividend[j] += borrow;

	      if (dividend[j] < 0)
		{
		  carry = 0;
		  for (i = b->ndigits; i > 0; i--)
		    {
		      carry += dividend[j + i] + divisor[i];
		      if (carry >= COL_NUM_NBASE)
			{
			  dividend[j + i] = carry - COL_NUM_NBASE;
			  carry = 1;
			}
		      else
			{
			  dividend[j + i] = carry;
			  carry = 0;
			}
		    }
		  assert (dividend[j] + carry == 0);
		  qhat--;
		}

	      res->digits[j] = (COL_NUM_DIGIT) qhat;
	    }
	  else
	    {
	      res->digits[j] = 0;
	    }
	}
    }

  res->ndigits = res_ndigits;
  res->weight = res_weight;
  res->sign = res_sign;
  columnar_num_round (res, rscale);
  return NO_ERROR;
}

int
columnar_num_div (const COL_NUMREF * a, const COL_NUMREF * b, COL_NUMVAR * res)
{
  return columnar_num_div_rscale (a, b, col_num_select_div_scale (a, b), res);
}
