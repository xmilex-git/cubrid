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
 * columnar_rawagg.c - fused aggregate transition + raw hash aggregation
 *                     (#23 D3/D7/D8/D9)
 */

#ident "$Id$"

#include "columnar_rawagg.h"

#include "dbtype.h"
#include "error_manager.h"
#include "expr_compile.h"
#include "language_support.h"
#include "object_domain.h"
#include "object_representation.h"
#include "query_opfunc.h"
#include "system_parameter.h"

#include <algorithm>

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#define RAWAGG_UNSUPPORTED(what) \
  (er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_COLUMNAR_UNSUPPORTED_EXPR, 1, (what)), ER_COLUMNAR_UNSUPPORTED_EXPR)

#define RAWAGG_ALIGN8(n)  (((n) + 7) & ~7)

/* ========================================================================== */
/* Accumulators                                                               */
/* ========================================================================== */

typedef enum
{
  RAWAGG_COUNT,			/* COUNT(*) and COUNT(expr) */
  RAWAGG_SUM_I64,
  RAWAGG_SUM_DBL,
  RAWAGG_SUM_NUM,
  RAWAGG_MINMAX_I64,
  RAWAGG_MINMAX_DBL,
  RAWAGG_MINMAX_NUM,
  RAWAGG_MINMAX_STR
} RAWAGG_KIND;

/*
 * Every accumulator starts with its non-NULL input count, which is what
 * distinguishes "no rows yet" from "an accumulated zero" and what AVG divides
 * by.  The rest is kind-specific and sized so a group entry costs only what
 * its aggregates actually need.
 */
typedef struct rawagg_spec RAWAGG_SPEC;
struct rawagg_spec
{
  AGGREGATE_TYPE *agg;		/* for the result domain at output time */
  RAWAGG_KIND kind;
  FUNC_CODE func;
  RAW_TYPE operand_type;
  TP_DOMAIN *operand_domain;	/* the operand's own domain (MIN/MAX result type) */
  int root;			/* RAW_PROG root of the operand; -1 for COUNT(*) */
  int cell;			/* cell index of that root */
  int offset;			/* byte offset of the accumulator inside an entry */
  bool is_max;
};

static int
rawagg_acc_size (RAWAGG_KIND kind)
{
  switch (kind)
    {
    case RAWAGG_COUNT:
      return 8;
    case RAWAGG_SUM_NUM:
    case RAWAGG_MINMAX_NUM:
      return 8 + RAWAGG_ALIGN8 ((int) sizeof (COL_NUMVAR));
    case RAWAGG_MINMAX_STR:
      return 24;		/* cnt + (offset, length, capacity) into the arena */
    default:
      return 16;		/* cnt + INT64 / double */
    }
}

/* ========================================================================== */
/* Hash table                                                                 */
/* ========================================================================== */

/*
 * Entry layout, all inside one bump arena:
 *
 *   UINT32 hash | INT32 next | key null bitmap | fixed key parts | accumulators
 *
 * Fixed-width key parts are copied into the entry (the chunk buffer they came
 * from dies with the chunk); NUMERIC and string keys are copied into a second
 * arena and referenced as (offset, length), which keeps the entry a fixed size
 * so entry i lives at i * entry_size (D9).
 */
typedef struct rawagg_entry_hdr RAWAGG_ENTRY_HDR;
struct rawagg_entry_hdr
{
  UINT32 hash;
  INT32 next;			/* next entry index in the bucket chain, -1 = end */
};

typedef struct rawagg_varref RAWAGG_VARREF;
struct rawagg_varref
{
  INT32 offset;
  INT32 length;
};

/*
 * MIN/MAX over strings keeps its current extreme in the arena.  It records
 * the CAPACITY it was given so a new extreme that fits overwrites in place:
 * without that, a column arriving in ascending order would allocate a fresh
 * slot for every row and consume the budget in proportion to rows.
 */
typedef struct rawagg_strref RAWAGG_STRREF;
struct rawagg_strref
{
  INT32 offset;
  INT32 length;
  INT32 capacity;
  INT32 reserved;
};

struct raw_agg
{
  RAW_PROG *prog;

  int n_keys;
  int *key_cell;		/* cell index of each key root */
  RAW_TYPE *key_type;
  TP_DOMAIN **key_domain;
  int *key_offset;		/* byte offset of each key part inside an entry */
  DB_VALUE **key_target;	/* value-list slot each key publishes into */

  int n_aggs;
  RAWAGG_SPEC *aggs;

  int null_bytes;		/* key null bitmap, 8-byte aligned */
  int key_bytes;
  int acc_bytes;
  int entry_size;

  char *arena;			/* entries */
  size_t arena_used;
  size_t arena_alloc;

  char *var;			/* variable key / string bytes */
  size_t var_used;
  size_t var_alloc;

  INT32 *buckets;
  int n_buckets;
  int n_entries;

  size_t budget;		/* columnar_agg_hash_size */

  /*
   * Per-row probe scratch for variable-width key parts.  A probe must NOT
   * write into the permanent variable arena: a hit would abandon those bytes
   * with no way to reclaim them, so a high-cardinality string group-by would
   * exhaust the budget in proportion to ROWS instead of GROUPS.  The image is
   * built here and copied into the arena only when the group is new.
   */
  char *probe_var;
  size_t probe_var_used;
  size_t probe_var_alloc;

  char *cur_entry;		/* entry the lookup step selected for this row */
  INT64 n_rows;			/* accumulated rows (statistics) */

  int *order;			/* group ordinal -> entry index; NULL = insertion order */
};

#define RAWAGG_ENTRY(ra, i)   ((ra)->arena + (size_t) (i) * (size_t) (ra)->entry_size)
#define RAWAGG_NULLS(ra, e)   ((unsigned char *) ((e) + sizeof (RAWAGG_ENTRY_HDR)))
#define RAWAGG_KEYS(ra, e)    ((e) + sizeof (RAWAGG_ENTRY_HDR) + (ra)->null_bytes)
#define RAWAGG_ACCS(ra, e)    (RAWAGG_KEYS (ra, e) + (ra)->key_bytes)

static int
rawagg_full (const RAW_AGG * ra)
{
  /* reported in MB: the parameter accepts up to 8GB, which does not fit an int */
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_COLUMNAR_AGG_HASH_FULL, 1, (int) (ra->budget >> 20));
  return ER_COLUMNAR_AGG_HASH_FULL;
}

static size_t
rawagg_footprint (const RAW_AGG * ra)
{
  return ra->arena_alloc + ra->var_alloc + (size_t) ra->n_buckets * sizeof (INT32);
}

/* reserve len bytes in the variable arena; returns the offset or -1 */
static int
rawagg_var_alloc (RAW_AGG * ra, int len)
{
  int off;

  if (ra->var_used + (size_t) len > ra->var_alloc)
    {
      size_t na = (ra->var_alloc == 0) ? 64 * 1024 : ra->var_alloc * 2;
      char *nv;

      while (na < ra->var_used + (size_t) len)
	{
	  na *= 2;
	}
      if (rawagg_footprint (ra) - ra->var_alloc + na > ra->budget)
	{
	  return -1;
	}
      nv = (char *) realloc (ra->var, na);
      if (nv == NULL)
	{
	  return -1;
	}
      ra->var = nv;
      ra->var_alloc = na;
    }
  off = (int) ra->var_used;
  ra->var_used += (size_t) len;
  return off;
}

/* reserve len bytes in the per-row probe scratch; returns the offset or -1 */
static int
rawagg_probe_alloc (RAW_AGG * ra, int len)
{
  int off;

  if (ra->probe_var_used + (size_t) len > ra->probe_var_alloc)
    {
      size_t na = (ra->probe_var_alloc == 0) ? 4096 : ra->probe_var_alloc * 2;
      char *nv;

      while (na < ra->probe_var_used + (size_t) len)
	{
	  na *= 2;
	}
      nv = (char *) realloc (ra->probe_var, na);
      if (nv == NULL)
	{
	  return -1;
	}
      ra->probe_var = nv;
      ra->probe_var_alloc = na;
    }
  off = (int) ra->probe_var_used;
  ra->probe_var_used += (size_t) len;
  return off;
}

static UINT32
rawagg_hash_bytes (UINT32 h, const void *p, int len)
{
  const unsigned char *b = (const unsigned char *) p;
  int i;

  for (i = 0; i < len; i++)
    {
      h ^= b[i];
      h *= 16777619u;		/* FNV-1a */
    }
  return h;
}

static int
rawagg_grow_buckets (RAW_AGG * ra)
{
  int nb = (ra->n_buckets == 0) ? 1024 : ra->n_buckets * 2;
  INT32 *nbuf;
  int i;

  if (rawagg_footprint (ra) - (size_t) ra->n_buckets * sizeof (INT32) + (size_t) nb * sizeof (INT32) > ra->budget)
    {
      return rawagg_full (ra);
    }
  nbuf = (INT32 *) malloc ((size_t) nb * sizeof (INT32));
  if (nbuf == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) nb * sizeof (INT32));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  for (i = 0; i < nb; i++)
    {
      nbuf[i] = -1;
    }
  free (ra->buckets);
  ra->buckets = nbuf;
  ra->n_buckets = nb;

  for (i = 0; i < ra->n_entries; i++)
    {
      RAWAGG_ENTRY_HDR *h = (RAWAGG_ENTRY_HDR *) RAWAGG_ENTRY (ra, i);
      int b = (int) (h->hash & (UINT32) (nb - 1));
      h->next = ra->buckets[b];
      ra->buckets[b] = i;
    }
  return NO_ERROR;
}

static int
rawagg_new_entry (RAW_AGG * ra, int *idx_out)
{
  if (ra->arena_used + (size_t) ra->entry_size > ra->arena_alloc)
    {
      size_t na = (ra->arena_alloc == 0) ? 256 * 1024 : ra->arena_alloc * 2;
      char *nb;

      while (na < ra->arena_used + (size_t) ra->entry_size)
	{
	  na *= 2;
	}
      if (rawagg_footprint (ra) - ra->arena_alloc + na > ra->budget)
	{
	  return rawagg_full (ra);
	}
      nb = (char *) realloc (ra->arena, na);
      if (nb == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, na);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      ra->arena = nb;
      ra->arena_alloc = na;
    }
  memset (ra->arena + ra->arena_used, 0, (size_t) ra->entry_size);
  ra->arena_used += (size_t) ra->entry_size;
  *idx_out = ra->n_entries++;
  return NO_ERROR;
}

/* ========================================================================== */
/* Group lookup step (D7)                                                     */
/* ========================================================================== */

/*
 * Build this row's key image into scratch, hash it, then find or create the
 * entry.  Key images are byte-comparable by construction: a NUMERIC key is
 * serialized with its display scale forced to zero so that 1.5 and 1.50 —
 * equal values that carry different display scales — produce identical bytes
 * and therefore land in the same group.
 */
static int
rawagg_lookup_step (RAW_STEP * s, RAW_EVAL_CTX * ctx)
{
  RAW_AGG *ra = (RAW_AGG *) ctx->user;
  RAW_PROG *p = ctx->prog;
  unsigned char nulls[64];
  char keybuf[512];
  UINT32 h = 2166136261u;	/* FNV-1a offset basis */
  int i, idx, bucket, error;

  (void) s;

  if (ra->n_keys == 0)
    {
      /* BUILDVALUE: the lookup degenerates to one fixed accumulator */
      if (ra->n_entries == 0)
	{
	  error = rawagg_new_entry (ra, &idx);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }
	}
      ra->cur_entry = RAWAGG_ENTRY (ra, 0);
      return NO_ERROR;
    }

  assert (ra->null_bytes <= (int) sizeof (nulls) && ra->key_bytes <= (int) sizeof (keybuf));
  memset (nulls, 0, (size_t) ra->null_bytes);
  memset (keybuf, 0, (size_t) ra->key_bytes);
  ra->probe_var_used = 0;	/* variable key parts go to probe scratch first */

  for (i = 0; i < ra->n_keys; i++)
    {
      int cell = ra->key_cell[i];
      char *dst = keybuf + ra->key_offset[i];

      if (p->nulls[cell])
	{
	  nulls[i >> 3] |= (unsigned char) (1 << (i & 7));
	  continue;
	}
      switch (ra->key_type[i])
	{
	case RAW_T_I64:
	  memcpy (dst, &p->cells[cell].i64, sizeof (INT64));
	  break;
	case RAW_T_DBL:
	  {
	    /* -0.0 and 0.0 are one group, so normalize the bit pattern */
	    double d = p->cells[cell].dbl;
	    if (d == 0.0)
	      {
		d = 0.0;
	      }
	    memcpy (dst, &d, sizeof (double));
	  }
	  break;
	case RAW_T_NUM:
	  {
	    COL_NUMVAR nv = *p->cells[cell].num;
	    RAWAGG_VARREF ref;
	    int len;

	    nv.dscale = 0;	/* display scale must not split a group */
	    len = columnar_num_serialized_size (&nv);
	    ref.offset = rawagg_probe_alloc (ra, len);
	    if (ref.offset < 0)
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) len);
		return ER_OUT_OF_VIRTUAL_MEMORY;
	      }
	    columnar_num_serialize (&nv, ra->probe_var + ref.offset);
	    ref.length = len;
	    memcpy (dst, &ref, sizeof (ref));
	  }
	  break;
	default:		/* RAW_T_STR */
	  {
	    RAWAGG_VARREF ref;
	    int len = p->cells[cell].str.len;

	    ref.offset = rawagg_probe_alloc (ra, len > 0 ? len : 1);
	    if (ref.offset < 0)
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) len);
		return ER_OUT_OF_VIRTUAL_MEMORY;
	      }
	    if (len > 0)
	      {
		memcpy (ra->probe_var + ref.offset, p->cells[cell].str.ptr, (size_t) len);
	      }
	    ref.length = len;
	    memcpy (dst, &ref, sizeof (ref));
	  }
	  break;
	}
    }

  h = rawagg_hash_bytes (h, nulls, ra->null_bytes);
  for (i = 0; i < ra->n_keys; i++)
    {
      const char *src = keybuf + ra->key_offset[i];

      if (nulls[i >> 3] & (1 << (i & 7)))
	{
	  continue;
	}
      if (ra->key_type[i] == RAW_T_NUM || ra->key_type[i] == RAW_T_STR)
	{
	  RAWAGG_VARREF ref;
	  memcpy (&ref, src, sizeof (ref));
	  h = rawagg_hash_bytes (h, ra->probe_var + ref.offset, ref.length);
	}
      else
	{
	  h = rawagg_hash_bytes (h, src, 8);
	}
    }

  if (ra->n_buckets == 0)
    {
      error = rawagg_grow_buckets (ra);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }

  bucket = (int) (h & (UINT32) (ra->n_buckets - 1));
  for (idx = ra->buckets[bucket]; idx >= 0;)
    {
      char *e = RAWAGG_ENTRY (ra, idx);
      RAWAGG_ENTRY_HDR *hdr = (RAWAGG_ENTRY_HDR *) e;

      if (hdr->hash == h && memcmp (RAWAGG_NULLS (ra, e), nulls, (size_t) ra->null_bytes) == 0)
	{
	  const char *ek = RAWAGG_KEYS (ra, e);
	  bool same = true;

	  for (i = 0; i < ra->n_keys && same; i++)
	    {
	      if (nulls[i >> 3] & (1 << (i & 7)))
		{
		  continue;
		}
	      if (ra->key_type[i] == RAW_T_NUM || ra->key_type[i] == RAW_T_STR)
		{
		  RAWAGG_VARREF a, b;
		  memcpy (&a, ek + ra->key_offset[i], sizeof (a));
		  memcpy (&b, keybuf + ra->key_offset[i], sizeof (b));
		  same = (a.length == b.length
			  && memcmp (ra->var + a.offset, ra->probe_var + b.offset, (size_t) a.length) == 0);
		}
	      else
		{
		  same = (memcmp (ek + ra->key_offset[i], keybuf + ra->key_offset[i], 8) == 0);
		}
	    }
	  if (same)
	    {
	      /* hit: the probe image is discarded, so no arena bytes were spent */
	      ra->cur_entry = e;
	      return NO_ERROR;
	    }
	}
      idx = hdr->next;
    }

  /* miss: this group keeps the key image, so now it moves into the arena */
  if (ra->n_entries + 1 > (ra->n_buckets * 3) / 4)
    {
      error = rawagg_grow_buckets (ra);
      if (error != NO_ERROR)
	{
	  return error;
	}
      bucket = (int) (h & (UINT32) (ra->n_buckets - 1));
    }

  for (i = 0; i < ra->n_keys; i++)
    {
      RAWAGG_VARREF ref;
      int off;

      if ((nulls[i >> 3] & (1 << (i & 7))) != 0
	  || (ra->key_type[i] != RAW_T_NUM && ra->key_type[i] != RAW_T_STR))
	{
	  continue;
	}
      memcpy (&ref, keybuf + ra->key_offset[i], sizeof (ref));
      off = rawagg_var_alloc (ra, ref.length > 0 ? ref.length : 1);
      if (off < 0)
	{
	  return rawagg_full (ra);
	}
      if (ref.length > 0)
	{
	  memcpy (ra->var + off, ra->probe_var + ref.offset, (size_t) ref.length);
	}
      ref.offset = off;
      memcpy (keybuf + ra->key_offset[i], &ref, sizeof (ref));
    }

  error = rawagg_new_entry (ra, &idx);
  if (error != NO_ERROR)
    {
      return error;
    }
  {
    char *e = RAWAGG_ENTRY (ra, idx);
    RAWAGG_ENTRY_HDR *hdr = (RAWAGG_ENTRY_HDR *) e;

    hdr->hash = h;
    hdr->next = ra->buckets[bucket];
    ra->buckets[bucket] = idx;
    memcpy (RAWAGG_NULLS (ra, e), nulls, (size_t) ra->null_bytes);
    memcpy (RAWAGG_KEYS (ra, e), keybuf, (size_t) ra->key_bytes);
    ra->cur_entry = e;
  }
  return NO_ERROR;
}

/* ========================================================================== */
/* Transition steps (D7)                                                      */
/* ========================================================================== */

static int
rawagg_trans_step (RAW_STEP * s, RAW_EVAL_CTX * ctx)
{
  RAW_AGG *ra = (RAW_AGG *) ctx->user;
  RAW_PROG *p = ctx->prog;
  const RAWAGG_SPEC *spec = &ra->aggs[s->aux];
  char *acc = RAWAGG_ACCS (ra, ra->cur_entry) + spec->offset;
  INT64 *cnt = (INT64 *) acc;

  if (spec->func == PT_COUNT_STAR)
    {
      (*cnt)++;
      return NO_ERROR;
    }
  if (p->nulls[spec->cell])
    {
      return NO_ERROR;		/* aggregates ignore NULL inputs */
    }

  switch (spec->kind)
    {
    case RAWAGG_COUNT:
      (*cnt)++;
      break;

    case RAWAGG_SUM_I64:
      {
	INT64 *sum = (INT64 *) (acc + 8);
	INT64 x = *sum, y = p->cells[spec->cell].i64;
	INT64 z = (INT64) ((UINT64) x + (UINT64) y);

	if (OR_CHECK_ADD_OVERFLOW (x, y, z))
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_ADDITION, 0);
	    return ER_QPROC_OVERFLOW_ADDITION;
	  }
	*sum = z;
	(*cnt)++;
      }
      break;

    case RAWAGG_SUM_DBL:
      {
	double *sum = (double *) (acc + 8);
	*sum += p->cells[spec->cell].dbl;
	(*cnt)++;
      }
      break;

    case RAWAGG_SUM_NUM:
      {
	COL_NUMVAR *sum = (COL_NUMVAR *) (acc + 8);
	COL_NUMREF a, b;
	COL_NUMVAR tmp;
	int error;

	if (*cnt == 0)
	  {
	    *sum = *p->cells[spec->cell].num;
	    (*cnt)++;
	    break;
	  }
	columnar_num_ref_var (sum, &a);
	columnar_num_ref_var (p->cells[spec->cell].num, &b);
	error = columnar_num_add (&a, &b, &tmp);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
	*sum = tmp;
	(*cnt)++;
      }
      break;

    case RAWAGG_MINMAX_I64:
      {
	INT64 *cur = (INT64 *) (acc + 8);
	INT64 v = p->cells[spec->cell].i64;

	if (*cnt == 0 || (spec->is_max ? (v > *cur) : (v < *cur)))
	  {
	    *cur = v;
	  }
	(*cnt)++;
      }
      break;

    case RAWAGG_MINMAX_DBL:
      {
	double *cur = (double *) (acc + 8);
	double v = p->cells[spec->cell].dbl;

	if (*cnt == 0 || (spec->is_max ? (v > *cur) : (v < *cur)))
	  {
	    *cur = v;
	  }
	(*cnt)++;
      }
      break;

    case RAWAGG_MINMAX_NUM:
      {
	COL_NUMVAR *cur = (COL_NUMVAR *) (acc + 8);

	if (*cnt == 0)
	  {
	    *cur = *p->cells[spec->cell].num;
	  }
	else
	  {
	    COL_NUMREF a, b;
	    int c;

	    columnar_num_ref_var (p->cells[spec->cell].num, &a);
	    columnar_num_ref_var (cur, &b);
	    c = columnar_num_cmp (&a, &b);
	    if (spec->is_max ? (c > 0) : (c < 0))
	      {
		*cur = *p->cells[spec->cell].num;
	      }
	  }
	(*cnt)++;
      }
      break;

    default:			/* RAWAGG_MINMAX_STR */
      {
	RAWAGG_STRREF *cur = (RAWAGG_STRREF *) (acc + 8);
	const char *sp = p->cells[spec->cell].str.ptr;
	int sl = p->cells[spec->cell].str.len;
	bool take = (*cnt == 0);

	if (!take)
	  {
	    int m = (cur->length < sl) ? cur->length : sl;
	    int c = (m > 0) ? memcmp (sp, ra->var + cur->offset, (size_t) m) : 0;

	    if (c == 0)
	      {
		c = (sl > cur->length) ? 1 : ((sl < cur->length) ? -1 : 0);
	      }
	    take = spec->is_max ? (c > 0) : (c < 0);
	  }
	if (take)
	  {
	    if (sl > cur->capacity)
	      {
		/* grow: reserve some slack so a monotonically increasing
		 * column does not reallocate on every improvement */
		int want = (sl > 0) ? (sl + sl / 2 + 8) : 8;
		int off = rawagg_var_alloc (ra, want);

		if (off < 0)
		  {
		    return rawagg_full (ra);
		  }
		cur->offset = off;
		cur->capacity = want;
	      }
	    if (sl > 0)
	      {
		memcpy (ra->var + cur->offset, sp, (size_t) sl);
	      }
	    cur->length = sl;
	  }
	(*cnt)++;
      }
      break;
    }
  return NO_ERROR;
}

/* ========================================================================== */
/* Build                                                                      */
/* ========================================================================== */

static RAWAGG_KIND
rawagg_kind_of (FUNC_CODE func, RAW_TYPE t, bool * ok)
{
  *ok = true;
  switch (func)
    {
    case PT_COUNT:
    case PT_COUNT_STAR:
      return RAWAGG_COUNT;
    case PT_SUM:
    case PT_AVG:
      switch (t)
	{
	case RAW_T_I64:
	  return RAWAGG_SUM_I64;
	case RAW_T_DBL:
	  return RAWAGG_SUM_DBL;
	case RAW_T_NUM:
	  return RAWAGG_SUM_NUM;
	default:
	  break;
	}
      break;
    case PT_MIN:
    case PT_MAX:
      switch (t)
	{
	case RAW_T_I64:
	  return RAWAGG_MINMAX_I64;
	case RAW_T_DBL:
	  return RAWAGG_MINMAX_DBL;
	case RAW_T_NUM:
	  return RAWAGG_MINMAX_NUM;
	case RAW_T_STR:
	  return RAWAGG_MINMAX_STR;
	default:
	  break;
	}
      break;
    default:
      break;
    }
  *ok = false;
  return RAWAGG_COUNT;
}

int
columnar_rawagg_build (THREAD_ENTRY * thread_p, COLUMNAR_SCAN * cs, REGU_VARIABLE ** key_roots, int n_keys,
		       AGGREGATE_TYPE * agg_list, REGU_VARIABLE ** operand_override, RAW_AGG ** out)
{
  RAW_AGG *ra;
  AGGREGATE_TYPE *agg_p;
  REGU_VARIABLE **roots = NULL;
  int n_aggs = 0, n_roots = 0, i, off, error = NO_ERROR;

  *out = NULL;

  for (agg_p = agg_list; agg_p != NULL; agg_p = agg_p->next)
    {
      if (agg_p->flag.agg_optimized)
	{
	  /* answered from an index; the raw path has no substitute for it */
	  return RAWAGG_UNSUPPORTED ("index-optimized aggregate");
	}
      if (agg_p->option == Q_DISTINCT)
	{
	  return RAWAGG_UNSUPPORTED ("DISTINCT aggregate");
	}
      n_aggs++;
    }

  ra = (RAW_AGG *) calloc (1, sizeof (RAW_AGG));
  if (ra == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (RAW_AGG));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  ra->n_keys = n_keys;
  ra->n_aggs = n_aggs;
  ra->budget = (size_t) prm_get_bigint_value (PRM_ID_COLUMNAR_AGG_HASH_SIZE);

  roots = (REGU_VARIABLE **) calloc ((size_t) (n_keys + n_aggs + 1), sizeof (REGU_VARIABLE *));
  ra->key_cell = (int *) calloc ((size_t) (n_keys + 1), sizeof (int));
  ra->key_type = (RAW_TYPE *) calloc ((size_t) (n_keys + 1), sizeof (RAW_TYPE));
  ra->key_domain = (TP_DOMAIN **) calloc ((size_t) (n_keys + 1), sizeof (TP_DOMAIN *));
  ra->key_offset = (int *) calloc ((size_t) (n_keys + 1), sizeof (int));
  ra->key_target = (DB_VALUE **) calloc ((size_t) (n_keys + 1), sizeof (DB_VALUE *));
  ra->aggs = (RAWAGG_SPEC *) calloc ((size_t) (n_aggs + 1), sizeof (RAWAGG_SPEC));
  if (roots == NULL || ra->key_cell == NULL || ra->key_type == NULL || ra->key_domain == NULL
      || ra->key_offset == NULL || ra->key_target == NULL || ra->aggs == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (RAW_AGG));
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      goto error_exit;
    }

  /* roots: every group-by key, then every aggregate operand */
  for (i = 0; i < n_keys; i++)
    {
      roots[n_roots++] = key_roots[i];
    }
  for (agg_p = agg_list, i = 0; agg_p != NULL; agg_p = agg_p->next, i++)
    {
      ra->aggs[i].agg = agg_p;
      ra->aggs[i].func = agg_p->function;
      ra->aggs[i].is_max = (agg_p->function == PT_MAX);
      if (agg_p->function == PT_COUNT_STAR)
	{
	  ra->aggs[i].root = -1;
	  continue;
	}
      if (agg_p->operands == NULL)
	{
	  error = RAWAGG_UNSUPPORTED ("aggregate without an operand");
	  goto error_exit;
	}
      ra->aggs[i].root = n_roots;
      roots[n_roots] = (operand_override != NULL && operand_override[i] != NULL)
	? operand_override[i] : &agg_p->operands->value;
      ra->aggs[i].operand_domain = roots[n_roots]->domain;
      n_roots++;
    }

  error = columnar_rawprog_compile (thread_p, cs, roots, n_roots, &ra->prog);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  /* entry layout */
  ra->null_bytes = RAWAGG_ALIGN8 ((n_keys + 7) / 8);
  off = 0;
  for (i = 0; i < n_keys; i++)
    {
      ra->key_cell[i] = ra->prog->root_cells[i];
      ra->key_type[i] = columnar_rawprog_type (ra->prog, i);
      ra->key_domain[i] = key_roots[i]->domain;
      ra->key_target[i] = key_roots[i]->vfetch_to;
      if (ra->key_type[i] == RAW_T_NONE)
	{
	  error = RAWAGG_UNSUPPORTED ("group-by key type not covered by the raw program");
	  goto error_exit;
	}
      ra->key_offset[i] = off;
      off += 8;			/* INT64 / double / (offset, length) */
    }
  ra->key_bytes = off;

  off = 0;
  for (i = 0; i < n_aggs; i++)
    {
      RAWAGG_SPEC *spec = &ra->aggs[i];
      bool ok;

      if (spec->root >= 0)
	{
	  spec->cell = ra->prog->root_cells[spec->root];
	  spec->operand_type = columnar_rawprog_type (ra->prog, spec->root);
	}
      else
	{
	  spec->cell = -1;
	  spec->operand_type = RAW_T_NONE;
	}
      spec->kind = rawagg_kind_of (spec->func, spec->operand_type, &ok);
      if (!ok)
	{
	  error = RAWAGG_UNSUPPORTED ("aggregate function/type not covered by the raw path");
	  goto error_exit;
	}
      spec->offset = off;
      off += rawagg_acc_size (spec->kind);
    }
  ra->acc_bytes = off;
  ra->entry_size = RAWAGG_ALIGN8 ((int) sizeof (RAWAGG_ENTRY_HDR) + ra->null_bytes + ra->key_bytes + ra->acc_bytes);

  /* the lookup step's stack scratch bounds the key width */
  if (ra->null_bytes > 64 || ra->key_bytes > 512)
    {
      error = RAWAGG_UNSUPPORTED ("too many group-by keys for the raw hash aggregation");
      goto error_exit;
    }

  /* fuse: lookup first, then one transition step per aggregate (D7) */
  {
    RAW_STEP *step;

    error = columnar_rawprog_append_step (ra->prog, rawagg_lookup_step, -1, -1, 0, &step);
    if (error != NO_ERROR)
      {
	goto error_exit;
      }
    for (i = 0; i < n_aggs; i++)
      {
	error = columnar_rawprog_append_step (ra->prog, rawagg_trans_step, -1, -1, i, &step);
	if (error != NO_ERROR)
	  {
	    goto error_exit;
	  }
      }
  }

  free (roots);
  *out = ra;
  return NO_ERROR;

error_exit:
  free (roots);
  columnar_rawagg_free (ra);
  return error;
}

/* ========================================================================== */
/* Run                                                                        */
/* ========================================================================== */

int
columnar_rawagg_run (THREAD_ENTRY * thread_p, RAW_AGG * ra, COLUMNAR_SCAN * cs)
{
  COLUMNAR_CHUNK_VIEW view;
  SCAN_CODE sc;

  while ((sc = columnar_scan_next_chunk (thread_p, cs, &view)) == S_SUCCESS)
    {
      int w;

      columnar_rawprog_new_chunk (ra->prog, cs);
      for (w = 0; w < view.n_words; w++)
	{
	  UINT64 bits = view.bitmap[w];

	  while (bits != 0)
	    {
	      int r = (w << 6) + __builtin_ctzll (bits);
	      int error;

	      bits &= bits - 1;
	      error = columnar_rawprog_eval_user (ra->prog, cs, r, ra);
	      if (error != NO_ERROR)
		{
		  return error;
		}
	      ra->n_rows++;
	    }
	}
    }
  if (sc == S_ERROR)
    {
      return ER_FAILED;
    }

  /* With no group-by keys there is always exactly one group, even over zero
   * rows: the caller then reads counts of 0 and NULL sums, which is what an
   * aggregate over an empty input must produce. */
  if (ra->n_keys == 0 && ra->n_entries == 0)
    {
      int idx;

      return rawagg_new_entry (ra, &idx);
    }
  return NO_ERROR;
}

/* ========================================================================== */
/* Output                                                                     */
/* ========================================================================== */

/* ========================================================================== */
/* Group ordering                                                             */
/* ========================================================================== */

/* compare key i of two entries; both known non-NULL */
static int
rawagg_cmp_key (const RAW_AGG * ra, int i, const char *ka, const char *kb)
{
  const char *pa = ka + ra->key_offset[i];
  const char *pb = kb + ra->key_offset[i];

  switch (ra->key_type[i])
    {
    case RAW_T_I64:
      {
	INT64 a, b;
	memcpy (&a, pa, sizeof (a));
	memcpy (&b, pb, sizeof (b));
	return (a < b) ? -1 : ((a > b) ? 1 : 0);
      }
    case RAW_T_DBL:
      {
	double a, b;
	memcpy (&a, pa, sizeof (a));
	memcpy (&b, pb, sizeof (b));
	return (a < b) ? -1 : ((a > b) ? 1 : 0);
      }
    case RAW_T_NUM:
      {
	RAWAGG_VARREF ra_ref, rb_ref;
	COL_NUMREF na, nb;

	memcpy (&ra_ref, pa, sizeof (ra_ref));
	memcpy (&rb_ref, pb, sizeof (rb_ref));
	columnar_num_ref (ra->var + ra_ref.offset, &na);
	columnar_num_ref (ra->var + rb_ref.offset, &nb);
	return columnar_num_cmp (&na, &nb);
      }
    default:			/* RAW_T_STR: binary collation, length tie-break */
      {
	RAWAGG_VARREF ra_ref, rb_ref;
	int m, c;

	memcpy (&ra_ref, pa, sizeof (ra_ref));
	memcpy (&rb_ref, pb, sizeof (rb_ref));
	m = (ra_ref.length < rb_ref.length) ? ra_ref.length : rb_ref.length;
	c = (m > 0) ? memcmp (ra->var + ra_ref.offset, ra->var + rb_ref.offset, (size_t) m) : 0;
	if (c != 0)
	  {
	    return c;
	  }
	return (ra_ref.length > rb_ref.length) ? 1 : ((ra_ref.length < rb_ref.length) ? -1 : 0);
      }
    }
}

static int
rawagg_cmp_groups (const RAW_AGG * ra, const bool * desc, const bool * nulls_last, int ea, int eb)
{
  const char *a = RAWAGG_ENTRY (ra, ea);
  const char *b = RAWAGG_ENTRY (ra, eb);
  const unsigned char *na = RAWAGG_NULLS (ra, a);
  const unsigned char *nb = RAWAGG_NULLS (ra, b);
  const char *ka = RAWAGG_KEYS (ra, a);
  const char *kb = RAWAGG_KEYS (ra, b);
  int i;

  for (i = 0; i < ra->n_keys; i++)
    {
      bool anull = (na[i >> 3] & (1 << (i & 7))) != 0;
      bool bnull = (nb[i >> 3] & (1 << (i & 7))) != 0;
      int c;

      if (anull || bnull)
	{
	  if (anull && bnull)
	    {
	      continue;
	    }
	  /* NULL placement is absolute: it is not flipped by DESC */
	  c = anull ? -1 : 1;
	  if (nulls_last != NULL && nulls_last[i])
	    {
	      c = -c;
	    }
	  return c;
	}

      c = rawagg_cmp_key (ra, i, ka, kb);
      if (c != 0)
	{
	  return (desc != NULL && desc[i]) ? -c : c;
	}
    }
  return 0;
}

int
columnar_rawagg_sort_groups (RAW_AGG * ra, const bool * desc, const bool * nulls_last)
{
  int *idx;
  int i;

  free (ra->order);
  ra->order = NULL;
  if (ra->n_entries <= 1 || ra->n_keys == 0)
    {
      return NO_ERROR;
    }

  idx = (int *) malloc ((size_t) ra->n_entries * sizeof (int));
  if (idx == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      (size_t) ra->n_entries * sizeof (int));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  for (i = 0; i < ra->n_entries; i++)
    {
      idx[i] = i;
    }

  std::stable_sort (idx, idx + ra->n_entries,
		    [ra, desc, nulls_last] (int x, int y)
		    {
		      return rawagg_cmp_groups (ra, desc, nulls_last, x, y) < 0;
		    });

  ra->order = idx;
  return NO_ERROR;
}

int
columnar_rawagg_group_count (const RAW_AGG * ra)
{
  return ra->n_entries;
}

int
columnar_rawagg_key_count (const RAW_AGG * ra)
{
  return ra->n_keys;
}

int
columnar_rawagg_agg_count (const RAW_AGG * ra)
{
  return ra->n_aggs;
}

INT64
columnar_rawagg_row_count (const RAW_AGG * ra)
{
  return ra->n_rows;
}

DB_VALUE **
columnar_rawagg_key_targets (RAW_AGG * ra)
{
  return ra->key_target;
}

int
columnar_rawagg_load_group (RAW_AGG * ra, int g, DB_VALUE ** keys_out, AGGREGATE_TYPE * agg_list)
{
  AGGREGATE_TYPE *agg_p;
  char *e;
  const unsigned char *nulls;
  const char *keys;
  char *accs;
  int i, error;

  assert (g >= 0 && g < ra->n_entries);
  if (ra->order != NULL)
    {
      g = ra->order[g];
    }
  e = RAWAGG_ENTRY (ra, g);
  nulls = RAWAGG_NULLS (ra, e);
  keys = RAWAGG_KEYS (ra, e);
  accs = RAWAGG_ACCS (ra, e);

  for (i = 0; i < ra->n_keys; i++)
    {
      RAW_CELL cell;
      COL_NUMVAR nv;
      bool is_null = (nulls[i >> 3] & (1 << (i & 7))) != 0;

      if (keys_out == NULL || keys_out[i] == NULL)
	{
	  continue;
	}
      memset (&cell, 0, sizeof (cell));
      if (!is_null)
	{
	  switch (ra->key_type[i])
	    {
	    case RAW_T_I64:
	      memcpy (&cell.i64, keys + ra->key_offset[i], sizeof (INT64));
	      break;
	    case RAW_T_DBL:
	      memcpy (&cell.dbl, keys + ra->key_offset[i], sizeof (double));
	      break;
	    case RAW_T_NUM:
	      {
		RAWAGG_VARREF ref;
		COL_NUMREF nref;

		memcpy (&ref, keys + ra->key_offset[i], sizeof (ref));
		columnar_num_ref (ra->var + ref.offset, &nref);
		columnar_num_from_ref (&nref, &nv);
		/* the key image dropped the display scale; the domain carries it */
		nv.dscale = (ra->key_domain[i] != NULL) ? ra->key_domain[i]->scale : 0;
		cell.num = &nv;
	      }
	      break;
	    default:
	      {
		RAWAGG_VARREF ref;
		memcpy (&ref, keys + ra->key_offset[i], sizeof (ref));
		cell.str.ptr = ra->var + ref.offset;
		cell.str.len = ref.length;
	      }
	      break;
	    }
	}
      error = columnar_rawprog_to_dbvalue (ra->key_type[i], &cell, is_null, ra->key_domain[i], keys_out[i]);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }

  for (i = 0, agg_p = agg_list; i < ra->n_aggs && agg_p != NULL; i++, agg_p = agg_p->next)
    {
      const RAWAGG_SPEC *spec = &ra->aggs[i];
      char *acc = accs + spec->offset;
      INT64 cnt = *(INT64 *) acc;
      /*
       * SUM widens, so its accumulator domain is the right target.  MIN/MAX
       * do not: they return one of the input values unchanged, so the result
       * type is the OPERAND's.  Using the accumulator domain for them turned
       * MIN(date_col) into a BIGINT the engine then refused to coerce back.
       */
      TP_DOMAIN *acc_dom = spec->agg->accumulator_domain.value_dom;
      bool is_minmax = (spec->func == PT_MIN || spec->func == PT_MAX);

      /*
       * The accumulator domain is not always resolved by the time a group is
       * emitted (a GROUP BY block resolves domains later than a BUILDVALUE
       * one).  An unresolved domain is a NULL-typed domain, not a NULL
       * pointer, and coercing into it fails with "cannot coerce to domain
       * *NULL*" -- so fall back to the aggregate's result domain, and treat
       * still-unresolved as "no target domain" rather than forcing one.
       */
      if (acc_dom == NULL || TP_DOMAIN_TYPE (acc_dom) == DB_TYPE_NULL)
	{
	  acc_dom = spec->agg->domain;
	}
      if (acc_dom != NULL && TP_DOMAIN_TYPE (acc_dom) == DB_TYPE_NULL)
	{
	  acc_dom = NULL;
	}
      TP_DOMAIN *dom = acc_dom;
      DB_VALUE *dest = agg_p->accumulator.value;

      if (is_minmax && spec->operand_domain != NULL
	  && TP_DOMAIN_TYPE (spec->operand_domain) != DB_TYPE_VARIABLE)
	{
	  dom = spec->operand_domain;
	}

      /*
       * agg_list is NOT necessarily the list the specs were built from:
       * qexec_initialize_groupby_state () gives each group-by dimension its
       * own copy, with the same nodes in the same order but distinct
       * accumulator DB_VALUEs.  So the results go into the CALLER's list,
       * while the (copied, hence identical) domain is read from the spec.
       */

      /* the destination must not carry a pending deferred NUMERIC sum */
      qdata_numeric_sum_discard (&agg_p->accumulator);
      agg_p->accumulator.curr_cnt = cnt;

      if (spec->func == PT_COUNT_STAR)
	{
	  /* qdata_finalize_aggregate_list () fills the value from curr_cnt */
	  db_make_bigint (dest, cnt);
	  continue;
	}
      if (spec->kind == RAWAGG_COUNT)
	{
	  db_make_bigint (dest, cnt);
	  continue;
	}
      if (cnt == 0)
	{
	  db_make_null (dest);
	  continue;
	}

      /*
       * Every accumulator is materialized through the one conversion that
       * knows how to reconcile a raw cell with a target domain.  Building the
       * DB_VALUE here by hand is what produced values whose type disagreed
       * with the domain the tuple writer sizes by.
       */
      {
	RAW_CELL cell;
	COL_NUMVAR nv;
	RAW_TYPE ctype;

	memset (&cell, 0, sizeof (cell));
	switch (spec->kind)
	  {
	  case RAWAGG_SUM_I64:
	  case RAWAGG_MINMAX_I64:
	    ctype = RAW_T_I64;
	    cell.i64 = *(INT64 *) (acc + 8);
	    break;
	  case RAWAGG_SUM_DBL:
	  case RAWAGG_MINMAX_DBL:
	    ctype = RAW_T_DBL;
	    cell.dbl = *(double *) (acc + 8);
	    break;
	  case RAWAGG_MINMAX_NUM:
	    ctype = RAW_T_NUM;
	    nv = *(COL_NUMVAR *) (acc + 8);
	    cell.num = &nv;
	    break;
	  case RAWAGG_SUM_NUM:
	    /* handled below: a NUMERIC sum accumulator is a float-numeric */
	    ctype = RAW_T_NUM;
	    nv = *(COL_NUMVAR *) (acc + 8);
	    cell.num = &nv;
	    break;
	  default:		/* RAWAGG_MINMAX_STR */
	    {
	      RAWAGG_STRREF *ref = (RAWAGG_STRREF *) (acc + 8);

	      ctype = RAW_T_STR;
	      cell.str.ptr = ra->var + ref->offset;
	      cell.str.len = ref->length;
	    }
	    break;
	  }

	if (spec->kind == RAWAGG_SUM_NUM)
	  {
	    /*
	     * Mirror numeric_sum_state_result (): the engine's NUMERIC SUM/AVG
	     * accumulator is a FLOATING-SCALE numeric, carrying its precision and
	     * scale in the DB_VALUE's numeric header rather than its domain, and
	     * it is then coerced to the accumulator domain.  Handing those
	     * consumers an ordinary fixed-scale numeric makes them read a scale
	     * of zero -- which is why AVG came out 10^scale too large, SUM
	     * collapsed to NULL, and the tuple writer sized a buffer wrongly.
	     */
	    error = columnar_num_to_dbvalue_ex (&nv, columnar_num_decimal_digits (&nv), nv.dscale, true, dest);
	    if (error != NO_ERROR)
	      {
		return error;
	      }
	    if (dom != NULL)
	      {
		error = expr_coerce_result_to_domain (dest, dom);
		if (error != NO_ERROR)
		  {
		    return error;
		  }
	      }
	  }
	else
	  {
	    error = columnar_rawprog_to_dbvalue (ctype, &cell, false, dom, dest);
	    if (error != NO_ERROR)
	      {
		return error;
	      }
	  }
      }
    }
  return NO_ERROR;
}

void
columnar_rawagg_free (RAW_AGG * ra)
{
  if (ra == NULL)
    {
      return;
    }
  columnar_rawprog_free (ra->prog);
  free (ra->key_cell);
  free (ra->key_type);
  free (ra->key_domain);
  free (ra->key_offset);
  free (ra->key_target);
  free (ra->aggs);
  free (ra->order);
  free (ra->arena);
  free (ra->var);
  free (ra->probe_var);
  free (ra->buckets);
  free (ra);
}
