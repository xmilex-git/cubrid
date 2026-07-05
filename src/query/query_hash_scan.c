/*
 *
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <math.h>
#if defined(sun) || defined(HPUX)
#include <sys/types.h>
#include <netinet/in.h>
#endif
#if defined(_AIX)
#include <net/nh.h>
#endif

#include "fetch.h"
#include "memory_alloc.h"
#include "memory_hash.h"
#include "object_domain.h"
#include "object_primitive.h"
#include "object_representation.h"
#include "query_opfunc.h"
#include "string_opfunc.h"
#include "query_hash_scan.h"
#include "query_manager.h"
#include "db_value_printer.hpp"
#include "dbtype.h"
#include "chartype.h"
#include "storage_common.h"
#include "error_manager.h"
#include "page_buffer.h"
#include "slotted_page.h"
#include "file_manager.h"
#include "db_date.h"
#include "thread_compat.hpp"
#include "oid.h"
#include "qfile_tape.hpp"
#include "qfile_buffile.hpp"
#include "query_workmem.hpp"	/* work_mem accountant: reserve_held / release_held */

#include <algorithm>
#include <atomic>
#include <string>
#include <vector>
// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"


static bool safe_memcpy (void *data, void *source, int size);
static DB_VALUE_COMPARE_RESULT qdata_hscan_key_compare (HASH_SCAN_KEY * ckey1, HASH_SCAN_KEY * ckey2, int *diff_pos);
/* Key hash for the HASH_FILE tier, reused by hls_spill through
 * qdata_hash_scan_key (HASH_METH_HASH_FILE). */
static unsigned int hls_spill_hash (void *original_key_p);
static unsigned int hls_spill_hash_four_bytes_type (char *key_p);


/****************************************************************************/
/************************ file hash structure *******************************/
/****************************************************************************/

/*
 * qdata_alloc_hscan_key () - allocate new hash key
 *   returns: pointer to new structure or NULL on error
 *   thread_p(in): thread
 *   val_cnt(in): size of key
 *   alloc_vals(in): if true will allocate dbvalues
 */
HASH_SCAN_KEY *
qdata_alloc_hscan_key (cubthread::entry * thread_p, int val_cnt, bool alloc_vals)
{
  HASH_SCAN_KEY *key;
  int i;

  key = (HASH_SCAN_KEY *) db_private_alloc (thread_p, sizeof (HASH_SCAN_KEY));
  if (key == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (HASH_SCAN_KEY));
      return NULL;
    }

  key->values = (DB_VALUE **) db_private_alloc (thread_p, sizeof (DB_VALUE *) * val_cnt);
  if (key->values == NULL)
    {
      db_private_free (thread_p, key);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (DB_VALUE *) * val_cnt);
      return NULL;
    }

  if (alloc_vals)
    {
      for (i = 0; i < val_cnt; i++)
	{
	  key->values[i] = pr_make_value ();
	  if (key->values[i] == NULL)
	    {
	      key->free_values = true;
	      qdata_free_hscan_key (thread_p, key, i);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (DB_VALUE *));
	      return NULL;
	    }
	}
    }

  key->val_count = val_cnt;
  key->free_values = alloc_vals;
  return key;
}

/*
 * qdata_free_hscan_key () - free hash key
 *   thread_p(in): thread
 *   key(in): hash key
 */
void
qdata_free_hscan_key (cubthread::entry * thread_p, HASH_SCAN_KEY * key, int val_count)
{
  if (key == NULL)
    {
      return;
    }

  if (key->values != NULL)
    {
      if (key->free_values)
	{
	  for (int i = 0; i < val_count; i++)
	    {
	      if (key->values[i])
		{
		  pr_free_value (key->values[i]);
		}
	    }
	}

      /* free values array */
      db_private_free (thread_p, key->values);
    }

  /* free structure */
  db_private_free (thread_p, key);
}

/*
 * qdata_hash_scan_key () - compute hash of aggregate key
 *   returns: hash value
 *   key(in): key
 *   ht_size(in): hash table size (in buckets)
 */
unsigned int
qdata_hash_scan_key (const void *key, unsigned int ht_size, HASH_METHOD hash_method)
{
  HASH_SCAN_KEY *ckey = (HASH_SCAN_KEY *) key;
  unsigned int hash_val = 0, tmp_hash_val;
  int i;

  /* build hash value */
  for (i = 0; i < ckey->val_count; i++)
    {
      hash_val = ROTL32 (hash_val, 13);
      tmp_hash_val = mht_get_hash_number (ht_size, ckey->values[i]);
      hash_val ^= tmp_hash_val;
      if (hash_val == 0)
	{
	  hash_val = tmp_hash_val;
	}
    }

  if (hash_method == HASH_METH_HASH_FILE)
    {
      hash_val = hls_spill_hash (&hash_val);
    }

  return hash_val;
}

/*
 * qdata_hscan_key_compare () - compare two aggregate keys
 *   returns: comparison result
 *   key1(in): first key
 *   key2(in): second key
 *   diff_pos(out): if not equal, position of difference, otherwise -1
 */
static DB_VALUE_COMPARE_RESULT
qdata_hscan_key_compare (HASH_SCAN_KEY * ckey1, HASH_SCAN_KEY * ckey2, int *diff_pos)
{
  DB_VALUE_COMPARE_RESULT result;
  int i;

  assert (diff_pos);
  *diff_pos = -1;

  if (ckey1 == ckey2)
    {
      /* same pointer, same values */
      return DB_EQ;
    }

  if (ckey1->val_count != ckey2->val_count)
    {
      /* can't compare keys of different sizes; shouldn't get here */
      assert (false);
      return DB_UNK;
    }

  for (i = 0; i < ckey1->val_count; i++)
    {
      result = tp_value_compare (ckey1->values[i], ckey2->values[i], 0, 1);
      if (result != DB_EQ)
	{
	  *diff_pos = i;
	  return result;
	}
    }

  /* if we got this far, it's equal */
  return DB_EQ;
}

/*
 * qdata_hscan_key_eq () - check equality of two aggregate keys
 *   returns: true if equal, false otherwise
 *   key1(in): first key
 *   key2(in): second key
 */
int
qdata_hscan_key_eq (const void *key1, const void *key2)
{
  int decoy;

  /* compare for equality */
  return (qdata_hscan_key_compare ((HASH_SCAN_KEY *) key1, (HASH_SCAN_KEY *) key2, &decoy) == DB_EQ);
}

/*
 * qdata_build_hscan_key () - build aggregate key structure from reguvar list
 *   returns: NO_ERROR or error code
 *   thread_p(in): thread
 *   key(out): aggregate key
 *   regu_list(in): reguvar list for fetching values
 */
int
qdata_build_hscan_key (THREAD_ENTRY * thread_p, val_descr * vd, REGU_VARIABLE_LIST regu_list, HASH_SCAN_KEY * key)
{
  int rc = NO_ERROR;

  /* build key */
  key->free_values = false;	/* references precreated DB_VALUES */
  key->val_count = 0;
  while (regu_list != NULL)
    {
      rc = fetch_peek_dbval (thread_p, &regu_list->value, vd, NULL, NULL, NULL, &key->values[key->val_count]);
      if (rc != NO_ERROR)
	{
	  return rc;
	}

      /* next */
      regu_list = regu_list->next;
      key->val_count++;
    }

  /* all ok */
  return NO_ERROR;
}

/*
 * qdata_print_hash_scan_entry () - Print the entry
 *                              Will be used by mht_dump_hls() function
 *   return:
 *   fp(in)     :
 *   key(in)    :
 *   data(in)   :
 *   args(in)   :
 */
int
qdata_print_hash_scan_entry (THREAD_ENTRY * thread_p, FILE * fp, const void *data, const void *type_list, void *args)
{
  HASH_SCAN_VALUE *data_p;
  HASH_METHOD hash_list_scan_type;
  QFILE_TUPLE_VALUE_TYPE_LIST *type_list_p;
  DB_VALUE dbval;
  const PR_TYPE *pr_type_p;
  int i;
  char *tuple_p;
  OR_BUF buf;

  if (data == NULL || type_list == NULL || args == NULL)
    {
      return false;
    }

  data_p = (HASH_SCAN_VALUE *) data;

  hash_list_scan_type = *((HASH_METHOD *) args);
  if (hash_list_scan_type == HASH_METH_NOT_USE)
    {
      return false;
    }

  type_list_p = (QFILE_TUPLE_VALUE_TYPE_LIST *) type_list;
  if (type_list_p->type_cnt <= 0)
    {
      return false;
    }

  db_make_null (&dbval);

  if (fp == NULL)
    {
      fp = stdout;
    }

  fprintf (fp, "LIST_CACHE_ENTRY (%p) - ", data);

  if (hash_list_scan_type == HASH_METH_IN_MEM)
    {
      fprintf (fp, "data_size = [%d], data = { ", QFILE_GET_TUPLE_LENGTH (data_p->tuple));

      tuple_p = (char *) data_p->tuple + QFILE_TUPLE_LENGTH_SIZE;

      for (i = 0; i < type_list_p->type_cnt; i++)
	{
	  if (QFILE_GET_TUPLE_VALUE_FLAG (tuple_p) == V_BOUND)
	    {
	      or_init (&buf, tuple_p + QFILE_TUPLE_VALUE_HEADER_SIZE, QFILE_GET_TUPLE_VALUE_LENGTH (tuple_p));

	      pr_type_p = type_list_p->domp[i]->type;
	      pr_type_p->data_readval (&buf, &dbval, type_list_p->domp[i], -1, false /* Don't copy */ , NULL, 0);

	      db_fprint_value (fp, &dbval);

	      if (db_value_need_clear (&dbval))
		{
		  pr_clear_value (&dbval);
		}
	    }
	  else
	    {
	      fprintf (fp, "VALUE_UNBOUND");
	    }

	  if (i != type_list_p->type_cnt - 1)
	    {
	      fprintf (stdout, " , ");
	    }

	  tuple_p += QFILE_TUPLE_VALUE_HEADER_SIZE + QFILE_GET_TUPLE_VALUE_LENGTH (tuple_p);
	}

      fprintf (fp, " }");
    }
  else if (hash_list_scan_type == HASH_METH_HYBRID)
    {
      if (qfile_tuple_simple_pos_is_spill (data_p->pos))
	{
	  fprintf (fp, "spill_segment_id = [%llu]  page_index = [%d]  tuple_offset = [%d]",
		   (unsigned long long) data_p->pos->spill_segment_id, data_p->pos->page_index,
		   data_p->pos->tuple_offset);
	}
      else
	{
	  fprintf (fp, "pageid = [%d]  volid = [%d]  offset = [%d]", data_p->pos->vpid.pageid,
		   data_p->pos->vpid.volid, data_p->pos->offset);
	}
    }
  else if (hash_list_scan_type == HASH_METH_HASH_FILE)
    {
      /* nothing to do */
    }
  else
    {
      /* nothing to do */
    }

  fprintf (fp, "\n");

  return true;
}

/*
 * qdata_copy_hscan_key () - deep copy hash key
 *   returns: pointer to new hash key
 *   thread_p(in): thread
 *   key(in): source key
 */
HASH_SCAN_KEY *
qdata_copy_hscan_key (cubthread::entry * thread_p, HASH_SCAN_KEY * key, REGU_VARIABLE_LIST probe_regu_list,
		      val_descr * vd)
{
  HASH_SCAN_KEY *new_key = NULL;
  int i = 0;
  DB_TYPE vtype1, vtype2;
  TP_DOMAIN_STATUS status = DOMAIN_COMPATIBLE;

  if (key)
    {
      /* make a copy */
      new_key = qdata_alloc_hscan_key (thread_p, key->val_count, false);
    }

  if (new_key)
    {
      /* copy values */
      new_key->val_count = key->val_count;
      new_key->free_values = true;
      for (i = 0; i < key->val_count; i++)
	{
	  vtype1 = REGU_VARIABLE_GET_TYPE (&probe_regu_list->value);
	  vtype2 = DB_VALUE_DOMAIN_TYPE (key->values[i]);

	  if (vtype1 != vtype2)
	    {
	      new_key->values[i] = pr_make_value ();
	      if (new_key->values[i] == NULL)
		{
		  qdata_free_hscan_key (thread_p, new_key, i);
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (DB_VALUE *));
		  return NULL;
		}

	      status = tp_value_coerce (key->values[i], new_key->values[i], probe_regu_list->value.domain);
	      if (status != DOMAIN_COMPATIBLE)
		{
		  qdata_free_hscan_key (thread_p, new_key, ++i);
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2, pr_type_name (vtype2),
			  pr_type_name (vtype1));
		  return NULL;
		}
	    }
	  else
	    {
	      new_key->values[i] = pr_copy_value (key->values[i]);
	      if (new_key->values[i] == NULL)
		{
		  qdata_free_hscan_key (thread_p, new_key, i);
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (DB_VALUE *));
		  return NULL;
		}
	    }
	  probe_regu_list = probe_regu_list->next;
	}
    }

  return new_key;
}

/*
 * qdata_copy_hscan_key_without_alloc () - deep copy hash key
 *   returns: pointer to new hash key
 *   thread_p(in): thread
 *   key(in): source key
 */
HASH_SCAN_KEY *
qdata_copy_hscan_key_without_alloc (cubthread::entry * thread_p, HASH_SCAN_KEY * key,
				    REGU_VARIABLE_LIST probe_regu_list, HASH_SCAN_KEY * new_key)
{
  DB_TYPE vtype1, vtype2;
  TP_DOMAIN_STATUS status = DOMAIN_COMPATIBLE;

  if (key == NULL)
    {
      return NULL;
    }
  if (new_key)
    {
      /* copy values */
      new_key->val_count = key->val_count;
      for (int i = 0; i < key->val_count; i++)
	{
	  vtype1 = REGU_VARIABLE_GET_TYPE (&probe_regu_list->value);
	  vtype2 = DB_VALUE_DOMAIN_TYPE (key->values[i]);

	  if (vtype1 != vtype2)
	    {
	      pr_clear_value (new_key->values[i]);
	      status = tp_value_coerce (key->values[i], new_key->values[i], probe_regu_list->value.domain);
	      if (status != DOMAIN_COMPATIBLE)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2, pr_type_name (vtype2),
			  pr_type_name (vtype1));
		  return NULL;
		}
	    }
	  else
	    {
	      pr_clear_value (new_key->values[i]);
	      if (pr_clone_value (key->values[i], new_key->values[i]) != NO_ERROR)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (DB_VALUE *));
		  return NULL;
		}
	    }
	  probe_regu_list = probe_regu_list->next;
	}
    }

  return new_key;
}

/*
 * qdata_alloc_hscan_value () - allocate new hash value
 *   returns: pointer to new structure or NULL on error
 *   thread_p(in): thread
 */
HASH_SCAN_VALUE *
qdata_alloc_hscan_value (cubthread::entry * thread_p, QFILE_TUPLE tpl)
{
  HASH_SCAN_VALUE *value;
  int tuple_size = QFILE_GET_TUPLE_LENGTH (tpl);

  /* alloc structure */
  value = (HASH_SCAN_VALUE *) db_private_alloc (thread_p, sizeof (HASH_SCAN_VALUE));
  if (value == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (HASH_SCAN_VALUE));
      return NULL;
    }

  value->tuple = (QFILE_TUPLE) db_private_alloc (thread_p, tuple_size);
  if (value->tuple == NULL)
    {
      qdata_free_hscan_value (thread_p, value);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, tuple_size);
      return NULL;
    }
  /* save tuple */
  if (!safe_memcpy (value->tuple, tpl, tuple_size))
    {
      qdata_free_hscan_value (thread_p, value);
      return NULL;
    }
  return value;
}

/*
 * qdata_alloc_hscan_value_OID () - allocate new hash OID value
 *   returns: pointer to new structure or NULL on error
 *   thread_p(in): thread
 */
HASH_SCAN_VALUE *
qdata_alloc_hscan_value_OID (cubthread::entry * thread_p, QFILE_LIST_SCAN_ID * scan_id_p)
{
  HASH_SCAN_VALUE *value;

  /* alloc structure */
  value = (HASH_SCAN_VALUE *) db_private_alloc (thread_p, sizeof (HASH_SCAN_VALUE));
  if (value == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (HASH_SCAN_VALUE));
      return NULL;
    }

  value->pos = (QFILE_TUPLE_SIMPLE_POS *) db_private_alloc (thread_p, sizeof (QFILE_TUPLE_SIMPLE_POS));
  if (value->pos == NULL)
    {
      qdata_free_hscan_value (thread_p, value);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (QFILE_TUPLE_SIMPLE_POS));
      return NULL;
    }

  /* save position */
  qdata_save_hscan_pos (scan_id_p, value->pos);

  return value;
}

/*
 * qdata_save_hscan_pos () - save the scan's current tuple position as a
 *   backing-aware SIMPLE_POS (TAPE / spill / VPID).  Shared by the HYBRID
 *   value producer above and the batch-spill build.
 */
void
qdata_save_hscan_pos (QFILE_LIST_SCAN_ID * scan_id_p, QFILE_TUPLE_SIMPLE_POS * pos)
{
  if (scan_id_p->tapeset_scan_ != NULL)
    {
      /* Tapeset list: the mirror curr_vpid is synthetic (volid = NULL_VOLID,
       * pageid = page offset within the current Tape) and loses tape_idx.
       * Save the scan's first-class TAPE coordinate instead. */
      QFILE_TUPLE_POSITION tape_pos;

      qfile_tapeset_scan_save_position (scan_id_p, &tape_pos);
      assert (qfile_tuple_position_is_tape (&tape_pos));
      qfile_tuple_simple_pos_set_tape (pos, tape_pos.tape_idx, tape_pos.tape_page_offset, tape_pos.tape_byte_offset);
    }
  else if (qmgr_tfile_has_fd_overflow (QFILE_LIST_ID_TFILE_VFID (&(scan_id_p->list_id)))
      && scan_id_p->curr_vpid.volid == NULL_VOLID
      && scan_id_p->curr_vpid.pageid > QFILE_LIST_ID_TFILE_VFID(&(scan_id_p->list_id))->membuf_last)
    {
      qfile_tuple_simple_pos_set_spill (pos,
					 qmgr_tfile_fd_overflow_segment_id (QFILE_LIST_ID_TFILE_VFID (&(scan_id_p->list_id))),
					 scan_id_p->curr_vpid.pageid, scan_id_p->curr_offset);
    }
  else
    {
      qfile_tuple_simple_pos_set_vpid (pos, &scan_id_p->curr_vpid, scan_id_p->curr_offset);
    }
}

static bool
safe_memcpy (void *data, void *source, int size)
{
  if (size < 0)
    {
      return false;
    }
  memcpy (data, source, (size_t) size);
  return true;
}

/*
 * qdata_free_hscan_value () - free hash value
 *   thread_p(in): thread
 *   key(in): hash value
 */
void
qdata_free_hscan_value (cubthread::entry * thread_p, HASH_SCAN_VALUE * value)
{
  if (value == NULL)
    {
      return;
    }

  /* free values */
  if (value->data != NULL)
    {
      db_private_free_and_init (thread_p, value->data);
    }
  /* free structure */
  db_private_free_and_init (thread_p, value);
}

/*
 * qdata_free_agg_hentry () - free key-value pair of hash entry
 *   returns: error code or NO_ERROR
 *   key(in): key pointer
 *   data(in): value pointer
 *   args(in): args passed by mht_rem (should be null)
 */
int
qdata_free_hscan_entry (const void *key, void *data, void *args)
{
  /* free key */
  qdata_free_hscan_key ((cubthread::entry *) args, (HASH_SCAN_KEY *) key, key ? ((HASH_SCAN_KEY *) key)->val_count : 0);

  /* free tuple */
  qdata_free_hscan_value ((cubthread::entry *) args, (HASH_SCAN_VALUE *) data);

  /* all ok */
  return NO_ERROR;
}

/*
 * #144 P3 D2: per-build bump arena for IN_MEM HASH_SCAN_VALUE + inline tuple.
 *
 * The IN_MEM hash-join build previously did two db_private_alloc per row (the
 * 8-byte value + the tuple copy) -> a ~2N-alloc mspace storm that made the
 * in-memory build the bottleneck (P4: 33% self in mspace_malloc).  The arena
 * bump-allocates value+tuple contiguously from large malloc'd blocks and frees
 * every block at once in hjoin_scan_clear.  Values are arena-owned and are NEVER
 * passed to qdata_free_hscan_value: the mht clear uses qdata_noop_free_hscan_entry
 * (mht_clear_hls passes key=NULL, so only the value would have been freed, and
 * the inline uint keys need no free -> no leak).  Owned by the build (single)
 * context; probe workers only read through the shared hash table, so the arena
 * must outlive probe (same lifetime as the shared table).  On any build error the
 * whole arena is reclaimed by hjoin_scan_clear (no per-entry poison needed).
 */
struct hscan_value_arena_block
{
  struct hscan_value_arena_block *next;
  size_t size;			/* usable bytes in data[] */
  size_t used;
  char data[1];			/* flexible array (over-allocated) */
};

struct hscan_value_arena
{
  struct hscan_value_arena_block *head;	/* most-recent block (bump target) */
  size_t default_block;		/* new-block size floor */
};

#define HSCAN_ARENA_DEFAULT_BLOCK (1024ULL * 1024ULL)	/* 1MiB blocks */

HSCAN_VALUE_ARENA *
hscan_value_arena_create (void)
{
  HSCAN_VALUE_ARENA *arena = (HSCAN_VALUE_ARENA *) malloc (sizeof (HSCAN_VALUE_ARENA));
  if (arena == NULL)
    {
      return NULL;		/* caller falls back to per-entry alloc */
    }
  arena->head = NULL;
  arena->default_block = HSCAN_ARENA_DEFAULT_BLOCK;
  return arena;
}

void
hscan_value_arena_destroy (HSCAN_VALUE_ARENA * arena)
{
  if (arena == NULL)
    {
      return;
    }
  struct hscan_value_arena_block *b = arena->head;
  while (b != NULL)
    {
      struct hscan_value_arena_block *next = b->next;
      free (b);
      b = next;
    }
  free (arena);
}

static char *
hscan_value_arena_bump (HSCAN_VALUE_ARENA * arena, size_t n)
{
  n = (n + 7) & ~((size_t) 7);	/* 8-byte align */
  if (arena->head == NULL || arena->head->used + n > arena->head->size)
    {
      size_t bsz = (n > arena->default_block) ? n : arena->default_block;
      struct hscan_value_arena_block *b =
	(struct hscan_value_arena_block *) malloc (offsetof (struct hscan_value_arena_block, data) + bsz);
      if (b == NULL)
	{
	  return NULL;
	}
      b->next = arena->head;
      b->size = bsz;
      b->used = 0;
      arena->head = b;
    }
  char *p = arena->head->data + arena->head->used;
  arena->head->used += n;
  return p;
}

HASH_SCAN_VALUE *
qdata_alloc_hscan_value_arena (HSCAN_VALUE_ARENA * arena, QFILE_TUPLE tpl)
{
  int tuple_size = QFILE_GET_TUPLE_LENGTH (tpl);
  char *p = hscan_value_arena_bump (arena, sizeof (HASH_SCAN_VALUE) + (size_t) tuple_size);
  if (p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (HASH_SCAN_VALUE) + (size_t) tuple_size);
      return NULL;
    }
  HASH_SCAN_VALUE *value = (HASH_SCAN_VALUE *) p;
  value->tuple = (QFILE_TUPLE) (p + sizeof (HASH_SCAN_VALUE));	/* inline, contiguous */
  if (!safe_memcpy (value->tuple, tpl, tuple_size))
    {
      return NULL;		/* arena reclaims the bumped bytes en masse */
    }
  return value;
}

/* mht-clear callback for arena-owned values: nothing to free per entry (the
 * whole arena is destroyed by hjoin_scan_clear); mht_clear_hls passes key=NULL. */
int
qdata_noop_free_hscan_entry (const void *key, void *data, void *args)
{
  (void) key;
  (void) data;
  (void) args;
  return NO_ERROR;
}


/*
 * hls_spill_hash () - Hash function
 *   return: unsigned int
 *   orig_key(in): original key to encode into a pseudo key
 *
 * Note: This function converts the given original key into a pseudo
 * key. Since the original key is presented as a character
 * string, its conversion into a int-compatible type is essential
 * prior to performing any operation on it.
 * This function does not change the value of parameter
 * orig_key, as it might be on a bucket.
 */
static unsigned int
hls_spill_hash (void *original_key_p)
{
  char *key = (char *) original_key_p;
  unsigned int hash_key = 0;
  hash_key = hls_spill_hash_four_bytes_type (key);

  return hash_key;
}

static unsigned int
hls_spill_hash_four_bytes_type (char *key_p)
{
  unsigned int hash_key = 0;
  unsigned int i;
  char Char;

  hash_key = (unsigned int) ntohl (*(unsigned int *) key_p);

  /* Go over the chars of the given pseudo key */
  Char = '\0';
  key_p = (char *) &hash_key;
  for (i = 0; i < sizeof (unsigned int); i++)
    {
      Char += (char) *key_p++;
    }

  /* Change the first byte of the pseudo key to the SUM of all of them */
  memcpy (&hash_key, &Char, sizeof (char));

  return hash_key;
}


/****************************************************************************/
/*********** batch-spill hash (HASH_FILE tier) *****************************/
/****************************************************************************/
/* Per-batch append-only BufFiles, borrowing the PostgreSQL nodeHash.c idiom
 * (ExecHashGetBucketAndBatch batch split by hash bits; ExecHashIncreaseNumBatches
 * growth when a batch exceeds its memory target).  Symmetric with the PHJ
 * partition spill.
 *
 * Build:    entries (hash_key, QFILE_TUPLE_SIMPLE_POS) are appended to
 *           nbatch BufFiles, batchno = hash & (nbatch - 1).  nbatch is sized
 *           up front from the exact list tuple_cnt (materialized list).
 * Finalize: each batch is loaded whole (<= target, work_mem-accounted),
 *           sorted by hash, written back as a sorted run + an in-memory
 *           page-fence index (first hash per page).  An overweight batch
 *           (hash skew or accountant pressure) is range-bisected by hash;
 *           when bisection cannot help (depth cap: all-equal hashes), we
 *           soft-reserve and overshoot.
 * Probe:    batchno -> run by hash range -> fence binary search -> one
 *           read_page -> in-page binary search; duplicates are adjacent.
 *
 * Values are coord_type-tagged SIMPLE_POS, so tapeset build lists work
 * (TAPE coords).  Entries carry no user data (hash + coordinates), so
 * spill files need no TDE. */

typedef struct hls_spill_entry HLS_SPILL_ENTRY;
struct hls_spill_entry
{
  UINT32 hash;
  UINT32 reserved;
  QFILE_TUPLE_SIMPLE_POS pos;
};

/* SIMPLE_POS is 8-byte aligned (spill UINT64), so 4+16+4 pads to 32 */
static_assert (sizeof (QFILE_TUPLE_SIMPLE_POS) == 32, "SIMPLE_POS layout drift");
static_assert (sizeof (HLS_SPILL_ENTRY) == 40, "spill entry layout drift");

static const int HLS_SPILL_MAX_NBATCH = 512;
static const int HLS_SPILL_BISECT_DEPTH_MAX = 4;

struct hls_spill_run
{
  qfile::buffile *bf;		/* sorted run */
  INT64 entry_cnt;
  UINT32 first_hash;		/* run hash range lower bound (runs are disjoint, ascending) */
  std::vector<UINT32> fences;	/* first hash per page */
};

struct hls_spill_batch
{
  qfile::buffile *raw;		/* build-phase append file (consumed by finalize) */
  INT64 entry_cnt;
  std::vector<hls_spill_run> runs;	/* finalize output */
};

struct hls_spill
{
  int nbatch;			/* power of two */
  std::vector<hls_spill_batch> batches;
  char *stage;			/* nbatch staging pages (build) + 1 finalize-scratch page (tail slot) */
  std::vector<int> stage_cnt;	/* staged entries per batch */
  std::string dir;
  std::uint64_t seq_base;
  std::uint64_t seq_next;
  bool finalized;
  /* accountant */
  size_t charged_bytes;
  int charged_shard;
  /* Build/finalize-only read scratch (single-threaded, runs to completion
   * before any probing starts) -- NOT shared with probe, see HLS_SPILL_CURSOR. */
  qfile::tde_read_scratch scratch;
  /* instrumentation: read_page calls across all probe cursors of this scan,
   * flushed in by hls_spill_cursor_destroy() as each worker tears down. */
  std::atomic<long> probe_page_reads;
};

/* PROBE-side mutable state: each HASH_LIST_SCAN that probes a given
 * HLS_SPILL owns exactly one of these.  In parallel probe, every worker
 * shares the same HLS_SPILL (built once) but must create its own cursor --
 * sharing a cursor across workers reintroduces the race this type fixes. */
struct hls_spill_cursor
{
  int probe_batch;
  size_t probe_run;
  INT64 probe_idx;		/* entry index within run */
  UINT32 probe_hash;
  qfile::buffile *probe_page_bf;	/* which run file the cached page belongs to */
  int probe_page_off;		/* cached page offset, -1 = none */
  char *page;			/* this cursor's own cached probe page (DB_PAGESIZE) */
  long page_reads;		/* read_page calls through this cursor */
  qfile::tde_read_scratch scratch;
};

/* entries per page — DB_PAGESIZE is a boot-time runtime value, so this must
 * not be a namespace-scope static initializer */
#define HLS_SPILL_EPP ((INT64) (DB_PAGESIZE / (int) sizeof (HLS_SPILL_ENTRY)))

static char *
hls_spill_stage_page (HLS_SPILL * spill, int batchno)
{
  return spill->stage + (size_t) batchno * DB_PAGESIZE;
}

/* Build/finalize-only scratch page (tail stage slot).  Safe to share: finalize
 * runs single-threaded to completion before any probe cursor is created. */
static char *
hls_spill_finalize_scratch_page (HLS_SPILL * spill)
{
  return spill->stage + (size_t) spill->nbatch * DB_PAGESIZE;
}

static qfile::buffile *
hls_spill_new_file (THREAD_ENTRY * thread_p, HLS_SPILL * spill)
{
  int os_error = 0;
  qfile::buffile *bf =
    qfile::buffile::create (thread_p, spill->dir.c_str (), spill->seq_next++, 0, TDE_ALGORITHM_NONE, &os_error);
  if (bf == NULL)
    {
      /* only fd-exhaustion/disk-full class errors are out-of-temp-space;
       * anything else (e.g. EACCES) reports ER_FAILED instead of being
       * mislabeled as temp-space exhaustion. */
      qfile::spill_file::set_os_error (os_error);
    }
  return bf;
}

/*
 * hls_spill_create () - create a batch-spill hash sized from the exact build
 *   list tuple_cnt.  Charges the staging pages (+ 1 probe page) to the
 *   work_mem accountant; NULL on reservation or scratch-dir failure.
 */
HLS_SPILL *
hls_spill_create (THREAD_ENTRY * thread_p, INT64 tuple_cnt)
{
  UINT64 work_mem = (UINT64) prm_get_bigint_value (PRM_ID_WORK_MEM);
  UINT64 target = (work_mem / 2 > (UINT64) DB_PAGESIZE) ? work_mem / 2 : (UINT64) DB_PAGESIZE;
  UINT64 total_bytes = (UINT64) tuple_cnt * sizeof (HLS_SPILL_ENTRY);

  int nbatch = 1;
  while (nbatch < HLS_SPILL_MAX_NBATCH && total_bytes / (UINT64) nbatch > target)
    {
      nbatch <<= 1;
    }

  std::string dir;
  if (!qfile::buffile::default_scratch_dir (dir))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OUT_OF_TEMP_SPACE, 0);
      return NULL;
    }

  size_t stage_bytes = ((size_t) nbatch + 1) * DB_PAGESIZE;	/* + probe page */
  int shard = -1;
  if (!temp_page_store::reserve_held (stage_bytes, &shard))
    {
      /* The spill tier is the floor: it must stay constructible under pool
       * saturation (refusing would fall to the plain list scan's O(n)-per-probe
       * cliff, and an unset-error NULL trips ASSERT_ERROR upstream).  The
       * staging footprint is small and bounded (<= MAX_NBATCH + 1 pages), so
       * overshoot soft-accounted. */
      temp_page_store::record_degrade ();
      temp_page_store::reserve_held_soft (stage_bytes, &shard);
    }

  HLS_SPILL *spill = new hls_spill ();
  spill->nbatch = nbatch;
  spill->batches.resize (nbatch);
  for (int b = 0; b < nbatch; b++)
    {
      spill->batches[b].raw = NULL;
      spill->batches[b].entry_cnt = 0;
    }
  spill->stage = (char *) malloc (stage_bytes);
  if (spill->stage == NULL)
    {
      temp_page_store::release_held (stage_bytes, shard);
      delete spill;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, stage_bytes);
      return NULL;
    }
  spill->stage_cnt.assign (nbatch, 0);
  spill->dir = dir;
  /* process-unique seq block so concurrent spills never collide on a BufFile
   * name (open is O_EXCL); distinct base from the tape producers'. */
  static std::atomic<std::uint64_t> seq_gen { 0x200000000ULL };
  spill->seq_base = seq_gen.fetch_add (0x10000ULL);
  spill->seq_next = spill->seq_base;
  spill->finalized = false;
  spill->charged_bytes = stage_bytes;
  spill->charged_shard = shard;
  spill->probe_page_reads = 0;
  return spill;
}

/*
 * hls_spill_cursor_create () - allocate a per-scan PROBE cursor.
 *   Call once per HASH_LIST_SCAN before its first hls_spill_search(); in
 *   parallel probe, each worker creates its own even though they share one
 *   HLS_SPILL.
 */
HLS_SPILL_CURSOR *
hls_spill_cursor_create (THREAD_ENTRY * thread_p)
{
  (void) thread_p;
  HLS_SPILL_CURSOR *cursor = new hls_spill_cursor ();
  cursor->probe_batch = -1;
  cursor->probe_run = 0;
  cursor->probe_idx = -1;
  cursor->probe_hash = 0;
  cursor->probe_page_bf = NULL;
  cursor->probe_page_off = -1;
  cursor->page = (char *) malloc (DB_PAGESIZE);
  if (cursor->page == NULL)
    {
      delete cursor;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) DB_PAGESIZE);
      return NULL;
    }
  cursor->page_reads = 0;
  return cursor;
}

/*
 * hls_spill_cursor_destroy () - free a PROBE cursor, folding its read-page
 *   count into `spill`'s instrumentation total.  Pass spill == NULL only if
 *   the spill itself was already destroyed (nothing left to flush into).
 */
void
hls_spill_cursor_destroy (THREAD_ENTRY * thread_p, HLS_SPILL * spill, HLS_SPILL_CURSOR * cursor)
{
  (void) thread_p;
  if (cursor == NULL)
    {
      return;
    }
  if (spill != NULL)
    {
      spill->probe_page_reads.fetch_add (cursor->page_reads, std::memory_order_relaxed);
    }
  free (cursor->page);
  delete cursor;
}

static int
hls_spill_flush_stage (THREAD_ENTRY * thread_p, HLS_SPILL * spill, int batchno)
{
  hls_spill_batch & batch = spill->batches[batchno];
  if (spill->stage_cnt[batchno] == 0)
    {
      return NO_ERROR;
    }
  if (batch.raw == NULL)
    {
      batch.raw = hls_spill_new_file (thread_p, spill);
      if (batch.raw == NULL)
	{
	  return ER_FAILED;
	}
    }
  if (batch.raw->append_page (thread_p, (PAGE_PTR) hls_spill_stage_page (spill, batchno)) != NO_ERROR)
    {
      return ER_FAILED;
    }
  spill->stage_cnt[batchno] = 0;
  return NO_ERROR;
}

/*
 * hls_spill_insert () - append one (hash, pos) entry (build phase).
 */
int
hls_spill_insert (THREAD_ENTRY * thread_p, HLS_SPILL * spill, unsigned int hash_key,
		  const QFILE_TUPLE_SIMPLE_POS * pos)
{
  assert (!spill->finalized);
  int batchno = (int) (hash_key & (unsigned int) (spill->nbatch - 1));
  HLS_SPILL_ENTRY *slot =
    (HLS_SPILL_ENTRY *) hls_spill_stage_page (spill, batchno) + spill->stage_cnt[batchno];

  slot->hash = (UINT32) hash_key;
  slot->reserved = 0;
  slot->pos = *pos;
  spill->batches[batchno].entry_cnt++;
  if (++spill->stage_cnt[batchno] == HLS_SPILL_EPP)
    {
      return hls_spill_flush_stage (thread_p, spill, batchno);
    }
  return NO_ERROR;
}

/* Write `cnt` sorted entries as a run (pages + fences). */
static int
hls_spill_write_run (THREAD_ENTRY * thread_p, HLS_SPILL * spill, hls_spill_batch & batch,
		     const HLS_SPILL_ENTRY * ents, INT64 cnt)
{
  if (cnt == 0)
    {
      return NO_ERROR;
    }
  hls_spill_run run;
  run.bf = hls_spill_new_file (thread_p, spill);
  if (run.bf == NULL)
    {
      return ER_FAILED;
    }
  run.entry_cnt = cnt;
  run.first_hash = ents[0].hash;

  INT64 pages = (cnt + HLS_SPILL_EPP - 1) / HLS_SPILL_EPP;
  char *page = hls_spill_finalize_scratch_page (spill);
  for (INT64 p = 0; p < pages; p++)
    {
      INT64 first = p * HLS_SPILL_EPP;
      INT64 n = std::min ((INT64) HLS_SPILL_EPP, cnt - first);
      memcpy (page, ents + first, (size_t) n * sizeof (HLS_SPILL_ENTRY));
      run.fences.push_back (ents[first].hash);
      if (run.bf->append_page (thread_p, (PAGE_PTR) page) != NO_ERROR)
	{
	  delete run.bf;
	  return ER_FAILED;
	}
    }
  if (run.bf->flush (thread_p) != NO_ERROR)
    {
      delete run.bf;
      return ER_FAILED;
    }
  batch.runs.push_back (std::move (run));
  return NO_ERROR;
}

/* Load a raw batch file whole, sort by hash, emit one sorted run.  When the
 * accountant refuses the load and the file still bisects, range-split by hash
 * (lazy ExecHashIncreaseNumBatches); at depth cap, soft-reserve and overshoot
 * (PG growEnabled=false escape). */
static int
hls_spill_finalize_file (THREAD_ENTRY * thread_p, HLS_SPILL * spill, hls_spill_batch & batch,
			 qfile::buffile * raw, INT64 entry_cnt, UINT32 lo, UINT32 hi, int depth)
{
  if (entry_cnt == 0)
    {
      delete raw;
      return NO_ERROR;
    }

  int rc = spill->scratch.ensure ();
  if (rc != NO_ERROR)
    {
      delete raw;
      return rc;
    }

  INT64 pages = raw->page_count ();
  size_t load_bytes = (size_t) pages * DB_PAGESIZE;
  int shard = -1;
  bool hard = temp_page_store::reserve_held (load_bytes, &shard);
  if (!hard && depth < HLS_SPILL_BISECT_DEPTH_MAX && lo < hi)
    {
      /* range-bisect: split by hash midpoint into two raw files, recurse.
       * The split key is the sort key's range, so child runs stay disjoint
       * and ordered (lo-run entirely below hi-run). */
      temp_page_store::record_degrade ();
      UINT32 mid = lo + (hi - lo) / 2;	/* lo..mid -> low file, mid+1..hi -> high file */
      qfile::buffile *lo_bf = NULL, *hi_bf = NULL;
      INT64 lo_cnt = 0, hi_cnt = 0;
      char *rd = hls_spill_finalize_scratch_page (spill);
      HLS_SPILL_ENTRY *lo_stage = (HLS_SPILL_ENTRY *) hls_spill_stage_page (spill, 0);
      HLS_SPILL_ENTRY *hi_stage = (HLS_SPILL_ENTRY *) hls_spill_stage_page (spill, 1 % spill->nbatch);
      int lo_n = 0, hi_n = 0;

      /* nbatch == 1 would alias the two staging slots; bisection needs 2 pages */
      if (spill->nbatch < 2)
	{
	  goto overshoot;
	}

      for (INT64 p = 0; p < pages; p++)
	{
	  if (raw->read_page (thread_p, (int) p, (PAGE_PTR) rd, &spill->scratch) != NO_ERROR)
	    {
	      delete lo_bf;
	      delete hi_bf;
	      delete raw;
	      return ER_FAILED;
	    }
	  INT64 first = p * HLS_SPILL_EPP;
	  INT64 n = std::min ((INT64) HLS_SPILL_EPP, entry_cnt - first);
	  const HLS_SPILL_ENTRY *ents = (const HLS_SPILL_ENTRY *) rd;
	  for (INT64 i = 0; i < n; i++)
	    {
	      const HLS_SPILL_ENTRY & e = ents[i];
	      HLS_SPILL_ENTRY *dst_stage = (e.hash <= mid) ? lo_stage : hi_stage;
	      int *dst_n = (e.hash <= mid) ? &lo_n : &hi_n;
	      qfile::buffile **dst_bf = (e.hash <= mid) ? &lo_bf : &hi_bf;
	      INT64 *dst_cnt = (e.hash <= mid) ? &lo_cnt : &hi_cnt;

	      dst_stage[*dst_n] = e;
	      (*dst_cnt)++;
	      if (++(*dst_n) == HLS_SPILL_EPP)
		{
		  if (*dst_bf == NULL && (*dst_bf = hls_spill_new_file (thread_p, spill)) == NULL)
		    {
		      delete lo_bf;
		      delete hi_bf;
		      delete raw;
		      return ER_FAILED;
		    }
		  if ((*dst_bf)->append_page (thread_p, (PAGE_PTR) dst_stage) != NO_ERROR)
		    {
		      delete lo_bf;
		      delete hi_bf;
		      delete raw;
		      return ER_FAILED;
		    }
		  *dst_n = 0;
		}
	    }
	}
      /* flush partial staging pages */
      if (lo_n > 0)
	{
	  if (lo_bf == NULL && (lo_bf = hls_spill_new_file (thread_p, spill)) == NULL)
	    {
	      delete hi_bf;
	      delete raw;
	      return ER_FAILED;
	    }
	  if (lo_bf->append_page (thread_p, (PAGE_PTR) lo_stage) != NO_ERROR)
	    {
	      delete lo_bf;
	      delete hi_bf;
	      delete raw;
	      return ER_FAILED;
	    }
	}
      if (hi_n > 0)
	{
	  if (hi_bf == NULL && (hi_bf = hls_spill_new_file (thread_p, spill)) == NULL)
	    {
	      delete lo_bf;
	      delete raw;
	      return ER_FAILED;
	    }
	  if (hi_bf->append_page (thread_p, (PAGE_PTR) hi_stage) != NO_ERROR)
	    {
	      delete lo_bf;
	      delete hi_bf;
	      delete raw;
	      return ER_FAILED;
	    }
	}
      if ((lo_bf != NULL && lo_bf->flush (thread_p) != NO_ERROR)
	  || (hi_bf != NULL && hi_bf->flush (thread_p) != NO_ERROR))
	{
	  delete lo_bf;
	  delete hi_bf;
	  delete raw;
	  return ER_FAILED;
	}
      delete raw;
      if (lo_bf != NULL)
	{
	  rc = hls_spill_finalize_file (thread_p, spill, batch, lo_bf, lo_cnt, lo, mid, depth + 1);
	  if (rc != NO_ERROR)
	    {
	      delete hi_bf;
	      return rc;
	    }
	}
      if (hi_bf != NULL)
	{
	  return hls_spill_finalize_file (thread_p, spill, batch, hi_bf, hi_cnt, mid + 1, hi, depth + 1);
	}
      return NO_ERROR;
    }

overshoot:
  if (!hard)
    {
      /* bisection unavailable (depth cap / degenerate range): overshoot,
       * soft-accounted -- mirrors PG's growEnabled=false. */
      temp_page_store::record_degrade ();
      temp_page_store::reserve_held_soft (load_bytes, &shard);
    }

  {
    /* pages carry HLS_SPILL_EPP entries + tail slack, so compact per page
     * into a contiguous entry array for the sort */
    HLS_SPILL_ENTRY *ents = (HLS_SPILL_ENTRY *) malloc ((size_t) entry_cnt * sizeof (HLS_SPILL_ENTRY));
    char *rd = hls_spill_finalize_scratch_page (spill);
    INT64 got = 0;

    if (ents == NULL)
      {
	temp_page_store::release_held (load_bytes, shard);
	delete raw;
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
		(size_t) entry_cnt * sizeof (HLS_SPILL_ENTRY));
	return ER_FAILED;
      }
    for (INT64 p = 0; p < pages; p++)
      {
	if (raw->read_page (thread_p, (int) p, (PAGE_PTR) rd, &spill->scratch) != NO_ERROR)
	  {
	    free (ents);
	    temp_page_store::release_held (load_bytes, shard);
	    delete raw;
	    return ER_FAILED;
	  }
	INT64 n = std::min (HLS_SPILL_EPP, entry_cnt - got);
	memcpy (ents + got, rd, (size_t) n * sizeof (HLS_SPILL_ENTRY));
	got += n;
      }
    assert (got == entry_cnt);
    delete raw;			/* consumed */

    std::sort (ents, ents + entry_cnt,
	       [] (const HLS_SPILL_ENTRY & a, const HLS_SPILL_ENTRY & b) { return a.hash < b.hash; });

    rc = hls_spill_write_run (thread_p, spill, batch, ents, entry_cnt);
    free (ents);
    temp_page_store::release_held (load_bytes, shard);
    return rc;
  }
}

/*
 * hls_spill_finalize () - end of build: flush staging, convert every raw
 *   batch file into sorted run(s) + fence indexes.
 */
int
hls_spill_finalize (THREAD_ENTRY * thread_p, HLS_SPILL * spill)
{
  assert (!spill->finalized);
  /* Flush ALL staging pages first: finalize_file's bisection reuses staging
   * slots 0/1 as split buffers, which must not still hold live staged entries
   * of later batches. */
  for (int b = 0; b < spill->nbatch; b++)
    {
      if (hls_spill_flush_stage (thread_p, spill, b) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }
  for (int b = 0; b < spill->nbatch; b++)
    {
      hls_spill_batch & batch = spill->batches[b];
      qfile::buffile *raw = batch.raw;
      batch.raw = NULL;
      if (raw == NULL)
	{
	  assert (batch.entry_cnt == 0);
	  continue;
	}
      if (raw->flush (thread_p) != NO_ERROR)
	{
	  delete raw;
	  return ER_FAILED;
	}
      if (hls_spill_finalize_file (thread_p, spill, batch, raw, batch.entry_cnt, 0, 0xFFFFFFFFU, 0) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }
  spill->finalized = true;
  return NO_ERROR;
}

/* Position the probe cursor on the first entry with hash == probe within the
 * run, starting the page scan at fence lower bound.  Returns EH_KEY_FOUND /
 * EH_KEY_NOTFOUND / EH_ERROR_OCCURRED and fills pos_out on found. */
static EH_SEARCH
hls_spill_probe_run (THREAD_ENTRY * thread_p, hls_spill_run & run, HLS_SPILL_CURSOR * cursor, UINT32 hash,
		     QFILE_TUPLE_SIMPLE_POS * pos_out)
{
  /* first page that can contain `hash`: the last fence <= hash */
  std::vector<UINT32>::const_iterator it = std::upper_bound (run.fences.begin (), run.fences.end (), hash);
  if (it == run.fences.begin ())
    {
      return EH_KEY_NOTFOUND;	/* hash below the run's first entry */
    }
  INT64 page = (INT64) (it - run.fences.begin ()) - 1;
  /* duplicates spanning a page boundary: a page starting exactly with `hash`
   * may be preceded by pages ending with it */
  while (page > 0 && run.fences[(size_t) page] == hash)
    {
      page--;
    }

  char *pg = cursor->page;
  for (; page < (INT64) run.fences.size (); page++)
    {
      if (run.fences[(size_t) page] > hash)
	{
	  return EH_KEY_NOTFOUND;
	}
      if (cursor->probe_page_bf != run.bf || cursor->probe_page_off != (int) page)
	{
	  if (run.bf->read_page (thread_p, (int) page, (PAGE_PTR) pg, &cursor->scratch) != NO_ERROR)
	    {
	      return EH_ERROR_OCCURRED;
	    }
	  cursor->probe_page_bf = run.bf;
	  cursor->probe_page_off = (int) page;
	  cursor->page_reads++;
	}
      const HLS_SPILL_ENTRY *ents = (const HLS_SPILL_ENTRY *) pg;
      INT64 first = page * HLS_SPILL_EPP;
      INT64 n = std::min ((INT64) HLS_SPILL_EPP, run.entry_cnt - first);
      const HLS_SPILL_ENTRY *lb = std::lower_bound (ents, ents + n, hash,
						    [] (const HLS_SPILL_ENTRY & e, UINT32 h) { return e.hash < h; });
      if (lb < ents + n)
	{
	  if (lb->hash != hash)
	    {
	      return EH_KEY_NOTFOUND;
	    }
	  cursor->probe_idx = first + (lb - ents);
	  cursor->probe_hash = hash;
	  *pos_out = lb->pos;
	  return EH_KEY_FOUND;
	}
      /* all entries on this page < hash; try next page */
    }
  return EH_KEY_NOTFOUND;
}

/*
 * hls_spill_search () - probe: first entry with the given hash key.
 */
EH_SEARCH
hls_spill_search (THREAD_ENTRY * thread_p, HLS_SPILL * spill, HLS_SPILL_CURSOR * cursor, unsigned int hash_key,
		  QFILE_TUPLE_SIMPLE_POS * pos_out)
{
  assert (spill->finalized);
  UINT32 hash = (UINT32) hash_key;
  int batchno = (int) (hash & (unsigned int) (spill->nbatch - 1));
  hls_spill_batch & batch = spill->batches[batchno];

  cursor->probe_batch = -1;
  cursor->probe_idx = -1;

  for (size_t r = 0; r < batch.runs.size (); r++)
    {
      hls_spill_run & run = batch.runs[r];
      if (run.entry_cnt == 0 || run.first_hash > hash)
	{
	  continue;
	}
      EH_SEARCH es = hls_spill_probe_run (thread_p, run, cursor, hash, pos_out);
      if (es == EH_KEY_FOUND)
	{
	  cursor->probe_batch = batchno;
	  cursor->probe_run = r;
	  return EH_KEY_FOUND;
	}
      if (es == EH_ERROR_OCCURRED)
	{
	  return es;
	}
      /* runs have disjoint hash ranges; a miss in the covering run is final,
       * but ranges are only bounded below (first_hash), so keep scanning
       * remaining runs whose range could still cover `hash`. */
    }
  return EH_KEY_NOTFOUND;
}

/*
 * hls_spill_search_next () - next duplicate of the last found hash key
 *   (duplicates are adjacent within a sorted run).
 */
EH_SEARCH
hls_spill_search_next (THREAD_ENTRY * thread_p, HLS_SPILL * spill, HLS_SPILL_CURSOR * cursor,
		       QFILE_TUPLE_SIMPLE_POS * pos_out)
{
  if (cursor->probe_batch < 0 || cursor->probe_idx < 0)
    {
      return EH_KEY_NOTFOUND;
    }
  hls_spill_run & run = spill->batches[cursor->probe_batch].runs[cursor->probe_run];
  INT64 idx = cursor->probe_idx + 1;
  if (idx >= run.entry_cnt)
    {
      return EH_KEY_NOTFOUND;
    }
  INT64 page = idx / HLS_SPILL_EPP;
  char *pg = cursor->page;
  if (cursor->probe_page_bf != run.bf || cursor->probe_page_off != (int) page)
    {
      if (run.bf->read_page (thread_p, (int) page, (PAGE_PTR) pg, &cursor->scratch) != NO_ERROR)
	{
	  return EH_ERROR_OCCURRED;
	}
      cursor->probe_page_bf = run.bf;
      cursor->probe_page_off = (int) page;
      cursor->page_reads++;
    }
  const HLS_SPILL_ENTRY & e = ((const HLS_SPILL_ENTRY *) pg)[idx - page * HLS_SPILL_EPP];
  if (e.hash != cursor->probe_hash)
    {
      return EH_KEY_NOTFOUND;
    }
  cursor->probe_idx = idx;
  *pos_out = e.pos;
  return EH_KEY_FOUND;
}

long
hls_spill_probe_page_reads (const HLS_SPILL * spill)
{
  return spill->probe_page_reads.load (std::memory_order_relaxed);
}

/*
 * hls_spill_destroy () - drop all spill files and release accountant charges.
 */
void
hls_spill_destroy (THREAD_ENTRY * thread_p, HLS_SPILL * spill)
{
  (void) thread_p;
  if (spill == NULL)
    {
      return;
    }
  for (hls_spill_batch & batch : spill->batches)
    {
      delete batch.raw;		/* build aborted before finalize */
      for (hls_spill_run & run : batch.runs)
	{
	  delete run.bf;
	}
    }
  free (spill->stage);
  if (spill->charged_bytes > 0)
    {
      temp_page_store::release_held (spill->charged_bytes, spill->charged_shard);
    }
  delete spill;
}

/*
 * qdata_hscan_wm_reserve () / qdata_hscan_wm_release () - work_mem accountant
 *   charge covering the IN_MEM/HYBRID build estimate.  The charge
 *   is the same quantity check_hash_list_scan compares against work_mem, so
 *   the comparison and the accounting can never disagree.
 */
bool
qdata_hscan_wm_reserve (HASH_LIST_SCAN * hlsid, size_t bytes)
{
  int shard = -1;
  assert (hlsid->wm_bytes == 0);
  if (!temp_page_store::reserve_held (bytes, &shard))
    {
      temp_page_store::record_degrade ();	/* caller falls to the next tier */
      return false;
    }
  hlsid->wm_bytes = bytes;
  hlsid->wm_shard = shard;
  return true;
}

void
qdata_hscan_wm_release (HASH_LIST_SCAN * hlsid)
{
  if (hlsid->wm_bytes > 0)
    {
      temp_page_store::release_held (hlsid->wm_bytes, hlsid->wm_shard);
      hlsid->wm_bytes = 0;
      hlsid->wm_shard = -1;
    }
}
