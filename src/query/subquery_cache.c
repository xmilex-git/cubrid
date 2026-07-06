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
 * subquery_cache.c - Correlated Scalar Subquery Result Cache.
 */


#ident "$Id$"

#include <stdio.h>
#include <string.h>

#include "xasl.h"
#include "dbtype.h"		/* db_make_int / db_get_int (#146 T3 S3b selftest) */
#include "query_executor.h"
#include "xasl_predicate.hpp"
#include "regu_var.hpp"
#include "object_representation.h"
#include "system_parameter.h"
#include "memory_alloc.h"
#include "list_file.h"

#include "subquery_cache.h"
#include "query_workmem.hpp"	/* work_mem accountant: op_limit_bytes / reserve_held_soft_at_shard / release_held / record_op_limit_spill_hash */

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

/**************************************************************************************/

/* Static functions for sq_cache hash table. */

static SQ_VAL *sq_make_val (THREAD_ENTRY * thread_p, REGU_VARIABLE * val);
static void sq_free_val (THREAD_ENTRY * thread_p, SQ_VAL * val);
static void sq_unpack_val (SQ_VAL * val, REGU_VARIABLE * retp);

static unsigned int sq_hash_func (const void *key, unsigned int ht_size);
static int sq_cmp_func (const void *key1, const void *key2);
static int sq_rem_func (const void *key, void *data, void *args);

/* #146 T3 S3 (D2/§5) */
static UINT64 sq_entry_size (SQ_KEY * key, SQ_VAL * val);
static void sq_evict_lru_to_fit (THREAD_ENTRY * thread_p, XASL_NODE * xasl, UINT64 incoming_entry_size);
static void sq_sync_wm_charge (XASL_NODE * xasl);

/**************************************************************************************/

/*
 * sq_make_key () - Creates a key for the scalar subquery cache.
 *   return: Pointer to a newly allocated SQ_KEY structure, or NULL if no constant predicate is present.
 *   xasl(in): The XASL node of the scalar subquery.
 *
 * This function generates a key for caching the results of a scalar subquery. It checks the provided XASL node
 * for predicates (where_key, where_pred, where_range) and creates a DB_VALUE array to represent the key.
 */
SQ_KEY *
sq_make_key (THREAD_ENTRY * thread_p, XASL_NODE * xasl)
{
  SQ_KEY *keyp;
  int i, cnt = 0;

  keyp = (SQ_KEY *) db_private_alloc (thread_p, sizeof (SQ_KEY));
  if (keyp == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, sizeof (SQ_KEY));
      return NULL;
    }
  keyp->n_elements = SQ_CACHE_KEY_STRUCT (xasl)->n_elements;
  keyp->dbv_array = (DB_VALUE **) db_private_alloc (thread_p, keyp->n_elements * sizeof (DB_VALUE *));
  if (keyp->dbv_array == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, keyp->n_elements * sizeof (DB_VALUE *));
      return NULL;
    }
  for (i = 0; i < keyp->n_elements; i++)
    {
      keyp->dbv_array[i] = db_value_copy (SQ_CACHE_KEY_STRUCT (xasl)->dbv_array[i]);
    }

  return keyp;
}

/*
 * sq_make_val () - Creates a value structure for the scalar subquery cache.
 *   return: Pointer to a newly created SQ_VAL structure.
 *   val(in): The REGU_VARIABLE for which to create the SQ_VAL structure.
 *
 * Allocates and initializes a new SQ_VAL structure based on the given REGU_VARIABLE. The function handles
 * different types of REGU_VARIABLE (e.g., TYPE_CONSTANT, TYPE_LIST_ID) appropriately by copying or cloning
 * the necessary data. It returns a pointer to the newly allocated and initialized SQ_VAL structure.
 */
SQ_VAL *
sq_make_val (THREAD_ENTRY * thread_p, REGU_VARIABLE * val)
{
  SQ_VAL *ret;
  ret = (SQ_VAL *) db_private_alloc (thread_p, sizeof (SQ_VAL));

  ret->type = val->type;

  switch (ret->type)
    {
    case TYPE_CONSTANT:
      ret->val.dbvalptr = db_value_copy (val->value.dbvalptr);
      break;

    case TYPE_LIST_ID:
      if (val->value.srlist_id->list_id->tuple_cnt > 0)
	{
	  ret->val.exists = true;
	}
      else
	{
	  ret->val.exists = false;
	}
      break;

    default:
      /* Never happens. */
      break;
    }

  return ret;
}

/*
 * sq_free_key () - Frees the memory allocated for a SQ_KEY structure.
 *   key(in): The SQ_KEY structure to be freed.
 *
 * This function releases the memory allocated for the DB_VAUE array within the SQ_KEY structure and then
 * frees the SQ_KEY structure itself.
 */
void
sq_free_key (THREAD_ENTRY * thread_p, SQ_KEY * key)
{
  int i;
  for (i = 0; i < key->n_elements; i++)
    {
      pr_free_ext_value (key->dbv_array[i]);
    }
  db_private_free_and_init (thread_p, key->dbv_array);
  db_private_free_and_init (thread_p, key);
}

/*
 * sq_free_val () - Frees the memory allocated for a SQ_VAL structure.
 *   v(in): The SQ_VAL structure to be freed.
 *
 * Depending on the type of the value in the SQ_VAL structure (e.g., TYPE_CONSTANT, TYPE_LIST_ID),
 * this function frees the associated resources and then the SQ_VAL structure itself.
 */
void
sq_free_val (THREAD_ENTRY * thread_p, SQ_VAL * v)
{
  switch (v->type)
    {
    case TYPE_CONSTANT:
      pr_free_ext_value (v->val.dbvalptr);
      break;

    case TYPE_LIST_ID:
      /* nothing to do */
      break;

    default:
      /* Never happens */
      break;
    }
  db_private_free_and_init (thread_p, v);
}

/*
 * sq_unpack_val () - Unpacks the value from a SQ_VAL structure into a REGU_VARIABLE.
 *   v(in): The SQ_VAL structure containing the value to be unpacked.
 *   retp(out): The REGU_VARIABLE to store the unpacked value.
 *
 * Based on the type of the value in the SQ_VAL structure, this function unpacks the value and stores
 * it in the provided REGU_VARIABLE. The function handles different types appropriately, such as copying
 * DB_VALUE or cloning a LIST_ID.
 */
void
sq_unpack_val (SQ_VAL * v, REGU_VARIABLE * retp)
{
  switch (v->type)
    {
    case TYPE_CONSTANT:
      if (retp->value.dbvalptr)
	{
	  pr_clear_value (retp->value.dbvalptr);
	  db_value_clone (v->val.dbvalptr, retp->value.dbvalptr);
	}
      else
	{
	  retp->value.dbvalptr = db_value_copy (v->val.dbvalptr);
	}
      break;

    case TYPE_LIST_ID:
      retp->value.srlist_id->list_id->tuple_cnt = v->val.exists ? 1 : 0;
      break;

    default:
      /* Never happens */
      break;
    }
}

/*
 * sq_hash_func () - Hash function for the scalar subquery cache keys.
 *   return: The hash value.
 *   key(in): The key to be hashed.
 *   ht_size(in): The size of the hash table.
 *   
 * Generates a hash value for the given key by hashing the elements of the DB_VALUE array within the SQ_KEY structure.
 * The hash value is then modulated by the size of the hash table to ensure it falls within valid bounds.
 */
unsigned int
sq_hash_func (const void *key, unsigned int ht_size)
{
  SQ_KEY *k = (SQ_KEY *) key;
  int i;
  unsigned int h = 0;

  for (i = 0; i < k->n_elements; i++)
    {
      h = ROTL32 (h, 13);
      h ^= mht_get_hash_number (ht_size, k->dbv_array[i]);
    }
  return h % ht_size;
}

/*
 * sq_cmp_func () - Comparison function for scalar subquery cache keys.
 *   return: 1 if the keys are equal, 0 otherwise.
 *   key1(in): The first key to compare.
 *   key2(in): The second key to compare.
 *
 * Compares two SQ_KEY structures to determine if they are equal. The comparison is based on the elements
 * of the DB_VALUE array within each key. The function returns 1 if the keys are considered equal, otherwise 0.
 */
int
sq_cmp_func (const void *key1, const void *key2)
{
  SQ_KEY *k1, *k2;
  int i, sz1, sz2;
  k1 = (SQ_KEY *) key1;
  k2 = (SQ_KEY *) key2;
  sz1 = k1->n_elements;
  sz2 = k2->n_elements;
  assert (sz1 == sz2);

  for (i = 0; i < sz1; i++)
    {
      if (!mht_compare_dbvalues_are_equal (k1->dbv_array[i], k2->dbv_array[i]))
	{
	  return 0;
	}
    }
  return 1;

}

/*
 * sq_rem_func () - Function to remove an entry from the scalar subquery cache.
 *   return: NO_ERROR on success.
 *   key(in): The key of the entry to remove.
 *   data(in): The data associated with the key.
 *   args(in): Additional arguments (unused).
 *
 * This function is called when an entry is removed from the scalar subquery cache. It frees the resources
 * allocated for the key and the data (SQ_VAL structure) using sq_free_key and sq_free_val functions.
 */
int
sq_rem_func (const void *key, void *data, void *args)
{
  THREAD_ENTRY *thread_p = (THREAD_ENTRY *) args;
  sq_free_key (thread_p, (SQ_KEY *) key);
  sq_free_val (thread_p, (SQ_VAL *) data);
  return NO_ERROR;
}

/*
 * sq_entry_size () - #146 T3 S3: byte footprint of one (key, val) pair, the
 *   same computation sq_put() used to do inline for the entry about to be
 *   inserted; also needed at eviction time, to know how much to subtract for
 *   an entry being removed, computed before it is destroyed.
 */
static UINT64
sq_entry_size (SQ_KEY * key, SQ_VAL * val)
{
  UINT64 size = DB_SIZEOF (HENTRY);
  int i;

  for (i = 0; i < key->n_elements; i++)
    {
      size += (UINT64) or_db_value_size (key->dbv_array[i]);
    }
  size += sizeof (SQ_KEY);
  size += DB_SIZEOF (DB_VALUE *) * key->n_elements;

  switch (val->type)
    {
    case TYPE_CONSTANT:
      size += (UINT64) or_db_value_size (val->val.dbvalptr) + sizeof (SQ_VAL);
      break;

    case TYPE_LIST_ID:
      size += sizeof (SQ_VAL);
      break;

    default:
      break;
    }

  return size;
}

/*
 * sq_evict_lru_to_fit () - #146 T3 S3 (D2/§5): evict least-recently-used
 *   entries (via the hash table's built-in LRU list, enabled in
 *   sq_cache_initialize) until incoming_entry_size fits under the budget or
 *   nothing is left to evict.  Replaces the old permanent "enabled = false"
 *   hard stop -- an evicted entry simply becomes a future cache miss (an
 *   already correct, tested path: sq_get() returns false, the caller
 *   re-evaluates the subquery and sq_put()s again), so eviction order has no
 *   correctness impact, only a performance one.
 */
static void
sq_evict_lru_to_fit (THREAD_ENTRY * thread_p, XASL_NODE * xasl, UINT64 incoming_entry_size)
{
  MHT_TABLE *ht = SQ_CACHE_HT (xasl);

  while (SQ_CACHE_SIZE (xasl) + incoming_entry_size > SQ_CACHE_SIZE_MAX (xasl) && ht->lru_head != NULL)
    {
      SQ_KEY *victim_key = (SQ_KEY *) ht->lru_head->key;
      SQ_VAL *victim_val = (SQ_VAL *) ht->lru_head->data;
      UINT64 victim_size = sq_entry_size (victim_key, victim_val);

      mht_rem (ht, victim_key, sq_rem_func, (void *) thread_p);
      SQ_CACHE_SIZE (xasl) -= MIN (victim_size, SQ_CACHE_SIZE (xasl));

      temp_page_store::record_op_limit_spill_hash ();
    }
}

/*
 * sq_sync_wm_charge () - #146 T3 S3 (D2/§5): reconcile the work_mem accountant
 *   charge with SQ_CACHE_SIZE(xasl), batched at 256KB (same convention as the
 *   other T3 growth charges) so this isn't a per-put() atomic. Soft
 *   (reserve_held_soft_at_shard): the memory is already real by the time an
 *   entry is in the map, so rejecting the charge would only make the
 *   accountant under-report it.
 */
static void
sq_sync_wm_charge (XASL_NODE * xasl)
{
  const UINT64 quantum_bytes = 256 * 1024;
  const UINT64 current = SQ_CACHE_SIZE (xasl);
  size_t *charged_bytes = &xasl->sq_cache->wm_charged_bytes;
  int *charged_shard = &xasl->sq_cache->wm_charged_shard;

  if (current > *charged_bytes && current - *charged_bytes >= quantum_bytes)
    {
      const size_t delta = (size_t) (current - *charged_bytes);

      temp_page_store::reserve_held_soft_at_shard (delta, charged_shard);
      *charged_bytes += delta;
    }
  else if (current < *charged_bytes && *charged_bytes - current >= quantum_bytes)
    {
      const size_t delta = *charged_bytes - (size_t) current;

      temp_page_store::release_held (delta, *charged_shard);
      *charged_bytes -= delta;
    }
}

/*
 * sq_cache_initialize () - Initializes the cache for a given XASL node.
 *   return: NO_ERROR if successful, ER_FAILED otherwise.
 *   xasl(in/out): The XASL node for which the cache is being initialized.
 *
 * This function creates a hash table for caching the results of the XASL node. It sets up initial values for cache hit and miss
 * counters and marks the cache as initialized. The function returns NO_ERROR upon successful initialization, or ER_FAILED if the
 * hash table could not be created.
 */
int
sq_cache_initialize (XASL_NODE * xasl)
{
  /* #146 T3 S3 (D6/D7/D8): max_subquery_cache_size is deprecated as the byte
   * budget -- replaced by the hash-shaped per-op limit (work_mem * session
   * hash_mem_multiplier), same treatment as memoize. */
  UINT64 max_subquery_cache_size = temp_page_store::op_limit_bytes (temp_page_store::op_workmem_kind::hash);
  int sq_hm_entries = (int) max_subquery_cache_size / SQ_CACHE_EXPECTED_ENTRY_SIZE;	// default 4096 (4K)
  int actual_entries;

  SQ_CACHE_HT (xasl) = mht_create ("sq_cache", sq_hm_entries, sq_hash_func, sq_cmp_func);
  if (!SQ_CACHE_HT (xasl))
    {
      return ER_FAILED;
    }
  /* #146 T3 S3 (§5): build the LRU list so sq_evict_lru_to_fit() can use
   * lru_head/mht_rem instead of the old permanent hard stop. */
  SQ_CACHE_HT (xasl)->build_lru_list = true;
  SQ_CACHE_ENABLED (xasl) = true;
  SQ_CACHE_SIZE_MAX (xasl) = max_subquery_cache_size;
  SQ_CACHE_SIZE (xasl) += DB_SIZEOF (SQ_CACHE);
  SQ_CACHE_SIZE (xasl) += DB_SIZEOF (MHT_TABLE);
  actual_entries = mht_calculate_htsize ((unsigned int) sq_hm_entries);
  SQ_CACHE_SIZE (xasl) += (DB_SIZEOF (HENTRY) * MAX (2, actual_entries / 2 + 1));
  SQ_CACHE_SIZE (xasl) += (DB_SIZEOF (HENTRY_PTR) * actual_entries);
  SQ_CACHE_SIZE (xasl) += sizeof (SQ_KEY);
  SQ_CACHE_SIZE (xasl) += DB_SIZEOF (DB_VALUE *) * SQ_CACHE_KEY_STRUCT (xasl)->n_elements;

  return NO_ERROR;
}

/*
 * sq_put () - Puts a value into the cache for a given XASL node.
 *   return: NO_ERROR if the value is successfully cached, ER_FAILED otherwise.
 *   xasl(in): The XASL node for which the value is being cached.
 *   regu_var(in): The regu variable containing the value to be cached.
 *
 * This function attempts to cache the result of a regu variable associated with a given XASL node. It generates a key based on
 * the XASL node's structure and creates a cache entry if such a key does not already exist in the cache. The function returns
 * NO_ERROR if the value is successfully cached, or ER_FAILED if the key could not be generated or the value could not be added
 * to the cache.
 */
int
sq_put (THREAD_ENTRY * thread_p, SQ_KEY * key, XASL_NODE * xasl, REGU_VARIABLE * regu_var)
{
  SQ_VAL *val;
  const void *ret;
  UINT64 new_entry_size;

  val = sq_make_val (thread_p, regu_var);

  if (!SQ_CACHE_HT (xasl))
    {
      if (sq_cache_initialize (xasl) == ER_FAILED)
	{
	  XASL_CLEAR_FLAG (xasl, XASL_USES_SQ_CACHE);
	  return ER_FAILED;
	}
    }

  new_entry_size = sq_entry_size (key, val);

  /* #146 T3 S3 (D2/§5): evict LRU entries to make room instead of the old
   * permanent "enabled = false" hard stop. */
  sq_evict_lru_to_fit (thread_p, xasl, new_entry_size);

  if (SQ_CACHE_SIZE_MAX (xasl) < SQ_CACHE_SIZE (xasl) + new_entry_size)
    {
      /* Even fully evicted, this one entry alone doesn't fit -- bypass
       * (skip caching just this entry); the cache stays enabled for
       * entries that do fit. */
      sq_free_val (thread_p, val);
      return ER_FAILED;
    }

  ret = mht_put_if_not_exists (SQ_CACHE_HT (xasl), key, val);

  if (!ret || ret != val)
    {
      sq_free_val (thread_p, val);
      return ER_FAILED;
    }
  SQ_CACHE_SIZE (xasl) += new_entry_size;
  sq_sync_wm_charge (xasl);
  return NO_ERROR;
}

/*
 * sq_get () - Retrieves a value from the cache for a given XASL node.
 *   return: True if a cached value is found and retrieved, False otherwise.
 *   xasl(in): The XASL node for which a cached value is being retrieved.
 *   regu_var(in/out): The regu variable where the retrieved value will be stored.
 *
 * This function attempts to retrieve a value from the cache for a given XASL node. It generates a key based on the XASL node's
 * structure and looks up the cache for a matching value. If a cached value is found, it is unpacked into the specified regu
 * variable, and the function returns True. Otherwise, the function updates cache miss counters and returns False.
 */
bool
sq_get (THREAD_ENTRY * thread_p, SQ_KEY * key, XASL_NODE * xasl, REGU_VARIABLE * regu_var)
{
  SQ_VAL *ret;

  if (SQ_CACHE_HT (xasl))
    {
      /* This conditional check acts as a mechanism to prevent the cache from being 
         overwhelmed by unsuccessful lookups. If the cache miss count exceeds a predefined 
         maximum, it evaluates the hit-to-miss ratio to decide whether continuing caching 
         is beneficial. This approach optimizes cache usage and performance by dynamically 
         adapting to the effectiveness of the cache. */
      if ((double) SQ_CACHE_SIZE (xasl) > (double) SQ_CACHE_SIZE_MAX (xasl) * 0.6)
	{
	  /* #146 T3 S3b: guard against div-by-zero -- with no miss recorded
	   * yet, there is no ratio to evaluate; do not disable. */
	  if (SQ_CACHE_MISS (xasl) > 0 && SQ_CACHE_HIT (xasl) / SQ_CACHE_MISS (xasl) < SQ_CACHE_MIN_HIT_RATIO)
	    {
	      SQ_CACHE_ENABLED (xasl) = false;
	      return false;
	    }
	}
    }

  if (!SQ_CACHE_HT (xasl))
    {
      if (sq_cache_initialize (xasl) == ER_FAILED)
	{
	  XASL_CLEAR_FLAG (xasl, XASL_USES_SQ_CACHE);
	  return false;
	}
      SQ_CACHE_MISS (xasl)++;
      return false;
    }

  ret = (SQ_VAL *) mht_get (SQ_CACHE_HT (xasl), key);
  if (ret == NULL)
    {
      SQ_CACHE_MISS (xasl)++;
      return false;
    }

  sq_unpack_val (ret, regu_var);

  SQ_CACHE_HIT (xasl)++;
  return true;
}

/*
 * sq_cache_destroy () - Destroys the cache for a given XASL node.
 *   xasl(in): The XASL node for which the cache is being destroyed.
 *
 * This function destroys the cache associated with a given XASL node. It clears all cache entries and then destroys the hash
 * table itself. It also resets cache-related flags and counters for the XASL node. This function is called when a XASL node is
 * no longer needed or before it is deallocated.
 */
void
sq_cache_destroy (THREAD_ENTRY * thread_p, SQ_CACHE * sq_cache)
{
  if (sq_cache)
    {
      if (sq_cache->ht)
	{
	  er_log_debug (ARG_FILE_LINE,
			"destroy sq_cache  %p\ncache info : \n\thit : %10d\n\tmiss: %10d\n\tsize: %10lu Bytes\n",
			sq_cache, sq_cache->stats.hit, sq_cache->stats.miss, sq_cache->size);
	  mht_clear (sq_cache->ht, sq_rem_func, (void *) thread_p);
	  mht_destroy (sq_cache->ht);
	  sq_cache->ht = NULL;
	}
      sq_cache->size_max = 0;
      sq_cache->size = 0;
      sq_cache->enabled = false;
      sq_cache->stats.hit = 0;
      sq_cache->stats.miss = 0;
      /* #146 T3 S3: release whatever sq_sync_wm_charge() accumulated. */
      if (sq_cache->wm_charged_bytes > 0)
	{
	  temp_page_store::release_held (sq_cache->wm_charged_bytes, sq_cache->wm_charged_shard);
	}
      sq_cache->wm_charged_bytes = 0;
      sq_cache->wm_charged_shard = -1;
    }
}

/*
 * sq_cache_selftest () - #146 T3 S3b: in-server self-test of sq_cache's LRU
 *   eviction (D2/§5), exercised directly against sq_put()/sq_get() with a
 *   minimal one-INT-key/one-INT-value XASL_NODE/SQ_CACHE fixture (bypassing
 *   the parser-level pt_prepare_corr_subquery_hash_result_cache eligibility
 *   check, which needs a real query plan). Gated by env
 *   CUBRID_WM_SQCACHE_SELFTEST. Checks:
 *     (a) a tiny budget actually evicts -- not all inserted keys stay resident
 *     (b) an evicted key's next sq_get() is a miss, and after recomputing +
 *         sq_put()ing again, sq_get() hits with the newly recomputed value
 *     (c) a single entry whose own footprint exceeds the whole budget is
 *         bypassed (never retained), but the cache stays enabled
 *     (d) the work_mem accountant nets back to the pre-test reading once
 *         the cache is destroyed (no charge leak)
 *   Returns 0 on PASS.
 */
int
sq_cache_selftest (THREAD_ENTRY * thread_p)
{
  UINT64 saved_work_mem = (UINT64) prm_get_bigint_value (PRM_ID_WORK_MEM);
  float saved_multiplier = prm_get_float_value (PRM_ID_HASH_MEM_MULTIPLIER);
  const std::size_t reserved_before = temp_page_store::reserved_bytes ();
  DB_VALUE key_dbval;
  SQ_KEY sq_key_struct;
  DB_VALUE *sq_key_dbv_array[1];
  SQ_CACHE sq_cache_obj;
  XASL_NODE xasl;
  int rc = NO_ERROR;

  db_make_int (&key_dbval, 0);
  sq_key_dbv_array[0] = &key_dbval;
  sq_key_struct.dbv_array = sq_key_dbv_array;
  sq_key_struct.n_elements = 1;

  memset (&sq_cache_obj, 0, sizeof (sq_cache_obj));
  sq_cache_obj.sq_key_struct = &sq_key_struct;
  sq_cache_obj.wm_charged_shard = -1;

  memset (&xasl, 0, sizeof (xasl));
  xasl.sq_cache = &sq_cache_obj;

  /* (a) + (b): tiny budget (fits only a handful of entries), 20 distinct keys */
  prm_set_bigint_value (PRM_ID_WORK_MEM, 2048);
  prm_set_float_value (PRM_ID_HASH_MEM_MULTIPLIER, 1.0f);
  {
    const int n_keys = 20;
    int i;

    for (i = 0; i < n_keys && rc == NO_ERROR; i++)
      {
	DB_VALUE v;
	REGU_VARIABLE regu;
	SQ_KEY *key;

	db_make_int (&key_dbval, i);
	key = sq_make_key (thread_p, &xasl);
	if (key == NULL)
	  {
	    rc = ER_FAILED;
	    break;
	  }

	memset (&regu, 0, sizeof (regu));
	db_make_int (&v, i * 10);
	regu.type = TYPE_CONSTANT;
	regu.value.dbvalptr = &v;

	if (sq_put (thread_p, key, &xasl, &regu) != NO_ERROR)
	  {
	    sq_free_key (thread_p, key);
	    rc = ER_FAILED;
	  }
      }

    if (rc == NO_ERROR && !SQ_CACHE_ENABLED (&xasl))
      {
	/* eviction must never fall back to the old permanent hard stop */
	rc = ER_FAILED;
      }

    if (rc == NO_ERROR)
      {
	/* (a) the oldest key must have been evicted (LRU) -- not everything fits */
	DB_VALUE v0;
	REGU_VARIABLE regu0;
	SQ_KEY *key0;

	db_make_int (&key_dbval, 0);
	key0 = sq_make_key (thread_p, &xasl);
	memset (&regu0, 0, sizeof (regu0));
	regu0.type = TYPE_CONSTANT;
	regu0.value.dbvalptr = &v0;

	if (key0 == NULL || sq_get (thread_p, key0, &xasl, &regu0))
	  {
	    rc = ER_FAILED;
	  }
	if (key0 != NULL)
	  {
	    sq_free_key (thread_p, key0);
	  }
	/* the miss just recorded above is what (a) is testing; reset the
	 * hit/miss counters so it doesn't also trip the pre-existing,
	 * orthogonal "disable on poor hit ratio" breaker (sq_get's 60%-full
	 * check) before (b) gets to test recompute-after-eviction
	 * correctness. */
	SQ_CACHE_HIT (&xasl) = 0;
	SQ_CACHE_MISS (&xasl) = 0;
      }

    if (rc == NO_ERROR)
      {
	/* (b) recompute + put again, then get() must hit with the NEW value */
	DB_VALUE v;
	REGU_VARIABLE regu;
	SQ_KEY *key;

	db_make_int (&key_dbval, 0);
	key = sq_make_key (thread_p, &xasl);
	memset (&regu, 0, sizeof (regu));
	db_make_int (&v, 999);
	regu.type = TYPE_CONSTANT;
	regu.value.dbvalptr = &v;

	if (key == NULL || sq_put (thread_p, key, &xasl, &regu) != NO_ERROR)
	  {
	    if (key != NULL)
	      {
		sq_free_key (thread_p, key);
	      }
	    rc = ER_FAILED;
	  }

	if (rc == NO_ERROR)
	  {
	    DB_VALUE v2;
	    REGU_VARIABLE regu2;
	    SQ_KEY *key2;

	    db_make_int (&key_dbval, 0);
	    key2 = sq_make_key (thread_p, &xasl);
	    memset (&regu2, 0, sizeof (regu2));
	    regu2.type = TYPE_CONSTANT;
	    regu2.value.dbvalptr = &v2;

	    if (key2 == NULL || !sq_get (thread_p, key2, &xasl, &regu2) || db_get_int (&v2) != 999)
	      {
		rc = ER_FAILED;
	      }
	    if (key2 != NULL)
	      {
		sq_free_key (thread_p, key2);
	      }
	  }
      }
  }
  sq_cache_destroy (thread_p, &sq_cache_obj);

  /* (c): a single entry whose own footprint exceeds the whole budget must be
   * bypassed (skipped, not retained), and must not disable the cache. */
  if (rc == NO_ERROR)
    {
      DB_VALUE v;
      REGU_VARIABLE regu;
      SQ_KEY *key;

      memset (&sq_cache_obj, 0, sizeof (sq_cache_obj));
      sq_cache_obj.sq_key_struct = &sq_key_struct;
      sq_cache_obj.wm_charged_shard = -1;
      xasl.sq_cache = &sq_cache_obj;

      prm_set_bigint_value (PRM_ID_WORK_MEM, 1);
      prm_set_float_value (PRM_ID_HASH_MEM_MULTIPLIER, 1.0f);

      db_make_int (&key_dbval, 0);
      key = sq_make_key (thread_p, &xasl);
      memset (&regu, 0, sizeof (regu));
      db_make_int (&v, 42);
      regu.type = TYPE_CONSTANT;
      regu.value.dbvalptr = &v;

      if (key == NULL)
	{
	  rc = ER_FAILED;
	}
      else
	{
	  /* #146 T3 S3b: sq_put()'s documented bypass contract is to return
	   * ER_FAILED here (see the "doesn't fit" branch in sq_put) -- that is
	   * the outcome under test, not a selftest failure. Per that same
	   * contract, sq_put() only frees val internally; key remains owned
	   * by the caller either way. */
	  int put_rc = sq_put (thread_p, key, &xasl, &regu);

	  sq_free_key (thread_p, key);

	  if (put_rc == NO_ERROR)
	    {
	      /* a 1-byte budget should never actually retain this entry */
	      rc = ER_FAILED;
	    }
	  else if (!SQ_CACHE_ENABLED (&xasl))
	    {
	      /* bypass of one oversized entry must not disable the cache */
	      rc = ER_FAILED;
	    }
	  else
	    {
	      DB_VALUE v2;
	      REGU_VARIABLE regu2;
	      SQ_KEY *key2;

	      db_make_int (&key_dbval, 0);
	      key2 = sq_make_key (thread_p, &xasl);
	      memset (&regu2, 0, sizeof (regu2));
	      regu2.type = TYPE_CONSTANT;
	      regu2.value.dbvalptr = &v2;

	      if (key2 == NULL || sq_get (thread_p, key2, &xasl, &regu2))
		{
		  /* must have been bypassed, not retained */
		  rc = ER_FAILED;
		}
	      if (key2 != NULL)
		{
		  sq_free_key (thread_p, key2);
		}
	    }
	}

      sq_cache_destroy (thread_p, &sq_cache_obj);
    }

  prm_set_bigint_value (PRM_ID_WORK_MEM, saved_work_mem);
  prm_set_float_value (PRM_ID_HASH_MEM_MULTIPLIER, saved_multiplier);

  /* (d): no charge leak once every sq_cache above has been destroyed */
  if (rc == NO_ERROR && temp_page_store::reserved_bytes () != reserved_before)
    {
      rc = ER_FAILED;
    }

  return rc;
}
