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
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "parallel_heap_scan.hpp"

#include "thread_manager.hpp"
#include "thread_entry_task.hpp"
#include "memory_alloc.h"
#include "memory_private_allocator.hpp"
#include "xasl.h"
#include "scan_manager.h"
#include "dbtype.h"
#include "query_reevaluation.hpp"
#include "oid.h"

#include "regu_var.hpp"



// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

#define HP_USE_PRIVATE_ALLOC 0  // 0으로 설정하면 malloc 사용

#if HP_USE_PRIVATE_ALLOC
#define HP_ALLOC(thrd, size) db_private_alloc(thrd, size)
#define HP_FREE(thrd, ptr) db_private_free(thrd, ptr)
#else
#define HP_ALLOC(thrd, size) malloc(size)
#define HP_FREE(thrd, ptr) free(ptr)
#endif

static int regu_var_list_len (REGU_VARIABLE_LIST list);
static REGU_VARIABLE_LIST regu_var_list_clone (REGU_VARIABLE_LIST *list);

static SCAN_CODE scan_next_heap_scan_1page_internal (THREAD_ENTRY *thread_p, SCAN_ID *scan_id, VPID *curr_vpid);

class parallel_heap_scan_context : public cubthread::entry_manager
{
  public:
    std::atomic<std::uint64_t> m_tasks_executed;
    int m_error_code;
    css_conn_entry *m_conn;
    int m_orig_tran_index;
    parallel_heap_scan_context () = default;

  protected:
    void on_create (context_type &context);
    void on_retire (context_type &context);
    void on_recycle (context_type &context);
};

class parallel_heap_scan_task : public cubthread::entry_task
{
  public:
    parallel_heap_scan_task () = delete;
    parallel_heap_scan_task (parallel_heap_scan_context &context);
    ~parallel_heap_scan_task ();

    void execute (cubthread::entry &thread_ref);
};

/*************************************************************************************************/
/* parallel_heap_scan_result_queue_entry */
class parallel_heap_scan_result_queue_entry
{
  public:
    DB_VALUE_ARRAY pred_val_array;
    DB_VALUE_ARRAY rest_val_array;
    SCAN_CODE scan_code;
    OID curr_oid;
    bool valid;

    int capture_pred_regu_var_list (REGU_VARIABLE_LIST list);
    int capture_rest_regu_var_list (REGU_VARIABLE_LIST list);
    int copy_to_pred_regu_var_list (REGU_VARIABLE_LIST list);
    int copy_to_rest_regu_var_list (REGU_VARIABLE_LIST list);
    void init (int n_pred_val, int n_rest_val);
    void clear ();

  private:
    void capture_regu_var_list (REGU_VARIABLE_LIST list, DB_VALUE_ARRAY *dbvalue_array);
    void copy_to_regu_var_list (DB_VALUE_ARRAY *dbvalue_array, REGU_VARIABLE_LIST list);
};

void parallel_heap_scan_result_queue_entry::capture_regu_var_list (REGU_VARIABLE_LIST list,
    DB_VALUE_ARRAY *dbvalue_array)
{
  REGU_VARIABLE_LIST iter = list;
  for (int i = 0; i < dbvalue_array->size; i++)
    {
      assert (iter);
      db_value_clone (iter->value.vfetch_to, &dbvalue_array->vals[i]);
      iter = iter->next;
    }
  return;
}

void parallel_heap_scan_result_queue_entry::copy_to_regu_var_list (DB_VALUE_ARRAY *dbvalue_array,
    REGU_VARIABLE_LIST list)
{
  REGU_VARIABLE_LIST iter = list;
  for (int i = 0; i < dbvalue_array->size; i++)
    {
      assert (iter);
      if (!DB_IS_NULL (iter->value.vfetch_to))
	{
	  pr_clear_value (iter->value.vfetch_to);
	}
      db_value_clone (&dbvalue_array->vals[i], iter->value.vfetch_to);
      iter = iter->next;
    }
  return;
}

int parallel_heap_scan_result_queue_entry::capture_pred_regu_var_list (REGU_VARIABLE_LIST list)
{
  capture_regu_var_list (list, &pred_val_array);
}

int parallel_heap_scan_result_queue_entry::capture_rest_regu_var_list (REGU_VARIABLE_LIST list)
{
  capture_regu_var_list (list, &rest_val_array);
}

int parallel_heap_scan_result_queue_entry::copy_to_pred_regu_var_list (REGU_VARIABLE_LIST list)
{
  copy_to_regu_var_list (&pred_val_array, list);
}

int parallel_heap_scan_result_queue_entry::copy_to_rest_regu_var_list (REGU_VARIABLE_LIST list)
{
  copy_to_regu_var_list (&rest_val_array, list);
}

void parallel_heap_scan_result_queue_entry::init (THREAD_ENTRY *thread_p, int n_pred_val, int n_rest_val)
{
  pred_val_array.size = n_pred_val;
  pred_val_array.vals = (DB_VALUE *)HP_ALLOC (thread_p, n_pred_val * sizeof (DB_VALUE));
  rest_val_array.size = n_rest_val;
  rest_val_array.vals = (DB_VALUE *)HP_ALLOC (thread_p, n_rest_val * sizeof (DB_VALUE));
  scan_code = S_END;
  valid = false;
  curr_oid = {0,0,0};
}
void parallel_heap_scan_result_queue_entry::clear ()
{
  db_value_clear_array (&pred_val_array);
  db_value_clear_array (&rest_val_array);
  scan_code = S_END;
  valid = false;
  curr_oid = {0,0,0};
}

/*************************************************************************************************/

/* parallel_heap_scan_result_queue */
class parallel_heap_scan_result_queue
{
  public:
    parallel_heap_scan_result_queue_entry entries[HP_RESULT_QUEUE_SIZE];
    class parallel_heap_scan_result_queue_variable
    {
      public:
	int start;
	int end;
	bool waiting;
	int n_entry;
	std::mutex mutex;
	std::condition_variable cond;
    };

    THREAD_ENTRY *owner_thread_p;

    static parallel_heap_scan_result_queue_variable var;

    ~parallel_heap_scan_result_queue ();

    static int init (THREAD_ENTRY *thread_p, int n_pred, int n_rest);
    static void clear ();
    static bool isFull ();
    static bool isEmpty ();
    static int enqueue (HEAP_SCAN_ID *hsidp, SCAN_CODE scan_code);
    static int dequeue (HEAP_SCAN_ID *hsidp, SCAN_CODE *scan_code);
    static int dequeue_immediate (HEAP_SCAN_ID *hsidp, SCAN_CODE *scan_code);

  private:
    static void lock ()
    {
      std::lock_guard<std::mutex> lock (var.mutex);
    }
    static void unlock ()
    {
      var.mutex.unlock();
    }
    static void wait()
    {
      var.cond.wait (var.mutex);
    }
    static void signal()
    {
      var.cond.notify_one();
    }
};

int parallel_heap_scan_result_queue::init (THREAD_ENTRY *thread_p, int n_pred, int n_rest)
{
  owner_thread_p = thread_p;
  for (int i = 0; i < HP_RESULT_QUEUE_SIZE; i++)
    {
      entries[i].init (owner_thread_p, n_pred, n_rest);
    }
  lock();
  var.start = var.end = 0;
  var.waiting = false;
  unlock();
}
void parallel_heap_scan_result_queue::clear ()
{
  lock();
  for (int i = 0; i < HP_RESULT_QUEUE_SIZE; i++)
    {
      if (entries[i].valid)
	{
	  entries[i].clear ();
	}
    }
  unlock();
}

parallel_heap_scan_result_queue::~parallel_heap_scan_result_queue ()
{
  clear ();
}

bool parallel_heap_scan_result_queue::isFull ()
{
  return (var.start == (var.end + 1) % HP_RESULT_QUEUE_SIZE);
}
bool parallel_heap_scan_result_queue::isEmpty ()
{
  return (var.start == var.end);
}
int parallel_heap_scan_result_queue::enqueue (HEAP_SCAN_ID *hsidp, SCAN_CODE scan_code)
{
  bool need_signal;
  lock();
  if (isFull())
    {
      var.waiting = true;
      wait();
      var.waiting = false;
    }

  var.end = (var.end + 1) % HP_RESULT_QUEUE_SIZE;
  if (entries[var.end].valid)
    {
      entries[var.end].clear ();
    }
  /* copy data from scan_id */
  entries[var.end].valid = true;
  entries[var.end].scan_code = scan_code;
  COPY_OID (&entries[var.end].curr_oid, &hsidp->curr_oid);
  entries[var.end].capture_pred_regu_var_list (hsidp->scan_pred.regu_list);
  entries[var.end].capture_rest_regu_var_list (hsidp->rest_regu_list);

  need_signal = var.waiting;
  unlock();
  if (need_signal)
    {
      signal();
    }
  return NO_ERROR;
}
int parallel_heap_scan_result_queue::dequeue (HEAP_SCAN_ID *hsidp, SCAN_CODE *scan_code)
{
  bool need_signal;
  if (isEmpty())
    {
      var.waiting = true;
      wait();
      var.waiting = false;
    }

  var.start = (var.start + 1) % HP_RESULT_QUEUE_SIZE;
  /* copy data from queue */
  entries[var.start].valid = true;
  entries[var.start].copy_to_pred_regu_var_list (hsidp->scan_pred.regu_list);
  entries[var.start].copy_to_rest_regu_var_list (hsidp->rest_regu_list);
  COPY_OID (&entries[var.start].curr_oid, &hsidp->curr_oid);
  *scan_code = entries[var.start].scan_code;

  need_signal = var.waiting;
  unlock();
  if (need_signal)
    {
      signal();
    }
  return NO_ERROR;
}

int parallel_heap_scan_result_queue::dequeue_immediate (HEAP_SCAN_ID *hsidp, SCAN_CODE *scan_code)
{
  bool need_signal;
  lock();
  if (isEmpty())
    {
      unlock();
      return FALSE;
    }
  var.start = (var.start + 1) % HP_RESULT_QUEUE_SIZE;
  /* copy data from queue */
  entries[var.start].valid = true;
  entries[var.start].copy_to_pred_regu_var_list (hsidp->scan_pred.regu_list);
  entries[var.start].copy_to_rest_regu_var_list (hsidp->rest_regu_list);
  COPY_OID (&entries[var.start].curr_oid, &hsidp->curr_oid);
  *scan_code = entries[var.start].scan_code;

  need_signal = var.waiting;
  unlock();
  if (need_signal)
    {
      signal();
    }
  return TRUE;
}




static int regu_var_list_len (REGU_VARIABLE_LIST list)
{
  int len = 0;
  for (REGU_VARIABLE_LIST iter = list; iter; iter = iter->next)
    {
      len++;
    }
  return len;
}
static REGU_VARIABLE_LIST regu_var_list_clone (THREAD_ENTRY *thread_p, REGU_VARIABLE_LIST src)
{
  REGU_VARIABLE_LIST dest_head = NULL;
  REGU_VARIABLE_LIST dest_tail = NULL;

  REGU_VARIABLE_LIST current = src;
  if (!src)
    {
      return NULL;
    }

  while (current)
    {
      REGU_VARIABLE_LIST new_node =
	      (REGU_VARIABLE_LIST) db_private_alloc (thread_p, sizeof (struct regu_variable_list_node));
      if (!new_node)
	{
	  REGU_VARIABLE_LIST temp = dest_head;
	  while (temp)
	    {
	      REGU_VARIABLE_LIST next = temp->next;
	      if (temp->value.vfetch_to)
		{
		  pr_clear_value (temp->value.vfetch_to);
		  HP_FREE (thread_p, temp->value.vfetch_to);
		}
	      HP_FREE (thread_p, temp);
	      temp = next;
	    }
	  return NULL;
	}

      new_node->value = current->value;
      new_node->value.vfetch_to = (DB_VALUE *) db_private_alloc (thread_p, sizeof (DB_VALUE));

      /* should we clone value? */
      pr_clone_value (current->value.vfetch_to, new_node->value.vfetch_to);

      new_node->next = NULL;

      if (dest_tail)
	{
	  dest_tail->next = new_node;
	}
      else
	{
	  dest_head = new_node;
	}
      dest_tail = new_node;

      current = current->next;
    }

  return dest_head;
}

typedef enum
{
  OBJ_GET_WITHOUT_LOCK = 0,
  OBJ_REPEAT_GET_WITH_LOCK = 1,
  OBJ_GET_WITH_LOCK_COMPLETE = 2
} OBJECT_GET_STATUS;

static SCAN_CODE
scan_next_heap_scan_1page_internal (THREAD_ENTRY *thread_p, SCAN_ID *scan_id, VPID *curr_vpid)
{
  HEAP_SCAN_ID *hsidp;
  FILTER_INFO data_filter;
  RECDES recdes = RECDES_INITIALIZER;
  SCAN_CODE sp_scan;
  DB_LOGICAL ev_res;
  OID current_oid, *p_current_oid = NULL;
  MVCC_SCAN_REEV_DATA mvcc_sel_reev_data;
  MVCC_REEV_DATA mvcc_reev_data;
  UPDDEL_MVCC_COND_REEVAL upd_reev;
  OID retry_oid;
  LOG_LSA ref_lsa;
  bool is_peeking;
  OBJECT_GET_STATUS object_get_status;
  regu_variable_list_node *p;

  hsidp = &scan_id->s.hsid;
  if (scan_id->mvcc_select_lock_needed)
    {
      COPY_OID (&current_oid, &hsidp->curr_oid);
      p_current_oid = &current_oid;
    }
  else
    {
      p_current_oid = &hsidp->curr_oid;
    }

  /* set data filter information */
  scan_init_filter_info (&data_filter, &hsidp->scan_pred, &hsidp->pred_attrs, scan_id->val_list, scan_id->vd,
			 &hsidp->cls_oid, 0, NULL, NULL, NULL);

  is_peeking = scan_id->fixed;
  if (scan_id->grouped)
    {
      is_peeking = PEEK;
    }

  if (data_filter.val_list)
    {
      for (p = data_filter.scan_pred->regu_list; p; p = p->next)
	{
	  if (DB_NEED_CLEAR (p->value.vfetch_to))
	    {
	      pr_clear_value (p->value.vfetch_to);
	    }
	}
    }

  while (1)
    {
      COPY_OID (&retry_oid, &hsidp->curr_oid);
      object_get_status = OBJ_GET_WITHOUT_LOCK;

restart_scan_oid:

      /* get next object */
      assert (!scan_id->grouped);

      {
	recdes.data = NULL;
	assert (scan_id->direction == S_FORWARD);
	assert (scan_id->type == S_HEAP_SCAN);
	{
	  sp_scan =
		  heap_next_1page (thread_p, &hsidp->hfid, curr_vpid, &hsidp->cls_oid, &hsidp->curr_oid, &recdes,
				   &hsidp->scan_cache, is_peeking);
	}
      }

      if (sp_scan != S_SUCCESS)
	{
	  /* scan error or end of scan */
	  return (sp_scan == S_END) ? S_END : S_ERROR;
	}

      if (hsidp->scan_cache.page_watcher.pgptr != NULL)
	{
	  LSA_COPY (&ref_lsa, pgbuf_get_lsa (hsidp->scan_cache.page_watcher.pgptr));
	}

      /* evaluate the predicates to see if the object qualifies */
      scan_id->scan_stats.read_rows++;

      ev_res = eval_data_filter (thread_p, p_current_oid, &recdes, &hsidp->scan_cache, &data_filter);
      if (ev_res == V_ERROR)
	{
	  return S_ERROR;
	}

      if (is_peeking == PEEK && hsidp->scan_cache.page_watcher.pgptr != NULL
	  && PGBUF_IS_PAGE_CHANGED (hsidp->scan_cache.page_watcher.pgptr, &ref_lsa))
	{
	  is_peeking = COPY;
	  COPY_OID (&hsidp->curr_oid, &retry_oid);
	  goto restart_scan_oid;
	}

      if (scan_id->qualification == QPROC_QUALIFIED)
	{
	  if (ev_res != V_TRUE)	/* V_FALSE || V_UNKNOWN */
	    {
	      continue;		/* not qualified, continue to the next tuple */
	    }
	}
      else if (scan_id->qualification == QPROC_NOT_QUALIFIED)
	{
	  if (ev_res != V_FALSE)	/* V_TRUE || V_UNKNOWN */
	    {
	      continue;		/* qualified, continue to the next tuple */
	    }
	}
      else if (scan_id->qualification == QPROC_QUALIFIED_OR_NOT)
	{
	  if (ev_res == V_TRUE)
	    {
	      scan_id->qualification = QPROC_QUALIFIED;
	    }
	  else if (ev_res == V_FALSE)
	    {
	      scan_id->qualification = QPROC_NOT_QUALIFIED;
	    }
	  else			/* V_UNKNOWN */
	    {
	      /* nop */
	      ;
	    }
	}
      else
	{
	  /* invalid value; the same as QPROC_QUALIFIED */
	  if (ev_res != V_TRUE)	/* V_FALSE || V_UNKNOWN */
	    {
	      continue;		/* not qualified, continue to the next tuple */
	    }
	}

      /* Data filter passed. If object should be locked and is not locked yet, lock it. */
      assert (!scan_id->mvcc_select_lock_needed);

      if (mvcc_is_mvcc_disabled_class (&hsidp->cls_oid))
	{
	  LOCK lock = NULL_LOCK;
	  int tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
	  TRAN_ISOLATION tran_isolation = logtb_find_isolation (tran_index);

	  if (scan_id->scan_op_type == S_DELETE || scan_id->scan_op_type == S_UPDATE)
	    {
	      lock = X_LOCK;
	    }
	  else if (oid_is_serial (&hsidp->cls_oid))
	    {
	      /* S_SELECT is currently handled only for serial, but may be extended to the other non-MVCC classes
	       * if needed */
	      lock = S_LOCK;
	    }

	  if (lock != NULL_LOCK && hsidp->scan_cache.page_watcher.pgptr != NULL)
	    {
	      if (tran_isolation == TRAN_READ_COMMITTED && lock == S_LOCK)
		{
		  if (lock_hold_object_instant (thread_p, &hsidp->curr_oid, &hsidp->cls_oid, lock) == LK_GRANTED)
		    {
		      lock = NULL_LOCK;
		      /* object_need_rescan needs to be kept false (page is still fixed, no other transaction could
		       * have change it) */
		    }
		}
	      else
		{
		  if (lock_object (thread_p, &hsidp->curr_oid, &hsidp->cls_oid, lock, LK_COND_LOCK) == LK_GRANTED)
		    {
		      /* successfully locked */
		      lock = NULL_LOCK;
		      /* object_need_rescan needs to be kept false (page is still fixed, no other transaction could
		       * have change it) */
		    }
		}
	    }

	  if (lock != NULL_LOCK)
	    {
	      VPID curr_vpid;

	      VPID_SET_NULL (&curr_vpid);

	      if (hsidp->scan_cache.page_watcher.pgptr != NULL)
		{
		  pgbuf_get_vpid (hsidp->scan_cache.page_watcher.pgptr, &curr_vpid);
		  pgbuf_ordered_unfix (thread_p, &hsidp->scan_cache.page_watcher);
		}
	      else
		{
		  if (object_get_status == OBJ_GET_WITHOUT_LOCK)
		    {
		      /* page not fixed, recdes was read without lock, object may have changed */
		      object_get_status = OBJ_REPEAT_GET_WITH_LOCK;
		    }
		  else if (object_get_status == OBJ_REPEAT_GET_WITH_LOCK)
		    {
		      /* already read with lock, set flag to continue scanning next object */
		      object_get_status = OBJ_GET_WITH_LOCK_COMPLETE;
		    }
		}

	      if (lock_object (thread_p, &hsidp->curr_oid, &hsidp->cls_oid, lock, LK_UNCOND_LOCK) != LK_GRANTED)
		{
		  return S_ERROR;
		}

	      if (!heap_does_exist (thread_p, NULL, &hsidp->curr_oid))
		{
		  /* not qualified, continue to the next tuple */
		  lock_unlock_object_donot_move_to_non2pl (thread_p, &hsidp->curr_oid, &hsidp->cls_oid, lock);
		  continue;
		}

	      if (tran_isolation == TRAN_READ_COMMITTED && lock == S_LOCK)
		{
		  /* release acquired lock in RC */
		  lock_unlock_object_donot_move_to_non2pl (thread_p, &hsidp->curr_oid, &hsidp->cls_oid, lock);
		}

	      assert (hsidp->scan_cache.page_watcher.pgptr == NULL);

	      if (!VPID_ISNULL (&curr_vpid)
		  && pgbuf_ordered_fix (thread_p, &curr_vpid, OLD_PAGE, PGBUF_LATCH_READ,
					&hsidp->scan_cache.page_watcher) != NO_ERROR)
		{
		  return S_ERROR;
		}

	      if (object_get_status == OBJ_REPEAT_GET_WITH_LOCK
		  || (hsidp->scan_cache.page_watcher.pgptr != NULL
		      && PGBUF_IS_PAGE_CHANGED (hsidp->scan_cache.page_watcher.pgptr, &ref_lsa)))
		{
		  is_peeking = COPY;
		  COPY_OID (&hsidp->curr_oid, &retry_oid);
		  goto restart_scan_oid;
		}
	    }
	}

      scan_id->scan_stats.qualified_rows++;

      if (hsidp->rest_regu_list)
	{
	  /* read the rest of the values from the heap into the attribute cache */
	  if (heap_attrinfo_read_dbvalues (thread_p, p_current_oid, &recdes, hsidp->rest_attrs.attr_cache) != NO_ERROR)
	    {
	      return S_ERROR;
	    }

	  if (is_peeking == PEEK && hsidp->scan_cache.page_watcher.pgptr != NULL
	      && PGBUF_IS_PAGE_CHANGED (hsidp->scan_cache.page_watcher.pgptr, &ref_lsa))
	    {
	      is_peeking = COPY;
	      COPY_OID (&hsidp->curr_oid, &retry_oid);
	      goto restart_scan_oid;
	    }

	  /* fetch the rest of the values from the object instance */
	  if (scan_id->val_list)
	    {
	      if (fetch_val_list (thread_p, hsidp->rest_regu_list, scan_id->vd, &hsidp->cls_oid, p_current_oid, NULL,
				  PEEK) != NO_ERROR)
		{
		  return S_ERROR;
		}

	      if (is_peeking != 0 && hsidp->scan_cache.page_watcher.pgptr != NULL
		  && PGBUF_IS_PAGE_CHANGED (hsidp->scan_cache.page_watcher.pgptr, &ref_lsa))
		{
		  is_peeking = COPY;
		  COPY_OID (&hsidp->curr_oid, &retry_oid);
		  goto restart_scan_oid;
		}
	    }
	}

      return S_SUCCESS;
    }
}