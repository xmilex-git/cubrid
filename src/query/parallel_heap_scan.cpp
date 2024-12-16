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
 * parallel_heap_scan.cpp - Parallel Heap Scan
 */


#ident "$Id$"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "parallel_heap_scan.hpp"

#include "thread_entry_task.hpp"
#include "memory_alloc.h"
#include "memory_private_allocator.hpp"
#include "xasl.h"
#include "query_reevaluation.hpp"
#include "oid.h"
#include "heap_file.h"
#include "fetch.h"
#include "regu_var.hpp"
#include "heap_attrinfo.h"
#include "query_executor.h"
#include "xasl_predicate.hpp"



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

enum ph_pred_expr_types
{
  PH_PRED_EXPR = 1,
  PH_PRED = 1 << 1,
  PH_EVAL_TERM = 1 << 2,
  PH_COMP_EVAL_TERM = 1 << 3,
  PH_ALSM_EVAL_TERM = 1 << 4,
  PH_LIKE_EVAL_TERM = 1 << 5,
  PH_RLIKE_EVAL_TERM = 1 << 6,
  PH_REGU_VAR = 1 << 7
};

static int regu_var_list_len (REGU_VARIABLE_LIST list);
static REGU_VARIABLE_LIST regu_var_list_clone (THREAD_ENTRY *thread_p, REGU_VARIABLE_LIST list);
static void regu_var_list_free (THREAD_ENTRY *thread_p, REGU_VARIABLE_LIST head);
static void *
pred_expr_clone (THREAD_ENTRY *thread_p, void *src, void *dest, enum ph_pred_expr_types type,
		 heap_cache_attrinfo *attr_info);
static void pred_expr_free (THREAD_ENTRY *thread_p, PRED_EXPR *src);
static heap_cache_attrinfo *attr_cache_clone (THREAD_ENTRY *thread_p, heap_cache_attrinfo *src);
static void attr_cache_free (THREAD_ENTRY *thread_p, heap_cache_attrinfo *src);
static void link_attr_cache (THREAD_ENTRY *thread_p, REGU_VARIABLE_LIST node,heap_cache_attrinfo *old_attr_info,
			     heap_cache_attrinfo *new_attr_info);
static SCAN_CODE scan_next_heap_scan_1page_internal (THREAD_ENTRY *thread_p, SCAN_ID *scan_id, VPID *curr_vpid);


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

    void capture_pred_regu_var_list (REGU_VARIABLE_LIST list);
    void capture_rest_regu_var_list (REGU_VARIABLE_LIST list);
    void copy_to_pred_regu_var_list (REGU_VARIABLE_LIST list);
    void copy_to_rest_regu_var_list (REGU_VARIABLE_LIST list);
    void init (THREAD_ENTRY *thread_p, int n_pred_val, int n_rest_val);
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
	  if (DB_NEED_CLEAR (iter->value.vfetch_to))
	    {
	      pr_clear_value (iter->value.vfetch_to);
	    }
	}
      db_value_clone (&dbvalue_array->vals[i], iter->value.vfetch_to);
      iter = iter->next;
    }
  return;
}

void parallel_heap_scan_result_queue_entry::capture_pred_regu_var_list (REGU_VARIABLE_LIST list)
{
  capture_regu_var_list (list, &pred_val_array);
}

void parallel_heap_scan_result_queue_entry::capture_rest_regu_var_list (REGU_VARIABLE_LIST list)
{
  capture_regu_var_list (list, &rest_val_array);
}

void parallel_heap_scan_result_queue_entry::copy_to_pred_regu_var_list (REGU_VARIABLE_LIST list)
{
  copy_to_regu_var_list (&pred_val_array, list);
}

void parallel_heap_scan_result_queue_entry::copy_to_rest_regu_var_list (REGU_VARIABLE_LIST list)
{
  copy_to_regu_var_list (&rest_val_array, list);
}

void parallel_heap_scan_result_queue_entry::init (THREAD_ENTRY *thread_p, int n_pred_val, int n_rest_val)
{
  pred_val_array.size = n_pred_val;
  if (n_pred_val > 0)
    {
      pred_val_array.vals = (DB_VALUE *)HP_ALLOC (thread_p, n_pred_val * sizeof (DB_VALUE));
    }
  else
    {
      pred_val_array.vals = NULL;
    }
  rest_val_array.size = n_rest_val;
  if (n_rest_val > 0)
    {
      rest_val_array.vals = (DB_VALUE *)HP_ALLOC (thread_p, n_rest_val * sizeof (DB_VALUE));
    }
  else
    {
      rest_val_array.vals = NULL;
    }
  scan_code = S_END;
  valid = false;
  curr_oid = {0,0,0};
}
void parallel_heap_scan_result_queue_entry::clear ()
{
  int i;
  for (i = 0; i < pred_val_array.size; i++)
    {
      if (DB_NEED_CLEAR (&pred_val_array.vals[i]))
	{
	  pr_clear_value (&pred_val_array.vals[i]);
	}
    }
  for (i = 0; i < rest_val_array.size; i++)
    {
      if (DB_NEED_CLEAR (&rest_val_array.vals[i]))
	{
	  pr_clear_value (&rest_val_array.vals[i]);
	}
    }
  scan_code = S_END;
  valid = false;
  curr_oid = {0,0,0};
}

/*************************************************************************************************/

/* parallel_heap_scan_result_queue */
class parallel_heap_scan_result_queue
{
  public:

    ~parallel_heap_scan_result_queue()
    {
      clear();
    }

    class parallel_heap_scan_result_queue_variable
    {
      public:
	int start;
	int end;
	bool waiting;
	std::mutex mutex;
	std::condition_variable cond;
    };
    std::atomic<bool> is_scan_ended;
    parallel_heap_scan_result_queue_variable var;

    void init (THREAD_ENTRY *thread_p, int n_pred, int n_rest);
    void clear ();
    bool isFull ();
    bool isEmpty ();
    bool isEmpty_with_lock();
    int enqueue (HEAP_SCAN_ID *hsidp, SCAN_CODE scan_code);
    int dequeue (PARALLEL_HEAP_SCAN_ID *phsidp, SCAN_CODE *scan_code);
    int dequeue_immediate (PARALLEL_HEAP_SCAN_ID *phsidp, SCAN_CODE *scan_code);
    int dequeue_without_lock (PARALLEL_HEAP_SCAN_ID *phsidp, SCAN_CODE *scan_code);

    parallel_heap_scan_result_queue();

  private:
    parallel_heap_scan_result_queue_entry entries[HP_RESULT_QUEUE_SIZE];

    THREAD_ENTRY *owner_thread_p;
};

parallel_heap_scan_result_queue::parallel_heap_scan_result_queue()
{
  var.start = var.end = 0;
  var.waiting = false;
  is_scan_ended = false;
}

void parallel_heap_scan_result_queue::init (THREAD_ENTRY *thread_p, int n_pred, int n_rest)
{
  owner_thread_p = thread_p;
  for (int i = 0; i < HP_RESULT_QUEUE_SIZE; i++)
    {
      entries[i].init (owner_thread_p, n_pred, n_rest);
    }
  std::unique_lock<std::mutex> (var.mutex);
  var.start = var.end = 0;
  var.waiting = false;
  is_scan_ended = false;
}

void parallel_heap_scan_result_queue::clear ()
{
  std::unique_lock<std::mutex> (var.mutex);
  for (int i = 0; i < HP_RESULT_QUEUE_SIZE; i++)
    {
      if (entries[i].valid)
	{
	  entries[i].clear ();
	}
    }
  var.start = var.end = 0;
  var.waiting = false;
  is_scan_ended = false;
}

bool parallel_heap_scan_result_queue::isFull ()
{
  return (var.start == (var.end + 1) % HP_RESULT_QUEUE_SIZE);
}

bool parallel_heap_scan_result_queue::isEmpty ()
{
  return (var.start == var.end);
}

bool parallel_heap_scan_result_queue::isEmpty_with_lock()
{
  std::unique_lock<std::mutex> lock (var.mutex);
  return var.start == var.end;
}

int parallel_heap_scan_result_queue::enqueue (HEAP_SCAN_ID *hsidp, SCAN_CODE scan_code)
{
  bool need_signal;
  std::unique_lock<std::mutex> lock (var.mutex);
  parallel_heap_scan_result_queue_entry *p;
  while (isFull())
    {
      var.waiting = true;
      var.cond.wait (lock);
      var.waiting = false;
    }

  var.end = (var.end + 1) % HP_RESULT_QUEUE_SIZE;
  if (entries[var.end].valid)
    {
      entries[var.end].clear ();
    }
  p = &entries[var.end];
  /* copy data from scan_id */
  p->valid = true;
  p->scan_code = scan_code;
  COPY_OID (&p->curr_oid, &hsidp->curr_oid);
  p->capture_pred_regu_var_list (hsidp->scan_pred.regu_list);
  p->capture_rest_regu_var_list (hsidp->rest_regu_list);

  need_signal = var.waiting;
  lock.unlock();
  if (need_signal)
    {
      var.cond.notify_one();
    }
  return NO_ERROR;
}

int parallel_heap_scan_result_queue::dequeue (PARALLEL_HEAP_SCAN_ID *phsidp, SCAN_CODE *scan_code)
{
  std::unique_lock<std::mutex> lock (var.mutex);
  parallel_heap_scan_result_queue_entry *p;
  if (isEmpty())
    {
      var.waiting = true;
      var.cond.wait (lock);
      var.waiting = false;
    }

  var.start = (var.start + 1) % HP_RESULT_QUEUE_SIZE;
  /* copy data from queue */
  p = &entries[var.start];
  p->valid = true;
  p->copy_to_pred_regu_var_list (phsidp->scan_pred.regu_list);
  p->copy_to_rest_regu_var_list (phsidp->rest_regu_list);
  COPY_OID (&phsidp->curr_oid, &p->curr_oid);
  *scan_code = p->scan_code;

  bool need_signal = var.waiting;
  lock.unlock();

  if (need_signal)
    {
      var.cond.notify_one();
    }
  return NO_ERROR;
}

int parallel_heap_scan_result_queue::dequeue_immediate (PARALLEL_HEAP_SCAN_ID *phsidp, SCAN_CODE *scan_code)
{
  std::unique_lock<std::mutex> lock (var.mutex);
  parallel_heap_scan_result_queue_entry *p;
  if (isEmpty())
    {
      return FALSE;
    }

  var.start = (var.start + 1) % HP_RESULT_QUEUE_SIZE;
  /* copy data from queue */
  p = &entries[var.start];
  p->valid = true;
  p->copy_to_pred_regu_var_list (phsidp->scan_pred.regu_list);
  p->copy_to_rest_regu_var_list (phsidp->rest_regu_list);
  COPY_OID (&phsidp->curr_oid, &p->curr_oid);
  *scan_code = p->scan_code;

  bool need_signal = var.waiting;
  lock.unlock();

  if (need_signal)
    {
      var.cond.notify_one();
    }
  return TRUE;
}

int parallel_heap_scan_result_queue::dequeue_without_lock (PARALLEL_HEAP_SCAN_ID *phsidp, SCAN_CODE *scan_code)
{
  parallel_heap_scan_result_queue_entry *p;
  if (isEmpty())
    {
      return FALSE;
    }
  var.start = (var.start + 1) % HP_RESULT_QUEUE_SIZE;
  /* copy data from queue */
  p = &entries[var.start];
  p->valid = true;
  p->copy_to_pred_regu_var_list (phsidp->scan_pred.regu_list);
  p->copy_to_rest_regu_var_list (phsidp->rest_regu_list);
  COPY_OID (&phsidp->curr_oid, &p->curr_oid);
  *scan_code = p->scan_code;

  return TRUE;
}

/*************************************************************************************************/
/* master, context, task */

class parallel_heap_scan_context : public cubthread::entry_manager
{
  public:
    std::atomic<std::uint64_t> m_tasks_executed;
    std::atomic<std::uint64_t> m_tasks_started;
    parallel_heap_scan_result_queue *m_result_queue;
    std::atomic<std::uint64_t> scan_ended_queue_count;
    int m_has_error;
    SCAN_ID *m_scan_id;
    int m_orig_tran_index;
    REGU_VARIABLE_LIST orig_pred_list;
    REGU_VARIABLE_LIST orig_rest_list;
    class locked_vpid
    {
      public:
	VPID vpid;
	bool is_ended;
	std::mutex mutex;
    } m_locked_vpid;

    parallel_heap_scan_context (SCAN_ID *scan_id, int tran_index, int parallelism);
    ~parallel_heap_scan_context();
    void set_regu_vars (REGU_VARIABLE_LIST pred_list, REGU_VARIABLE_LIST rest_list);
};

class parallel_heap_scan_task : public cubthread::entry_task
{
  public:
    parallel_heap_scan_context *m_context;
    parallel_heap_scan_result_queue *m_result_queue;

    parallel_heap_scan_task() = default;

    parallel_heap_scan_task (parallel_heap_scan_context *context)
      : m_context (context)
    {
    }
    ~parallel_heap_scan_task()
    {
      m_context = nullptr;
      m_result_queue = nullptr;
    }
    virtual void execute (cubthread::entry &thread_ref) override;
    void set_result_queue (parallel_heap_scan_result_queue *queue)
    {
      m_result_queue = queue;
    }
    SCAN_CODE page_next (THREAD_ENTRY *thread_p, HFID *hfid, VPID *vpid);
};

parallel_heap_scan_context::parallel_heap_scan_context (SCAN_ID *scan_id, int tran_index,
    int parallelism)
{
  m_scan_id = scan_id;
  m_orig_tran_index = tran_index;
  VPID_SET_NULL (&m_locked_vpid.vpid);
  m_locked_vpid.is_ended = false;
  m_result_queue = new parallel_heap_scan_result_queue[parallelism] {parallel_heap_scan_result_queue()};
  m_tasks_started = 0;
  m_tasks_executed = 0;
  m_has_error = NO_ERROR;
  scan_ended_queue_count = 0;
}

parallel_heap_scan_context::~parallel_heap_scan_context()
{
  delete[] m_result_queue;
}

void parallel_heap_scan_context::set_regu_vars (REGU_VARIABLE_LIST pred_list,
    REGU_VARIABLE_LIST rest_list)
{
  orig_pred_list = pred_list;
  orig_rest_list = rest_list;
}

SCAN_CODE parallel_heap_scan_task::page_next (THREAD_ENTRY *thread_p, HFID *hfid, VPID *vpid)
{
  std::unique_lock<std::mutex> lock (m_context->m_locked_vpid.mutex);
  if (m_context->m_locked_vpid.is_ended)
    {
      return S_END;
    }
  else
    {
      SCAN_CODE page_scan_code = heap_page_next (thread_p, NULL, hfid, &m_context->m_locked_vpid.vpid, NULL);
      VPID_COPY (vpid, &m_context->m_locked_vpid.vpid);
      if (page_scan_code == S_END)
	{
	  m_context->m_locked_vpid.is_ended = true;
	  return S_END;
	}
      return page_scan_code;
    }
}

void parallel_heap_scan_task::execute (cubthread::entry &thread_ref)
{
  int tran_index = m_context->m_orig_tran_index;
  int ret = NO_ERROR;
  THREAD_ENTRY *thread_p = &thread_ref;
  SCAN_ID *scan_id, *orig_scan_id = m_context->m_scan_id;
  PARALLEL_HEAP_SCAN_ID *phsidp = &orig_scan_id->s.phsid;
  SCAN_CODE page_scan_code, rec_scan_code;
  VPID vpid;
  HFID hfid;
  HL_HEAPID orig_heap_id = db_change_private_heap (thread_p, 0);
  scan_id = (SCAN_ID *) HP_ALLOC (thread_p, sizeof (SCAN_ID));
  HEAP_SCAN_ID *hsidp = &scan_id->s.hsid;
  thread_p->tran_index = tran_index;

  scan_open_heap_scan (thread_p, scan_id, orig_scan_id->mvcc_select_lock_needed, orig_scan_id->scan_op_type,
		       orig_scan_id->fixed, orig_scan_id->grouped, orig_scan_id->single_fetch, orig_scan_id->join_dbval,
		       orig_scan_id->val_list, orig_scan_id->vd, &phsidp->cls_oid, &phsidp->hfid,
		       phsidp->scan_pred.regu_list, phsidp->scan_pred.pred_expr, phsidp->rest_regu_list,
		       phsidp->pred_attrs.num_attrs, phsidp->pred_attrs.attr_ids, phsidp->pred_attrs.attr_cache,
		       phsidp->rest_attrs.num_attrs, phsidp->rest_attrs.attr_ids, phsidp->rest_attrs.attr_cache,
		       S_HEAP_SCAN, phsidp->cache_recordinfo, phsidp->recordinfo_regu_list, false);

  hsidp->scan_pred.regu_list = regu_var_list_clone (thread_p, m_context->orig_pred_list);
  hsidp->rest_regu_list = regu_var_list_clone (thread_p, m_context->orig_rest_list);
  hsidp->pred_attrs.attr_cache = attr_cache_clone (thread_p, phsidp->pred_attrs.attr_cache);
  hsidp->rest_attrs.attr_cache = attr_cache_clone (thread_p, phsidp->rest_attrs.attr_cache);
  hsidp->scan_pred.pred_expr = (PRED_EXPR *)pred_expr_clone (thread_p, (void *)phsidp->scan_pred.pred_expr, NULL,
			       PH_PRED_EXPR, hsidp->pred_attrs.attr_cache);
  link_attr_cache (thread_p, hsidp->scan_pred.regu_list, phsidp->pred_attrs.attr_cache, hsidp->pred_attrs.attr_cache);
  link_attr_cache (thread_p, hsidp->rest_regu_list, phsidp->rest_attrs.attr_cache, hsidp->rest_attrs.attr_cache);
  hsidp->caches_inited = false;
  ret = scan_start_scan (thread_p, scan_id);
  reset_pred_or_regu_var_list (hsidp->scan_pred.pred_expr, true);
  reset_pred_or_regu_var_list (hsidp->scan_pred.regu_list, false);
  reset_pred_or_regu_var_list (hsidp->rest_regu_list, false);
  /* phsidp->scan_pred.pred_expr, phsidp->pred_attrs.attr_cache phsidp->rest_attrs.attr_cache 를 독립적으로 운용해야함 */

  hfid = phsidp->hfid;
  OID_SET_NULL (&hsidp->curr_oid);

  while (TRUE)
    {
      page_scan_code = page_next (thread_p, &hfid, &vpid);

      if (page_scan_code == S_END)
	{
	  m_result_queue->enqueue (hsidp, page_scan_code);
	  break;
	}

      while (TRUE)
	{
	  rec_scan_code = scan_next_heap_scan_1page_internal (thread_p, scan_id, &vpid);
	  assert (rec_scan_code == S_SUCCESS || rec_scan_code == S_END);
	  if (rec_scan_code == S_END)
	    {
	      break;
	    }
	  else if (rec_scan_code == S_SUCCESS)
	    {
	      m_result_queue->enqueue (hsidp, rec_scan_code);
	    }
	}

    }

  scan_end_scan (thread_p, scan_id);
  scan_close_scan (thread_p, scan_id);
  if (hsidp->caches_inited)
    {
      heap_attrinfo_end (thread_p, hsidp->pred_attrs.attr_cache);
      heap_attrinfo_end (thread_p, hsidp->rest_attrs.attr_cache);
      hsidp->caches_inited = false;
    }

  regu_var_list_free (thread_p, hsidp->scan_pred.regu_list);
  regu_var_list_free (thread_p, hsidp->rest_regu_list);
  pred_expr_free (thread_p, hsidp->scan_pred.pred_expr);
  attr_cache_free (thread_p, hsidp->pred_attrs.attr_cache);
  attr_cache_free (thread_p, hsidp->rest_attrs.attr_cache);
  HP_FREE (thread_p, scan_id);
  db_change_private_heap (thread_p, orig_heap_id);
  m_context->m_tasks_executed++;
}

parallel_heap_scan_master::parallel_heap_scan_master (int tran_index, SCAN_ID *scan_id, size_t pool_size,
    size_t task_max_count, std::size_t core_count)
{
  parallelism = (int)core_count;
  m_context = new parallel_heap_scan_context (scan_id, tran_index, parallelism);
  m_workpool = thread_get_manager()->create_worker_pool (pool_size, task_max_count, "Parallel heap scan pool",
	       m_context, parallelism, 1);
  m_scan_id = scan_id;
  m_is_reset_once = false;
  m_is_start_once = false;
}

parallel_heap_scan_master::~parallel_heap_scan_master()
{
  if (m_workpool != nullptr)
    {
      thread_get_manager()->destroy_worker_pool (m_workpool);
    }
}

SCAN_CODE parallel_heap_scan_master::get_result (THREAD_ENTRY *thread_p, SCAN_ID *scan_id)
{
  SCAN_CODE scan_code;
  int result = FALSE;
  int timeout_count = 0;
  if (m_context->scan_ended_queue_count == parallelism)
    {
      return S_END;
    }

  for (int i = 0; i < parallelism; i++)
    {
      if (!m_context->m_result_queue[i].is_scan_ended)
	{
	  std::unique_lock<std::mutex> lock (m_context->m_result_queue[i].var.mutex, std::try_to_lock);
	  if (lock.owns_lock())
	    {
	      result = m_context->m_result_queue[i].dequeue_without_lock (&scan_id->s.phsid, &scan_code);
	      if (result == TRUE)
		{
		  if (scan_code == S_END)
		    {
		      lock.unlock();
		      m_context->scan_ended_queue_count++;
		      m_context->m_result_queue[i].is_scan_ended = true;
		      result = FALSE;
		      continue;
		    }
		  return scan_code;
		}
	      else
		{
		  lock.unlock();
		  continue;
		}
	    }
	  else
	    {
	      continue;
	    }
	}

    }

  if (m_context->scan_ended_queue_count == parallelism)
    {
      return S_END;
    }

  while (result == FALSE)
    {
      for (int i=0; i<parallelism; i++)
	{
	  if (!m_context->m_result_queue[i].is_scan_ended)
	    {
	      if (timeout_count > 100)
		{
		  return S_ERROR;
		}
	      std::unique_lock<std::mutex> lock (m_context->m_result_queue[i].var.mutex, std::try_to_lock);
	      if (lock.owns_lock())
		{
		  result = m_context->m_result_queue[i].dequeue_without_lock (&scan_id->s.phsid, &scan_code);
		  if (result == TRUE)
		    {
		      if (scan_code == S_END)
			{
			  lock.unlock();
			  m_context->scan_ended_queue_count++;
			  m_context->m_result_queue[i].is_scan_ended = true;
			  result = FALSE;
			  continue;
			}
		      return scan_code;
		    }
		  else
		    {
		      bool need_signal = m_context->m_result_queue[i].var.waiting;
		      lock.unlock();

		      if (need_signal)
			{
			  m_context->m_result_queue[i].var.cond.notify_one();
			}
		      continue;
		    }
		}
	      else
		{
		  thread_sleep (10);
		  timeout_count++;
		}
	    }
	  else
	    {
	      continue;
	    }
	}
      if (m_context->scan_ended_queue_count == parallelism)
	{
	  return S_END;
	}
    }
  return S_ERROR;
}

void parallel_heap_scan_master::start (THREAD_ENTRY *thread_p)
{
  //std::unique_ptr<parallel_heap_scan_task> task = NULL;
  m_context->set_regu_vars (m_scan_id->s.phsid.scan_pred.regu_list, m_scan_id->s.phsid.rest_regu_list);
  m_context->scan_ended_queue_count = 0;
  for (int i = 0; i < parallelism; i++)
    {
      m_context->m_result_queue[i].init (thread_p,
					 regu_var_list_len (m_scan_id->s.phsid.scan_pred.regu_list),
					 regu_var_list_len (m_scan_id->s.phsid.rest_regu_list));
    }
}

void parallel_heap_scan_master::reset (SCAN_ID *scan_id)
{
  std::unique_ptr<parallel_heap_scan_task> task = NULL;
  while (m_context->m_tasks_executed < m_context->m_tasks_started)
    {
      thread_sleep (10);
    }

  for (int i = 0; i < parallelism; i++)
    {
      m_context->m_result_queue[i].clear();
    }
  m_context->scan_ended_queue_count = 0;
  std::unique_lock<std::mutex> lock (m_context->m_locked_vpid.mutex);
  VPID_SET_NULL (&m_context->m_locked_vpid.vpid);
  m_context->m_locked_vpid.is_ended = false;
  scan_id->single_fetched = false;
  scan_id->null_fetched = false;
  scan_id->position = (scan_id->direction == S_FORWARD) ? S_BEFORE : S_AFTER;
  OID_SET_NULL (&scan_id->s.hsid.curr_oid);
  lock.unlock();
  start_tasks (scan_id);
  m_is_start_once = true;
}

void parallel_heap_scan_master::start_tasks (SCAN_ID *scan_id)
{
  std::unique_ptr<parallel_heap_scan_task> task = NULL;
  for (int i = 0; i < parallelism; i++)
    {
      task.reset (new parallel_heap_scan_task (m_context));
      task->set_result_queue (&m_context->m_result_queue[i]);
      thread_get_manager()->push_task (m_workpool, task.release());
      m_context->m_tasks_started++;
    }
}

void parallel_heap_scan_master::end()
{
  if (m_context->m_has_error)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IB_ERROR_ABORT, 0);
    }
  while (m_context->m_tasks_executed < m_context->m_tasks_started)
    {
      thread_sleep (10);
    }
  m_is_start_once = false;
  m_is_reset_once = false;
}

/*************************************************************************************************/
/* public functions */

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
	      (REGU_VARIABLE_LIST) HP_ALLOC (thread_p, sizeof (struct regu_variable_list_node));
      if (!new_node)
	{
	  REGU_VARIABLE_LIST temp = dest_head;
	  while (temp)
	    {
	      REGU_VARIABLE_LIST next = temp->next;
	      if (temp->value.vfetch_to)
		{
		  if (DB_NEED_CLEAR (temp->value.vfetch_to))
		    {
		      pr_clear_value (temp->value.vfetch_to);
		    }
		  HP_FREE (thread_p, temp->value.vfetch_to);
		}
	      HP_FREE (thread_p, temp);
	      temp = next;
	    }
	  return NULL;
	}

      new_node->value = current->value;
      new_node->value.vfetch_to = (DB_VALUE *) HP_ALLOC (thread_p, sizeof (DB_VALUE));

      /* should we clone value? */
      pr_clone_value (current->value.vfetch_to, new_node->value.vfetch_to);
      if (current->value.type == TYPE_ATTR_ID || current->value.type == TYPE_CLASS_ATTR_ID
	  || current->value.type == TYPE_SHARED_ATTR_ID)
	{
	  new_node->value.value.attr_descr.cache_dbvalp = nullptr;
	}
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

void
regu_var_list_free (THREAD_ENTRY *thread_p, REGU_VARIABLE_LIST head)
{
  REGU_VARIABLE_LIST current = head;
  while (current)
    {
      REGU_VARIABLE_LIST next = current->next;

      if (current->value.vfetch_to)
	{
	  if (DB_NEED_CLEAR (current->value.vfetch_to))
	    {
	      pr_clear_value (current->value.vfetch_to);
	    }
	  HP_FREE (thread_p, current->value.vfetch_to);
	}
      HP_FREE (thread_p, current);

      current = next;
    }
}

static void *
pred_expr_clone (THREAD_ENTRY *thread_p, void *src, void *dest, enum ph_pred_expr_types type,
		 heap_cache_attrinfo *attr_info)
{
  void *ret = nullptr;
  PRED_EXPR *src_pred_expr;
  PRED_EXPR *dest_pred_expr;
  PRED *src_pred;
  PRED *dest_pred;
  EVAL_TERM *src_eval_term;
  EVAL_TERM *dest_eval_term;
  COMP_EVAL_TERM *src_comp_eval_term;
  COMP_EVAL_TERM *dest_comp_eval_term;
  ALSM_EVAL_TERM *src_alsm_eval_term;
  ALSM_EVAL_TERM *dest_alsm_eval_term;
  LIKE_EVAL_TERM *src_like_eval_term;
  LIKE_EVAL_TERM *dest_like_eval_term;
  RLIKE_EVAL_TERM *src_rlike_eval_term;
  RLIKE_EVAL_TERM *dest_rlike_eval_term;
  regu_variable_node *src_regu_var;
  regu_variable_node *dest_regu_var;
  if (!src)
    {
      return NULL;
    }
  switch (type)
    {
    case PH_PRED_EXPR:
      src_pred_expr = (PRED_EXPR *) src;
      dest_pred_expr = (PRED_EXPR *) HP_ALLOC (thread_p, sizeof (PRED_EXPR));
      *dest_pred_expr = *src_pred_expr;
      switch (src_pred_expr->type)
	{
	case T_PRED:
	  pred_expr_clone (thread_p, (void *)&src_pred_expr->pe.m_pred, (void *)&dest_pred_expr->pe.m_pred, PH_PRED, attr_info);
	  break;
	case T_EVAL_TERM:
	  pred_expr_clone (thread_p, (void *)&src_pred_expr->pe.m_eval_term, (void *)&dest_pred_expr->pe.m_eval_term,
			   PH_EVAL_TERM, attr_info);
	  break;
	case T_NOT_TERM:
	  dest_pred_expr->pe.m_not_term = (PRED_EXPR *) pred_expr_clone (thread_p, (void *)src_pred_expr->pe.m_not_term,
					  (void *)dest_pred_expr->pe.m_not_term, PH_PRED_EXPR, attr_info);
	  break;
	default:
	  assert (false);
	  break;
	}
      ret = (void *) dest_pred_expr;
      break;
    case PH_PRED:
      src_pred = (PRED *) src;
      dest_pred = (PRED *) dest;
      *dest_pred = *src_pred;
      dest_pred->lhs = (PRED_EXPR *) pred_expr_clone (thread_p, (void *)src_pred->lhs, (void *)dest_pred->lhs, PH_PRED_EXPR,
		       attr_info);
      dest_pred->rhs = (PRED_EXPR *) pred_expr_clone (thread_p, (void *)src_pred->rhs, (void *)dest_pred->rhs, PH_PRED_EXPR,
		       attr_info);
      break;
    case PH_EVAL_TERM:
      src_eval_term = (EVAL_TERM *) src;
      dest_eval_term = (EVAL_TERM *) dest;
      switch (src_eval_term->et_type)
	{
	case T_COMP_EVAL_TERM:
	  pred_expr_clone (thread_p, (void *)&src_eval_term->et.et_comp, (void *)&dest_eval_term->et.et_comp, PH_COMP_EVAL_TERM,
			   attr_info);
	  break;
	case T_ALSM_EVAL_TERM:
	  pred_expr_clone (thread_p, (void *)&src_eval_term->et.et_alsm, (void *)&dest_eval_term->et.et_alsm, PH_ALSM_EVAL_TERM,
			   attr_info);
	  break;
	case T_LIKE_EVAL_TERM:
	  pred_expr_clone (thread_p, (void *)&src_eval_term->et.et_like, (void *)&dest_eval_term->et.et_like, PH_LIKE_EVAL_TERM,
			   attr_info);
	  break;
	case T_RLIKE_EVAL_TERM:
	  pred_expr_clone (thread_p, (void *)&src_eval_term->et.et_rlike, (void *)&dest_eval_term->et.et_rlike,
			   PH_RLIKE_EVAL_TERM, attr_info);
	  break;
	}
      break;
    case PH_COMP_EVAL_TERM:
      src_comp_eval_term = (COMP_EVAL_TERM *) src;
      dest_comp_eval_term = (COMP_EVAL_TERM *) dest;
      dest_comp_eval_term->lhs = (regu_variable_node *) pred_expr_clone (thread_p, (void *)src_comp_eval_term->lhs,
				 (void *)dest_comp_eval_term->lhs, PH_REGU_VAR, attr_info);
      dest_comp_eval_term->rhs = (regu_variable_node *) pred_expr_clone (thread_p, (void *)src_comp_eval_term->rhs,
				 (void *)dest_comp_eval_term->rhs, PH_REGU_VAR, attr_info);
      break;
    case PH_ALSM_EVAL_TERM:
      src_alsm_eval_term = (ALSM_EVAL_TERM *) src;
      dest_alsm_eval_term = (ALSM_EVAL_TERM *) dest;
      dest_alsm_eval_term->elem = (regu_variable_node *) pred_expr_clone (thread_p, (void *)src_alsm_eval_term->elem,
				  (void *)dest_alsm_eval_term->elem, PH_REGU_VAR, attr_info);
      dest_alsm_eval_term->elemset = (regu_variable_node *) pred_expr_clone (thread_p, (void *)src_alsm_eval_term->elemset,
				     (void *)dest_alsm_eval_term->elemset, PH_REGU_VAR, attr_info);
      break;
    case PH_LIKE_EVAL_TERM:
      src_like_eval_term = (LIKE_EVAL_TERM *) src;
      dest_like_eval_term = (LIKE_EVAL_TERM *) dest;
      dest_like_eval_term->src = (regu_variable_node *) pred_expr_clone (thread_p, (void *)src_like_eval_term->src,
				 (void *)dest_like_eval_term->src, PH_REGU_VAR, attr_info);
      dest_like_eval_term->pattern = (regu_variable_node *) pred_expr_clone (thread_p, (void *)src_like_eval_term->pattern,
				     (void *)dest_like_eval_term->pattern, PH_REGU_VAR, attr_info);
      dest_like_eval_term->esc_char = (regu_variable_node *) pred_expr_clone (thread_p, (void *)src_like_eval_term->esc_char,
				      (void *)dest_like_eval_term->esc_char, PH_REGU_VAR, attr_info);
      break;
    case PH_RLIKE_EVAL_TERM:
      src_rlike_eval_term = (RLIKE_EVAL_TERM *) src;
      dest_rlike_eval_term = (RLIKE_EVAL_TERM *) dest;
      dest_rlike_eval_term->src = (regu_variable_node *) pred_expr_clone (thread_p, (void *)src_rlike_eval_term->src,
				  (void *)dest_rlike_eval_term->src, PH_REGU_VAR, attr_info);
      dest_rlike_eval_term->pattern = (regu_variable_node *) pred_expr_clone (thread_p, (void *)src_rlike_eval_term->pattern,
				      (void *)dest_rlike_eval_term->pattern, PH_REGU_VAR, attr_info);
      dest_rlike_eval_term->case_sensitive = (regu_variable_node *) pred_expr_clone (thread_p,
					     (void *)src_rlike_eval_term->case_sensitive, (void *)dest_rlike_eval_term->case_sensitive, PH_REGU_VAR, attr_info);
      break;
    case PH_REGU_VAR:
      src_regu_var = (regu_variable_node *) src;
      dest_regu_var = (regu_variable_node *) HP_ALLOC (thread_p, sizeof (regu_variable_node));
      *dest_regu_var = *src_regu_var;
      if (src_regu_var->vfetch_to)
	{
	  dest_regu_var->vfetch_to = (DB_VALUE *) HP_ALLOC (thread_p, sizeof (DB_VALUE));
	  /* should we clone value? */
	  pr_clone_value (src_regu_var->vfetch_to, dest_regu_var->vfetch_to);
	}


      if (src_regu_var->type == TYPE_ATTR_ID || src_regu_var->type == TYPE_CLASS_ATTR_ID
	  || src_regu_var->type == TYPE_SHARED_ATTR_ID)
	{
	  dest_regu_var->value.attr_descr.cache_dbvalp = NULL;
	  dest_regu_var->value.attr_descr.cache_attrinfo = attr_info;
	}
      ret = (void *) dest_regu_var;
      break;
    default:
      assert (false);
      break;
    }
  return ret;
}

static void
pred_expr_free (THREAD_ENTRY *thread_p, PRED_EXPR *src)
{
  if (!src)
    {
      return;
    }
  switch (src->type)
    {
    case T_PRED:
      pred_expr_free (thread_p, src->pe.m_pred.lhs);
      pred_expr_free (thread_p, src->pe.m_pred.rhs);
      break;
    case T_EVAL_TERM:
      switch (src->pe.m_eval_term.et_type)
	{
	case T_COMP_EVAL_TERM:
	{
	  COMP_EVAL_TERM *comp_term = &src->pe.m_eval_term.et.et_comp;
	  HP_FREE (thread_p, comp_term->lhs->vfetch_to);
	  HP_FREE (thread_p, comp_term->lhs);
	  HP_FREE (thread_p, comp_term->rhs->vfetch_to);
	  HP_FREE (thread_p, comp_term->rhs);
	}
	break;
	case T_ALSM_EVAL_TERM:
	{
	  ALSM_EVAL_TERM *alsm_term = &src->pe.m_eval_term.et.et_alsm;
	  HP_FREE (thread_p, alsm_term->elem->vfetch_to);
	  HP_FREE (thread_p, alsm_term->elem);
	  HP_FREE (thread_p, alsm_term->elemset->vfetch_to);
	  HP_FREE (thread_p, alsm_term->elemset);
	}
	break;
	case T_LIKE_EVAL_TERM:
	{
	  LIKE_EVAL_TERM *like_term = &src->pe.m_eval_term.et.et_like;
	  HP_FREE (thread_p, like_term->src->vfetch_to);
	  HP_FREE (thread_p, like_term->src);
	  HP_FREE (thread_p, like_term->pattern->vfetch_to);
	  HP_FREE (thread_p, like_term->pattern);
	  HP_FREE (thread_p, like_term->esc_char->vfetch_to);
	  HP_FREE (thread_p, like_term->esc_char);
	}
	break;
	case T_RLIKE_EVAL_TERM:
	{
	  RLIKE_EVAL_TERM *rlike_term = &src->pe.m_eval_term.et.et_rlike;
	  HP_FREE (thread_p, rlike_term->src->vfetch_to);
	  HP_FREE (thread_p, rlike_term->src);
	  HP_FREE (thread_p, rlike_term->pattern->vfetch_to);
	  HP_FREE (thread_p, rlike_term->pattern);
	  HP_FREE (thread_p, rlike_term->case_sensitive->vfetch_to);
	  HP_FREE (thread_p, rlike_term->case_sensitive);
	}
	break;
	}
      break;
    case T_NOT_TERM:
      pred_expr_free (thread_p, src->pe.m_not_term);
      break;
    }
  HP_FREE (thread_p, src);
}

static heap_cache_attrinfo *
attr_cache_clone (THREAD_ENTRY *thread_p, heap_cache_attrinfo *src)
{
  heap_cache_attrinfo *dest = (heap_cache_attrinfo *) HP_ALLOC (thread_p, sizeof (heap_cache_attrinfo));
  *dest = *src;
  if (src->num_values > 0)
    {
      dest->values = (HEAP_ATTRVALUE *) HP_ALLOC (thread_p, sizeof (HEAP_ATTRVALUE) * src->num_values);
      for (int i = 0; i < src->num_values; i++)
	{
	  dest->values[i] = src->values[i];
	}
    }


  return dest;
}

static void
attr_cache_free (THREAD_ENTRY *thread_p, heap_cache_attrinfo *src)
{
  if (!src)
    {
      return;
    }
  if (src->num_values > 0)
    {
      for (int i = 0; i < src->num_values; i++)
	{
	  if (DB_NEED_CLEAR (&src->values[i].dbvalue))
	    {
	      pr_clear_value (&src->values[i].dbvalue);
	    }
	}

      HP_FREE (thread_p, src->values);
    }

  HP_FREE (thread_p, src);
}

static void
link_attr_cache (THREAD_ENTRY *thread_p, REGU_VARIABLE_LIST node,heap_cache_attrinfo *old_attr_info,
		 heap_cache_attrinfo *new_attr_info)
{
  while (node)
    {
      if (node->value.type == TYPE_ATTR_ID || node->value.type == TYPE_CLASS_ATTR_ID
	  || node->value.type == TYPE_SHARED_ATTR_ID)
	{
	  if (node->value.value.attr_descr.cache_attrinfo == old_attr_info)
	    {
	      node->value.value.attr_descr.cache_attrinfo = new_attr_info;
	    }
	  else
	    {
	      assert (false);
	    }
	}
      node = node->next;
    }
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
      assert (hsidp->pred_attrs.attr_cache->last_classrepr != NULL);
      assert (hsidp->rest_attrs.attr_cache->last_classrepr != NULL);
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



SCAN_CODE
scan_next_parallel_heap_scan (THREAD_ENTRY *thread_p, SCAN_ID *scan_id)
{
  if (!scan_id->s.phsid.master->m_is_start_once)
    {
      scan_id->s.phsid.master->start_tasks (scan_id);
      scan_id->s.phsid.master->m_is_start_once = true;
    }
  return scan_id->s.phsid.master->get_result (thread_p, scan_id);
}

int
scan_reset_scan_block_parallel_heap_scan (THREAD_ENTRY *thread_p, SCAN_ID *scan_id)
{
  HL_HEAPID orig_heap_id;
  if (scan_id->s.phsid.master->m_is_reset_once)
    {
      orig_heap_id = db_change_private_heap (thread_p, 0);
      scan_id->s.phsid.master->reset (scan_id);
      //scan_id->s.phsid.master->reset(scan_id); //WHY???????????
      db_change_private_heap (thread_p, orig_heap_id);
    }
  else
    {
      scan_id->s.phsid.master->m_is_reset_once = true;
    }
  return TRUE;
}

void
scan_end_parallel_heap_scan (THREAD_ENTRY *thread_p, SCAN_ID *scan_id)
{
  scan_id->s.phsid.master->end();
}

void
scan_close_parallel_heap_scan (THREAD_ENTRY *thread_p, SCAN_ID *scan_id)
{
  delete scan_id->s.phsid.master;
}

int
scan_start_parallel_heap_scan (THREAD_ENTRY *thread_p, SCAN_ID *scan_id)
{
  scan_id->s.phsid.master->start (thread_p); //scan_ptr이라면 여기서 시작하면 안됨
  return TRUE;
}

int
scan_open_parallel_heap_scan (THREAD_ENTRY *thread_p, SCAN_ID *scan_id,
			      /* fields of SCAN_ID */
			      bool mvcc_select_lock_needed, SCAN_OPERATION_TYPE scan_op_type, int fixed,
			      int grouped, QPROC_SINGLE_FETCH single_fetch, DB_VALUE *join_dbval,
			      val_list_node *val_list, VAL_DESCR *vd,
			      /* fields of HEAP_SCAN_ID */
			      OID *cls_oid, HFID *hfid, regu_variable_list_node *regu_list_pred,
			      PRED_EXPR *pr, regu_variable_list_node *regu_list_rest, int num_attrs_pred,
			      ATTR_ID *attrids_pred, HEAP_CACHE_ATTRINFO *cache_pred, int num_attrs_rest,
			      ATTR_ID *attrids_rest, HEAP_CACHE_ATTRINFO *cache_rest, SCAN_TYPE scan_type,
			      DB_VALUE **cache_recordinfo, regu_variable_list_node *regu_list_recordinfo,
			      bool is_partition_table)
{
  int ret;
  assert (scan_type == S_PARALLEL_HEAP_SCAN);
  scan_id->type = S_HEAP_SCAN;
  ret = scan_open_heap_scan (thread_p, scan_id, mvcc_select_lock_needed, scan_op_type, fixed, grouped, single_fetch,
			     join_dbval,
			     val_list, vd, cls_oid, hfid, regu_list_pred, pr, regu_list_rest, num_attrs_pred, attrids_pred, cache_pred,
			     num_attrs_rest, attrids_rest, cache_rest, S_HEAP_SCAN, cache_recordinfo, regu_list_recordinfo, is_partition_table);
  scan_id->type = S_PARALLEL_HEAP_SCAN;

  scan_id->s.phsid.master = new parallel_heap_scan_master (thread_p->tran_index, scan_id, HP_PARALLELISM,
      HP_PARALLELISM, HP_PARALLELISM);
  return ret;
}
