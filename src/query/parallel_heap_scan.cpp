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
#include "error_context.hpp"

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
#include "system_parameter.h"

#define START_END_LOG 1

#if START_END_LOG
#include <unistd.h>
#include <sys/syscall.h>
#endif


// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

#define HP_USE_PRIVATE_ALLOC 0 // 0으로 설정하면 malloc 사용

#if HP_USE_PRIVATE_ALLOC
#define HP_ALLOC(thrd, size) db_private_alloc(thrd, size)
#define HP_FREE(thrd, ptr) db_private_free(thrd, ptr)
#else
#define HP_ALLOC(thrd, size) malloc(size)
#define HP_FREE(thrd, ptr) free(ptr)
#endif

#define ALLOC_FREE_LOG 0 //alloc free log

enum ph_pred_expr_types
{
  PH_PRED_EXPR,
  PH_PRED,
  PH_EVAL_TERM,
  PH_COMP_EVAL_TERM,
  PH_ALSM_EVAL_TERM,
  PH_LIKE_EVAL_TERM,
  PH_RLIKE_EVAL_TERM,
  PH_REGU_VAR
};

static int regu_var_list_len (struct regu_variable_list_node   *list);

static SCAN_CODE scan_next_heap_scan_1page_internal (THREAD_ENTRY *thread_p, SCAN_ID *scan_id, VPID *curr_vpid);


static int regu_list_clear (THREAD_ENTRY *thread_p, struct regu_variable_list_node   *list);
static int regu_var_clear (THREAD_ENTRY *thread_p, REGU_VARIABLE *regu_var);
static int pred_clear (THREAD_ENTRY *thread_p, PRED_EXPR *pred);
static int arith_list_clear (THREAD_ENTRY *thread_p, ARITH_TYPE *list);
static void pos_desc_clear (THREAD_ENTRY *thread_p, QFILE_TUPLE_VALUE_POSITION *pos_desc);
static int regu_val_list_clear (THREAD_ENTRY *thread_p, REGU_VALUE_LIST *list);

/***********************************************************************************************/
class parallel_heap_scan_checker
{
  public:
    int check (REGU_VARIABLE *src);
    int check (PRED_EXPR *src);
    int check (struct regu_variable_list_node   *src);
    int check (ARITH_TYPE *src);
    int check (PRED *src);
    int check (EVAL_TERM *src);
    int check (COMP_EVAL_TERM *src);
    int check (ALSM_EVAL_TERM *src);
    int check (LIKE_EVAL_TERM *src);
    int check (RLIKE_EVAL_TERM *src);

    void add_not_parallel_heap_scan_flag (XASL_NODE *xasl);
};

void parallel_heap_scan_checker::add_not_parallel_heap_scan_flag (XASL_NODE *xasl)
{
  ACCESS_SPEC_TYPE *curr_spec;
  XASL_NODE *xaslp;
  if (xasl->spec_list)
    {
      curr_spec = xasl->spec_list;
      while (curr_spec)
	{
	  curr_spec->flags = (ACCESS_SPEC_FLAG) (curr_spec->flags | ACCESS_SPEC_FLAG_NOT_FOR_PARALLEL_HEAP_SCAN);
	  curr_spec = curr_spec->next;
	}
    }
  if (xasl->merge_spec)
    {
      curr_spec = xasl->merge_spec;
      while (curr_spec)
	{
	  curr_spec->flags = (ACCESS_SPEC_FLAG) (curr_spec->flags | ACCESS_SPEC_FLAG_NOT_FOR_PARALLEL_HEAP_SCAN);
	  curr_spec = curr_spec->next;
	}
    }
  if (xasl->aptr_list)
    {
      xaslp = xasl->aptr_list;
      while (xaslp)
	{
	  add_not_parallel_heap_scan_flag (xaslp);
	  xaslp = xaslp->next;
	}
    }
  if (xasl->dptr_list)
    {
      xaslp = xasl->dptr_list;
      while (xaslp)
	{
	  add_not_parallel_heap_scan_flag (xaslp);
	  xaslp = xaslp->next;
	}
    }
  if (xasl->scan_ptr)
    {
      xaslp = xasl->scan_ptr;
      while (xaslp)
	{
	  add_not_parallel_heap_scan_flag (xaslp);
	  xaslp = xaslp->next;
	}
    }
}

int parallel_heap_scan_checker::check (ARITH_TYPE *src)
{
  if (!src)
    {
      return 0;
    }
  int cnt = 0;
  cnt += check (src->leftptr);
  cnt += check (src->rightptr);
  cnt += check (src->thirdptr);
  return cnt;
}

int parallel_heap_scan_checker::check (REGU_VARIABLE *src)
{
  int cnt = 0;
  if (!src)
    {
      return 0;
    }
  if (src->xasl)
    {
      cnt++;
      add_not_parallel_heap_scan_flag (src->xasl);
    }

  switch (src->type)
    {
    case TYPE_ATTR_ID:		/* fetch object attribute value */
    case TYPE_SHARED_ATTR_ID:
    case TYPE_CLASS_ATTR_ID:
      break;
    case TYPE_CONSTANT:
      break;
    case TYPE_INARITH:
    case TYPE_OUTARITH:
      cnt += check (src->value.arithptr);
      break;
    case TYPE_SP:
      cnt+=check (src->value.sp_ptr->args);
      cnt++;
      break;
    case TYPE_FUNC:
      cnt+=check (src->value.funcp->operand);
      break;
    case TYPE_DBVAL:
      break;
    case TYPE_REGUVAL_LIST:
      cnt++;
      break;
    case TYPE_REGU_VAR_LIST:
      cnt+=check (src->value.regu_var_list);
      break;
    default:
      break;
    }
  return cnt;
}

int parallel_heap_scan_checker::check (PRED_EXPR *src)
{
  if (!src)
    {
      return 0;
    }

  switch (src->type)
    {
    case T_PRED:
      return check (&src->pe.m_pred);
      break;
    case T_EVAL_TERM:
      return check (&src->pe.m_eval_term);
      break;
    case T_NOT_TERM:
      return check (src->pe.m_not_term);
      break;
    default:
      return 0;
      break;
    }
}

int parallel_heap_scan_checker::check (PRED *src)
{
  if (!src)
    {
      return 0;
    }
  return check (src->lhs) + check (src->rhs);
}

int parallel_heap_scan_checker::check (EVAL_TERM *src)
{
  if (!src)
    {
      return 0;
    }
  int cnt = 0;
  switch (src->et_type)
    {
    case T_COMP_EVAL_TERM:
      cnt += check (&src->et.et_comp);
      break;
    case T_ALSM_EVAL_TERM:
      cnt += check (&src->et.et_alsm);
      break;
    case T_LIKE_EVAL_TERM:
      cnt += check (&src->et.et_like);
      break;
    case T_RLIKE_EVAL_TERM:
      cnt += check (&src->et.et_rlike);
      break;
    default:
      break;
    }
  return cnt;
}

int parallel_heap_scan_checker::check (COMP_EVAL_TERM *src)
{
  if (!src)
    {
      return 0;
    }
  return check (src->lhs) + check (src->rhs);
}

int parallel_heap_scan_checker::check (ALSM_EVAL_TERM *src)
{
  if (!src)
    {
      return 0;
    }
  return check (src->elem) + check (src->elemset);
}

int parallel_heap_scan_checker::check (LIKE_EVAL_TERM *src)
{
  if (!src)
    {
      return 0;
    }
  return check (src->src) + check (src->pattern) + check (src->esc_char);
}

int parallel_heap_scan_checker::check (RLIKE_EVAL_TERM *src)
{
  if (!src)
    {
      return 0;
    }
  return check (src->src) + check (src->pattern) + check (src->case_sensitive);
}

int parallel_heap_scan_checker::check (struct regu_variable_list_node   *src)
{
  if (!src)
    {
      return 0;
    }
  int cnt = 0;
  struct regu_variable_list_node   *curr = src;
  while (curr)
    {
      cnt += check (&curr->value);
      curr = curr->next;
    }
  return cnt;
}



int
scan_check_parallel_heap_scan_possible (THREAD_ENTRY *thread_p, void *spec,
					bool mvcc_select_lock_needed)
{
#if defined(SERVER_MODE)
  ACCESS_SPEC_TYPE *curr_spec = (ACCESS_SPEC_TYPE *)spec;
  int parallel_heap_scan_threads = prm_get_integer_value (PRM_ID_PARALLEL_HEAP_SCAN_THREADS);
  if (parallel_heap_scan_threads == 0)
    {
      return false;
    }

  if (!mvcc_select_lock_needed)
    {
      if (thread_p->private_heap_id != 0)
	{
	  if (!oid_is_cached_class_oid (&curr_spec->s.cls_node.cls_oid)
	      && ! (curr_spec->flags & ACCESS_SPEC_FLAG_NOT_FOR_PARALLEL_HEAP_SCAN) && ! curr_spec->parts)	/* Only for User table */
	    {
	      int cnt = 0;
	      parallel_heap_scan_checker checker;
	      cnt += checker.check (curr_spec->s.cls_node.cls_regu_list_pred);
	      cnt += checker.check (curr_spec->where_pred);
	      cnt += checker.check (curr_spec->s.cls_node.cls_regu_list_rest);
	      if (cnt == 0)
		{
		  return true;
		}
	    }
	}
    }
#endif
  return false;
}

/*************************************************************************************************/
/* xasl_memory_mapper (for attr_cache and regu_var, db_value) */
class xasl_memory_mapper
{
  public:
    REGU_VARIABLE *copy_and_map (THREAD_ENTRY *thread_p, REGU_VARIABLE *regu_var);
    PRED_EXPR *copy_and_map (THREAD_ENTRY *thread_p, PRED_EXPR *pred);
    struct regu_variable_list_node *copy_and_map (THREAD_ENTRY *thread_p, struct regu_variable_list_node *regu_list);
    DB_VALUE *copy_and_map (THREAD_ENTRY *thread_p, DB_VALUE *db_value);
    ARITH_TYPE *copy_and_map (THREAD_ENTRY *thread_p, ARITH_TYPE *arith_list);
    heap_cache_attrinfo *copy_and_map (THREAD_ENTRY *thread_p, heap_cache_attrinfo *attr_cache);
    struct function_node *copy_and_map (THREAD_ENTRY *thread_p, struct function_node *func);
    SP_TYPE *copy_and_map (THREAD_ENTRY *thread_p, SP_TYPE *sp);
    val_descr *copy_and_map (THREAD_ENTRY *thread_p, val_descr *vd);
    void clear_and_free (val_descr *orig_vd, val_descr *vd);
    ~xasl_memory_mapper();
    xasl_memory_mapper()
    {
      allocated_attr_cache_cnt = 0;
      allocated_regu_var_cnt = 0;
      allocated_db_value_cnt = 0;
      allocated_pred_expr_cnt = 0;
      allocated_arith_cnt = 0;
      allocated_regu_var_list_cnt = 0;
      allocated_function_node_cnt = 0;
      allocated_sp_cnt = 0;
    }
  private:
    int allocated_attr_cache_cnt;
    int allocated_regu_var_cnt;
    int allocated_db_value_cnt;
    int allocated_pred_expr_cnt;
    int allocated_arith_cnt;
    int allocated_regu_var_list_cnt;
    int allocated_function_node_cnt;
    int allocated_sp_cnt;
    std::map<heap_cache_attrinfo *, heap_cache_attrinfo *> attr_cache_map;
    std::map<REGU_VARIABLE *, REGU_VARIABLE *> regu_var_map;
    std::map<DB_VALUE *, DB_VALUE *> db_value_map;
    std::map<ARITH_TYPE *, ARITH_TYPE *> arith_type_map;
    std::map<PRED_EXPR *, PRED_EXPR *> pred_expr_map;
    std::map<struct regu_variable_list_node *, struct regu_variable_list_node *> regu_var_list_map;
    std::map<struct function_node *, struct function_node *> function_node_map;
    std::map<SP_TYPE *, SP_TYPE *> sp_map;
    val_descr *mapped_vd;


    void clear_and_free (REGU_VARIABLE *regu_var);
    void clear_and_free (PRED_EXPR *pred);
    void clear_and_free (struct regu_variable_list_node *regu_list);
    void clear_and_free (DB_VALUE *db_value);
    void clear_and_free (ARITH_TYPE *arith_list);
    void clear_and_free (heap_cache_attrinfo *attr_cache);
    void clear_and_free (struct function_node *func);
    void clear_and_free (SP_TYPE *sp);

    void *copy_and_map_pred_expr (THREAD_ENTRY *thread_p, void *src, void *dest, enum ph_pred_expr_types type);
};

void xasl_memory_mapper::clear_and_free (SP_TYPE *sp)
{
  memset (sp, 0, sizeof (SP_TYPE));
#if ALLOC_FREE_LOG
  er_log_debug (ARG_FILE_LINE, "free : sp %p\n", sp);
#endif
  HP_FREE (NULL, sp);
  allocated_sp_cnt--;
}

void xasl_memory_mapper::clear_and_free (struct function_node *func)
{
  memset (func, 0, sizeof (struct function_node));
#if ALLOC_FREE_LOG
  er_log_debug (ARG_FILE_LINE, "free : function_node %p\n", func);
#endif
  HP_FREE (NULL, func);
  allocated_function_node_cnt--;
}

void xasl_memory_mapper::clear_and_free ( REGU_VARIABLE *regu_var)
{
  memset (regu_var, 0, sizeof (REGU_VARIABLE));
#if ALLOC_FREE_LOG
  er_log_debug (ARG_FILE_LINE, "free : regu_var %p\n", regu_var);
#endif
  HP_FREE (NULL, regu_var);
  allocated_regu_var_cnt--;
}
void xasl_memory_mapper::clear_and_free (PRED_EXPR *pred)
{
  if (pred->type == T_EVAL_TERM && pred->pe.m_eval_term.et_type == T_RLIKE_EVAL_TERM)
    {
      RLIKE_EVAL_TERM *et_rlike = &pred->pe.m_eval_term.et.et_rlike;
      if (et_rlike->compiled_regex)
	{
	  delete et_rlike->compiled_regex;
	  et_rlike->compiled_regex = NULL;
	}
    }

  memset (pred, 0, sizeof (PRED_EXPR));
#if ALLOC_FREE_LOG
  er_log_debug (ARG_FILE_LINE, "free : pred %p\n", pred);
#endif
  HP_FREE (NULL, pred);
  allocated_pred_expr_cnt--;
}

void xasl_memory_mapper::clear_and_free ( struct regu_variable_list_node   *regu_list)
{
  memset (regu_list, 0, sizeof (struct regu_variable_list_node));
#if ALLOC_FREE_LOG
  er_log_debug (ARG_FILE_LINE, "free : regu_var_list %p\n", regu_list);
#endif
  HP_FREE (NULL, regu_list);
  allocated_regu_var_list_cnt--;
}
void xasl_memory_mapper::clear_and_free ( DB_VALUE *db_value)
{
  pr_clear_value (db_value);
  memset (db_value, 0, sizeof (DB_VALUE));
#if ALLOC_FREE_LOG
  er_log_debug (ARG_FILE_LINE, "free : db_value %p\n", db_value);
#endif
  HP_FREE (NULL, db_value);
  allocated_db_value_cnt--;
}
void xasl_memory_mapper::clear_and_free (ARITH_TYPE *arith_list)
{
  if (arith_list->rand_seed)
    {
      HP_FREE (NULL, arith_list->rand_seed);
      arith_list->rand_seed = NULL;
    }
  memset (arith_list, 0, sizeof (ARITH_TYPE));
#if ALLOC_FREE_LOG
  er_log_debug (ARG_FILE_LINE, "free : arith_list %p\n", arith_list);
#endif
  HP_FREE (NULL, arith_list);
  allocated_arith_cnt--;
}
void xasl_memory_mapper::clear_and_free (heap_cache_attrinfo *attr_cache)
{
  heap_attrinfo_end (NULL, attr_cache);
  memset (attr_cache, 0, sizeof (heap_cache_attrinfo));
  HP_FREE (NULL, attr_cache);
#if ALLOC_FREE_LOG
  er_log_debug (ARG_FILE_LINE, "free : attr_cache %p\n", attr_cache);
#endif
  allocated_attr_cache_cnt--;
}

void xasl_memory_mapper::clear_and_free (val_descr *orig_vd, val_descr *vd)
{
  if (!vd)
    {
      return;
    }
  if (vd->dbval_ptr && vd->dbval_cnt > 0)
    {
      for (int i = 0; i < vd->dbval_cnt; i++)
	{
	  pr_clear_value (&vd->dbval_ptr[i]);
	  db_value_map.erase (&orig_vd->dbval_ptr[i]);
	}
      HP_FREE (NULL, vd->dbval_ptr);
      allocated_db_value_cnt -= vd->dbval_cnt;
      vd->dbval_ptr = nullptr;
      vd->dbval_cnt = 0;
    }
  HP_FREE (NULL, vd);
  mapped_vd = nullptr;
}

xasl_memory_mapper::~xasl_memory_mapper()
{
  // DB_VALUE 메모리 해제
  for (auto &pair : db_value_map)
    {
      DB_VALUE *value = pair.second;  // second만 해제
      if (value != nullptr)
	{
	  clear_and_free (value);
	}
    }
  db_value_map.clear();

  // heap_cache_attrinfo 메모리 해제
  for (auto &pair : attr_cache_map)
    {
      heap_cache_attrinfo *cache = pair.second;  // second만 해제 (first는 원본)
      if (cache != nullptr)
	{
	  clear_and_free (cache);
	}
    }
  attr_cache_map.clear();

  // REGU_VARIABLE 메모리 해제
  for (auto &pair : regu_var_map)
    {
      REGU_VARIABLE *var = pair.second;  // second만 해제
      if (var != nullptr)
	{
	  clear_and_free (var);
	}
    }
  regu_var_map.clear();

  // ARITH_TYPE 메모리 해제
  for (auto &pair : arith_type_map)
    {
      ARITH_TYPE *arith = pair.second;  // second만 해제
      if (arith != nullptr)
	{
	  clear_and_free (arith);
	}
    }
  arith_type_map.clear();

  for (auto &pair : pred_expr_map)
    {
      PRED_EXPR *pred = pair.second;  // second만 해제
      if (pred != nullptr)
	{
	  clear_and_free (pred);
	}
    }
  pred_expr_map.clear();

  for (auto &pair : regu_var_list_map)
    {
      struct regu_variable_list_node *list = pair.second;   // second만 해제
      if (list != nullptr)
	{
	  clear_and_free (list);
	}
    }
  regu_var_list_map.clear();

  for (auto &pair : function_node_map)
    {
      struct function_node *func = pair.second;  // second만 해제
      if (func != nullptr)
	{
	  clear_and_free (func);
	}
    }
  function_node_map.clear();

  for (auto &pair : sp_map)
    {
      SP_TYPE *sp = pair.second;  // second만 해제
      if (sp != nullptr)
	{
	  clear_and_free (sp);
	}
    }
  sp_map.clear();

  assert (allocated_attr_cache_cnt == 0);
  assert (allocated_regu_var_cnt == 0);
  assert (allocated_db_value_cnt == 0);
  assert (allocated_pred_expr_cnt == 0);
  assert (allocated_arith_cnt == 0);
  assert (allocated_regu_var_list_cnt == 0);
  assert (allocated_function_node_cnt == 0);
  assert (allocated_sp_cnt == 0);
}

val_descr *xasl_memory_mapper::copy_and_map (THREAD_ENTRY *thread_p, val_descr *vd)
{
  val_descr *dest = nullptr;
  if (!vd)
    {
      return nullptr;
    }

  dest = (val_descr *) HP_ALLOC (thread_p, sizeof (val_descr));
  memcpy (dest, vd, sizeof (val_descr));
  if (vd->dbval_ptr && vd->dbval_cnt > 0)
    {
      dest->dbval_ptr = (DB_VALUE *) HP_ALLOC (thread_p, sizeof (DB_VALUE) * vd->dbval_cnt);
      memset (dest->dbval_ptr, 0, sizeof (DB_VALUE) * vd->dbval_cnt);
      for (int i = 0; i < vd->dbval_cnt; i++)
	{
	  pr_clone_value (&vd->dbval_ptr[i], &dest->dbval_ptr[i]);
	  db_value_map[&vd->dbval_ptr[i]] = &dest->dbval_ptr[i];
	  allocated_db_value_cnt++;
	}
    }

  mapped_vd = dest;
  return mapped_vd;
}

struct function_node *xasl_memory_mapper::copy_and_map (THREAD_ENTRY *thread_p, struct function_node *func)
{
  struct function_node *dest = nullptr;
  if (!func)
    {
      return NULL;
    }
  auto it = xasl_memory_mapper::function_node_map.find (func);
  if (it != xasl_memory_mapper::function_node_map.end())
    {
      dest = it->second;
    }
  else
    {
      dest = (struct function_node *) HP_ALLOC (thread_p, sizeof (struct function_node));
#if ALLOC_FREE_LOG
      er_log_debug (ARG_FILE_LINE, "allocated : function_node %p, size %zu\n", dest, sizeof (struct function_node));
#endif
      *dest = *func;
      dest->value = copy_and_map (thread_p, func->value);
      dest->operand = copy_and_map (thread_p, func->operand);
      function_node_map[func] = dest;
      allocated_function_node_cnt++;
    }
  return dest;
}

REGU_VARIABLE *xasl_memory_mapper::copy_and_map (THREAD_ENTRY *thread_p, REGU_VARIABLE *regu_var)
{
  REGU_VARIABLE *src = regu_var, *dest = nullptr;
  if (!regu_var)
    {
      return NULL;
    }
  auto it = xasl_memory_mapper::regu_var_map.find (src);
  if (it != xasl_memory_mapper::regu_var_map.end())
    {
      dest = it->second;
    }
  else
    {
      dest = (REGU_VARIABLE *) HP_ALLOC (thread_p, sizeof (REGU_VARIABLE));
#if ALLOC_FREE_LOG
      er_log_debug (ARG_FILE_LINE, "allocated : regu_var %p, size %zu\n", dest, sizeof (REGU_VARIABLE));
#endif
      *dest = *src;
      regu_var_map[src] = dest;
      allocated_regu_var_cnt++;
      switch (src->type)
	{
	case TYPE_ATTR_ID:		/* fetch object attribute value */
	case TYPE_SHARED_ATTR_ID:
	case TYPE_CLASS_ATTR_ID:
	  dest->value.attr_descr.cache_dbvalp = NULL;
	  dest->value.attr_descr.cache_attrinfo = copy_and_map (thread_p, src->value.attr_descr.cache_attrinfo);
	  break;
	case TYPE_CONSTANT:
	  dest->value.dbvalptr = copy_and_map (thread_p, src->value.dbvalptr);
	  break;
	case TYPE_INARITH:
	case TYPE_OUTARITH:
	  dest->value.arithptr = copy_and_map (thread_p, src->value.arithptr);
	  break;
	case TYPE_SP:
	  dest->value.sp_ptr = copy_and_map (thread_p, src->value.sp_ptr);
	  break;
	case TYPE_FUNC:
	  dest->value.funcp = copy_and_map (thread_p, src->value.funcp);
	  break;
	case TYPE_DBVAL:
	  pr_clone_value (&src->value.dbval, &dest->value.dbval);
	  break;
	case TYPE_REGUVAL_LIST:
	  assert (false);
	  break;
	case TYPE_REGU_VAR_LIST:
	  dest->value.regu_var_list = copy_and_map (thread_p, src->value.regu_var_list);
	  break;
	default:
	  break;
	}

      if (src->vfetch_to != NULL)
	{
	  dest->vfetch_to = copy_and_map (thread_p, src->vfetch_to);
	}
    }
  return dest;
}

PRED_EXPR *xasl_memory_mapper::copy_and_map (THREAD_ENTRY *thread_p, PRED_EXPR *src)
{
  PRED_EXPR *dest = nullptr;
  dest = (PRED_EXPR *) copy_and_map_pred_expr (thread_p, src, dest, PH_PRED_EXPR);
  return dest;
}

void *xasl_memory_mapper::copy_and_map_pred_expr (THREAD_ENTRY *thread_p, void *src, void *dest,
    enum ph_pred_expr_types type)
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
    {
      src_pred_expr = (PRED_EXPR *) src;
      auto it = xasl_memory_mapper::pred_expr_map.find (src_pred_expr);
      if (it != xasl_memory_mapper::pred_expr_map.end())
	{
	  dest = (void *) it->second;
	  return dest;
	}
      else
	{
	  dest_pred_expr = (PRED_EXPR *) HP_ALLOC (thread_p, sizeof (PRED_EXPR));
#if ALLOC_FREE_LOG
	  er_log_debug (ARG_FILE_LINE, "allocated : pred %p, size %zu\n", dest_pred_expr, sizeof (PRED_EXPR));
#endif
	  *dest_pred_expr = *src_pred_expr;
	  pred_expr_map[src_pred_expr] = dest_pred_expr;
	  allocated_pred_expr_cnt++;
	  switch (src_pred_expr->type)
	    {
	    case T_PRED:
	      copy_and_map_pred_expr (thread_p, (void *)&src_pred_expr->pe.m_pred, (void *)&dest_pred_expr->pe.m_pred, PH_PRED);
	      break;
	    case T_EVAL_TERM:
	      copy_and_map_pred_expr (thread_p, (void *)&src_pred_expr->pe.m_eval_term, (void *)&dest_pred_expr->pe.m_eval_term,
				      PH_EVAL_TERM);
	      break;
	    case T_NOT_TERM:
	      dest_pred_expr->pe.m_not_term = (PRED_EXPR *) copy_and_map_pred_expr (thread_p, (void *)src_pred_expr->pe.m_not_term,
					      (void *)dest_pred_expr->pe.m_not_term, PH_PRED_EXPR);
	      break;
	    default:
	      assert (false);
	      break;
	    }
	  ret = (void *) dest_pred_expr;
	}
    }

    break;
    case PH_PRED:
      src_pred = (PRED *) src;
      dest_pred = (PRED *) dest;
      *dest_pred = *src_pred;
      dest_pred->lhs = (PRED_EXPR *) copy_and_map_pred_expr (thread_p, (void *)src_pred->lhs, (void *)dest_pred->lhs,
		       PH_PRED_EXPR);
      dest_pred->rhs = (PRED_EXPR *) copy_and_map_pred_expr (thread_p, (void *)src_pred->rhs, (void *)dest_pred->rhs,
		       PH_PRED_EXPR);
      break;
    case PH_EVAL_TERM:
      src_eval_term = (EVAL_TERM *) src;
      dest_eval_term = (EVAL_TERM *) dest;
      switch (src_eval_term->et_type)
	{
	case T_COMP_EVAL_TERM:
	  copy_and_map_pred_expr (thread_p, (void *)&src_eval_term->et.et_comp, (void *)&dest_eval_term->et.et_comp,
				  PH_COMP_EVAL_TERM);
	  break;
	case T_ALSM_EVAL_TERM:
	  copy_and_map_pred_expr (thread_p, (void *)&src_eval_term->et.et_alsm, (void *)&dest_eval_term->et.et_alsm,
				  PH_ALSM_EVAL_TERM);
	  break;
	case T_LIKE_EVAL_TERM:
	  copy_and_map_pred_expr (thread_p, (void *)&src_eval_term->et.et_like, (void *)&dest_eval_term->et.et_like,
				  PH_LIKE_EVAL_TERM);
	  break;
	case T_RLIKE_EVAL_TERM:
	  copy_and_map_pred_expr (thread_p, (void *)&src_eval_term->et.et_rlike, (void *)&dest_eval_term->et.et_rlike,
				  PH_RLIKE_EVAL_TERM);
	  break;
	}
      break;
    case PH_COMP_EVAL_TERM:
      src_comp_eval_term = (COMP_EVAL_TERM *) src;
      dest_comp_eval_term = (COMP_EVAL_TERM *) dest;
      dest_comp_eval_term->lhs = copy_and_map (thread_p, src_comp_eval_term->lhs);
      dest_comp_eval_term->rhs = copy_and_map (thread_p, src_comp_eval_term->rhs);
      break;
    case PH_ALSM_EVAL_TERM:
      src_alsm_eval_term = (ALSM_EVAL_TERM *) src;
      dest_alsm_eval_term = (ALSM_EVAL_TERM *) dest;
      dest_alsm_eval_term->elem = copy_and_map (thread_p, src_alsm_eval_term->elem);
      dest_alsm_eval_term->elemset = copy_and_map (thread_p, src_alsm_eval_term->elemset);
      break;
    case PH_LIKE_EVAL_TERM:
      src_like_eval_term = (LIKE_EVAL_TERM *) src;
      dest_like_eval_term = (LIKE_EVAL_TERM *) dest;
      dest_like_eval_term->src = copy_and_map (thread_p, src_like_eval_term->src);
      dest_like_eval_term->pattern = copy_and_map (thread_p, src_like_eval_term->pattern);
      dest_like_eval_term->esc_char = copy_and_map (thread_p, src_like_eval_term->esc_char);
      break;
    case PH_RLIKE_EVAL_TERM:
      src_rlike_eval_term = (RLIKE_EVAL_TERM *) src;
      dest_rlike_eval_term = (RLIKE_EVAL_TERM *) dest;
      dest_rlike_eval_term->src = copy_and_map (thread_p, src_rlike_eval_term->src);
      dest_rlike_eval_term->pattern = copy_and_map (thread_p, src_rlike_eval_term->pattern);
      dest_rlike_eval_term->case_sensitive = copy_and_map (thread_p, src_rlike_eval_term->case_sensitive);
      break;
    case PH_REGU_VAR:
      src_regu_var = (regu_variable_node *) src;
      dest_regu_var = copy_and_map (thread_p, src_regu_var);
      ret = (void *) dest_regu_var;
      break;
    default:
      assert (false);
      break;
    }
  return ret;
}

struct regu_variable_list_node *xasl_memory_mapper::copy_and_map (THREAD_ENTRY *thread_p,
    struct regu_variable_list_node *src_list)
{
  struct regu_variable_list_node *dest_list = nullptr;
  if (!src_list)
    {
      return nullptr;
    }

  auto it = xasl_memory_mapper::regu_var_list_map.find (src_list);
  if (it != xasl_memory_mapper::regu_var_list_map.end())
    {
      dest_list = it->second;
    }
  else
    {
      REGU_VARIABLE *src = &src_list->value;
      dest_list = (struct regu_variable_list_node *) HP_ALLOC (thread_p, sizeof (struct regu_variable_list_node));
#if ALLOC_FREE_LOG
      er_log_debug (ARG_FILE_LINE, "allocated : regu_var_list %p, size %zu\n", dest_list,
		    sizeof (struct regu_variable_list_node));
#endif
      *dest_list = *src_list;
      regu_var_list_map[src_list] = dest_list;
      allocated_regu_var_list_cnt++;
      REGU_VARIABLE *dest = &dest_list->value;
      switch (src->type)
	{
	case TYPE_ATTR_ID:		/* fetch object attribute value */
	case TYPE_SHARED_ATTR_ID:
	case TYPE_CLASS_ATTR_ID:
	  dest->value.attr_descr.cache_dbvalp = NULL;
	  dest->value.attr_descr.cache_attrinfo = copy_and_map (thread_p, src->value.attr_descr.cache_attrinfo);
	  break;
	case TYPE_CONSTANT:
	  dest->value.dbvalptr = copy_and_map (thread_p, src->value.dbvalptr);
	  break;
	case TYPE_INARITH:
	case TYPE_OUTARITH:
	  dest->value.arithptr = copy_and_map (thread_p, src->value.arithptr);
	  break;
	case TYPE_SP:
	  dest->value.sp_ptr = copy_and_map (thread_p, src->value.sp_ptr);
	  break;
	case TYPE_FUNC:
	  dest->value.funcp = copy_and_map (thread_p, src->value.funcp);
	  break;
	case TYPE_DBVAL:
	  pr_clone_value (&src->value.dbval, &dest->value.dbval);
	  break;
	case TYPE_REGUVAL_LIST:
	  assert (false);
	  break;
	case TYPE_REGU_VAR_LIST:
	  dest->value.regu_var_list = copy_and_map (thread_p, src->value.regu_var_list);
	  break;
	default:
	  break;
	}

      if (src->vfetch_to != NULL)
	{
	  dest->vfetch_to = copy_and_map (thread_p, src->vfetch_to);
	}
      if (src_list->next)
	{
	  dest_list->next = copy_and_map (thread_p, src_list->next);
	}
      else
	{
	  dest_list->next = NULL;
	}
    }
  return dest_list;
}

DB_VALUE *xasl_memory_mapper::copy_and_map (THREAD_ENTRY *thread_p, DB_VALUE *db_value)
{
  DB_VALUE *dest = nullptr;
  if (!db_value)
    {
      return NULL;
    }
  DB_TYPE src_dbtype;
  auto it = db_value_map.find (db_value);
  if (it != db_value_map.end())
    {
      dest = it->second;
    }
  else
    {
      dest = (DB_VALUE *) HP_ALLOC (thread_p, sizeof (DB_VALUE));
#if ALLOC_FREE_LOG
      er_log_debug (ARG_FILE_LINE, "allocated : db_value %p, size %zu\n", dest, sizeof (DB_VALUE));
#endif
      pr_clone_value (db_value, dest);
      db_value_map[db_value] = dest;
      allocated_db_value_cnt++;
    }
  return dest;
}
ARITH_TYPE *xasl_memory_mapper::copy_and_map (THREAD_ENTRY *thread_p, ARITH_TYPE *src)
{
  ARITH_TYPE *dest = nullptr;
  if (!src)
    {
      return nullptr;
    }
  auto it = arith_type_map.find (src);
  if (it != arith_type_map.end())
    {
      dest = it->second;
    }
  else
    {
      dest = (ARITH_TYPE *) HP_ALLOC (thread_p, sizeof (ARITH_TYPE));
#if ALLOC_FREE_LOG
      er_log_debug (ARG_FILE_LINE, "allocated : arith_type %p, size %zu\n", dest, sizeof (ARITH_TYPE));
#endif
      *dest = *src;
      dest->value = copy_and_map (thread_p, src->value);
      dest->leftptr = copy_and_map (thread_p, src->leftptr);
      dest->rightptr = copy_and_map (thread_p, src->rightptr);
      dest->thirdptr = copy_and_map (thread_p, src->thirdptr);
      dest->pred = copy_and_map (thread_p, src->pred);
      arith_type_map[src] = dest;
      allocated_arith_cnt++;

      if (src->rand_seed)
	{
	  dest->rand_seed = (struct drand48_data *) HP_ALLOC (thread_p, sizeof (struct drand48_data));
	  if (dest->rand_seed)
	    {
	      *dest->rand_seed = *src->rand_seed;
	    }
	}
    }
  return dest;
}

SP_TYPE *xasl_memory_mapper::copy_and_map (THREAD_ENTRY *thread_p, SP_TYPE *sp)
{
  SP_TYPE *dest = nullptr;
  if (!sp)
    {
      return nullptr;
    }
  auto it = sp_map.find (sp);
  if (it != sp_map.end())
    {
      dest = it->second;
    }
  else
    {
      dest = (SP_TYPE *) HP_ALLOC (thread_p, sizeof (SP_TYPE));
#if ALLOC_FREE_LOG
      er_log_debug (ARG_FILE_LINE, "allocated : sp %p, size %zu\n", dest, sizeof (SP_TYPE));
#endif
      *dest = *sp;
      dest->args = copy_and_map (thread_p, sp->args);
      dest->value = copy_and_map (thread_p, sp->value);
      sp_map[sp] = dest;
      allocated_sp_cnt++;
    }
  return dest;
}

heap_cache_attrinfo *xasl_memory_mapper::copy_and_map (THREAD_ENTRY *thread_p, heap_cache_attrinfo *attr_cache)
{
  heap_cache_attrinfo *dest = nullptr;
  if (!attr_cache)
    {
      return NULL;
    }
  auto it = attr_cache_map.find (attr_cache);
  if (it != attr_cache_map.end())
    {
      dest = it->second;
    }
  else
    {
      dest = (heap_cache_attrinfo *) HP_ALLOC (thread_p, sizeof (heap_cache_attrinfo));
#if ALLOC_FREE_LOG
      er_log_debug (ARG_FILE_LINE, "allocated : heap_cache_attrinfo %p, size %zu\n", dest, sizeof (heap_cache_attrinfo));
#endif
      memset (dest, 0, sizeof (heap_cache_attrinfo));
      attr_cache_map[attr_cache] = dest;
      allocated_attr_cache_cnt++;
    }
  return dest;
}

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

    void capture_pred_regu_var_list (struct regu_variable_list_node   *list);
    void capture_rest_regu_var_list (struct regu_variable_list_node   *list);
    void copy_to_pred_regu_var_list (struct regu_variable_list_node   *list);
    void copy_to_rest_regu_var_list (struct regu_variable_list_node   *list);
    void init (THREAD_ENTRY *thread_p, int n_pred_val, int n_rest_val);
    void clear ();

    ~parallel_heap_scan_result_queue_entry();
    parallel_heap_scan_result_queue_entry();

  private:
    void capture_regu_var_list (struct regu_variable_list_node   *list, DB_VALUE_ARRAY *dbvalue_array);
    void copy_to_regu_var_list (DB_VALUE_ARRAY *dbvalue_array, struct regu_variable_list_node   *list);
};

parallel_heap_scan_result_queue_entry::parallel_heap_scan_result_queue_entry()
{
  pred_val_array.size = 0;
  pred_val_array.vals = NULL;
  rest_val_array.size = 0;
  rest_val_array.vals = NULL;
  valid = false;
  scan_code = S_END;
  curr_oid = {0,0,0};
}

parallel_heap_scan_result_queue_entry::~parallel_heap_scan_result_queue_entry()
{
  // 먼저 값들을 정리
  if (pred_val_array.vals != nullptr)
    {
      for (int i = 0; i < pred_val_array.size; i++)
	{
	  pr_clear_value (&pred_val_array.vals[i]);
	}
      HP_FREE (NULL, pred_val_array.vals);
#if ALLOC_FREE_LOG
      er_log_debug (ARG_FILE_LINE, "free : %p\n", pred_val_array.vals);
#endif
      pred_val_array.vals = nullptr;
    }

  if (rest_val_array.vals != nullptr)
    {
      for (int i = 0; i < rest_val_array.size; i++)
	{
	  pr_clear_value (&rest_val_array.vals[i]);
	}
      HP_FREE (NULL, rest_val_array.vals);
#if ALLOC_FREE_LOG
      er_log_debug (ARG_FILE_LINE, "free : %p\n", rest_val_array.vals);
#endif
      rest_val_array.vals = nullptr;
    }
}

void parallel_heap_scan_result_queue_entry::capture_regu_var_list (struct regu_variable_list_node   *list,
    DB_VALUE_ARRAY *dbvalue_array)
{
  if (dbvalue_array == nullptr || dbvalue_array->vals == nullptr)
    {
      return;
    }

  struct regu_variable_list_node   *iter = list;
  for (int i = 0; i < dbvalue_array->size && iter != nullptr; i++)
    {
      if (iter->value.vfetch_to != nullptr)
	{
	  db_value_clone (iter->value.vfetch_to, &dbvalue_array->vals[i]);
	}
      iter = iter->next;
    }
  return;
}

void parallel_heap_scan_result_queue_entry::copy_to_regu_var_list (DB_VALUE_ARRAY *dbvalue_array,
    struct regu_variable_list_node   *list)
{
  if (dbvalue_array == nullptr || dbvalue_array->vals == nullptr || list == nullptr)
    {
      return;
    }

  struct regu_variable_list_node   *iter = list;
  for (int i = 0; i < dbvalue_array->size && iter != nullptr; i++)
    {
      if (iter->value.vfetch_to != nullptr)
	{
	  if (!DB_IS_NULL (iter->value.vfetch_to))
	    {
	      pr_clear_value (iter->value.vfetch_to);
	    }
	  pr_clone_value (&dbvalue_array->vals[i], iter->value.vfetch_to);
	}
      iter = iter->next;
    }
  return;
}

void parallel_heap_scan_result_queue_entry::capture_pred_regu_var_list (struct regu_variable_list_node   *list)
{
  capture_regu_var_list (list, &pred_val_array);
}

void parallel_heap_scan_result_queue_entry::capture_rest_regu_var_list (struct regu_variable_list_node   *list)
{
  capture_regu_var_list (list, &rest_val_array);
}

void parallel_heap_scan_result_queue_entry::copy_to_pred_regu_var_list (struct regu_variable_list_node   *list)
{
  copy_to_regu_var_list (&pred_val_array, list);
}

void parallel_heap_scan_result_queue_entry::copy_to_rest_regu_var_list (struct regu_variable_list_node   *list)
{
  copy_to_regu_var_list (&rest_val_array, list);
}

void parallel_heap_scan_result_queue_entry::init (THREAD_ENTRY *thread_p, int n_pred_val, int n_rest_val)
{
  pred_val_array.size = n_pred_val;
  rest_val_array.size = n_rest_val;

  // 초기화
  pred_val_array.vals = nullptr;
  rest_val_array.vals = nullptr;

  if (n_pred_val > 0)
    {
      pred_val_array.vals = (DB_VALUE *)HP_ALLOC (thread_p, n_pred_val * sizeof (DB_VALUE));
      if (pred_val_array.vals != nullptr)
	{
#if ALLOC_FREE_LOG
	  er_log_debug (ARG_FILE_LINE, "allocated : %p\n", pred_val_array.vals);
#endif
	  // 새로 할당된 메모리 초기화
	  memset (pred_val_array.vals, 0, n_pred_val * sizeof (DB_VALUE));
	}
    }

  if (n_rest_val > 0)
    {
      rest_val_array.vals = (DB_VALUE *)HP_ALLOC (thread_p, n_rest_val * sizeof (DB_VALUE));
      if (rest_val_array.vals != nullptr)
	{
#if ALLOC_FREE_LOG
	  er_log_debug (ARG_FILE_LINE, "allocated : %p\n", rest_val_array.vals);
#endif
	  // 새로 할당된 메모리 초기화
	  memset (rest_val_array.vals, 0, n_rest_val * sizeof (DB_VALUE));
	}
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
      pr_clear_value (&pred_val_array.vals[i]);
    }
  for (i = 0; i < rest_val_array.size; i++)
    {
      pr_clear_value (&rest_val_array.vals[i]);
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
      if (is_scan_ended)
	{
	  return -1;
	}
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
    std::atomic<bool> m_has_error;
    std::atomic<cuberr::er_message *> m_error_msg;
    SCAN_ID *m_scan_id;
    int m_orig_tran_index;
    THREAD_ENTRY *m_orig_thread_p;
    struct regu_variable_list_node   *orig_pred_list;
    struct regu_variable_list_node   *orig_rest_list;
    class locked_vpid
    {
      public:
	VPID vpid;
	bool is_ended;
	std::mutex mutex;
    } m_locked_vpid;

    parallel_heap_scan_context (SCAN_ID *scan_id, int tran_index, int parallelism);
    ~parallel_heap_scan_context();
    void set_regu_vars (struct regu_variable_list_node   *pred_list, struct regu_variable_list_node   *rest_list);
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
  m_has_error = false;
  m_error_msg = nullptr;
  scan_ended_queue_count = 0;
}

parallel_heap_scan_context::~parallel_heap_scan_context()
{
  delete[] m_result_queue;
  if (m_error_msg)
    {
      delete m_error_msg;
      m_error_msg = nullptr;
    }
}

void parallel_heap_scan_context::set_regu_vars (struct regu_variable_list_node   *pred_list,
    struct regu_variable_list_node   *rest_list)
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
  int orig_tran_index = thread_p->tran_index;
  css_conn_entry *orig_conn_entry = thread_p->conn_entry;
  VPID vpid;
  HFID hfid;
  if (m_context->m_has_error)
    {
      m_context->m_tasks_executed++;
#if START_END_LOG
      er_log_debug (ARG_FILE_LINE, "parallel heap scan thread %ld finished caused by error", syscall (SYS_gettid));
#endif
      return;
    }
#if !HP_USE_PRIVATE_ALLOC
  HL_HEAPID orig_heap_id = db_change_private_heap (thread_p, 0);
#endif
  xasl_memory_mapper *mapper = new xasl_memory_mapper();
  scan_id = (SCAN_ID *) HP_ALLOC (thread_p, sizeof (SCAN_ID));
#if ALLOC_FREE_LOG
  er_log_debug (ARG_FILE_LINE, "allocated : %p\n", scan_id);
#endif
  HEAP_SCAN_ID *hsidp = &scan_id->s.hsid;
  thread_p->tran_index = tran_index;
  thread_p->conn_entry = m_context->m_orig_thread_p->conn_entry;
#if START_END_LOG
  er_log_debug (ARG_FILE_LINE, "parallel heap scan thread %ld started.", syscall (SYS_gettid));
#endif
  scan_open_heap_scan (thread_p, scan_id, orig_scan_id->mvcc_select_lock_needed, orig_scan_id->scan_op_type,
		       orig_scan_id->fixed, orig_scan_id->grouped, orig_scan_id->single_fetch, orig_scan_id->join_dbval,
		       orig_scan_id->val_list, orig_scan_id->vd, &phsidp->cls_oid, &phsidp->hfid,
		       phsidp->scan_pred.regu_list, phsidp->scan_pred.pred_expr, phsidp->rest_regu_list,
		       phsidp->pred_attrs.num_attrs, phsidp->pred_attrs.attr_ids, phsidp->pred_attrs.attr_cache,
		       phsidp->rest_attrs.num_attrs, phsidp->rest_attrs.attr_ids, phsidp->rest_attrs.attr_cache,
		       S_HEAP_SCAN, phsidp->cache_recordinfo, phsidp->recordinfo_regu_list, false);
  hsidp->pred_attrs.attr_cache = mapper->copy_and_map (thread_p, phsidp->pred_attrs.attr_cache);
  hsidp->rest_attrs.attr_cache = mapper->copy_and_map (thread_p, phsidp->rest_attrs.attr_cache);
  hsidp->scan_pred.regu_list = mapper->copy_and_map (thread_p, phsidp->scan_pred.regu_list);
  hsidp->rest_regu_list = mapper->copy_and_map (thread_p, phsidp->rest_regu_list);
  hsidp->scan_pred.pred_expr = mapper->copy_and_map (thread_p, phsidp->scan_pred.pred_expr);
  scan_id->vd = mapper->copy_and_map (thread_p, orig_scan_id->vd);
  hsidp->caches_inited = false;
  ret = scan_start_scan (thread_p, scan_id);
  /* phsidp->scan_pred.pred_expr, phsidp->pred_attrs.attr_cache phsidp->rest_attrs.attr_cache 를 독립적으로 운용해야함 */

  hfid = phsidp->hfid;
  OID_SET_NULL (&hsidp->curr_oid);

  while (TRUE)
    {
      if (m_context->m_has_error || m_result_queue->is_scan_ended)
	{
	  break;
	}
      page_scan_code = page_next (thread_p, &hfid, &vpid);

      if (page_scan_code == S_END)
	{
	  m_result_queue->enqueue (hsidp, page_scan_code);
	  break;
	}

      while (TRUE)
	{
	  if (m_context->m_has_error || m_result_queue->is_scan_ended)
	    {
	      break;
	    }
	  rec_scan_code = scan_next_heap_scan_1page_internal (thread_p, scan_id, &vpid);
	  if (rec_scan_code == S_ERROR)
	    {
	      if (m_context->m_has_error || m_result_queue->is_scan_ended)
		{
		  break;
		}
	      m_context->m_has_error = true;
	      cuberr::er_message *error_msg = new cuberr::er_message (false);
	      cuberr::er_message &crt_error = cuberr::context::get_thread_local_context ().get_current_error_level ();
	      error_msg->swap (crt_error);
	      m_context->m_error_msg.exchange (error_msg);
	      break;
	    }
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
  mapper->clear_and_free (orig_scan_id->vd, scan_id->vd);
  delete mapper;
  HP_FREE (thread_p, scan_id);
#if ALLOC_FREE_LOG
  er_log_debug (ARG_FILE_LINE, "free : %p\n", scan_id);
#endif
  scan_id = NULL;
#if !HP_USE_PRIVATE_ALLOC
  db_change_private_heap (thread_p, orig_heap_id);
#endif
  m_context->m_tasks_executed++;
  thread_p->tran_index = orig_tran_index;
  thread_p->conn_entry = orig_conn_entry;
#if START_END_LOG
  er_log_debug (ARG_FILE_LINE, "parallel heap scan thread %ld finished.", syscall (SYS_gettid));
#endif
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
  delete m_context;
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
      if (m_context->m_has_error)
	{
	  break;
	}
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
  m_context->m_has_error = false;
  m_context->m_error_msg = nullptr;
  m_context->m_orig_thread_p = thread_p;
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
  assert (m_context->m_has_error == false);
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
      m_is_start_once = false;
      m_is_reset_once = false;
      m_context->m_has_error = false;
      m_context->m_error_msg = nullptr;
      return;
    }
  for (int i = 0; i < parallelism; i++)
    {
      m_context->m_result_queue[i].is_scan_ended = true;
      m_context->scan_ended_queue_count++;
      std::unique_lock<std::mutex> lock (m_context->m_result_queue[i].var.mutex);
      bool is_waiting = m_context->m_result_queue[i].var.waiting;
      lock.unlock();
      if (is_waiting)
	{
	  m_context->m_result_queue[i].var.cond.notify_one();
	}
    }
  while (m_context->m_tasks_executed < m_context->m_tasks_started)
    {
      thread_sleep (10);
      for (int i = 0; i < parallelism; i++)
	{
	  m_context->m_result_queue[i].is_scan_ended = true;
	  m_context->scan_ended_queue_count++;
	  std::unique_lock<std::mutex> lock (m_context->m_result_queue[i].var.mutex);
	  bool is_waiting = m_context->m_result_queue[i].var.waiting;
	  lock.unlock();
	  if (is_waiting)
	    {
	      m_context->m_result_queue[i].var.cond.notify_one();
	    }
	}
    }
  m_is_start_once = false;
  m_is_reset_once = false;
}

/*************************************************************************************************/
/* public functions */

static int regu_var_list_len (struct regu_variable_list_node   *list)
{
  int len = 0;
  for (struct regu_variable_list_node   *iter = list; iter; iter = iter->next)
    {
      len++;
    }
  return len;
}

int pred_clear (THREAD_ENTRY *thread_p, PRED_EXPR *pr)
{
  int pg_cnt;
  PRED_EXPR *expr;

  pg_cnt = 0;

  if (pr == NULL)
    {
      return pg_cnt;
    }

  switch (pr->type)
    {
    case T_PRED:
      pg_cnt += pred_clear (thread_p, pr->pe.m_pred.lhs);
      for (expr = pr->pe.m_pred.rhs; expr && expr->type == T_PRED; expr = expr->pe.m_pred.rhs)
	{
	  pg_cnt += pred_clear (thread_p, expr->pe.m_pred.lhs);
	}
      pg_cnt += pred_clear (thread_p, expr);
      break;
    case T_EVAL_TERM:
      switch (pr->pe.m_eval_term.et_type)
	{
	case T_COMP_EVAL_TERM:
	{
	  COMP_EVAL_TERM *et_comp = &pr->pe.m_eval_term.et.et_comp;

	  pg_cnt += regu_var_clear (thread_p, et_comp->lhs);
	  pg_cnt += regu_var_clear (thread_p, et_comp->rhs);
	}
	break;
	case T_ALSM_EVAL_TERM:
	{
	  ALSM_EVAL_TERM *et_alsm = &pr->pe.m_eval_term.et.et_alsm;

	  pg_cnt += regu_var_clear (thread_p, et_alsm->elem);
	  pg_cnt += regu_var_clear (thread_p, et_alsm->elemset);
	}
	break;
	case T_LIKE_EVAL_TERM:
	{
	  LIKE_EVAL_TERM *et_like = &pr->pe.m_eval_term.et.et_like;

	  pg_cnt += regu_var_clear (thread_p, et_like->src);
	  pg_cnt += regu_var_clear (thread_p, et_like->pattern);
	  pg_cnt += regu_var_clear (thread_p, et_like->esc_char);
	}
	break;
	case T_RLIKE_EVAL_TERM:
	{
	  RLIKE_EVAL_TERM *et_rlike = &pr->pe.m_eval_term.et.et_rlike;

	  pg_cnt += regu_var_clear (thread_p, et_rlike->src);
	  pg_cnt += regu_var_clear (thread_p, et_rlike->pattern);
	  pg_cnt += regu_var_clear (thread_p, et_rlike->case_sensitive);

	  /* free memory of compiled regex */
	  if (et_rlike->compiled_regex)
	    {
	      delete et_rlike->compiled_regex;
	      et_rlike->compiled_regex = NULL;
	    }
	}
	break;
	}
      break;
    case T_NOT_TERM:
      pg_cnt += pred_clear (thread_p, pr->pe.m_not_term);
      break;
    }

  return pg_cnt;
}

int arith_list_clear (THREAD_ENTRY *thread_p, ARITH_TYPE *list)
{
  int pg_cnt = 0;

  if (list == NULL)
    {
      return NO_ERROR;
    }

  /* restore the original domain, in order to avoid coerce when the XASL clones will be used again */
  list->domain = list->original_domain;
  //pr_clear_value (list->value);
  //pg_cnt += regu_var_clear (thread_p, list->leftptr);
  //pg_cnt += regu_var_clear (thread_p, list->rightptr);
  //pg_cnt += regu_var_clear (thread_p, list->thirdptr);
  //pg_cnt += pred_clear (thread_p, list->pred);

  if (list->rand_seed != NULL)
    {
      free_and_init (list->rand_seed);
    }

  return pg_cnt;
}

void pos_desc_clear (THREAD_ENTRY *thread_p, QFILE_TUPLE_VALUE_POSITION *pos_desc)
{
  pos_desc->dom = pos_desc->original_domain;
}

int regu_list_clear (THREAD_ENTRY *thread_p, struct regu_variable_list_node   *list)
{
  struct regu_variable_list_node   *p;
  int pg_cnt;

  pg_cnt = 0;
  for (p = list; p; p = p->next)
    {
      //pg_cnt += regu_var_clear (thread_p, &p->value);
    }

  return pg_cnt;
}

int regu_val_list_clear (THREAD_ENTRY *thread_p, REGU_VALUE_LIST *list)
{
  REGU_VALUE_ITEM *list_node;
  int pg_cnt = 0;
  for (list_node = list->regu_list; list_node; list_node = list_node->next)
    {
      pg_cnt += regu_var_clear (thread_p, list_node->value);
    }
  return pg_cnt;
}

int regu_var_clear (THREAD_ENTRY *thread_p, REGU_VARIABLE *regu_var)
{
  int pg_cnt;

  pg_cnt = 0;
  if (!regu_var)
    {
      return pg_cnt;
    }
  er_log_debug (ARG_FILE_LINE, "clear regu_var : %p, type : %d\n", regu_var, regu_var->type);
  /* restore the original domain, in order to avoid coerce when the XASL clones will be used again */
  regu_var->domain = regu_var->original_domain;

  switch (regu_var->type)
    {
    case TYPE_ATTR_ID:		/* fetch object attribute value */
    case TYPE_SHARED_ATTR_ID:
    case TYPE_CLASS_ATTR_ID:
      regu_var->value.attr_descr.cache_dbvalp = NULL;
      break;
    case TYPE_CONSTANT:
      //(void) pr_clear_value (regu_var->value.dbvalptr);
      break;
    case TYPE_INARITH:
    case TYPE_OUTARITH:
      pg_cnt += arith_list_clear (thread_p, regu_var->value.arithptr);
      break;
    case TYPE_SP:
      //pr_clear_value (regu_var->value.sp_ptr->value);
      pg_cnt += regu_list_clear (thread_p, regu_var->value.sp_ptr->args);

      delete regu_var->value.sp_ptr->sig;
      regu_var->value.sp_ptr->sig = nullptr;

      break;
    case TYPE_FUNC:
      //pr_clear_value (regu_var->value.funcp->value);
      pg_cnt += regu_list_clear (thread_p, regu_var->value.funcp->operand);
      if (regu_var->value.funcp->tmp_obj != NULL)
	{
	  switch (regu_var->value.funcp->ftype)
	    {
	    case F_REGEXP_COUNT:
	    case F_REGEXP_INSTR:
	    case F_REGEXP_LIKE:
	    case F_REGEXP_REPLACE:
	    case F_REGEXP_SUBSTR:
	    {
	      if (regu_var->value.funcp->tmp_obj->compiled_regex)
		{
		  delete regu_var->value.funcp->tmp_obj->compiled_regex;
		  regu_var->value.funcp->tmp_obj->compiled_regex = NULL;
		}
	    }
	    break;
	    default:
	      // any member of union func_tmp_obj may have been erased
	      assert (false);
	      break;
	    }

	  delete regu_var->value.funcp->tmp_obj;
	  regu_var->value.funcp->tmp_obj = NULL;
	}

      break;
    case TYPE_REGUVAL_LIST:
      pg_cnt += regu_val_list_clear (thread_p, regu_var->value.reguval_list);
      break;
    case TYPE_DBVAL:
      break;
    case TYPE_REGU_VAR_LIST:
      pg_cnt += regu_list_clear (thread_p, regu_var->value.regu_var_list);
      break;
    case TYPE_POSITION:
      pos_desc_clear (thread_p, &regu_var->value.pos_descr);
      break;
    default:
      break;
    }

  return pg_cnt;
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
  struct regu_variable_list_node *p;

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



SCAN_CODE
scan_next_parallel_heap_scan (THREAD_ENTRY *thread_p, SCAN_ID *scan_id)
{
  SCAN_CODE ret;
  if (!scan_id->s.phsid.master->m_is_start_once)
    {
      scan_id->s.phsid.master->start_tasks (scan_id);
      scan_id->s.phsid.master->m_is_start_once = true;
    }
  ret = scan_id->s.phsid.master->get_result (thread_p, scan_id);
  if (ret == S_ERROR)
    {
      cuberr::er_message *error_msg = nullptr;
      while (error_msg == nullptr)
	{
	  error_msg = scan_id->s.phsid.master->m_context->m_error_msg.exchange (nullptr);
	}
      assert (scan_id->s.phsid.master->m_context->m_has_error);
      cuberr::er_message &crt_error = cuberr::context::get_thread_local_context ().get_current_error_level ();
      crt_error.swap (*error_msg);
      delete error_msg;
      return S_ERROR;
    }

  return ret;
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
  HL_HEAPID orig_heap_id;
  orig_heap_id = db_change_private_heap (thread_p, 0);
  delete scan_id->s.phsid.master;
  db_change_private_heap (thread_p, orig_heap_id);
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
			      OID *cls_oid, HFID *hfid,  struct regu_variable_list_node *regu_list_pred,
			      PRED_EXPR *pr,  struct regu_variable_list_node *regu_list_rest, int num_attrs_pred,
			      ATTR_ID *attrids_pred, HEAP_CACHE_ATTRINFO *cache_pred, int num_attrs_rest,
			      ATTR_ID *attrids_rest, HEAP_CACHE_ATTRINFO *cache_rest, SCAN_TYPE scan_type,
			      DB_VALUE **cache_recordinfo,  struct regu_variable_list_node *regu_list_recordinfo,
			      bool is_partition_table)
{
  int ret;
  int parallel_heap_scan_threads = prm_get_integer_value (PRM_ID_PARALLEL_HEAP_SCAN_THREADS);
  assert (scan_type == S_PARALLEL_HEAP_SCAN);
  scan_id->type = S_HEAP_SCAN;
  ret = scan_open_heap_scan (thread_p, scan_id, mvcc_select_lock_needed, scan_op_type, fixed, grouped, single_fetch,
			     join_dbval,
			     val_list, vd, cls_oid, hfid, regu_list_pred, pr, regu_list_rest, num_attrs_pred, attrids_pred, cache_pred,
			     num_attrs_rest, attrids_rest, cache_rest, S_HEAP_SCAN, cache_recordinfo, regu_list_recordinfo, is_partition_table);
  scan_id->type = S_PARALLEL_HEAP_SCAN;
#if START_END_LOG
  er_log_debug (ARG_FILE_LINE, "parallel heap scan main thread %ld", syscall (SYS_gettid));
#endif

  scan_id->s.phsid.master = new parallel_heap_scan_master (thread_p->tran_index, scan_id, parallel_heap_scan_threads,
      parallel_heap_scan_threads, parallel_heap_scan_threads);
  return ret;
}
