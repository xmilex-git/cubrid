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
 * query_rewrite_subquery.c - Subquery Rewrite Optimization
 */

#ident "$Id$"

#include "parse_tree.h"

#include <assert.h>
#include "query_rewrite.h"


/*
 * qo_rewrite_subqueries () - Rewrite uncorrelated subquery to join query
 *   return: PT_NODE *
 *   parser(in):
 *   node(in): SELECT node
 *   arg(in):
 *   continue_walk(in):
 *
 * Note: do parser_walk_tree() pre function
 */
PT_NODE *
qo_rewrite_subqueries (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk)
{
  PT_NODE *cnf_node, *arg1, *arg2, *select_list, *arg2_list;
  PT_OP_TYPE op_type;
  PT_NODE *new_spec, *new_attr, *new_func;
  int *idx = (int *) arg;
  bool do_rewrite;
  PT_NODE *save_next, *arg1_next, *new_attr_next, *tmp, *arg2_next;
  PT_OP_TYPE saved_op_type;

  if (node->node_type != PT_SELECT)
    {
      return node;
    }

  /* traverse CNF list */
  for (cnf_node = node->info.query.q.select.where; cnf_node; cnf_node = cnf_node->next)
    {

      if (cnf_node->or_next != NULL)
	{
	  continue;
	}

      if (cnf_node->node_type != PT_EXPR)
	{
	  continue;
	}

      op_type = cnf_node->info.expr.op;
      arg1 = cnf_node->info.expr.arg1;
      arg2 = cnf_node->info.expr.arg2;

      if (arg1 && arg2
	  && (op_type == PT_EQ || op_type == PT_IS_IN || op_type == PT_EQ_SOME || op_type == PT_GT_SOME
	      || op_type == PT_GE_SOME || op_type == PT_LT_SOME || op_type == PT_LE_SOME))
	{
	  /* go ahead */
	}
      else
	{
	  continue;
	}

      select_list = pt_get_select_list (parser, arg2);
      if ((op_type == PT_EQ || op_type == PT_IS_IN || op_type == PT_EQ_SOME) && select_list
	  && PT_IS_COLLECTION_TYPE (arg1->type_enum) && PT_IS_FUNCTION (arg1)
	  && PT_IS_COLLECTION_TYPE (arg2->type_enum) && (PT_IS_FUNCTION (select_list) || PT_IS_CONST (select_list)))
	{
	  /* collection case : (col1,col2) [in or =] (select col1,col2 ...) */
	  arg1 = arg1->info.function.arg_list;
	  if (PT_IS_FUNCTION (select_list))
	    {
	      arg2_list = select_list->info.function.arg_list;
	    }
	  else
	    {
	      arg2_list = select_list->info.value.data_value.set;
	    }
	}
      else if (op_type == PT_EQ)
	{
	  /* one column subquery is not rewrited to join with derived table. ex) col1 = (select col1 ... ) */
	  continue;
	}
      else
	{
	  arg2_list = arg2;
	}

      do_rewrite = false;
      select_list = NULL;

      /* should be 'attr op uncorr-subquery', and select list of the subquery should be indexable-column */
      for (arg1_next = arg1, arg2_next = arg2_list; arg1_next && arg2_next;
	   arg1_next = arg1_next->next, arg2_next = arg2_next->next)
	{
	  if (tp_valid_indextype (pt_type_enum_to_db (arg1_next->type_enum))
	      && (pt_is_attr (arg1_next) || pt_is_function_index_expression (arg1_next)))
	    {
	      if (tp_valid_indextype (pt_type_enum_to_db (arg2_next->type_enum)) && !pt_has_analytic (parser, arg2))
		{
		  select_list = pt_get_select_list (parser, arg2);
		  if (select_list != NULL && arg2->info.query.correlation_level == 0)
		    {
		      assert (pt_length_of_select_list (select_list, EXCLUDE_HIDDEN_COLUMNS) == 1);

		      /* match 'indexable-attr op indexable-uncorr-subquery' */
		      do_rewrite = true;
		    }
		  else
		    {
		      do_rewrite = false;
		      break;
		    }
		}
	      else
		{
		  do_rewrite = false;
		  break;
		}
	    }
	  else
	    {
	      do_rewrite = false;
	      break;
	    }
	}

      if (do_rewrite)
	{
	  /* rewrite subquery to join with derived table */
	  switch (op_type)
	    {
	    case PT_EQ:	/* arg1 = set_func_elements */
	    case PT_IS_IN:	/* arg1 = set_func_elements, attr */
	    case PT_EQ_SOME:	/* arg1 = attr */
	      if (PT_IS_COLLECTION_TYPE (arg2->type_enum) && select_list
		  && (PT_IS_FUNCTION (select_list) || PT_IS_CONST (select_list)))
		{
		  /* if arg2 is collection type then select_list is rewrited to multi col */
		  pt_select_list_to_one_col (parser, arg2, false);
		}

	      /* make new derived spec and append it to FROM */
	      if (mq_make_derived_spec (parser, node, arg2, idx, &new_spec, &new_attr) == NULL)
		{
		  return NULL;
		}

	      /* convert to 'attr op attr' */
	      cnf_node->info.expr.arg1 = arg1;
	      arg1 = arg1->next;
	      cnf_node->info.expr.arg1->next = NULL;

	      cnf_node->info.expr.arg2 = new_attr;
	      saved_op_type = cnf_node->info.expr.op;
	      cnf_node->info.expr.op = PT_EQ;

	      if (new_attr != NULL)
		{
		  new_attr = new_attr->next;
		  cnf_node->info.expr.arg2->next = NULL;
		}

	      /* save, cut-off link */
	      save_next = cnf_node->next;
	      cnf_node->next = NULL;

	      /* create the following 'attr op attr' */
	      for (tmp = NULL; arg1 && new_attr; arg1 = arg1_next, new_attr = new_attr_next)
		{
		  tmp = parser_new_node (parser, PT_EXPR);
		  if (tmp == NULL)
		    {
		      PT_INTERNAL_ERROR (parser, "allocate new node");
		      return NULL;
		    }

		  /* save, cut-off link */
		  arg1_next = arg1->next;
		  arg1->next = NULL;
		  new_attr_next = new_attr->next;
		  new_attr->next = NULL;

		  tmp->info.expr.arg1 = arg1;
		  tmp->info.expr.arg2 = new_attr;
		  tmp->info.expr.op = PT_EQ;

		  cnf_node = parser_append_node (tmp, cnf_node);
		}

	      if (tmp)
		{		/* move to the last cnf */
		  cnf_node = tmp;
		}
	      cnf_node->next = save_next;	/* restore link */

	      /* apply qo_rewrite_subqueries() to derived table's subquery */
	      (void) parser_walk_tree (parser, new_spec->info.spec.derived_table, qo_rewrite_subqueries, idx, NULL,
				       NULL);
	      break;

	    case PT_GT_SOME:	/* arg1 = attr */
	    case PT_GE_SOME:	/* arg1 = attr */
	    case PT_LT_SOME:	/* arg1 = attr */
	    case PT_LE_SOME:	/* arg1 = attr */
	      if (arg2->node_type == PT_UNION || arg2->node_type == PT_INTERSECTION || arg2->node_type == PT_DIFFERENCE
		  || pt_has_aggregate (parser, arg2) || arg2->info.query.orderby_for)
		{
		  PT_NODE *rewritten = NULL;

		  /* if it is composite query, rewrite to simple query */
		  rewritten = mq_rewrite_query_as_derived (parser, arg2);
		  if (rewritten == NULL)
		    {
		      return NULL;
		    }
		  else
		    {
		      /* fix list */
		      PT_NODE_MOVE_NUMBER_OUTERLINK (rewritten, arg2);
		      arg2 = rewritten;
		    }

		  /* set as uncorrelated subquery */
		  arg2->info.query.q.select.flavor = PT_USER_SELECT;
		  arg2->info.query.is_subquery = PT_IS_SUBQUERY;
		  arg2->info.query.correlation_level = 0;

		  /* free old composite query */
		  parser_free_tree (parser, cnf_node->info.expr.arg2);
		  cnf_node->info.expr.arg2 = arg2;
		}

	      /* make new derived spec and append it to FROM */
	      if (mq_make_derived_spec (parser, node, arg2, idx, &new_spec, &new_attr) == NULL)
		{
		  return NULL;
		}

	      /* apply qo_rewrite_subqueries() to derived table's subquery */
	      (void) parser_walk_tree (parser, new_spec->info.spec.derived_table, qo_rewrite_subqueries, idx, NULL,
				       NULL);

	      select_list = pt_get_select_list (parser, arg2);
	      if (select_list == NULL)
		{
		  return NULL;
		}

	      /* convert select list of subquery to MIN()/MAX() */
	      new_func = parser_new_node (parser, PT_FUNCTION);
	      if (new_func == NULL)
		{
		  PT_INTERNAL_ERROR (parser, "allocate new node");
		  return NULL;
		}

	      new_func->info.function.function_type =
		((op_type == PT_GT_SOME || op_type == PT_GE_SOME) ? PT_MIN : PT_MAX);
	      new_func->info.function.all_or_distinct = PT_ALL;
	      new_func->info.function.arg_list = select_list;
	      new_func->type_enum = select_list->type_enum;
	      new_func->data_type = parser_copy_tree (parser, select_list->data_type);
	      arg2->info.query.q.select.list = new_func;
	      /* mark as agg select */
	      PT_SELECT_INFO_SET_FLAG (arg2, PT_SELECT_INFO_HAS_AGG);

	      /* convert to 'attr > new_attr' */
	      cnf_node->info.expr.arg2 = new_attr;
	      if (op_type == PT_GT_SOME)
		{
		  cnf_node->info.expr.op = PT_GT;
		}
	      else if (op_type == PT_GE_SOME)
		{
		  cnf_node->info.expr.op = PT_GE;
		}
	      else if (op_type == PT_LT_SOME)
		{
		  cnf_node->info.expr.op = PT_LT;
		}
	      else
		{
		  cnf_node->info.expr.op = PT_LE;
		}
	      break;

	    default:
	      break;
	    }
	}
    }				/* for (cnf_node = ...) */

  *continue_walk = PT_LIST_WALK;

  return node;
}

/*
 * qo_rewrite_hidden_col_as_derived () - Rewrite subquery with ORDER BY
 *				      hidden column as derived one
 *   return: PT_NODE *
 *   parser(in):
 *   node(in): QUERY node
 *   parent_node(in):
 *
 * Note: Keep out hidden column from derived select list
 */
PT_NODE *
qo_rewrite_hidden_col_as_derived (PARSER_CONTEXT * parser, PT_NODE * node, PT_NODE * parent_node)
{
  PT_NODE *t_node, *next, *derived;

  switch (node->node_type)
    {
    case PT_SELECT:
      if (node->info.query.order_by)
	{
	  bool remove_order_by = true;	/* guessing */

	  /* check parent context */
	  if (parent_node)
	    {
	      switch (parent_node->node_type)
		{
		case PT_FUNCTION:
		  switch (parent_node->info.function.function_type)
		    {
		    case F_TABLE_SEQUENCE:
		      remove_order_by = false;
		      break;
		    default:
		      break;
		    }
		  break;
		default:
		  break;
		}
	    }
	  else
	    {
	      remove_order_by = false;
	    }

	  /* check node context */
	  if (remove_order_by == true)
	    {
	      if (node->info.query.orderby_for)
		{
		  remove_order_by = false;
		}
	    }

	  if (remove_order_by == true)
	    {
	      for (t_node = node->info.query.q.select.list; t_node; t_node = t_node->next)
		{
		  if (t_node->node_type == PT_EXPR && t_node->info.expr.op == PT_ORDERBY_NUM)
		    {
		      remove_order_by = false;
		      break;
		    }
		}
	    }

	  /* remove unnecessary ORDER BY clause */
	  if (remove_order_by == true && !node->info.query.q.select.connect_by)
	    {
	      parser_free_tree (parser, node->info.query.order_by);
	      node->info.query.order_by = NULL;

	      for (t_node = node->info.query.q.select.list; t_node && t_node->next; t_node = next)
		{
		  next = t_node->next;
		  if (next->flag.is_hidden_column)
		    {
		      parser_free_tree (parser, next);
		      t_node->next = NULL;
		      break;
		    }
		}
	    }
	  else
	    {
	      /* Check whether we can rewrite query as derived. */
	      bool skip_query_rewrite_as_derived = false;
	      if (node->info.query.is_subquery == PT_IS_SUBQUERY && node->info.query.order_by != NULL)
		{
		  /* If all nodes in select list are hidden columns, we do not rewrite the query as derived
		   * since we want to avoid null select list. This will avoid the crash for queries like:
		   * set @a = 1; SELECT  (SELECT @a := @a + 1 FROM db_root ORDER BY @a + 1)
		   */
		  skip_query_rewrite_as_derived = true;
		  for (t_node = node->info.query.q.select.list; t_node; t_node = t_node->next)
		    {
		      if (!t_node->flag.is_hidden_column)
			{
			  skip_query_rewrite_as_derived = false;
			}
		    }
		}

	      if (!skip_query_rewrite_as_derived)
		{
		  for (t_node = node->info.query.q.select.list; t_node; t_node = t_node->next)
		    {
		      if (t_node->flag.is_hidden_column)
			{
			  /* make derived query */
			  derived = mq_rewrite_query_as_derived (parser, node);
			  if (derived == NULL)
			    {
			      break;
			    }

			  PT_NODE_MOVE_NUMBER_OUTERLINK (derived, node);
			  derived->info.query.q.select.flavor = node->info.query.q.select.flavor;
			  derived->info.query.is_subquery = node->info.query.is_subquery;
			  derived->type_enum = node->type_enum;

			  /* free old composite query */
			  parser_free_tree (parser, node);
			  node = derived;
			  break;
			}
		    }
		}
	    }			/* else */
	}
      break;

    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      node->info.query.q.union_.arg1 = qo_rewrite_hidden_col_as_derived (parser, node->info.query.q.union_.arg1, NULL);
      node->info.query.q.union_.arg2 = qo_rewrite_hidden_col_as_derived (parser, node->info.query.q.union_.arg2, NULL);
      break;
    default:
      return node;
    }

  return node;
}

/*
 * qo_add_keylimit_clause () - Add limit clause to subquery exists
 *   return: void
 *   parser(in):
 *   node(in): QUERY node
 */
void
qo_add_limit_clause (PARSER_CONTEXT * parser, PT_NODE * node)
{
  bool has_instnum = false, has_orderbynum = false, has_groupbynum = false;
  if (PT_IS_SELECT (node))
    {
      (void) parser_walk_tree (parser, node->info.query.q.select.where, pt_check_instnum_pre, NULL,
			       pt_check_instnum_post, &has_instnum);
      (void) parser_walk_tree (parser, node->info.query.orderby_for, pt_check_orderbynum_pre, NULL,
			       pt_check_orderbynum_post, &has_orderbynum);
      (void) parser_walk_tree (parser, node->info.query.q.select.having, pt_check_groupbynum_pre, NULL,
			       pt_check_groupbynum_post, &has_groupbynum);
    }
  if (node->info.query.limit != NULL || has_instnum || has_orderbynum || has_groupbynum)
    {
      return;			/* give up */
    }

  PT_NODE *ins_num = parser_new_node (parser, PT_VALUE);
  ins_num->type_enum = PT_TYPE_INTEGER;
  ins_num->info.value.data_value.i = 1;

  node->info.query.limit = ins_num;
  node->info.query.limit->next = NULL;
  node->info.query.flag.rewrite_limit = 1;
}

/*
 * Correlated EXISTS/NOT EXISTS/IN/NOT IN subquery -> SEMI/ANTI JOIN rewrite (approach B: derived-table).
 *
 * The branch already provides SEMI/ANTI execution (PT_JOIN_SEMI/ANTI on a spec, semantic rules in
 * pt_check_semi_anti_join, plan-gen mark_access_as_semi_anti_join, optimizer-graph freeze). We only build an
 * eligible correlated subquery into a derived spec whose join_type is SEMI/ANTI and ride that pipeline.
 *
 * Correctness first: transform only when statically proven safe; otherwise leave the conjunct as a
 * dependent subquery (no mutation). This pass runs in qo_rewrite_queries (PRE), after name resolution /
 * semantic check, before the cost optimizer. The SEMI/ANTI specs it produces are NOT re-checked by
 * pt_check_semi_anti_join (that ran at pt_compile), so this code self-guarantees the two invariants:
 *   (1) the ON predicate references the local outer side;
 *   (2) ANTI inner columns appear only inside the ANTI ON predicate.
 */

#define QO_CORR_MAX_TERMS 16	/* max correlated conjuncts handled per subquery (else: keep dependent) */
#define QO_CORR_MAX_PROJ  32	/* max derived projection columns tracked for SEMI->INNER coverage */

/* classification of a subquery WHERE conjunct relative to inner base spec / outer from-list */
typedef enum
{
  QO_CORR_REF_CONST,		/* references neither inner nor outer (constant / inner-only-via-const) */
  QO_CORR_REF_INNER,		/* references inner base spec only */
  QO_CORR_REF_OUTER,		/* references outer from-list only */
  QO_CORR_REF_CORRELATED,	/* references both inner and outer */
  QO_CORR_REF_FOREIGN		/* references a spec that is neither inner nor in the outer from-list */
} QO_CORR_REF_KIND;

typedef struct qo_corr_ref_info QO_CORR_REF_INFO;
struct qo_corr_ref_info
{
  PARSER_CONTEXT *parser;
  UINTPTR inner_id;		/* the subquery's single base spec id */
  PT_NODE *outer_from;		/* the outer SELECT's from-list */
  bool found_inner;
  bool found_outer;
  bool found_foreign;
};

/* one recorded correlated conjunct, captured before it is detached from the subquery WHERE */
typedef struct qo_corr_term QO_CORR_TERM;
struct qo_corr_term
{
  PT_OP_TYPE op;		/* comparison op */
  bool inner_is_arg1;		/* true: arg1 is the inner column, arg2 is the outer expr */
  PT_NODE *inner_name;		/* the bare inner column (PT_NAME, spec_id == inner_id) */
  PT_NODE *outer_expr;		/* the outer-only operand (copied into the ON) */
  int proj_idx;			/* derived projection index of inner_name (filled at decorrelation) */
};

static PT_NODE *qo_corr_ref_pre (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk);
static QO_CORR_REF_KIND qo_corr_classify (PARSER_CONTEXT * parser, PT_NODE * tree, UINTPTR inner_id,
					  PT_NODE * outer_from);
static bool qo_corr_col_is_nonnull (PARSER_CONTEXT * parser, PT_NODE * from, PT_NODE * name);
static bool qo_corr_value_is_nonnull (PT_NODE * node);
static bool qo_corr_unique_covered (MOP cls, const char **s_names, int s_count);
static PT_NODE *qo_corr_nth (PT_NODE * list, int n);
static void qo_corr_stamp_location (PARSER_CONTEXT * parser, PT_NODE * on_cond, short location);
static bool qo_corr_rewrite_one (PARSER_CONTEXT * parser, PT_NODE * node, PT_NODE * cnf, int *idx);

/*
 * qo_corr_ref_pre () - walk helper: classify each resolved PT_NAME as inner / outer(local from-list) / foreign.
 */
static PT_NODE *
qo_corr_ref_pre (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk)
{
  QO_CORR_REF_INFO *info = (QO_CORR_REF_INFO *) arg;

  *continue_walk = PT_CONTINUE_WALK;

  if (node->node_type == PT_NAME && node->info.name.spec_id != 0
      && (node->info.name.meta_class == PT_NORMAL || node->info.name.meta_class == PT_SHARED
	  || node->info.name.meta_class == PT_OID_ATTR || node->info.name.meta_class == PT_VID_ATTR))
    {
      if (node->info.name.spec_id == info->inner_id)
	{
	  info->found_inner = true;
	}
      else if (pt_find_entity (parser, info->outer_from, node->info.name.spec_id) != NULL)
	{
	  info->found_outer = true;
	}
      else
	{
	  info->found_foreign = true;
	}
    }

  return node;
}

/*
 * qo_corr_classify () - classify a subtree's column references.
 */
static QO_CORR_REF_KIND
qo_corr_classify (PARSER_CONTEXT * parser, PT_NODE * tree, UINTPTR inner_id, PT_NODE * outer_from)
{
  QO_CORR_REF_INFO info;

  info.parser = parser;
  info.inner_id = inner_id;
  info.outer_from = outer_from;
  info.found_inner = false;
  info.found_outer = false;
  info.found_foreign = false;

  (void) parser_walk_tree (parser, tree, qo_corr_ref_pre, &info, NULL, NULL);

  if (info.found_foreign)
    {
      return QO_CORR_REF_FOREIGN;
    }
  if (info.found_inner && info.found_outer)
    {
      return QO_CORR_REF_CORRELATED;
    }
  if (info.found_inner)
    {
      return QO_CORR_REF_INNER;
    }
  if (info.found_outer)
    {
      return QO_CORR_REF_OUTER;
    }
  return QO_CORR_REF_CONST;
}

/*
 * qo_corr_col_is_nonnull () - true iff a base-table column is provably NOT NULL (NOT NULL constraint or PK).
 */
static bool
qo_corr_col_is_nonnull (PARSER_CONTEXT * parser, PT_NODE * from, PT_NODE * name)
{
  PT_NODE *spec;
  MOP cls = NULL;
  SM_CLASS_CONSTRAINT *consp, *iter;
  int i;

  if (name == NULL || name->node_type != PT_NAME || !pt_is_attr (name))
    {
      return false;
    }

  spec = pt_find_entity (parser, from, name->info.name.spec_id);
  if (spec == NULL || spec->info.spec.entity_name == NULL || spec->info.spec.join_type == PT_JOIN_LEFT_OUTER)
    {
      return false;
    }

  PT_SPEC_GET_DB_OBJECT (spec, cls);
  if (cls == NULL)
    {
      return false;
    }

  if (sm_att_constrained (cls, name->info.name.original, SM_ATTFLAG_NON_NULL))
    {
      return true;
    }

  /* PK / NOT NULL family constraints */
  consp = sm_class_constraints (cls);
  for (iter = consp; iter != NULL; iter = iter->next)
    {
      if (SM_IS_CONSTRAINT_NOT_NULL_FAMILY (iter->type))
	{
	  for (i = 0; iter->attributes[i] != NULL; i++)
	    {
	      if (intl_identifier_casecmp (name->info.name.original, iter->attributes[i]->header.name) == 0)
		{
		  return true;
		}
	    }
	}
    }

  return false;
}

/*
 * qo_corr_value_is_nonnull () - true iff node is a non-NULL literal value.
 */
static bool
qo_corr_value_is_nonnull (PT_NODE * node)
{
  return node != NULL && PT_IS_VALUE_NODE (node) && !PT_IS_NULL_NODE (node);
}

/*
 * qo_corr_unique_covered () - true iff the inner base class has a UNIQUE/PK constraint whose every member
 *      column name is present in s_names (the set of inner base columns equi-pinned to an outer/const value
 *      in the ON). When all columns of a UNIQUE/PK are pinned, at most one base row matches per outer row
 *      (a NULL pin makes "=" UNKNOWN -> 0 matches), so a SEMI join over it is equivalent to INNER.
 */
static bool
qo_corr_unique_covered (MOP cls, const char **s_names, int s_count)
{
  SM_CLASS_CONSTRAINT *consp, *iter;
  int i, j;

  if (cls == NULL)
    {
      return false;
    }

  consp = sm_class_constraints (cls);
  for (iter = consp; iter != NULL; iter = iter->next)
    {
      if (!SM_IS_CONSTRAINT_UNIQUE_FAMILY (iter->type) || iter->attributes[0] == NULL)
	{
	  continue;
	}
      {
	bool all_in = true;
	for (i = 0; iter->attributes[i] != NULL; i++)
	  {
	    bool found = false;
	    for (j = 0; j < s_count; j++)
	      {
		if (s_names[j] != NULL
		    && intl_identifier_casecmp (s_names[j], iter->attributes[i]->header.name) == 0)
		  {
		    found = true;
		    break;
		  }
	      }
	    if (!found)
	      {
		all_in = false;
		break;
	      }
	  }
	if (all_in)
	  {
	    return true;
	  }
      }
    }

  return false;
}

/*
 * qo_corr_nth () - n-th (0-based) element of a ->next-linked list.
 */
static PT_NODE *
qo_corr_nth (PT_NODE * list, int n)
{
  int i;

  for (i = 0; list != NULL && i < n; i++)
    {
      list = list->next;
    }
  return list;
}

/*
 * qo_corr_set_location_pre () - walk helper: stamp expr/name/value location from *arg.
 */
static PT_NODE *
qo_corr_set_location_pre (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk)
{
  short loc = *(short *) arg;

  *continue_walk = PT_CONTINUE_WALK;

  if (node->node_type == PT_EXPR)
    {
      node->info.expr.location = loc;
    }
  else if (node->node_type == PT_NAME)
    {
      node->info.name.location = loc;
    }
  else if (node->node_type == PT_VALUE)
    {
      node->info.value.location = loc;
    }

  return node;
}

/*
 * qo_corr_stamp_location () - stamp every expr/name/value node in the ON predicate with the inner spec location
 *      (equivalent to mq_mark_location for a single spec; that helper is not exported).
 */
static void
qo_corr_stamp_location (PARSER_CONTEXT * parser, PT_NODE * on_cond, short location)
{
  short loc = location;
  PT_NODE *term;

  for (term = on_cond; term != NULL; term = term->next)
    {
      PT_NODE *saved_next = term->next;
      term->next = NULL;
      (void) parser_walk_tree (parser, term, qo_corr_set_location_pre, &loc, NULL, NULL);
      term->next = saved_next;
    }
}

/*
 * qo_rewrite_correlated_subqueries () - rewrite eligible correlated EXISTS/NOT EXISTS/IN/NOT IN WHERE
 *      conjuncts of a SELECT into SEMI/ANTI derived-table joins. PRE walk callback.
 *   return: node (possibly with derived SEMI/ANTI specs appended to FROM and conjuncts removed from WHERE)
 */
PT_NODE *
qo_rewrite_correlated_subqueries (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk)
{
  int *idx = (int *) arg;
  PT_NODE *cnf, *prev, *save_next;

  if (node == NULL || node->node_type != PT_SELECT)
    {
      return node;
    }

  /* hazard: the outer query is itself correlated to a still-higher scope */
  if (node->info.query.correlation_level > 0)
    {
      return node;
    }

  /* opt-out (ii): NO_MERGE on the outer query block */
  if (node->info.query.q.select.hint & PT_HINT_NO_MERGE)
    {
      return node;
    }

  prev = NULL;
  for (cnf = node->info.query.q.select.where; cnf != NULL; cnf = save_next)
    {
      save_next = cnf->next;

      if (qo_corr_rewrite_one (parser, node, cnf, idx))
	{
	  /* the conjunct was consumed; unlink and free it (subquery already re-homed in the derived spec) */
	  if (prev != NULL)
	    {
	      prev->next = save_next;
	    }
	  else
	    {
	      node->info.query.q.select.where = save_next;
	    }
	  cnf->next = NULL;
	  parser_free_tree (parser, cnf);
	}
      else
	{
	  prev = cnf;
	}
    }

  return node;
}

/*
 * qo_corr_rewrite_one () - validate and (if eligible) transform a single WHERE conjunct.
 *   return: true iff the conjunct was transformed and must be removed from WHERE.
 *
 * All eligibility checks are performed before any mutation; on rejection the tree is left untouched.
 */
static bool
qo_corr_rewrite_one (PARSER_CONTEXT * parser, PT_NODE * node, PT_NODE * cnf, int *idx)
{
  PT_OP_TYPE op;
  bool anti = false;
  PT_NODE *subq = NULL;
  PT_NODE *outer_key = NULL;	/* IN/NOT IN left-hand side (NULL for EXISTS/NOT EXISTS) */
  PT_NODE *inner_from, *sub_where, *sub_list, *w, *col;
  UINTPTR inner_id;
  MOP inner_cls = NULL;		/* inner base class MOP (captured pre-mutation; used by unique/PK demotion) */
  QO_CORR_TERM terms[QO_CORR_MAX_TERMS];
  int nterms = 0;
  int i;
  PT_NODE *new_spec = NULL, *new_attr = NULL;
  PT_NODE *on_cond = NULL, *on_tail = NULL;
  PT_NODE *corr_head = NULL, *corr_tail = NULL;	/* detached correlated conjuncts (kept alive for copying) */
  PT_NODE *proj_name[QO_CORR_MAX_PROJ];
  PT_NODE *pending_proj[QO_CORR_MAX_PROJ];	/* projection copies pre-allocated before any mutation */
  int npending = 0;
  int nproj = 0;
  const char *in_key_col = NULL;	/* IN/=ANY match column name iff it is a bare inner base column */
  bool all_on_equi = true;
  bool bound[QO_CORR_MAX_PROJ];
  short location;
  PT_NODE *sp;

  if (cnf->or_next != NULL || cnf->node_type != PT_EXPR)
    {
      return false;
    }

  /* ---- form detection ---- */
  op = cnf->info.expr.op;
  if (op == PT_EXISTS)
    {
      anti = false;
      subq = cnf->info.expr.arg1;
    }
  else if (op == PT_NOT && cnf->info.expr.arg1 != NULL && cnf->info.expr.arg1->node_type == PT_EXPR
	   && cnf->info.expr.arg1->info.expr.op == PT_EXISTS)
    {
      anti = true;
      subq = cnf->info.expr.arg1->info.expr.arg1;
    }
  else if (op == PT_IS_IN || op == PT_EQ_SOME)
    {
      anti = false;
      outer_key = cnf->info.expr.arg1;
      subq = cnf->info.expr.arg2;
    }
  else if (op == PT_IS_NOT_IN)
    {
      anti = true;
      outer_key = cnf->info.expr.arg1;
      subq = cnf->info.expr.arg2;
    }
  else
    {
      return false;
    }

  if (!PT_IS_SELECT (subq))
    {
      return false;
    }

  /* ---- eligibility: subquery shape (2.3) ---- */
  if (!PT_IS_CORRELATED_SUBQUERY (subq) || subq->info.query.correlation_level != 1)
    {
      return false;
    }

  inner_from = subq->info.query.q.select.from;
  if (inner_from == NULL || inner_from->next != NULL || inner_from->info.spec.entity_name == NULL
      || inner_from->info.spec.derived_table != NULL)
    {
      return false;			/* must be a single flat base table */
    }
  inner_id = inner_from->info.spec.id;

  if (PT_SELECT_INFO_IS_FLAGED (subq, PT_SELECT_INFO_HAS_AGG)
      || PT_SELECT_INFO_IS_FLAGED (subq, PT_SELECT_INFO_HAS_ANALYTIC)
      || subq->info.query.q.select.group_by != NULL || subq->info.query.q.select.having != NULL
      || subq->info.query.q.select.connect_by != NULL || subq->info.query.q.select.start_with != NULL
      || subq->info.query.order_by != NULL || subq->info.query.orderby_for != NULL
      || subq->info.query.limit != NULL)
    {
      return false;
    }
  if (pt_has_aggregate (parser, subq) || pt_has_analytic (parser, subq))
    {
      return false;
    }

  /* hazard gates (2.4): partitioned inner / MERGE-generated / subquery NO_MERGE */
  if (subq->info.query.q.select.hint & PT_HINT_NO_MERGE)
    {
      return false;
    }
  if (PT_SELECT_INFO_IS_FLAGED (subq, PT_SELECT_INFO_IS_MERGE_QUERY))
    {
      return false;
    }
  PT_SPEC_GET_DB_OBJECT (inner_from, inner_cls);
  if (inner_cls == NULL || sm_is_partitioned_class (inner_cls) > 0)
    {
      return false;
    }

  /* IN/NOT IN: scalar (single-column) match only */
  if (outer_key != NULL)
    {
      PT_NODE *match;

      if (PT_IS_COLLECTION_TYPE (outer_key->type_enum) || PT_IS_FUNCTION (outer_key))
	{
	  return false;
	}
      match = pt_get_select_list (parser, subq);
      if (pt_length_of_select_list (match, EXCLUDE_HIDDEN_COLUMNS) != 1)
	{
	  return false;
	}
      /* remember the match column name iff it is a bare inner base column (for unique/PK demotion) */
      if (match != NULL && match->node_type == PT_NAME && pt_is_attr (match)
	  && match->info.name.spec_id == inner_id)
	{
	  in_key_col = match->info.name.original;
	}
    }

  /* ---- collect & validate correlated conjuncts of the subquery WHERE ---- */
  sub_where = subq->info.query.q.select.where;
  for (w = sub_where; w != NULL; w = w->next)
    {
      QO_CORR_REF_KIND kind;
      PT_NODE *inner_side, *outer_side;
      bool inner_is_arg1;

      if (w->or_next != NULL)
	{
	  return false;			/* OR-combined predicate: keep dependent */
	}

      /* reject any nested subquery inside the subquery body */
      if (w->node_type == PT_EXPR
	  && ((w->info.expr.arg1 != NULL && PT_IS_QUERY (w->info.expr.arg1))
	      || (w->info.expr.arg2 != NULL && PT_IS_QUERY (w->info.expr.arg2))
	      || (w->info.expr.arg3 != NULL && PT_IS_QUERY (w->info.expr.arg3))))
	{
	  return false;
	}

      kind = qo_corr_classify (parser, w, inner_id, node->info.query.q.select.from);
      if (kind == QO_CORR_REF_FOREIGN || kind == QO_CORR_REF_OUTER)
	{
	  return false;			/* references a higher scope, or pure-outer pred in subquery */
	}
      if (kind == QO_CORR_REF_INNER || kind == QO_CORR_REF_CONST)
	{
	  continue;			/* inner-only filter: validated for ANTI below */
	}

      /* CORRELATED conjunct: must be a binary comparison, inner-bare-column op outer-only-expr */
      if (w->node_type != PT_EXPR
	  || (w->info.expr.op != PT_EQ && w->info.expr.op != PT_NE && w->info.expr.op != PT_GT
	      && w->info.expr.op != PT_GE && w->info.expr.op != PT_LT && w->info.expr.op != PT_LE))
	{
	  return false;
	}

      {
	QO_CORR_REF_KIND k1 = qo_corr_classify (parser, w->info.expr.arg1, inner_id, node->info.query.q.select.from);
	QO_CORR_REF_KIND k2 = qo_corr_classify (parser, w->info.expr.arg2, inner_id, node->info.query.q.select.from);

	if (k1 == QO_CORR_REF_INNER && (k2 == QO_CORR_REF_OUTER || k2 == QO_CORR_REF_CONST))
	  {
	    inner_is_arg1 = true;
	    inner_side = w->info.expr.arg1;
	    outer_side = w->info.expr.arg2;
	  }
	else if ((k1 == QO_CORR_REF_OUTER || k1 == QO_CORR_REF_CONST) && k2 == QO_CORR_REF_INNER)
	  {
	    inner_is_arg1 = false;
	    inner_side = w->info.expr.arg2;
	    outer_side = w->info.expr.arg1;
	  }
	else
	  {
	    return false;		/* not a clean inner-col / outer-expr comparison */
	  }

	if (!(inner_side->node_type == PT_NAME && pt_is_attr (inner_side)
	      && inner_side->info.name.spec_id == inner_id))
	  {
	    return false;		/* inner side must be a bare base column */
	  }
      }

      if (nterms >= QO_CORR_MAX_TERMS)
	{
	  return false;
	}
      terms[nterms].op = w->info.expr.op;
      terms[nterms].inner_is_arg1 = inner_is_arg1;
      terms[nterms].inner_name = inner_side;
      terms[nterms].outer_expr = outer_side;
      terms[nterms].proj_idx = -1;
      nterms++;
    }

  if (nterms == 0)
    {
      return false;			/* no correlation predicate: not our case */
    }

  /* immediate-parent correlation check (2.3): every outer-side name resolves in THIS node's from */
  for (i = 0; i < nterms; i++)
    {
      if (qo_corr_classify (parser, terms[i].outer_expr, inner_id, node->info.query.q.select.from)
	  != QO_CORR_REF_OUTER && qo_corr_classify (parser, terms[i].outer_expr, inner_id,
						    node->info.query.q.select.from) != QO_CORR_REF_CONST)
	{
	  return false;
	}
    }

  /* ANTI: no inner-only filter allowed (would otherwise leak inner-only restriction) */
  if (anti)
    {
      for (w = sub_where; w != NULL; w = w->next)
	{
	  QO_CORR_REF_KIND kind = qo_corr_classify (parser, w, inner_id, node->info.query.q.select.from);
	  if (kind == QO_CORR_REF_INNER)
	    {
	      return false;
	    }
	}
    }

  /* ---- NOT IN NULL-safety gate (2.5): anti only when key and inner column are both provably non-null ---- */
  if (op == PT_IS_NOT_IN)
    {
      PT_NODE *inner_match = pt_get_select_list (parser, subq);

      /* inner NOT-IN projection element must be a bare base column (else: cannot prove non-null) */
      if (inner_match == NULL || !(inner_match->node_type == PT_NAME && pt_is_attr (inner_match)
				   && inner_match->info.name.spec_id == inner_id))
	{
	  return false;
	}
      if (!qo_corr_col_is_nonnull (parser, subq->info.query.q.select.from, inner_match))
	{
	  return false;
	}

      /* outer key: bare non-null column OR non-null literal; anything else cannot be proven safe */
      if (outer_key->node_type == PT_NAME && pt_is_attr (outer_key))
	{
	  if (!qo_corr_col_is_nonnull (parser, node->info.query.q.select.from, outer_key))
	    {
	      return false;
	    }
	}
      else if (!qo_corr_value_is_nonnull (outer_key))
	{
	  /* NULL in the key would make NOT IN three-valued; ANTI would emit a wrong row -> reject */
	  return false;
	}
    }

  /* ---- projection planning (read-only; precedes all mutation) ----
   * Register the current select-list columns and map each correlated inner column to a projection
   * index, pre-allocating any NEW projection copy now. The capacity check and the only fallible
   * allocations happen here, BEFORE the tree is touched, so the mutation phase below has no
   * allocation-failure or capacity early-return (Task 1 invariant: never return false after mutating). */
  sub_list = pt_get_select_list (parser, subq);
  for (col = sub_list; col != NULL; col = col->next)
    {
      if (nproj >= QO_CORR_MAX_PROJ)
	{
	  return false;		/* wide select-list (e.g. SELECT * over a >=33-col table): keep dependent */
	}
      proj_name[nproj++] = col;
    }
  for (i = 0; i < nterms; i++)
    {
      int found = -1;
      int j;
      for (j = 0; j < nproj; j++)
	{
	  if (proj_name[j]->node_type == PT_NAME
	      && proj_name[j]->info.name.spec_id == terms[i].inner_name->info.name.spec_id
	      && intl_identifier_casecmp (proj_name[j]->info.name.original,
					  terms[i].inner_name->info.name.original) == 0)
	    {
	      found = j;
	      break;
	    }
	}
      if (found >= 0)
	{
	  terms[i].proj_idx = found;
	}
      else
	{
	  PT_NODE *proj;
	  if (nproj >= QO_CORR_MAX_PROJ)
	    {
	      return false;	/* no projection room: keep dependent (no mutation yet) */
	    }
	  proj = parser_copy_tree (parser, terms[i].inner_name);
	  if (proj == NULL)
	    {
	      return false;	/* allocation failure, still pre-mutation: safe to bail */
	    }
	  proj->next = NULL;
	  terms[i].proj_idx = nproj;
	  proj_name[nproj] = proj;	/* the pre-built copy; appended to the subquery in the mutation phase */
	  pending_proj[npending++] = proj;
	  nproj++;
	}
    }

  /* =========================================================================
   * Point of no return: every gate passed and all fallible allocations done. Begin mutation.
   * Past this point the function never returns false; a residual allocation failure raises a
   * parser error (statement aborts) rather than leaving a silently corrupted tree.
   * ========================================================================= */

  /* (6a) detach correlated conjuncts from the subquery WHERE, keep them alive to copy outer operands */
  {
    PT_NODE *p = subq->info.query.q.select.where, *pprev = NULL, *pnext;
    for (; p != NULL; p = pnext)
      {
	pnext = p->next;
	{
	  QO_CORR_REF_KIND kind = qo_corr_classify (parser, p, inner_id, node->info.query.q.select.from);
	  if (kind == QO_CORR_REF_CORRELATED)
	    {
	      if (pprev != NULL)
		{
		  pprev->next = pnext;
		}
	      else
		{
		  subq->info.query.q.select.where = pnext;
		}
	      p->next = NULL;
	      if (corr_head == NULL)
		{
		  corr_head = corr_tail = p;
		}
	      else
		{
		  corr_tail->next = p;
		  corr_tail = p;
		}
	    }
	  else
	    {
	      pprev = p;
	    }
	}
      }
  }

  /* the detached subtrees are what terms[].inner_name / outer_expr point into; they remain valid */

  /* (6b) append the pre-built projection copies for correlated inner columns (planned above) */
  for (i = 0; i < npending; i++)
    {
      subq->info.query.q.select.list = parser_append_node (pending_proj[i], subq->info.query.q.select.list);
    }

  /* decorrelated subquery body is now self-contained */
  subq->info.query.correlation_level = 0;

  /* (7) wrap subquery as a derived spec appended to the outer FROM */
  if (mq_make_derived_spec (parser, node, subq, idx, &new_spec, &new_attr) == NULL)
    {
      /* residual allocation failure mid-mutation: abort the statement instead of corrupting the tree */
      PT_INTERNAL_ERROR (parser, "allocate new node");
      return true;
    }
  new_spec->info.spec.join_type = (anti ? PT_JOIN_ANTI : PT_JOIN_SEMI);

  /* (8) build the ON predicate as a ->next-linked conjunct list */
  for (i = 0; i < QO_CORR_MAX_PROJ; i++)
    {
      bound[i] = false;
    }

  /* IN/NOT IN: outer_key = derived.col0 */
  if (outer_key != NULL)
    {
      PT_NODE *av0 = qo_corr_nth (new_attr, 0);
      PT_NODE *t = parser_new_node (parser, PT_EXPR);
      if (t == NULL || av0 == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  return true;
	}
      t->info.expr.op = PT_EQ;
      t->type_enum = PT_TYPE_LOGICAL;
      t->info.expr.arg1 = parser_copy_tree (parser, outer_key);
      t->info.expr.arg2 = parser_copy_tree (parser, av0);
      t->info.expr.arg1->next = NULL;
      t->info.expr.arg2->next = NULL;
      t->next = NULL;
      if (on_cond == NULL)
	{
	  on_cond = on_tail = t;
	}
      else
	{
	  on_tail->next = t;
	  on_tail = t;
	}
      if (0 < QO_CORR_MAX_PROJ)
	{
	  bound[0] = true;
	}
    }

  for (i = 0; i < nterms; i++)
    {
      PT_NODE *avk = qo_corr_nth (new_attr, terms[i].proj_idx);
      PT_NODE *t = parser_new_node (parser, PT_EXPR);
      if (t == NULL || avk == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  return true;
	}
      t->info.expr.op = terms[i].op;
      t->type_enum = PT_TYPE_LOGICAL;
      if (terms[i].inner_is_arg1)
	{
	  t->info.expr.arg1 = parser_copy_tree (parser, avk);
	  t->info.expr.arg2 = parser_copy_tree (parser, terms[i].outer_expr);
	}
      else
	{
	  t->info.expr.arg1 = parser_copy_tree (parser, terms[i].outer_expr);
	  t->info.expr.arg2 = parser_copy_tree (parser, avk);
	}
      t->info.expr.arg1->next = NULL;
      t->info.expr.arg2->next = NULL;
      t->next = NULL;
      if (on_cond == NULL)
	{
	  on_cond = on_tail = t;
	}
      else
	{
	  on_tail->next = t;
	  on_tail = t;
	}
      if (terms[i].op != PT_EQ)
	{
	  all_on_equi = false;
	}
      if (terms[i].proj_idx >= 0 && terms[i].proj_idx < QO_CORR_MAX_PROJ)
	{
	  bound[terms[i].proj_idx] = true;
	}
    }

  new_spec->info.spec.on_cond = on_cond;

  /* (3.4) location stamp: inner spec gets max(outer locations)+1, ON nodes inherit it */
  location = 0;
  for (sp = node->info.query.q.select.from; sp != NULL; sp = sp->next)
    {
      if (sp != new_spec && sp->info.spec.location >= location)
	{
	  location = (short) (sp->info.spec.location + 1);
	}
    }
  new_spec->info.spec.location = location;
  qo_corr_stamp_location (parser, on_cond, location);

  /* (Phase 4) SEMI -> INNER when the derived inner is provably <=1 row per outer row (ANTI never demoted).
   * Two independent sufficient proofs (OR):
   *   (a) DISTINCT proof: subquery is DISTINCT over the full projection AND every projected column is
   *       equi-bound in the ON -> the bound tuple is unique -> <=1 match.
   *   (b) UNIQUE/PK proof: the inner BASE class has a UNIQUE/PK constraint whose every column is equi-
   *       pinned (PT_EQ) to an outer/const value in the ON. A fixed outer row pins each such column to a
   *       value, so at most one base row carries that unique tuple (a NULL pin -> "=" UNKNOWN -> 0 rows);
   *       inner-only filters and the projection only preserve/reduce rows. This recovers the unique/PK
   *       demotion for EXISTS-form SEMI joins (which the DISTINCT proof cannot see). */
  if (!anti)
    {
      bool demote = false;

      if (subq->info.query.all_distinct == PT_DISTINCT && all_on_equi && nproj <= QO_CORR_MAX_PROJ)
	{
	  bool all_bound = true;
	  for (i = 0; i < nproj; i++)
	    {
	      if (!bound[i])
		{
		  all_bound = false;
		  break;
		}
	    }
	  demote = all_bound;
	}

      if (!demote)
	{
	  const char *s_names[QO_CORR_MAX_TERMS + 1];
	  int s_count = 0;

	  for (i = 0; i < nterms; i++)
	    {
	      if (terms[i].op == PT_EQ)
		{
		  s_names[s_count++] = terms[i].inner_name->info.name.original;
		}
	    }
	  if (in_key_col != NULL)
	    {
	      s_names[s_count++] = in_key_col;
	    }
	  demote = qo_corr_unique_covered (inner_cls, s_names, s_count);
	}

      if (demote)
	{
	  new_spec->info.spec.join_type = PT_JOIN_INNER;
	}
    }

  /* (3.6) self-guarantee debug asserts (no-op in release) */
#if !defined(NDEBUG)
  {
    QO_CORR_REF_INFO chk;
    chk.parser = parser;
    chk.inner_id = new_spec->info.spec.id;
    chk.outer_from = node->info.query.q.select.from;
    chk.found_inner = false;
    chk.found_outer = false;
    chk.found_foreign = false;
    (void) parser_walk_tree (parser, new_spec->info.spec.on_cond, qo_corr_ref_pre, &chk, NULL, NULL);
    /* rule (1): ON references the outer side */
    assert (chk.found_outer);

    if (new_spec->info.spec.join_type == PT_JOIN_ANTI)
      {
	/* rule (2): ANTI inner columns appear only in the ON predicate */
	QO_CORR_REF_INFO leak;
	leak.parser = parser;
	leak.inner_id = new_spec->info.spec.id;
	leak.outer_from = node->info.query.q.select.from;
	leak.found_inner = false;
	leak.found_outer = false;
	leak.found_foreign = false;
	(void) parser_walk_tree (parser, node->info.query.q.select.list, qo_corr_ref_pre, &leak, NULL, NULL);
	(void) parser_walk_tree (parser, node->info.query.q.select.where, qo_corr_ref_pre, &leak, NULL, NULL);
	assert (!leak.found_inner);
      }
  }
#endif

  /* (11) detach the subquery pointer from the original conjunct so freeing it does not free the derived body */
  if (op == PT_EXISTS)
    {
      cnf->info.expr.arg1 = NULL;
    }
  else if (op == PT_NOT)
    {
      cnf->info.expr.arg1->info.expr.arg1 = NULL;
    }
  else				/* PT_IS_IN / PT_EQ_SOME / PT_IS_NOT_IN */
    {
      cnf->info.expr.arg2 = NULL;
    }

  /* free the detached correlated conjuncts (their outer operands were copied into the ON) */
  if (corr_head != NULL)
    {
      parser_free_tree (parser, corr_head);
    }

  return true;
}
