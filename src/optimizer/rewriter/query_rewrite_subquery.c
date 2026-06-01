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

/* context for counting correlated (outer) attribute references inside a subquery */
typedef struct qo_corr_ref_info QO_CORR_REF_INFO;
struct qo_corr_ref_info
{
  PT_NODE *root;		/* subquery node being analyzed; skipped in the query-node check */
  UINTPTR inner_spec_id;	/* spec id of the subquery's single base table */
  int outer_count;		/* attribute refs bound to a spec other than inner */
  bool has_nested_query;	/* nested query node present -> not foldable */
};

/*
 * qo_corr_count_refs () - parser_walk_tree pre function: tally outer attribute
 *			   references and flag any nested query node
 */
static PT_NODE *
qo_corr_count_refs (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk)
{
  QO_CORR_REF_INFO *info = (QO_CORR_REF_INFO *) arg;

  if (node != info->root && PT_IS_QUERY (node))
    {
      info->has_nested_query = true;
      *continue_walk = PT_STOP_WALK;
      return node;
    }

  if (node->node_type == PT_NAME && pt_is_attr (node) && node->info.name.spec_id != 0
      && node->info.name.spec_id != info->inner_spec_id)
    {
      info->outer_count++;
    }

  return node;
}

/*
 * qo_corr_analyze_refs () - walk subtree (with its 'next' link severed so AND
 *			     siblings are excluded; 'or_next' kept) collecting
 *			     outer-ref count and nested-query flag
 */
static void
qo_corr_analyze_refs (PARSER_CONTEXT * parser, PT_NODE * subtree, PT_NODE * root, UINTPTR inner_spec_id,
		      int *outer_count, bool *has_nested_query)
{
  QO_CORR_REF_INFO info;
  PT_NODE *save_next;

  info.root = root;
  info.inner_spec_id = inner_spec_id;
  info.outer_count = 0;
  info.has_nested_query = false;

  save_next = subtree->next;
  subtree->next = NULL;
  (void) parser_walk_tree (parser, subtree, qo_corr_count_refs, &info, NULL, NULL);
  subtree->next = save_next;

  *outer_count = info.outer_count;
  *has_nested_query = info.has_nested_query;
}

/*
 * qo_rewrite_correlated_subqueries () - Unnest correlated IN subquery to inner-join + DISTINCT
 *   return: PT_NODE *
 *   parser(in):
 *   node(in): SELECT node
 *   arg(in): derived-spec name index
 *   continue_walk(in):
 *
 * Note: v1 scope only - x IN (SELECT y FROM t WHERE y = x AND <inner-only>), level 1,
 *	 the single correlated predicate redundant with the IN key. Anything outside this
 *	 narrow shape is left untouched for the dependent-subquery path (always correct).
 */
PT_NODE *
qo_rewrite_correlated_subqueries (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk)
{
  PT_NODE *cnf_node, *in_key, *subq, *sel, *from_spec;
  PT_NODE *prev_w, *w, *corr_pred, *prev_corr;
  PT_NODE *new_spec, *new_attr, *a, *b, *inner_side, *outer_side;
  int *idx = (int *) arg;
  UINTPTR inner_spec_id;
  int total_outer, pred_outer, found_preds;
  bool has_nested;

  if (node->node_type != PT_SELECT)
    {
      return node;
    }

  for (cnf_node = node->info.query.q.select.where; cnf_node; cnf_node = cnf_node->next)
    {
      if (cnf_node->or_next != NULL || cnf_node->node_type != PT_EXPR)
	{
	  continue;
	}
      if (cnf_node->info.expr.op != PT_IS_IN)
	{
	  continue;
	}

      in_key = cnf_node->info.expr.arg1;
      subq = cnf_node->info.expr.arg2;

      /* in_key: single scalar indexable attribute */
      if (in_key == NULL || in_key->next != NULL || !pt_is_attr (in_key) || PT_IS_COLLECTION_TYPE (in_key->type_enum)
	  || !tp_valid_indextype (pt_type_enum_to_db (in_key->type_enum)))
	{
	  continue;
	}

      /* subq: plain correlated (level 1) SELECT */
      if (!PT_IS_SELECT (subq) || subq->info.query.correlation_level != 1)
	{
	  continue;
	}

      /* single flat base table in FROM */
      from_spec = subq->info.query.q.select.from;
      if (from_spec == NULL || from_spec->next != NULL || from_spec->info.spec.derived_table != NULL
	  || from_spec->info.spec.entity_name == NULL)
	{
	  continue;
	}
      inner_spec_id = from_spec->info.spec.id;

      /* no aggregate / analytic / grouping / ordering / limit / hierarchical */
      if (pt_has_aggregate (parser, subq) || pt_has_analytic (parser, subq)
	  || subq->info.query.q.select.group_by != NULL || subq->info.query.q.select.having != NULL
	  || subq->info.query.q.select.connect_by != NULL || subq->info.query.orderby_for != NULL
	  || subq->info.query.order_by != NULL || subq->info.query.limit != NULL)
	{
	  continue;
	}

      /* single indexable attribute projected from the inner table */
      sel = pt_get_select_list (parser, subq);
      if (sel == NULL || sel->next != NULL || !pt_is_attr (sel)
	  || pt_length_of_select_list (sel, EXCLUDE_HIDDEN_COLUMNS) != 1
	  || !tp_valid_indextype (pt_type_enum_to_db (sel->type_enum)) || sel->info.name.spec_id != inner_spec_id)
	{
	  continue;
	}

      /* exactly one outer reference in the whole subquery, no nested query */
      qo_corr_analyze_refs (parser, subq, subq, inner_spec_id, &total_outer, &has_nested);
      if (has_nested || total_outer != 1)
	{
	  continue;
	}

      /* locate the single WHERE conjunct carrying the outer reference */
      corr_pred = NULL;
      prev_corr = NULL;
      found_preds = 0;
      for (prev_w = NULL, w = subq->info.query.q.select.where; w; prev_w = w, w = w->next)
	{
	  qo_corr_analyze_refs (parser, w, subq, inner_spec_id, &pred_outer, &has_nested);
	  if (pred_outer > 0)
	    {
	      corr_pred = w;
	      prev_corr = prev_w;
	      found_preds++;
	    }
	}
      if (found_preds != 1 || corr_pred == NULL)
	{
	  continue;
	}

      /* redundant form: <inner sel> = <in_key>, either order, plain equality */
      if (corr_pred->or_next != NULL || corr_pred->node_type != PT_EXPR || corr_pred->info.expr.op != PT_EQ)
	{
	  continue;
	}
      a = corr_pred->info.expr.arg1;
      b = corr_pred->info.expr.arg2;
      if (a == NULL || b == NULL || !pt_is_attr (a) || !pt_is_attr (b))
	{
	  continue;
	}
      if (a->info.name.spec_id == inner_spec_id && b->info.name.spec_id != inner_spec_id)
	{
	  inner_side = a;
	  outer_side = b;
	}
      else if (b->info.name.spec_id == inner_spec_id && a->info.name.spec_id != inner_spec_id)
	{
	  inner_side = b;
	  outer_side = a;
	}
      else
	{
	  continue;
	}
      /* inner side must be the projected column; outer side must be the IN key */
      if (!pt_name_equal (parser, inner_side, sel) || !pt_name_equal (parser, outer_side, in_key))
	{
	  continue;
	}

      /* transform: drop the redundant correlated predicate */
      if (prev_corr == NULL)
	{
	  subq->info.query.q.select.where = corr_pred->next;
	}
      else
	{
	  prev_corr->next = corr_pred->next;
	}
      corr_pred->next = NULL;
      parser_free_tree (parser, corr_pred);

      /* derived subquery becomes self-contained: DISTINCT + uncorrelated */
      subq->info.query.all_distinct = PT_DISTINCT;
      subq->info.query.correlation_level = 0;
      subq->info.query.is_subquery = PT_IS_SUBQUERY;

      /* append derived spec (inner join via default PT_JOIN_NONE) */
      if (mq_make_derived_spec (parser, node, subq, idx, &new_spec, &new_attr) == NULL)
	{
	  return NULL;
	}

      /* IN-membership becomes an equi-join: in_key = av.col */
      cnf_node->info.expr.arg1 = in_key;
      cnf_node->info.expr.arg2 = new_attr;
      cnf_node->info.expr.op = PT_EQ;
      if (new_attr != NULL)
	{
	  new_attr->next = NULL;
	}

      /* recurse into derived table to unnest any nested correlated IN */
      (void) parser_walk_tree (parser, new_spec->info.spec.derived_table, qo_rewrite_correlated_subqueries, idx, NULL,
			       NULL);
    }

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
