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
 * db_shared_stmt.cpp - process-wide shared prepared-statement descriptors (workspace #266 S3)
 *
 * Design: docs/cas-merge/shared-prepare-design.md (tooling repo, branch design/wf266-shared-prepare).
 *
 * D-KEY  : key = (DB user OID, key_flags, compile-affecting session parameters, SQL text).  The parameter
 *          string is the same one alias_print bakes into the XASL cache key (PRM_FOR_QRY_STRING values
 *          and cost overrides), so two sessions that would compile to different plans never share.
 * D-P4   : a statement that the XASL-only executor below cannot run (client-side DML, triggers, INTO,
 *          SP calls, DDL, cached subqueries, bind-sensitive replans, ...) is simply not published; its
 *          session keeps the DB_SESSION as before.  The publish predicate errs towards refusing.
 * D-EVICT: descriptor bytes are charged to the XASL cache budget; unreferenced descriptors are trimmed
 *          when the cache asks (xcache_cleanup) or when the entry cap is hit.
 *
 * Only SERVER_MODE (the CAS fold) keeps a cache.  CS/SA builds get the stubs at the bottom.
 */

#include "db_shared_stmt.hpp"

#if defined (SERVER_MODE)

#include <atomic>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "authenticate.h"
#include "cache_time.h"
#include "compile_context.h"
#include "cursor.h"
#include "db_session.h"
#include "dbi.h"
#include "error_manager.h"
#include "locator.h"
#include "locator_cl.h"
#include "memory_alloc.h"
#include "network_interface_cl.h"
#include "object_domain.h"
#include "object_primitive.h"
#include "parse_tree.h"
#include "parser.h"
#include "query_cl.h"
#include "query_list.h"
#include "schema_manager.h"
#include "system_parameter.h"
#include "transaction_cl.h"
#include "trigger_manager.h"
#include "work_space.h"
#include "xasl.h"
#include "xasl_cache.h"

/* the statement kinds the XASL-only executor handles (see design §4) */
enum shared_stmt_kind
{
  SHARED_KIND_SELECT,		/* PT_SELECT / PT_UNION / PT_INTERSECTION / PT_DIFFERENCE */
  SHARED_KIND_UPDATE,		/* server-side UPDATE */
  SHARED_KIND_DELETE,		/* server-side DELETE */
  SHARED_KIND_INSERT		/* server-side INSERT */
};

struct shared_class_ref
{
  OID oid;
  int chn;			/* cache coherency number seen at publication (pt_has_modified_class) */
  bool is_class_ct;		/* SM_CLASS_CT: chn is compared; views/proxies only need the fetch */
  bool modified_target;		/* PT_SPEC_FLAG_UPDATE / DELETE: flushed and decached after the DML */
};

struct db_shared_stmt
{
  /* identity */
  std::string key;		/* user|flags|params|sql */
  std::string sql;
  int key_flags;
  OID user_oid;

  /* immutable compilation result */
  shared_stmt_kind kind;
  CUBRID_STMT_TYPE stmt_type;
  XASL_ID xasl_id;		/* refreshed in place under xasl_id_mutex when the cache entry is replaced */
  int num_markers;		/* user host variables */
  int auto_param_count;		/* auto-parameterized literals, appended after the user values */
  DB_VALUE *auto_params;
  TP_DOMAIN **expected_domains;	/* num_markers entries, process-wide cached (MOP-free) domains */
  DB_QUERY_TYPE *columns;	/* SELECT only; MOP-free */
  int num_columns;
  bool oids_included;
  bool do_cache;		/* info.query.flag.do_cache */
  bool do_not_cache;		/* info.query.flag.do_not_cache */
  bool reexecute;		/* info.query.flag.reexecute */
  bool use_plan_cache;
  bool use_query_cache;
  bool jdbc_cache_hint;
  int jdbc_life_time;
  bool can_execute_with_autocommit;
  std::vector<shared_class_ref> classes;

  /* lifetime */
  std::atomic<int> refcount;
  std::atomic<bool> invalid;
  std::mutex xasl_id_mutex;
  size_t bytes;

  /* P7 */
  std::atomic<long long> hits;
  std::atomic<long long> executions;
  std::atomic<long long> refreshes;

    db_shared_stmt ():key_flags (0), kind (SHARED_KIND_SELECT), stmt_type (CUBRID_STMT_NONE), num_markers (0),
    auto_param_count (0), auto_params (NULL), expected_domains (NULL), columns (NULL), num_columns (0),
    oids_included (false), do_cache (false), do_not_cache (false), reexecute (false), use_plan_cache (false),
    use_query_cache (false), jdbc_cache_hint (false), jdbc_life_time (-1), can_execute_with_autocommit (false),
    refcount (0), invalid (false), bytes (0), hits (0), executions (0), refreshes (0)
  {
    OID_SET_NULL (&user_oid);
    XASL_ID_SET_NULL (&xasl_id);
  }
   ~db_shared_stmt ()
  {
    if (auto_params != NULL)
      {
	for (int i = 0; i < auto_param_count; i++)
	  {
	    pr_clear_value (&auto_params[i]);
	  }
	free (auto_params);
      }
    if (expected_domains != NULL)
      {
	free (expected_domains);
      }
    if (columns != NULL)
      {
	db_query_format_free (columns);
      }
  }
};

/* the cache: sharded by key hash; one entry per key.  A shard mutex is only held for map operations. */
namespace
{
  const int SHARED_STMT_SHARDS = 64;

  struct shared_stmt_shard
  {
    std::mutex mutex;
    std::unordered_map<std::string, db_shared_stmt *> map;
  };

  struct shared_stmt_cache
  {
    shared_stmt_shard shards[SHARED_STMT_SHARDS];
    std::atomic<int> entries;
    std::atomic<long long> bytes;
    std::atomic<long long> lookups;
    std::atomic<long long> hits;
    std::atomic<long long> publishes;
    std::atomic<long long> refused;
    std::atomic<long long> evictions;
    std::atomic<long long> stale;
    std::atomic<long long> fallbacks;

      shared_stmt_cache ():entries (0), bytes (0), lookups (0), hits (0), publishes (0), refused (0), evictions (0),
      stale (0), fallbacks (0)
    {
    }
  };

  shared_stmt_cache Shared_stmt_cache;

  inline shared_stmt_shard &shard_of (const std::string & key)
  {
    return Shared_stmt_cache.shards[std::hash < std::string > () (key) % SHARED_STMT_SHARDS];
  }

  /* the entry cap follows the plan cache's own cap (D-EVICT: one budget) */
  inline int max_entries ()
  {
    return prm_get_integer_value (PRM_ID_XASL_CACHE_MAX_ENTRIES);
  }

  void
  drop_ref (db_shared_stmt * stmt)
  {
    if (stmt->refcount.fetch_sub (1) == 1)
      {
	Shared_stmt_cache.bytes -= (long long) stmt->bytes;
	xcache_shared_stmt_account (-(INT32) stmt->bytes, 0);
	delete stmt;
      }
  }

  /* unlink a descriptor from its shard (it stays alive for the current holders) */
  void
  unlink (db_shared_stmt * stmt)
  {
    shared_stmt_shard & shard = shard_of (stmt->key);
    std::lock_guard < std::mutex > lock (shard.mutex);
    auto it = shard.map.find (stmt->key);
    if (it != shard.map.end () && it->second == stmt)
      {
	shard.map.erase (it);
	Shared_stmt_cache.entries--;
	xcache_shared_stmt_account (0, -1);
	drop_ref (stmt);	/* the map's own reference */
      }
  }

  /* D-EVICT: trim descriptors nobody references.  Called under the cap or by the plan cache cleanup. */
  void
  trim (int target_entries)
  {
    for (int s = 0; s < SHARED_STMT_SHARDS && Shared_stmt_cache.entries > target_entries; s++)
      {
	shared_stmt_shard & shard = Shared_stmt_cache.shards[s];
	std::lock_guard < std::mutex > lock (shard.mutex);
	for (auto it = shard.map.begin (); it != shard.map.end () && Shared_stmt_cache.entries > target_entries;)
	  {
	    db_shared_stmt *stmt = it->second;
	    if (stmt->refcount.load () == 1 || stmt->invalid.load ())
	      {
		it = shard.map.erase (it);
		Shared_stmt_cache.entries--;
		Shared_stmt_cache.evictions++;
		xcache_shared_stmt_account (0, -1);
		drop_ref (stmt);
	      }
	    else
	      {
		++it;
	      }
	  }
      }
  }

  /* does the domain chain embed a workspace MOP (B4-D9 session-scoped domain)?  Such a descriptor cannot
   * outlive the publishing session, so it is refused. */
  bool
  domain_carries_mop (const TP_DOMAIN * d)
  {
    for (; d != NULL; d = d->next)
      {
	if (d->class_mop != NULL || (d->setdomain != NULL && domain_carries_mop (d->setdomain)))
	  {
	    return true;
	  }
      }
    return false;
  }

  bool
  columns_carry_mop (const DB_QUERY_TYPE * col)
  {
    for (; col != NULL; col = col->next)
      {
	if (domain_carries_mop (col->domain) || domain_carries_mop (col->src_domain))
	  {
	    return true;
	  }
      }
    return false;
  }

  /* key material: user OID, key flags, the session-effective compile parameters, the SQL text */
  bool
  make_key (const char *sql, int key_flags, std::string & key, OID * user_oid)
  {
    char *prm = sysprm_print_parameters_for_qry_string ();
    if (prm == NULL)
      {
	return false;
      }
    if (Au_user == NULL)
      {
	free (prm);
	return false;
      }
    *user_oid = *WS_OID (Au_user);
    char head[64];
    snprintf (head, sizeof (head), "%d|%d|%d|%x|", user_oid->volid, user_oid->pageid, user_oid->slotid, key_flags);
    key.reserve (strlen (head) + strlen (prm) + 1 + strlen (sql));
    key.append (head);
    key.append (prm);
    key.push_back ('|');
    key.append (sql);
    free (prm);
    return true;
  }

  /* publish-time walkers */
  struct collect_ctx
  {
    std::vector<shared_class_ref> *classes;
    bool refuse;
  };

  PT_NODE *
  collect_specs (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk)
  {
    collect_ctx *ctx = (collect_ctx *) arg;
    *continue_walk = PT_CONTINUE_WALK;
    if (ctx->refuse)
      {
	*continue_walk = PT_STOP_WALK;
	return node;
      }
    if (node->node_type == PT_SPEC)
      {
	if (node->info.spec.remote_server_name != NULL)
	  {
	    ctx->refuse = true;	/* dblink / remote DML: not an XASL-only execution */
	    *continue_walk = PT_STOP_WALK;
	    return node;
	  }
	for (PT_NODE * class_ = node->info.spec.flat_entity_list; class_ != NULL; class_ = class_->next)
	  {
	    MOP clsmop = class_->info.name.db_object;
	    SM_CLASS *smcls = NULL;
	    if (clsmop == NULL || WS_ISVID (clsmop) || clsmop->decached)
	      {
		ctx->refuse = true;
		*continue_walk = PT_STOP_WALK;
		return node;
	      }
	    if (au_fetch_class_force (clsmop, &smcls, AU_FETCH_READ) != NO_ERROR || smcls == NULL)
	      {
		er_clear ();
		ctx->refuse = true;
		*continue_walk = PT_STOP_WALK;
		return node;
	      }
	    shared_class_ref ref;
	    COPY_OID (&ref.oid, WS_OID (clsmop));
	    ref.is_class_ct = (sm_get_class_type (smcls) == SM_CLASS_CT);
	    ref.chn = locator_get_cache_coherency_number (clsmop);
	    ref.modified_target = (node->info.spec.flag & (PT_SPEC_FLAG_UPDATE | PT_SPEC_FLAG_DELETE)) != 0;
	    ctx->classes->push_back (ref);
	  }
      }
    return node;
  }

  PT_NODE *
  find_cached_subquery (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk)
  {
    bool *found = (bool *) arg;
    *continue_walk = PT_CONTINUE_WALK;
    if (PT_IS_QUERY (node) && node->info.query.flag.subquery_cached)
      {
	*found = true;
	*continue_walk = PT_STOP_WALK;
      }
    return node;
  }

  bool
  statement_is_bind_sensitive (PT_NODE * statement)
  {
    PT_HINT_ENUM hint = PT_HINT_NONE;
    if (PT_IS_QUERY (statement))
      {
	hint = statement->info.query.hint;
      }
    else if (statement->node_type == PT_UPDATE)
      {
	hint = statement->info.update.hint;
      }
    else if (statement->node_type == PT_DELETE)
      {
	hint = statement->info.delete_.hint;
      }
    if ((hint & PT_HINT_BIND_SENSITIVE) != 0)
      {
	return true;
      }
    return prm_get_bool_value (PRM_ID_PLAN_CACHE_BIND_SENSITIVITY);
  }

  /* execute-time helpers mirroring the tree path */

  /* pt_has_modified_class over the descriptor's class list */
  DB_CLASS_MODIFICATION_STATUS
  classes_modified (const db_shared_stmt * stmt)
  {
    for (const shared_class_ref & ref : stmt->classes)
      {
	MOP clsmop = ws_mop (&ref.oid, NULL);
	SM_CLASS *smcls = NULL;
	if (clsmop == NULL)
	  {
	    return DB_CLASS_ERROR;
	  }
	if (clsmop->decached)
	  {
	    return DB_CLASS_MODIFIED;
	  }
	int error = au_fetch_class_force (clsmop, &smcls, AU_FETCH_READ);
	if (error != NO_ERROR)
	  {
	    return (error == ER_HEAP_UNKNOWN_OBJECT) ? DB_CLASS_MODIFIED : DB_CLASS_ERROR;
	  }
	if (ref.is_class_ct && ref.chn != locator_get_cache_coherency_number (clsmop))
	  {
	    return DB_CLASS_MODIFIED;
	  }
      }
    return DB_CLASS_NOT_MODIFIED;
  }

  /* pt_flush_classes over the descriptor's class list (only when the workspace has updates) */
  int
  flush_classes (const db_shared_stmt * stmt)
  {
    int isvirt;
    for (const shared_class_ref & ref : stmt->classes)
      {
	MOP clsmop = ws_mop (&ref.oid, NULL);
	SM_CLASS *smcls = NULL;
	if (clsmop == NULL)
	  {
	    return er_errid () != NO_ERROR ? er_errid () : ER_FAILED;
	  }
	if (WS_ISDIRTY (clsmop) || ws_has_dirty_objects (clsmop, &isvirt))
	  {
	    int err = sm_flush_objects (clsmop);
	    if (err != NO_ERROR)
	      {
		return err;
	      }
	  }
	if (locator_is_class (clsmop, DB_FETCH_READ) <= 0)
	  {
	    continue;
	  }
	if (au_fetch_class_force (clsmop, &smcls, AU_FETCH_READ) != NO_ERROR)
	  {
	    return er_errid () != NO_ERROR ? er_errid () : ER_FAILED;
	  }
	if (smcls != NULL && smcls->partition != NULL)
	  {
	    for (DB_OBJLIST * user = smcls->users; user != NULL; user = user->next)
	      {
		if (WS_ISDIRTY (user->op) || ws_has_dirty_objects (user->op, &isvirt))
		  {
		    int err = sm_flush_objects (user->op);
		    if (err != NO_ERROR)
		      {
			return err;
		      }
		  }
	      }
	  }
      }
    return NO_ERROR;
  }

  /* re-find the plan by SHA-1 after the cache entry was replaced (statistics recompile, another session's
   * recompile); returns true when a plan is available again */
  bool
  refresh_xasl_id (db_shared_stmt * stmt)
  {
    COMPILE_CONTEXT context;
    XASL_STREAM stream;
    XASL_ID xasl_id;
    XASL_NODE_HEADER header;

    memset (&context, 0, sizeof (context));
    memset (&stream, 0, sizeof (stream));
    context.sha1 = stmt->xasl_id.sha1;
    context.recompile_xasl = false;
    stream.xasl_id = &xasl_id;
    stream.xasl_header = &header;
    XASL_ID_SET_NULL (&xasl_id);

    if (prepare_query (&context, &stream) != NO_ERROR)
      {
	er_clear ();
	return false;
      }
    if (XASL_ID_IS_NULL (&xasl_id) || context.recompile_xasl)
      {
	return false;
      }
    std::lock_guard < std::mutex > lock (stmt->xasl_id_mutex);
    XASL_ID_COPY (&stmt->xasl_id, &xasl_id);
    stmt->refreshes++;
    return true;
  }

  /* pt_set_host_variables: cast the user values to the compiled expected domains.  Returns false when the
   * tree path must produce the (identical) error. */
  bool
  bind_values (const db_shared_stmt * stmt, int num_values, DB_VALUE * in, DB_VALUE * out)
  {
    for (int i = 0; i < num_values; i++)
      {
	DB_VALUE *val = &in[i];
	DB_VALUE *hv = &out[i];
	TP_DOMAIN *hv_dom = stmt->expected_domains[i];

	if (db_value_type (val) == DB_TYPE_OBJECT)
	  {
	    return false;	/* reusable-OID reference checks belong to the tree path */
	  }
	db_make_null (hv);
	if (TP_DOMAIN_TYPE (hv_dom) == DB_TYPE_UNKNOWN || hv_dom->type->id == DB_TYPE_ENUMERATION)
	  {
	    if (pr_clone_value (val, hv) != NO_ERROR)
	      {
		return false;
	      }
	  }
	else
	  {
	    DB_TYPE val_type = db_value_type (val);
	    if (tp_value_cast_preserve_domain (val, hv, hv_dom, false, true) != DOMAIN_COMPATIBLE)
	      {
		return false;
	      }
	    if (TP_IS_CHAR_TYPE (hv_dom->type->id) && hv_dom->type->id != val_type && val_type == DB_TYPE_VARCHAR)
	      {
		pr_clear_value (hv);
		if (pr_clone_value (val, hv) != NO_ERROR)
		  {
		    return false;
		  }
	      }
	  }
      }
    return true;
  }

  /* pt_new_query_result_descriptor for a descriptor-backed SELECT */
  DB_QUERY_RESULT *
  make_select_result (const db_shared_stmt * stmt, QUERY_ID query_id, QFILE_LIST_ID * list_id, bool holdable,
		      const CACHE_TIME * srv_cache_time)
  {
    DB_QUERY_RESULT *r = db_alloc_query_result (T_SELECT, stmt->num_columns);
    if (r == NULL)
      {
	return NULL;
      }
    db_init_query_result (r, T_SELECT);
    r->type = T_SELECT;
    r->col_cnt = stmt->num_columns;
    r->oid_included = stmt->oids_included;
    r->res.s.query_id = query_id;
    r->res.s.stmt_id = 1;
    r->res.s.stmt_type = CUBRID_STMT_SELECT;
    r->res.s.cache_time = *srv_cache_time;
    memset (&r->res.s.cursor_id.list_id, 0, sizeof (QFILE_LIST_ID));
    r->res.s.cursor_id.query_id = query_id;
    r->res.s.cursor_id.buffer = NULL;
    r->res.s.cursor_id.tuple_record.tpl = NULL;
    r->res.s.holdable = holdable;
    r->type_cnt = stmt->num_columns;

    bool failure;
    if (list_id != NULL)
      {
	failure = !cursor_open (&r->res.s.cursor_id, list_id, false, r->oid_included);
	cursor_free_self_list_id (list_id);
      }
    else
      {
	QFILE_LIST_ID empty_list_id;
	QFILE_CLEAR_LIST_ID (&empty_list_id);
	failure = !cursor_open (&r->res.s.cursor_id, &empty_list_id, false, r->oid_included);
      }
    if (failure)
      {
	db_free_query_result (r);
	return NULL;
      }
    r->query_type = db_cp_query_type (stmt->columns, false);
    return r;
  }
}				// namespace

/*
 * public API
 */

bool
db_shared_stmt_enabled (void)
{
  return prm_get_bool_value (PRM_ID_SHARED_PREPARED_STATEMENT);
}

DB_SHARED_STMT *
db_shared_stmt_lookup (const char *sql, int key_flags)
{
  if (!db_shared_stmt_enabled () || sql == NULL)
    {
      return NULL;
    }
  std::string key;
  OID user_oid;
  if (!make_key (sql, key_flags, key, &user_oid))
    {
      er_clear ();
      return NULL;
    }
  Shared_stmt_cache.lookups++;
  shared_stmt_shard & shard = shard_of (key);
  std::lock_guard < std::mutex > lock (shard.mutex);
  auto it = shard.map.find (key);
  if (it == shard.map.end () || it->second->invalid.load ())
    {
      return NULL;
    }
  it->second->refcount++;
  it->second->hits++;
  Shared_stmt_cache.hits++;
  return it->second;
}

/*
 * db_shared_stmt_publish () - build a descriptor from a compiled (and, for statements whose first execution
 *   fixes the plan, executed) statement and publish it under its key.  *out receives a referenced descriptor
 *   or NULL when the statement is not shareable; that is not an error.
 */
int
db_shared_stmt_publish (DB_SESSION * session, int stmt_id, const char *sql, int key_flags, DB_SHARED_STMT ** out)
{
  *out = NULL;
  if (!db_shared_stmt_enabled () || session == NULL || session->parser == NULL || sql == NULL)
    {
      return NO_ERROR;
    }
  if (session->dimension != 1 || stmt_id != 1 || session->statements == NULL || session->statements[0] == NULL)
    {
      Shared_stmt_cache.refused++;
      return NO_ERROR;
    }
  PARSER_CONTEXT *parser = session->parser;
  PT_NODE *st = session->statements[0];

  if (st->xasl_id == NULL || XASL_ID_IS_NULL (st->xasl_id) || st->flag.cannot_prepare || st->flag.recompile
      || st->flag.hv_pred_plan_unpeeked || st->flag.si_datetime || st->flag.si_tran_id
      || st->flag.do_not_use_subquery_cache || statement_is_bind_sensitive (st)
      || (parser->host_var_count > 0 && parser->host_var_expected_domains == NULL))
    {
      Shared_stmt_cache.refused++;
      return NO_ERROR;
    }

  shared_stmt_kind kind;
  switch (st->node_type)
    {
    case PT_SELECT:
    case PT_UNION:
    case PT_INTERSECTION:
    case PT_DIFFERENCE:
      if (st->info.query.into_list != NULL)
	{
	  Shared_stmt_cache.refused++;
	  return NO_ERROR;
	}
      kind = SHARED_KIND_SELECT;
      break;
    case PT_UPDATE:
      if (!st->info.update.server_update || st->info.update.do_class_attrs || st->info.update.object != NULL
	  || st->info.update.orderby_for != NULL
	  || (st->info.update.hint & (PT_HINT_USE_SBR | PT_HINT_LK_TIMEOUT)) != 0)
	{
	  Shared_stmt_cache.refused++;
	  return NO_ERROR;
	}
      kind = SHARED_KIND_UPDATE;
      break;
    case PT_DELETE:
      if (!st->info.delete_.server_delete || st->info.delete_.del_stmt_list != NULL
	  || (st->info.delete_.hint & (PT_HINT_USE_SBR | PT_HINT_LK_TIMEOUT)) != 0)
	{
	  Shared_stmt_cache.refused++;
	  return NO_ERROR;
	}
      kind = SHARED_KIND_DELETE;
      break;
    case PT_INSERT:
      if ((st->info.insert.hint & PT_HINT_USE_SBR) != 0)
	{
	  Shared_stmt_cache.refused++;
	  return NO_ERROR;
	}
      kind = SHARED_KIND_INSERT;
      break;
    default:
      Shared_stmt_cache.refused++;
      return NO_ERROR;
    }

  if (pt_is_allowed_result_cache ())
    {
      bool found = false;
      (void) parser_walk_tree (parser, st, find_cached_subquery, &found, NULL, NULL);
      if (found)
	{
	  Shared_stmt_cache.refused++;
	  return NO_ERROR;
	}
    }

  std::string key;
  OID user_oid;
  if (!make_key (sql, key_flags, key, &user_oid))
    {
      er_clear ();
      return NO_ERROR;
    }

  db_shared_stmt *stmt = new db_shared_stmt ();
  stmt->key = key;
  stmt->sql = sql;
  stmt->key_flags = key_flags;
  stmt->user_oid = user_oid;
  stmt->kind = kind;
  stmt->stmt_type = pt_node_to_cmd_type (st);
  XASL_ID_COPY (&stmt->xasl_id, st->xasl_id);
  stmt->num_markers = parser->host_var_count;
  stmt->auto_param_count = parser->auto_param_count;
  stmt->use_plan_cache = st->flag.use_plan_cache;
  stmt->use_query_cache = st->flag.use_query_cache;
  stmt->can_execute_with_autocommit = db_statement_can_execute_with_autocommit (session, 1);

  /* classes */
  collect_ctx cctx = { &stmt->classes, false };
  (void) parser_walk_tree (parser, st, collect_specs, &cctx, NULL, NULL);
  if (cctx.refuse)
    {
      delete stmt;
      Shared_stmt_cache.refused++;
      return NO_ERROR;
    }

  /* host variables */
  if (stmt->num_markers > 0)
    {
      stmt->expected_domains = (TP_DOMAIN **) malloc (sizeof (TP_DOMAIN *) * stmt->num_markers);
      if (stmt->expected_domains == NULL)
	{
	  delete stmt;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
		  sizeof (TP_DOMAIN *) * stmt->num_markers);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      for (int i = 0; i < stmt->num_markers; i++)
	{
	  TP_DOMAIN *dom = parser->host_var_expected_domains[i];
	  if (dom == NULL || domain_carries_mop (dom))
	    {
	      delete stmt;
	      Shared_stmt_cache.refused++;
	      return NO_ERROR;
	    }
	  stmt->expected_domains[i] = dom;
	}
    }
  if (stmt->auto_param_count > 0)
    {
      stmt->auto_params = (DB_VALUE *) malloc (sizeof (DB_VALUE) * stmt->auto_param_count);
      if (stmt->auto_params == NULL)
	{
	  delete stmt;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
		  sizeof (DB_VALUE) * stmt->auto_param_count);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      for (int i = 0; i < stmt->auto_param_count; i++)
	{
	  db_make_null (&stmt->auto_params[i]);
	}
      for (int i = 0; i < stmt->auto_param_count; i++)
	{
	  DB_VALUE *src = &parser->host_variables[stmt->num_markers + i];
	  if (db_value_type (src) == DB_TYPE_OBJECT || pr_clone_value (src, &stmt->auto_params[i]) != NO_ERROR)
	    {
	      delete stmt;
	      Shared_stmt_cache.refused++;
	      return NO_ERROR;
	    }
	}
    }

  /* result columns */
  if (kind == SHARED_KIND_SELECT)
    {
      stmt->oids_included = st->info.query.oids_included != 0;
      stmt->do_cache = st->info.query.flag.do_cache != 0;
      stmt->do_not_cache = st->info.query.flag.do_not_cache != 0;
      stmt->reexecute = st->info.query.flag.reexecute != 0;
      if (st->node_type == PT_SELECT && (st->info.query.q.select.hint & PT_HINT_JDBC_CACHE)
	  && st->info.query.q.select.jdbc_life_time != NULL
	  && st->info.query.q.select.jdbc_life_time->info.name.original != NULL)
	{
	  stmt->jdbc_cache_hint = true;
	  stmt->jdbc_life_time = atoi (st->info.query.q.select.jdbc_life_time->info.name.original);
	}
      stmt->columns = db_get_query_type_list (session, 1);
      if (stmt->columns == NULL)
	{
	  er_clear ();
	  delete stmt;
	  Shared_stmt_cache.refused++;
	  return NO_ERROR;
	}
      if (columns_carry_mop (stmt->columns))
	{
	  delete stmt;
	  Shared_stmt_cache.refused++;
	  return NO_ERROR;
	}
      for (DB_QUERY_TYPE * col = stmt->columns; col != NULL; col = col->next)
	{
	  stmt->num_columns++;
	}
    }

  /* accounting: a coarse but stable byte figure (P7 reports it; xcache charges it) */
  stmt->bytes = sizeof (db_shared_stmt) + stmt->key.size () + stmt->sql.size ()
    + stmt->classes.size () * sizeof (shared_class_ref) + stmt->num_markers * sizeof (TP_DOMAIN *)
    + stmt->auto_param_count * sizeof (DB_VALUE) + stmt->num_columns * sizeof (DB_QUERY_TYPE);

  /* publish: the map holds one reference, the caller one more */
  stmt->refcount = 2;
  {
    shared_stmt_shard & shard = shard_of (key);
    std::lock_guard < std::mutex > lock (shard.mutex);
    auto it = shard.map.find (key);
    if (it != shard.map.end ())
      {
	db_shared_stmt *old = it->second;
	if (!old->invalid.load ())
	  {
	    /* somebody published meanwhile: adopt theirs */
	    old->refcount++;
	    delete stmt;
	    *out = old;
	    return NO_ERROR;
	  }
	it->second = stmt;
	drop_ref (old);
	xcache_shared_stmt_account ((INT32) stmt->bytes, 0);
      }
    else
      {
	shard.map.emplace (key, stmt);
	Shared_stmt_cache.entries++;
	xcache_shared_stmt_account ((INT32) stmt->bytes, 1);
      }
  }
  Shared_stmt_cache.bytes += (long long) stmt->bytes;
  Shared_stmt_cache.publishes++;
  if (Shared_stmt_cache.entries > max_entries ())
    {
      trim (max_entries () - max_entries () / 10);
    }
  *out = stmt;
  return NO_ERROR;
}

void
db_shared_stmt_release (DB_SHARED_STMT * stmt)
{
  if (stmt != NULL)
    {
      drop_ref (stmt);
    }
}

void
db_shared_stmt_invalidate (DB_SHARED_STMT * stmt)
{
  if (stmt != NULL && !stmt->invalid.exchange (true))
    {
      unlink (stmt);
    }
}

int
db_shared_stmt_stmt_type (const DB_SHARED_STMT * stmt)
{
  return stmt->stmt_type;
}

int
db_shared_stmt_num_markers (const DB_SHARED_STMT * stmt)
{
  return stmt->num_markers;
}

bool
db_shared_stmt_oids_included (const DB_SHARED_STMT * stmt)
{
  return stmt->oids_included;
}

const DB_QUERY_TYPE *
db_shared_stmt_columns (const DB_SHARED_STMT * stmt)
{
  return stmt->columns;
}

void
db_shared_stmt_cacheinfo (const DB_SHARED_STMT * stmt, bool * use_plan_cache, bool * use_query_cache)
{
  if (use_plan_cache != NULL)
    {
      *use_plan_cache = stmt->use_plan_cache;
    }
  if (use_query_cache != NULL)
    {
      *use_query_cache = stmt->use_query_cache;
    }
}

bool
db_shared_stmt_jdbc_cache_hint (const DB_SHARED_STMT * stmt, int *life_time)
{
  if (stmt->jdbc_cache_hint && life_time != NULL)
    {
      *life_time = stmt->jdbc_life_time;
    }
  return stmt->jdbc_cache_hint;
}

TP_DOMAIN *
db_shared_stmt_marker_domain (const DB_SHARED_STMT * stmt, int index)
{
  if (index < 0 || index >= stmt->num_markers)
    {
      return NULL;
    }
  return stmt->expected_domains[index];
}

/*
 * db_shared_stmt_execute () - the XASL-only execution path (design §4, §12.2).  Mirrors
 *   db_execute_and_keep_statement_local () + do_execute_statement () + do_execute_{select,update,delete,insert} ()
 *   for the statement shapes the publish predicate admits.
 */
int
db_shared_stmt_execute (DB_SHARED_STMT * stmt, int num_values, DB_VALUE * values, const DB_SHARED_EXEC_OPTS * opts,
			DB_QUERY_RESULT ** result, CACHE_TIME * clt_cache_time, bool * clt_cache_reusable,
			bool * needs_tree)
{
  int err = NO_ERROR;
  int total = stmt->num_markers + stmt->auto_param_count;
  DB_VALUE *vals = NULL;
  QFILE_LIST_ID *list_id = NULL;
  QUERY_ID query_id = NULL_QUERY_ID;
  QUERY_FLAG query_flag;
  CACHE_TIME srv_cache_time;
  XASL_ID xasl_id;
  LC_FETCH_VERSION_TYPE saved_fetch_version = TM_TRAN_READ_FETCH_VERSION ();
  bool use_auto_commit = false;
  bool retried = false;
  int au_save;

  *needs_tree = false;
  if (result != NULL)
    {
      *result = NULL;
    }
  if (clt_cache_reusable != NULL)
    {
      *clt_cache_reusable = false;
    }
  CACHE_TIME_RESET (&srv_cache_time);

  if (stmt->invalid.load () || num_values != stmt->num_markers)
    {
      *needs_tree = true;
      return NO_ERROR;
    }

  /* bind */
  if (total > 0)
    {
      vals = (DB_VALUE *) malloc (sizeof (DB_VALUE) * total);
      if (vals == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (DB_VALUE) * total);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      for (int i = 0; i < total; i++)
	{
	  db_make_null (&vals[i]);
	}
      if (!bind_values (stmt, num_values, values, vals))
	{
	  for (int i = 0; i < num_values; i++)
	    {
	      pr_clear_value (&vals[i]);
	    }
	  free (vals);
	  er_clear ();
	  Shared_stmt_cache.fallbacks++;
	  *needs_tree = true;
	  return NO_ERROR;
	}
      /* auto-parameterized literals are read-only for the executor: share the payload, never clear */
      for (int i = 0; i < stmt->auto_param_count; i++)
	{
	  vals[stmt->num_markers + i] = stmt->auto_params[i];
	}
    }

  /* db_set_statement_auto_commit: a single statement, so "last statement" holds */
  if (opts->auto_commit && stmt->can_execute_with_autocommit)
    {
      bool has_user_trigger = false;
      err = tr_has_user_trigger (&has_user_trigger);
      if (err != NO_ERROR)
	{
	  goto cleanup;
	}
      use_auto_commit = !has_user_trigger;
    }

  /* do_execute_statement: fetch version per statement kind; DML also honours the modification guard */
  if (stmt->kind == SHARED_KIND_UPDATE || stmt->kind == SHARED_KIND_DELETE)
    {
      if (db_Disable_modifications > 0)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_DB_NO_MODIFICATIONS, 0);
	  err = ER_DB_NO_MODIFICATIONS;
	  goto cleanup;
	}
      db_set_read_fetch_instance_version (LC_FETCH_DIRTY_VERSION);
    }
  else
    {
      if (stmt->kind == SHARED_KIND_INSERT && db_Disable_modifications > 0)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_DB_NO_MODIFICATIONS, 0);
	  err = ER_DB_NO_MODIFICATIONS;
	  goto cleanup;
	}
      db_set_read_fetch_instance_version (LC_FETCH_MVCC_VERSION);
    }

  /* query flags (do_execute_select / update / delete / insert) */
  query_flag = DEFAULT_EXEC_MODE;
  switch (stmt->kind)
    {
    case SHARED_KIND_SELECT:
      if (stmt->reexecute)
	{
	  query_flag |= NOT_FROM_RESULT_CACHE;
	}
      if (stmt->do_cache)
	{
	  query_flag |= RESULT_CACHE_REQUIRED;
	}
      if (stmt->do_not_cache || stmt->oids_included)
	{
	  query_flag |= RESULT_CACHE_INHIBITED;
	}
      if (opts->holdable)
	{
	  query_flag |= RESULT_HOLDABLE;
	}
      break;
    case SHARED_KIND_INSERT:
      query_flag |= NOT_FROM_RESULT_CACHE;
      query_flag |= RESULT_CACHE_INHIBITED;
      break;
    default:
      query_flag |= NOT_FROM_RESULT_CACHE;
      break;
    }
  if (opts->pinned_reference)
    {
      query_flag |= XASL_CACHE_PINNED_REFERENCE;
    }
  if (use_auto_commit)
    {
      query_flag |= EXECUTE_QUERY_WITH_COMMIT;
    }
  if (opts->auto_commit)
    {
      query_flag |= TRAN_AUTO_COMMIT;
    }

  /* flush what the statement may read or write */
  if (use_auto_commit && ws_need_flush ())
    {
      if (stmt->kind == SHARED_KIND_SELECT)
	{
	  if (tm_Use_OID_preflush)
	    {
	      (void) locator_assign_all_permanent_oids ();
	    }
	  err = locator_all_flush ();
	}
      else
	{
	  err = tran_flush_to_commit ();
	}
      if (err != NO_ERROR)
	{
	  goto cleanup;
	}
    }
  if (ws_has_updated ())
    {
      err = flush_classes (stmt);
      if (err != NO_ERROR)
	{
	  goto cleanup;
	}
    }

execute:
  {
    std::lock_guard < std::mutex > lock (stmt->xasl_id_mutex);
    XASL_ID_COPY (&xasl_id, &stmt->xasl_id);
  }
  AU_SAVE_AND_ENABLE (au_save);	/* this insures authorization checking for method */
  query_id = NULL_QUERY_ID;
  list_id = NULL;
  err = execute_query (&xasl_id, &query_id, total, vals, &list_id, query_flag,
		       (stmt->kind == SHARED_KIND_SELECT) ? clt_cache_time : NULL,
		       (stmt->kind == SHARED_KIND_SELECT) ? &srv_cache_time : NULL);
  AU_RESTORE (au_save);
  stmt->executions++;

  if (err != NO_ERROR && !retried
      && (err == ER_QPROC_INVALID_XASLNODE || err == ER_QPROC_XASLNODE_RECOMPILE_REQUESTED
	  || err == ER_QPROC_RESULT_CACHE_INVALID))
    {
      /* the plan was replaced or dropped before execution started: same recovery as
       * db_execute_and_keep_statement_local (), minus the tree */
      Shared_stmt_cache.stale++;
      DB_CLASS_MODIFICATION_STATUS cls_status = classes_modified (stmt);
      if (cls_status == DB_CLASS_MODIFIED)
	{
	  db_shared_stmt_invalidate (stmt);
	  er_clear ();
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
	  err = ER_QPROC_INVALID_XASLNODE;
	  goto cleanup;
	}
      if (cls_status == DB_CLASS_ERROR)
	{
	  err = er_errid () != NO_ERROR ? er_errid () : ER_FAILED;
	  goto cleanup;
	}
      if (err != ER_QPROC_RESULT_CACHE_INVALID && refresh_xasl_id (stmt))
	{
	  er_clear ();
	  retried = true;
	  goto execute;
	}
      /* no plan for this SHA-1 any more: the session compiles one and republishes */
      db_shared_stmt_invalidate (stmt);
      er_clear ();
      err = NO_ERROR;
      Shared_stmt_cache.fallbacks++;
      *needs_tree = true;
      goto cleanup;
    }
  if (err != NO_ERROR)
    {
      goto cleanup;
    }

  /* results */
  switch (stmt->kind)
    {
    case SHARED_KIND_SELECT:
      if (clt_cache_time != NULL && clt_cache_reusable != NULL && CACHE_TIME_EQ (clt_cache_time, &srv_cache_time))
	{
	  *clt_cache_reusable = true;
	}
      if (result != NULL)
	{
	  if (*clt_cache_reusable)
	    {
	      *result = pt_make_cache_hit_result_descriptor ();
	      if (list_id != NULL)
		{
		  cursor_free_self_list_id (list_id);
		}
	    }
	  else
	    {
	      *result = make_select_result (stmt, query_id, list_id, opts->holdable, &srv_cache_time);
	    }
	  list_id = NULL;
	  if (*result == NULL)
	    {
	      err = er_errid () != NO_ERROR ? er_errid () : ER_FAILED;
	      goto cleanup;
	    }
	  err = db_query_tuple_count (*result);
	}
      else if (list_id != NULL)
	{
	  err = list_id->tuple_cnt;
	  cursor_free_self_list_id (list_id);
	  list_id = NULL;
	}
      break;
    case SHARED_KIND_UPDATE:
    case SHARED_KIND_DELETE:
      err = 0;
      if (list_id != NULL)
	{
	  if (list_id->tuple_cnt > 0)
	    {
	      for (const shared_class_ref & ref : stmt->classes)
		{
		  if (!ref.modified_target)
		    {
		      continue;
		    }
		  MOP clsmop = ws_mop (&ref.oid, NULL);
		  int ferr;
		  if (use_auto_commit && tran_was_latest_query_committed ())
		    {
		      ferr = sm_decache_instances_after_query_executed_with_commit (clsmop);
		    }
		  else
		    {
		      ferr = sm_flush_and_decache_objects (clsmop, true);
		    }
		  if (ferr != NO_ERROR)
		    {
		      err = ferr;
		      break;
		    }
		}
	    }
	  if (err >= NO_ERROR)
	    {
	      err = list_id->tuple_cnt;
	    }
	  cursor_free_self_list_id (list_id);
	  list_id = NULL;
	}
      if (query_id > 0 && !tran_was_latest_query_ended () && er_errid () != ER_LK_UNILATERALLY_ABORTED)
	{
	  qmgr_end_query (query_id);
	}
      query_id = NULL_QUERY_ID;
      break;
    case SHARED_KIND_INSERT:
      err = 0;
      if (list_id != NULL)
	{
	  err = list_id->tuple_cnt;
	  cursor_free_self_list_id (list_id);
	  list_id = NULL;
	}
      if (query_id > 0 && !tran_was_latest_query_ended () && er_errid () != ER_LK_UNILATERALLY_ABORTED)
	{
	  qmgr_end_query (query_id);
	}
      query_id = NULL_QUERY_ID;
      break;
    }

cleanup:
  if (list_id != NULL)
    {
      cursor_free_self_list_id (list_id);
    }
  db_set_read_fetch_instance_version (saved_fetch_version);
  if (er_errid () == ER_LK_UNILATERALLY_ABORTED)
    {
      err = ER_LK_UNILATERALLY_ABORTED;
    }
  if (err == ER_LK_UNILATERALLY_ABORTED || tran_was_latest_query_aborted ())
    {
      (void) tran_abort_only_client (false);
    }
  if (vals != NULL)
    {
      for (int i = 0; i < stmt->num_markers; i++)
	{
	  pr_clear_value (&vals[i]);
	}
      free (vals);
    }
  if (err < 0 && !(*needs_tree))
    {
      if (result != NULL && *result != NULL)
	{
	  db_free_query_result (*result);
	  *result = NULL;
	}
      return ((err == ER_FAILED && (err = er_errid ()) == NO_ERROR) ? ER_GENERIC_ERROR : err);
    }
  return err;
}

void
db_shared_stmt_dump (FILE * fp)
{
  fprintf (fp, "Shared statement descriptors:\n");
  fprintf (fp, "  Entries:                    %d\n", Shared_stmt_cache.entries.load ());
  fprintf (fp, "  Bytes:                      %lld\n", Shared_stmt_cache.bytes.load ());
  fprintf (fp, "  Lookups:                    %lld\n", Shared_stmt_cache.lookups.load ());
  fprintf (fp, "  Hits:                       %lld\n", Shared_stmt_cache.hits.load ());
  fprintf (fp, "  Publishes:                  %lld\n", Shared_stmt_cache.publishes.load ());
  fprintf (fp, "  Refused:                    %lld\n", Shared_stmt_cache.refused.load ());
  fprintf (fp, "  Evictions:                  %lld\n", Shared_stmt_cache.evictions.load ());
  fprintf (fp, "  Stale executions:           %lld\n", Shared_stmt_cache.stale.load ());
  fprintf (fp, "  Tree fallbacks:             %lld\n", Shared_stmt_cache.fallbacks.load ());
}

void
db_shared_stmt_trim_to (int target_entries)
{
  trim (target_entries);
}

void
db_shared_stmt_final (void)
{
  trim (0);
}

#else /* !SERVER_MODE */

bool
db_shared_stmt_enabled (void)
{
  return false;
}

DB_SHARED_STMT *
db_shared_stmt_lookup (const char *sql, int key_flags)
{
  return NULL;
}

int
db_shared_stmt_publish (struct db_session *session, int stmt_id, const char *sql, int key_flags,
			DB_SHARED_STMT ** out)
{
  *out = NULL;
  return NO_ERROR;
}

void
db_shared_stmt_release (DB_SHARED_STMT * stmt)
{
}

void
db_shared_stmt_invalidate (DB_SHARED_STMT * stmt)
{
}

int
db_shared_stmt_stmt_type (const DB_SHARED_STMT * stmt)
{
  return 0;
}

int
db_shared_stmt_num_markers (const DB_SHARED_STMT * stmt)
{
  return 0;
}

bool
db_shared_stmt_oids_included (const DB_SHARED_STMT * stmt)
{
  return false;
}

const DB_QUERY_TYPE *
db_shared_stmt_columns (const DB_SHARED_STMT * stmt)
{
  return NULL;
}

void
db_shared_stmt_cacheinfo (const DB_SHARED_STMT * stmt, bool * use_plan_cache, bool * use_query_cache)
{
}

bool
db_shared_stmt_jdbc_cache_hint (const DB_SHARED_STMT * stmt, int *life_time)
{
  return false;
}

struct tp_domain *
db_shared_stmt_marker_domain (const DB_SHARED_STMT * stmt, int index)
{
  return NULL;
}

int
db_shared_stmt_execute (DB_SHARED_STMT * stmt, int num_values, DB_VALUE * values, const DB_SHARED_EXEC_OPTS * opts,
			DB_QUERY_RESULT ** result, CACHE_TIME * clt_cache_time, bool * clt_cache_reusable,
			bool * needs_tree)
{
  *needs_tree = true;
  return NO_ERROR;
}

void
db_shared_stmt_dump (FILE * fp)
{
}

void
db_shared_stmt_trim_to (int target_entries)
{
}

void
db_shared_stmt_final (void)
{
}

#endif /* !SERVER_MODE */
