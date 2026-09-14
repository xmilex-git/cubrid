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
 * db_shared_stmt.hpp - process-wide shared prepared-statement descriptors (workspace #266, P1/P2/P3)
 *
 * A descriptor is the immutable, MOP-free result of one compilation: the SQL text, the statement kind,
 * the result column metadata, the host-variable domains, the auto-parameterized literals, the XASL id and
 * the referenced classes.  Sessions (CAS statement handles) that prepare the same text under the same
 * DB user and the same compile-affecting settings share one descriptor instead of each parsing, compiling
 * and keeping a DB_SESSION / parse tree of their own.  Everything mutable per execution (bind values,
 * cursor, result) stays on the handle; the descriptor never changes after publication.
 *
 * Only the server fold (SERVER_MODE) keeps a cache; the CS/SA builds compile the stubs below so that the
 * CAS speaker code has one shape.
 */

#ifndef _DB_SHARED_STMT_HPP_
#define _DB_SHARED_STMT_HPP_

#include <stdio.h>

#include "dbtype_def.h"
#include "db_query.h"
#include "cache_time.h"

struct db_session;
struct tp_domain;

#ifdef __cplusplus
extern "C"
{
#endif

  typedef struct db_shared_stmt DB_SHARED_STMT;

  /* key bits that change the compilation result but are not part of the SQL text nor of alias_print */
#define DB_SHARED_STMT_KEY_INCLUDE_OID   0x1	/* CCI_PREPARE_INCLUDE_OID / UPDATABLE: row OID column added */

  typedef struct db_shared_exec_opts DB_SHARED_EXEC_OPTS;
  struct db_shared_exec_opts
  {
    bool auto_commit;		/* handle auto-commit mode (parser->flag.is_auto_commit of the tree path) */
    bool holdable;		/* handle holdable flag */
    bool pinned_reference;	/* CCI_PREPARE_XASL_CACHE_PINNED still armed on the handle */
  };

  /* descriptor cache */
  extern bool db_shared_stmt_enabled (void);
  extern DB_SHARED_STMT *db_shared_stmt_lookup (const char *sql, int key_flags);
  extern int db_shared_stmt_publish (struct db_session *session, int stmt_id, const char *sql, int key_flags,
				     DB_SHARED_STMT ** out);
  extern void db_shared_stmt_release (DB_SHARED_STMT * stmt);
  extern void db_shared_stmt_invalidate (DB_SHARED_STMT * stmt);

  /* immutable properties */
  extern int db_shared_stmt_stmt_type (const DB_SHARED_STMT * stmt);	/* CUBRID_STMT_TYPE */
  extern int db_shared_stmt_num_markers (const DB_SHARED_STMT * stmt);
  extern bool db_shared_stmt_oids_included (const DB_SHARED_STMT * stmt);
  extern const DB_QUERY_TYPE *db_shared_stmt_columns (const DB_SHARED_STMT * stmt);	/* NULL unless SELECT */
  extern void db_shared_stmt_cacheinfo (const DB_SHARED_STMT * stmt, bool * use_plan_cache, bool * use_query_cache);
  extern bool db_shared_stmt_jdbc_cache_hint (const DB_SHARED_STMT * stmt, int *life_time);
  extern struct tp_domain *db_shared_stmt_marker_domain (const DB_SHARED_STMT * stmt, int index);

  /*
   * Execute the shared statement with the given user bind values (num_values == num_markers).
   * Returns the affected/tuple count (>= 0) or an error code (< 0); *result is set for SELECT.
   * *needs_tree == true means the descriptor could not serve this execution (stale XASL, coercion
   * error, unsupported option): no side effect happened and the caller must fall back to the
   * per-session compile path (which republishes).  Errors returned with *needs_tree == false carry
   * the same meaning as the tree path's errors (e.g. ER_QPROC_INVALID_XASLNODE after a schema change).
   */
  extern int db_shared_stmt_execute (DB_SHARED_STMT * stmt, int num_values, DB_VALUE * values,
				     const DB_SHARED_EXEC_OPTS * opts, DB_QUERY_RESULT ** result,
				     CACHE_TIME * clt_cache_time, bool * clt_cache_reusable, bool * needs_tree);

  /* P7 accounting */
  extern void db_shared_stmt_dump (FILE * fp);
  extern void db_shared_stmt_trim_to (int target_entries);
  extern void db_shared_stmt_final (void);

  /* db_vdb.c: the tree path's auto-commit eligibility of a compiled statement (immutable per statement) */
  extern bool db_statement_can_execute_with_autocommit (struct db_session *session, int stmt_ndx);

#ifdef __cplusplus
}
#endif

#endif /* _DB_SHARED_STMT_HPP_ */
