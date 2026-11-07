/*
 *  Copyright 2016 CUBRID Corporation
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

/*
 * server_compile_tracer.cpp - in-process compile tracer
 *
 * Each session thread calls the client half (parser/compiler) in the same
 * address space, compiles one SQL statement, hands the XASL to this process's
 * executor and fetches a result, with no network hop. A smoke driver until the
 * wire path drives the same client half.
 *
 * Gate: env CUBRID_M0_TRACER_SQL is set when cub_server boots.  Output goes to
 * the file named by CUBRID_M0_TRACER_OUT (default: m0_tracer.out in cwd).
 * Optional: CUBRID_M0_TRACER_ISOLATION=RR, CUBRID_M0_TRACER_PRE (statements run
 * first), CUBRID_M0_TRACER_BIND (integers bound to the case's host variables),
 * CUBRID_M0_TRACER_SESSIONS (concurrent sessions, each with its own client
 * context) and CUBRID_M0_TRACER_END=1 (end the session from inside it).
 *
 * Known limit (deliberate): a session never shuts its client down
 * (boot_shutdown_client would finalize modules shared with the server half);
 * its context is retired with its server session.
 */

#if defined (SERVER_MODE)

#include "server_compile_tracer.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <system_error>
#include <mutex>
#include <thread>
#include <vector>

#include "client_session_context.hpp"
#include "connection_defs.h"
#include "connection_sr.h"
#include "memory_alloc.h"	// free_and_init
#include "network_interface_cl.h"	// boot_unregister_client
#include "session.h"		// session_adopt_client_context
#include "storage_common.h"	// NULL_TRAN_INDEX
#include "transaction_cl.h"	// tm_Tran_index
#include "db.h"
#include "db_client_type.hpp"
#include "dbtype.h"
#include "db_value_printer.hpp"
#include "error_manager.h"
#include "thread_entry.hpp"
#include "thread_manager.hpp"
// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

static void tracer_main (char *server_name, char *sql, char *out_path);

/* thread-local server/client boundary flag (see network_interface_sr.cpp) */
extern thread_local unsigned int db_on_server;

void
boot_tracer_start_if_requested (const char *server_name)
{
  const char *sql = std::getenv ("CUBRID_M0_TRACER_SQL");
  if (sql == NULL || *sql == '\0')
    {
      return;
    }
  const char *out = std::getenv ("CUBRID_M0_TRACER_OUT");
  if (out == NULL || *out == '\0')
    {
      out = "m0_tracer.out";
    }

  char *server_name_dup = strdup (server_name);
  char *sql_dup = strdup (sql);
  char *out_dup = strdup (out);
  if (server_name_dup == NULL || sql_dup == NULL || out_dup == NULL)
    {
      fprintf (stderr, "M0_TRACER: FAIL argument allocation\n");
      free_and_init (server_name_dup);
      free_and_init (sql_dup);
      free_and_init (out_dup);
      return;
    }

  /* std::thread is the engine idiom (connection_worker, coordinator,
   * master_server_monitor all spawn it directly); its constructor throws on
   * resource exhaustion, so catch it — the tracer must fail, not cub_server */
  try
    {
      std::thread (tracer_main, server_name_dup, sql_dup, out_dup).detach ();
    }
  catch (const std::system_error &)
    {
      fprintf (stderr, "M0_TRACER: FAIL thread creation\n");
      free_and_init (server_name_dup);
      free_and_init (sql_dup);
      free_and_init (out_dup);
    }
}

/* client boot and unregistration are not reentrant yet: one session in them at a time */
static std::mutex tracer_Boot_mutex;

static FILE *tracer_Fp = NULL;
static std::mutex tracer_Log_mutex;

static void
tracer_log (const char *fmt, ...)
{
  std::lock_guard<std::mutex> guard (tracer_Log_mutex);
  va_list ap;
  va_start (ap, fmt);
  vfprintf (tracer_Fp, fmt, ap);
  va_end (ap);
  fputc ('\n', tracer_Fp);
  fflush (tracer_Fp);
}

/* one in-process client session: register, run the statements, commit, unregister; true when every stage
 * succeeded */
static bool
tracer_session (int sid, const char *server_name, const char *sql)
{
  int err;
  bool succeeded = false;
  bool registered = false;
  bool adopted = false;
  CSS_CONN_ENTRY *conn = NULL;
  client_session_context *ctx = NULL;

  // register this foreign thread with the thread manager (same ritual as
  // connection_worker.cpp)
  cubthread::entry *entry_p = cubthread::get_manager ()->claim_entry ();
  if (entry_p == NULL)
    {
      tracer_log ("M0_TRACER: S%d FAIL claim_entry", sid);
      return false;
    }
  entry_p->register_id ();
  entry_p->type = TT_SERVER;
  entry_p->tran_index = -1;
  entry_p->m_status = cubthread::entry::status::TS_RUN;
  entry_p->shutdown = false;
  entry_p->get_error_context ().register_thread_local ();

  /* the session's client context; the server session adopts it after registration and retires it when the
   * session ends */
  ctx = new client_session_context ();
  csc_activate (ctx);

  /* the server half anchors connection state and the session on
   * thread_p->conn_entry; give this in-process client a socketless entry from
   * the server's own pool (status = CONN_OPEN, fd = INVALID_SOCKET) */
  conn = css_make_conn (INVALID_SOCKET);
  if (conn == NULL)
    {
      tracer_log ("M0_TRACER: S%d FAIL css_make_conn", sid);
      goto retire;
    }
  /* the session layer's debug invariant (session_state_verify_ref_count)
   * counts references by walking css_Active_conn_anchor, so this conn must be
   * discoverable there like any real client's */
  css_insert_into_active_conn_list (conn);
  entry_p->conn_entry = conn;

  /* this thread now acts as the in-process client: start in client context;
   * enter_server/exit_server (network_interface_cl.c) toggle it per x-call */
  db_on_server = 0;

  {
    std::lock_guard<std::mutex> boot_guard (tracer_Boot_mutex);
    err = db_restart_ex ("m0_tracer", server_name, "DBA", "", NULL, DB_CLIENT_TYPE_DEFAULT);
  }
  if (err != NO_ERROR)
    {
      tracer_log ("M0_TRACER: S%d FAIL db_restart_ex err=%d msg=[%s]", sid, err, er_msg () ? er_msg () : "");
      goto retire;
    }
  registered = true;
  tracer_log ("M0_TRACER: S%d in-process client registered (0-hop)", sid);

  if (session_adopt_client_context (entry_p, ctx) != NO_ERROR)
    {
      tracer_log ("M0_TRACER: S%d FAIL session_adopt_client_context", sid);
      goto retire;
    }
  adopted = true;

  /* CUBRID_M0_TRACER_ISOLATION=RR: compile under REPEATABLE READ, which takes the RR transaction lock */
  if (const char *iso_env = std::getenv ("CUBRID_M0_TRACER_ISOLATION"))
    {
      if (strcmp (iso_env, "RR") != 0 || db_set_isolation (TRAN_REPEATABLE_READ) != NO_ERROR)
	{
	  tracer_log ("M0_TRACER: S%d FAIL isolation [%s] msg=[%s]", sid, iso_env, er_msg () ? er_msg () : "");
	  goto retire;
	}
      tracer_log ("M0_TRACER: S%d isolation set to REPEATABLE READ", sid);
    }

  /* CUBRID_M0_TRACER_PRE: statements (';'-separated) run in the same transaction before the case, each must succeed */
  if (const char *pre_env = std::getenv ("CUBRID_M0_TRACER_PRE"))
    {
      DB_SESSION *pre = db_open_buffer (pre_env);
      int pre_id;

      if (pre == NULL)
	{
	  tracer_log ("M0_TRACER: S%d FAIL pre db_open_buffer msg=[%s]", sid, er_msg () ? er_msg () : "");
	  goto retire;
	}
      while ((pre_id = db_compile_statement (pre)) > 0)
	{
	  DB_QUERY_RESULT *pre_result = NULL;

	  err = db_execute_statement (pre, pre_id, &pre_result);
	  if (pre_result != NULL)
	    {
	      db_query_end (pre_result);
	    }
	  if (err < 0)
	    {
	      break;
	    }
	  tracer_log ("M0_TRACER: S%d pre statement %d executed, result=%d", sid, pre_id, err);
	}
      if (pre_id < 0 || err < 0)
	{
	  tracer_log ("M0_TRACER: S%d FAIL pre statement err=%d msg=[%s]", sid, pre_id < 0 ? pre_id : err,
		      er_msg () ? er_msg () : "");
	  db_close_session (pre);
	  goto retire;
	}
      db_close_session (pre);
    }

  {
    DB_SESSION *session = db_open_buffer (sql);
    if (session == NULL)
      {
	tracer_log ("M0_TRACER: S%d FAIL db_open_buffer msg=[%s]", sid, er_msg () ? er_msg () : "");
	goto retire;
      }

    int stmt_id = db_compile_statement (session);
    if (stmt_id < 0)
      {
	tracer_log ("M0_TRACER: S%d FAIL db_compile_statement err=%d msg=[%s]", sid, stmt_id,
		    er_msg () ? er_msg () : "");
	db_close_session (session);
	goto retire;
      }
    tracer_log ("M0_TRACER: S%d compiled in server address space, stmt_id=%d", sid, stmt_id);

    /* CUBRID_M0_TRACER_BIND: comma-separated integers bound to the case's host variables */
    if (const char *bind_env = std::getenv ("CUBRID_M0_TRACER_BIND"))
      {
	DB_VALUE bind_vals[16];
	int bind_cnt = 0;
	const char *p = bind_env;
	char *endp = NULL;

	while (bind_cnt < 16)
	  {
	    long v = strtol (p, &endp, 10);
	    if (endp == p)
	      {
		break;
	      }
	    db_make_int (&bind_vals[bind_cnt++], (int) v);
	    if (*endp != ',')
	      {
		break;
	      }
	    p = endp + 1;
	  }
	if (db_push_values (session, bind_cnt, bind_vals) != NO_ERROR)
	  {
	    tracer_log ("M0_TRACER: S%d FAIL db_push_values msg=[%s]", sid, er_msg () ? er_msg () : "");
	    db_close_session (session);
	    goto retire;
	  }
	tracer_log ("M0_TRACER: S%d bound %d host variable(s)", sid, bind_cnt);
      }

    DB_QUERY_RESULT *result = NULL;
    err = db_execute_statement (session, stmt_id, &result);
    if (err < 0)
      {
	tracer_log ("M0_TRACER: S%d FAIL db_execute_statement err=%d msg=[%s]", sid, err, er_msg () ? er_msg () : "");
	db_close_session (session);
	goto retire;
      }
    tracer_log ("M0_TRACER: S%d executed, row_count=%d", sid, err);

    /* the gate contract is connect→prepare→execute→fetch→commit: every stage
     * must fail loudly, or a fetch/commit regression sails through as SUCCESS */
    if (result == NULL)
      {
	tracer_log ("M0_TRACER: S%d FAIL no result to fetch", sid);
	db_close_session (session);
	goto retire;
      }
    err = db_query_first_tuple (result);
    if (err != DB_CURSOR_SUCCESS)
      {
	tracer_log ("M0_TRACER: S%d FAIL db_query_first_tuple err=%d msg=[%s]", sid, err, er_msg () ? er_msg () : "");
	db_query_end (result);
	db_close_session (session);
	goto retire;
      }
    DB_VALUE value;
    err = db_query_get_tuple_value (result, 0, &value);
    if (err != NO_ERROR)
      {
	tracer_log ("M0_TRACER: S%d FAIL db_query_get_tuple_value err=%d msg=[%s]", sid, err,
		    er_msg () ? er_msg () : "");
	db_query_end (result);
	db_close_session (session);
	goto retire;
      }
    {
      std::lock_guard<std::mutex> guard (tracer_Log_mutex);
      fprintf (tracer_Fp, "M0_TRACER: first value = ");
      db_fprint_value (tracer_Fp, &value);
      fputc ('\n', tracer_Fp);
      fflush (tracer_Fp);
    }
    db_value_clear (&value);
    db_query_end (result);
    db_close_session (session);

    err = db_commit_transaction ();
    if (err != NO_ERROR)
      {
	tracer_log ("M0_TRACER: S%d FAIL db_commit_transaction err=%d msg=[%s]", sid, err, er_msg () ? er_msg () : "");
	goto retire;
      }

    /* CUBRID_M0_TRACER_END=1: end the server session from inside the bracket (db_end_session), so the context is
     * retired by its own bracket exit */
    if (const char *end_env = std::getenv ("CUBRID_M0_TRACER_END"))
      {
	if (strcmp (end_env, "1") == 0)
	  {
	    err = db_end_session ();
	    if (err != NO_ERROR)
	      {
		tracer_log ("M0_TRACER: S%d FAIL db_end_session err=%d msg=[%s]", sid, err, er_msg () ? er_msg () : "");
		goto retire;
	      }
	    tracer_log ("M0_TRACER: S%d session ended", sid);
	  }
      }
    succeeded = true;
  }

retire:
  // the conn and the entry must not outlive this thread dirty:
  // - the socketless conn left in css_Active_conn_anchor makes the shutdown
  //   path "close" an orphan client on the main thread, stamping Main_entry_p
  //   with the tracer's tran_index and TS_FREE — the next suspend on the main
  //   entry (shutdown checkpoint's DWB wait) then assert-aborts
  //   (thread_entry.cpp)
  // - retire_entry() returns the entry to the pool as-is, so scrub the fields
  //   this tracer stamped before another thread reuses it
  if (registered)
    {
      /* log the tran out the way a real disconnect does (frees the tdes /
       * tran index). Committing is not enough: the committed tdes goes back
       * to ACTIVE for the next transaction, and a tran left ACTIVE at
       * shutdown makes log_abort_all_active_transaction push its abort task
       * inline onto the main thread, whose execute() stamps Main_entry_p
       * with TS_FREE — the shutdown checkpoint's DWB wait then asserts. */
      std::lock_guard<std::mutex> boot_guard (tracer_Boot_mutex);
      (void) boot_unregister_client (tm_Tran_index);
      tm_Tran_index = NULL_TRAN_INDEX;
    }
  if (conn != NULL)
    {
      /* drops the conn's session reference: from here the session can expire and retire the context, which waits
       * for this bracket */
      entry_p->conn_entry = NULL;
      css_free_conn (conn);
    }
  entry_p->tran_index = NULL_TRAN_INDEX;
  entry_p->m_status = cubthread::entry::status::TS_DEAD;
  csc_deactivate ();
  if (!adopted)
    {
      /* no session owns it: the creator retires it */
      csc_retire_and_delete (ctx);
    }
  entry_p->get_error_context ().deregister_thread_local ();
  entry_p->unregister_id ();
  cubthread::get_manager ()->retire_entry (*entry_p);
  return succeeded;
}

static void
tracer_main (char *server_name, char *sql, char *out_path)
{
  // let the server finish entering its service loop before we register as an
  // in-process client
  std::this_thread::sleep_for (std::chrono::seconds (2));

  tracer_Fp = fopen (out_path, "w");
  if (tracer_Fp == NULL)
    {
      tracer_Fp = stderr;
    }

  /* CUBRID_M0_TRACER_SESSIONS: concurrent in-process sessions (default 1), each with its own context */
  int sessions = 1;
  if (const char *sess_env = std::getenv ("CUBRID_M0_TRACER_SESSIONS"))
    {
      sessions = atoi (sess_env);
      if (sessions < 1 || sessions > 64)
	{
	  sessions = 1;
	}
    }
  tracer_log ("M0_TRACER: start db=%s sessions=%d sql=[%s]", server_name, sessions, sql);

  std::atomic<int> ok_count (0);
  std::vector<std::thread> threads;
  bool spawn_failed = false;
  for (int sid = 1; sid <= sessions; sid++)
    {
      try
	{
	  threads.emplace_back ([sid, server_name, sql, &ok_count] ()
	  {
	    if (tracer_session (sid, server_name, sql))
	      {
		ok_count.fetch_add (1);
	      }
	  });
	}
      catch (const std::system_error &)
	{
	  tracer_log ("M0_TRACER: FAIL thread creation for S%d", sid);
	  spawn_failed = true;
	  break;
	}
    }
  for (auto &th : threads)
    {
      th.join ();
    }

  // SUCCESS is the harness's cue to stop the server, so it must be the very
  // last act — logging it before the sessions' teardown re-opens the shutdown
  // race that teardown exists to close (the orphan conn corrupting Main_entry_p).
  if (!spawn_failed && ok_count.load () == sessions)
    {
      tracer_log ("M0_TRACER: SUCCESS");
    }
  else
    {
      tracer_log ("M0_TRACER: FAIL %d/%d sessions succeeded", ok_count.load (), sessions);
    }
  if (tracer_Fp != stderr)
    {
      fclose (tracer_Fp);
      tracer_Fp = NULL;
    }
  free_and_init (server_name);
  free_and_init (sql);
  free_and_init (out_path);
}

#endif /* SERVER_MODE */
