/*
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
 * client_session_context.cpp - see client_session_context.hpp
 */

#if defined (SERVER_MODE)

#include "client_session_context.hpp"

#include "parser.h"		// pt_free_label_table

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <cstring>
// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

/* client/server boundary flag (network_interface_sr.cpp) */
extern thread_local unsigned int db_on_server;

static thread_local client_session_context *tl_Csc_active = NULL;

static void csc_teardown (client_session_context *ctx);

client_session_context::client_session_context ()
{
  /* the CS build's static initializer of the credential; boot_client_common
   * fills it at registration */
  memset (&boot_server_credential, 0, sizeof (boot_server_credential));
  boot_server_credential.process_id = -1;
  OID_SET_NULL (&boot_server_credential.root_class_oid);
  HFID_SET_NULL (&boot_server_credential.root_class_hfid);
  boot_server_credential.page_size = -1;
  boot_server_credential.log_page_size = -1;
  boot_server_credential.ha_server_state = HA_SERVER_STATE_NA;
  memset (boot_server_credential.server_session_key, 0xFF, SERVER_SESSION_KEY_SIZE);
  boot_server_credential.db_charset = INTL_CODESET_NONE;
}

void
csc_activate (client_session_context *ctx)
{
  assert (ctx != NULL);
  if (tl_Csc_active != NULL)
    {
      /* a nested bracket would self-deadlock on the non-recursive mutex in a
       * release build; fail fast instead */
      assert (false);
      abort ();
    }
  ctx->bracket_mutex.lock ();
  tl_Csc_active = ctx;
}

void
csc_deactivate (void)
{
  client_session_context *ctx = tl_Csc_active;

  assert (ctx != NULL);
  /* read while the bracket still holds the context: once unlocked, a thread
   * retiring it may delete it */
  const bool orphaned = ctx->orphaned;
  if (orphaned)
    {
      csc_teardown (ctx);
    }
  tl_Csc_active = NULL;
  ctx->bracket_mutex.unlock ();
  if (orphaned)
    {
      /* the session is gone, so no other thread can reach this context */
      delete ctx;
    }
}

client_session_context *
csc_current (void)
{
  assert (tl_Csc_active != NULL);
  return tl_Csc_active;
}

bool
csc_bracket_is_active (void)
{
  return tl_Csc_active != NULL;
}

ws_context *
csc_ws (void)
{
  return &csc_current ()->ws;
}

tm_context *
csc_tm (void)
{
  return &csc_current ()->tm;
}

sm_context *
csc_sm (void)
{
  return &csc_current ()->sm;
}

tr_context *
csc_tr (void)
{
  return &csc_current ()->tr;
}

obt_context *
csc_obt (void)
{
  return &csc_current ()->obt;
}

db_cl_context *
csc_db (void)
{
  return &csc_current ()->db;
}

int *
csc_db_connect_status (void)
{
  /* a server thread outside any bracket sees the server's own status */
  static int server_connect_status = DB_CONNECTION_STATUS_NOT_CONNECTED;

  return tl_Csc_active != NULL ? &tl_Csc_active->db.connect_status : &server_connect_status;
}

PAGEID *
csc_next_tempid (void)
{
  return &csc_current ()->ws.next_tempid;
}

/* per-session teardown; the calling thread's bracket holds ctx */
static void
csc_teardown (client_session_context *ctx)
{
  assert (tl_Csc_active == ctx);

  /* client-half work: allocation routing must resolve to the session
   * workspace even when a server thread (session expiry) drives it */
  unsigned int save_on_server = db_on_server;
  db_on_server = 0;

  if (ctx->label_table != NULL)
    {
      pt_free_label_table ();
    }
  free_and_init (ctx->plan_dump_filename);
  free_and_init (ctx->obj_method_error_msg);
  free_and_init (ctx->db.execution_plan);

  if (ctx->ws.mop_table != NULL)
    {
      /* the MOP table, classname cache, resident classes and the session's
       * lea heap; the process-shared areas stay */
      ws_final (false);
    }
  else if (ctx->ws.heap_id != 0)
    {
      /* boot failed between heap creation and table build */
      db_destroy_workspace_heap ();
    }

  db_on_server = save_on_server;
}

void
csc_retire_and_delete (client_session_context *ctx)
{
  assert (ctx != NULL);

  if (tl_Csc_active == ctx)
    {
      /* the session ended from inside this context (db_end_session): its
       * caller still works on it, and re-activating would self-deadlock */
      ctx->orphaned = true;
      return;
    }

  csc_activate (ctx);
  csc_teardown (ctx);
  tl_Csc_active = NULL;
  ctx->bracket_mutex.unlock ();
  delete ctx;
}

#endif /* SERVER_MODE */
