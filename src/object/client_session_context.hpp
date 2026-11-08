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
 * client_session_context.hpp - session-scoped state of the client half
 *                              compiled into cub_server
 *
 * One instance holds what the client half keeps in process-wide globals in a
 * client process, for one session.  The session owns it (session_state csc_p)
 * and retires it when the session ends.
 *
 * A worker thread reaches a context only through an explicit activation
 * bracket around the work it does for that session.  The thread_local carries
 * the bracket, never the session state itself: worker threads are pooled per
 * request and have no session affinity.
 */

#ifndef _CLIENT_SESSION_CONTEXT_HPP_
#define _CLIENT_SESSION_CONTEXT_HPP_

#if defined (SERVER_MODE)

#include "authenticate_context.hpp"
#include "boot.h"
#include "db.h"
#include "object_template.h"
#include "schema_manager.h"
#include "transaction_cl.h"
#include "trigger_manager.h"
#include "work_space.h"

#include <mutex>
#include <unordered_map>
#include <vector>

// *INDENT-OFF*
struct mht_table;

namespace cubmethod
{
  class callback_handler;
}

class client_session_context
{
  public:
    /* worker threads are pooled per request with no session affinity: every
     * bracket holds this for its whole extent, so the client-half work of one
     * session is serialized, and a retirement waits for the bracket inside */
    std::mutex bracket_mutex;

    authenticate_context au_context;

    /* the module state of the client half, one per session */
    ws_context ws;
    tm_context tm;
    sm_context sm;
    tr_context tr;
    obt_context obt;
    db_cl_context db;

    /* boot_cl.c: the credential of this session's registration
     * (server_session_key must not be shared between sessions) */
    BOOT_SERVER_CREDENTIAL boot_server_credential;

    /* object_accessor.c: method invocation nesting and the last method error */
    int obj_method_call_level = 0;
    char *obj_method_error_msg = nullptr;

    /* execute_schema.c: online-index thread count of the running DDL */
    int ib_thread_count = 0;

    /* execute_statement.c: savepoint name counters */
    int insert_savepoint_number = 0;
    int update_savepoint_number = 0;
    int delete_savepoint_number = 0;
    int merge_savepoint_number = 0;
    int tr_savepoint_number = 0;

    /* interpreter labels are session data; created lazily by
     * pt_associate_label_with_value, released by pt_free_label_table */
    struct mht_table *label_table = nullptr;

    /* the plan dump file the session's requests name (db_query_plan_dump_file) */
    char *plan_dump_filename = nullptr;

    /* query_planner.c: SET OPTIMIZATION COST, two bits per plan kind (0 = every plan at its default cost) */
    unsigned int qo_cost_overrides = 0;

    /* method_callback.cpp: the query handles a PL routine keeps refer to this session's workspace, so the
     * callback handler is the session's (one CAS served one session); created on first use, retired by the
     * teardown before ws_final, in the bracket */
    cubmethod::callback_handler *method_callback_handler = nullptr;

    /* query_method.cpp: builtin C method runtime arguments, keyed by invoke group id */
    std::unordered_map<UINT64, std::vector<DB_VALUE>> method_runtime_args;

    /* query_method.cpp: error-stack depth at the innermost method dispatch; the client half's clear-all
     * (transaction_cl.c) stops there, the frames below belong to the server executor that raised the callback */
    int er_dispatch_floor = 0;

    /* set when the session ended from inside this very context (its last
     * reference was dropped by the bracket's own thread): the bracket exit
     * tears it down and deletes it */
    bool orphaned = false;

    client_session_context ();
};
// *INDENT-ON*

/* activation bracket; brackets do not nest (a nested activation aborts).
 * Holds the context's bracket_mutex for the bracket's extent. */
extern void csc_activate (client_session_context *ctx);
extern void csc_deactivate (void);

/* the context the calling thread's bracket installed; a caller outside any
 * bracket is an ownership bug, so it fails fast */
extern client_session_context *csc_current (void);

/* does the calling thread hold a bracket, i.e. own a session workspace?  (no
 * assert: shared server paths probe it on threads that never hold one) */
extern bool csc_bracket_is_active (void);

/* tear the context down and delete it; called by its owner (session_state_uninit,
 * or the creator of a context no session adopted).  A caller inside the context
 * only marks it orphaned: its csc_deactivate does the rest. */
extern void csc_retire_and_delete (client_session_context *ctx);

/* the error-stack floor of the innermost method dispatch; 0 (a plain clear-all) outside a bracket */
extern int csc_er_stack_floor (void);

/* is the calling thread inside an in-process method dispatch (the client half of a callback)? */
extern bool csc_in_method_dispatch (void);

/* does the session keep callback resources (query handles, deferred handles, replies) past a query's end? */
extern bool csc_has_method_callback_state (void);

#else /* SERVER_MODE */

/* a client process has one workspace and no bracket */
#define csc_bracket_is_active() false
#define csc_er_stack_floor() 0
#define csc_in_method_dispatch() false

#endif /* SERVER_MODE */

#endif /* _CLIENT_SESSION_CONTEXT_HPP_ */
