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
 * client process, for one session.  Authorization is the first tenant.
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

// *INDENT-OFF*
struct mht_table;

class client_session_context
{
  public:
    authenticate_context au_context;

    /* interpreter labels are session data; created lazily by
     * pt_associate_label_with_value, released by pt_free_label_table */
    struct mht_table *label_table = nullptr;

    /* the plan dump file the session's requests name (db_query_plan_dump_file) */
    char *plan_dump_filename = nullptr;
};
// *INDENT-ON*

/* activation bracket; brackets do not nest */
extern void csc_activate (client_session_context *ctx);
extern void csc_deactivate (void);

/* the context the calling thread's bracket installed; a caller outside any
 * bracket is an ownership bug, so it fails fast */
extern client_session_context *csc_current (void);

/* does the calling thread hold a bracket, i.e. own a session workspace?  (no
 * assert: shared server paths probe it on threads that never hold one) */
extern bool csc_bracket_is_active (void);

#else /* SERVER_MODE */

/* a client process has one workspace and no bracket */
#define csc_bracket_is_active() false

#endif /* SERVER_MODE */

#endif /* _CLIENT_SESSION_CONTEXT_HPP_ */
