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
 * adoption.hpp - broker->server connection adoption endpoint (stage B1, #117)
 *
 * The server listens on a per-database UNIX domain socket.  A broker keeps one
 * persistent control channel per database on it and multiplexes fd handoffs
 * (SCM_RIGHTS), cancel/status forwarding, session-end notifications and the
 * restart re-sync handshake over that channel (#117 D1/D3/D4/D7).
 *
 * The wire structs below are the control-channel protocol v1.  Both ends run
 * on the same host over AF_UNIX, so fields are host byte order.
 */

#ifndef _ADOPTION_HPP_
#define _ADOPTION_HPP_

#if !defined (SERVER_MODE) && !defined (ADOPTION_PROTOCOL_ONLY)
#error adoption.hpp is a SERVER_MODE header (brokers include it with ADOPTION_PROTOCOL_ONLY)
#endif

#include <cstddef>
#include <cstdint>

namespace cubconn
{
  namespace adoption
  {
    /* ------------------------------------------------------------------ */
    /* control-channel protocol v1                                        */
    /* ------------------------------------------------------------------ */

    static const std::uint32_t PROTO_MAGIC = 0x41444F50;	/* "ADOP" */
    static const std::uint32_t PROTO_VERSION = 1;

    enum class msg_op : std::uint32_t
    {
      /* broker -> server */
      HELLO = 1,		/* open a control channel; body: hello_body */
      HANDOFF = 2,		/* adopt a driver connection; body: handoff_body + SCM_RIGHTS fd */
      CANCEL = 3,		/* query cancel; body: token_body */
      STATUS = 4,		/* "ST" status probe; body: token_body */
      RESYNC = 5,		/* restart re-sync; no body */
      YIELD_TRY = 6,		/* every slot is taken: yield one idle AUTO session of this broker; no body */
      IDLE_WATCH = 7,		/* ask for (1) or withdraw (0) one IDLE_HINT; body: token_body */
      RESET = 8,		/* cubrid broker reset: every AUTO session of this broker yields; no body */
      SESSION_CONFIG = 17,	/* the broker's conf changed: every session of the broker takes it; body: session_config */

      /* server -> broker */
      HELLO_ACK = 9,		/* body: hello_ack_body */
      HANDOFF_ACK = 10,		/* body: resync_token_body (the server-issued cancel token and the peer
				 * the server registered; the broker's channel reader admits it) */
      HANDOFF_REJECT = 11,	/* body: reject_body */
      STATUS_REPLY = 12,	/* body: status_reply_body */
      RESYNC_REPLY = 13,	/* body: resync_reply_body + live_count resync_token_body */
      SESSION_END = 14,		/* async; body: token_body (frees a broker slot, #117 D3) */
      YIELD_TRY_REPLY = 15,	/* body: token_body (1: a session yields and will end, 0: none was idle) */
      IDLE_HINT = 16,		/* async; no body: a session of this broker is idle, YIELD_TRY may find it */
      SESSION_CONFIG_REPLY = 18	/* body: session_change_reply */
    };

    struct msg_header
    {
      std::uint32_t magic;
      std::uint32_t op;
      std::uint32_t length;	/* payload bytes following this header */
    };

    static const std::size_t BROKER_NAME_MAX = 32;

    struct hello_body
    {
      std::uint32_t proto_version;
      char broker_name[BROKER_NAME_MAX];	/* NUL-terminated */
    };

    struct hello_ack_body
    {
      std::uint32_t proto_version;
    };

    /* driver-facing sizes; kept in sync with cas_protocol.h by static_asserts
     * in adoption.cpp (this header must stay includable from the broker) */
    static const std::size_t DRIVER_HEADER_SIZE = 10;	/* SRV_CON_CLIENT_INFO_SIZE */
    static const std::size_t DRIVER_DB_INFO_SIZE = 628;	/* SRV_CON_DB_INFO_SIZE (V12) */
    static const std::size_t DRIVER_BROKER_INFO_SIZE = 8;	/* BROKER_INFO_SIZE */

    static const std::size_t CONFIG_PATH_MAX = 1024;

    /* what the broker's conf gives a session: the values a CAS read from its broker's shared memory.  The
     * broker sends them with the handoff and again when broker_changer changes one.  seq orders the copies of
     * one broker name: it grows with every copy the broker takes and across the broker's restarts, and a
     * server keeps, for a session and for the sessions it will be handed, the copy with the largest. */
    struct session_config
    {
      std::uint64_t seq;
      std::int32_t sql_log_mode;
      std::int32_t slow_log_mode;
      std::int32_t sql_log_max_size;
      std::int32_t access_log;
      std::int32_t access_log_max_size;
      std::int32_t long_query_time;
      std::int32_t long_transaction_time;
      std::int32_t jdbc_cache;
      std::int32_t jdbc_cache_only_hint;
      std::int32_t jdbc_cache_life_time;
      std::int32_t statement_pooling;
      std::int32_t cci_default_autocommit;
      std::int32_t max_prepared_stmt_count;
      std::int32_t session_timeout;
      std::int32_t query_timeout;
      std::int32_t max_string_length;
      std::int32_t trigger_action_flag;
      std::int32_t stripped_column_name;
      std::int32_t net_buf_size;
      std::int32_t query_replace_shm_key;	/* the broker's query replace rules, 0 when it has none */
      std::int32_t broker_shm_id;
      char log_dir[CONFIG_PATH_MAX];
      char slow_log_dir[CONFIG_PATH_MAX];
      char err_log_dir[CONFIG_PATH_MAX];
      char access_log_file[CONFIG_PATH_MAX];
    };

    struct session_change_reply
    {
      std::int32_t affected;	/* sessions the change reached */
    };

    struct handoff_body
    {
      std::uint32_t client_ip;	/* network byte order, as read from the peer */
      std::uint16_t client_port;
      std::uint16_t reserved;
      std::int32_t slot_idx;	/* broker slot index; echoed in the connect reply */
      char broker_info[DRIVER_BROKER_INFO_SIZE];	/* echoed in the connect reply */
      char driver_header[DRIVER_HEADER_SIZE];	/* the peeked 10-byte client header */
      char db_info[DRIVER_DB_INFO_SIZE];	/* the peeked db_info packet */
      char keep_con_auto;	/* the broker's KEEP_CONNECTION is AUTO: the session may yield and be resumed */
      char pad;
      session_config config;
    };

    struct token_body
    {
      std::uint32_t token;
    };

    enum class reject_reason : std::int32_t
    {
      CLIENTS_EXCEEDED = 1,	/* css_Conn_rules refused; broker requeues (#117 D3) */
      DBNAME_MISMATCH = 2,	/* routed to the wrong server */
      SHUTDOWN = 3,		/* server is going down */
      MALFORMED = 4,		/* bad handoff payload */
      UNSUPPORTED_DRIVER = 5,	/* protocol below V12 (#116 D3) */
      NO_RESOURCES = 6		/* out of memory for the session */
    };

    struct reject_body
    {
      std::int32_t reason;
    };

    struct status_reply_body
    {
      std::int32_t fn_status;
    };

    struct resync_reply_body
    {
      std::uint32_t live_count;	/* adopted sessions of the requesting broker */
    };

    /* one admitted session: a HANDOFF_ACK body, and each of the live_count
     * entries following resync_reply_body, so a restarted broker rebuilds its
     * token table and every survivor's SESSION_END finds its token */
    struct resync_token_body
    {
      std::uint32_t token;
      std::uint32_t client_ip;	/* raw bytes as the handoff carried them */
      std::uint16_t client_port;
      std::uint16_t reserved;
    };

#if defined (SERVER_MODE)
    /* ------------------------------------------------------------------ */
    /* server-side endpoint lifecycle (called from css_init)              */
    /* ------------------------------------------------------------------ */

    /* create <TMP>/<prefix>_adopt_<db_name>, start the accept thread */
    int start (const char *db_name);
    /* close everything: channels, adopted sessions, listen socket */
    void stop (void);
    /* read cas_access_control_file again (cas_server_acl_reload) */
    void acl_reload (void);

    /* adopted-session registry hooks (driver_session.cpp) */
    void registry_set_tran_index (std::uint32_t token, int tran_index);
    void registry_set_fn_status (std::uint32_t token, int fn_status);
    /* session thread signs off: notify SESSION_END and drop the entry */
    /* before the session releases its transaction: waits out cancels that
     * pinned it, and no later cancel or stop() reaches it */
    void registry_begin_session_cleanup (std::uint32_t token);
    void registry_session_finished (std::uint32_t token);

    /* The session's thread enters: it has the token's entry for its own calls below.  False when the entry
     * is gone (the server stops). */
    bool registry_session_begin (std::uint32_t token);
    /* true when the session's broker changed its conf since the session last took it: config is the new one */
    bool registry_take_config (session_config &config);

    /* SHOW SESSION STATUS: one row per driver session, as the session's thread last published it */
    struct session_stat_row
    {
      std::uint32_t token;
      int log_index;		/* N of the session's <broker>_<N>.sql.log */
      unsigned int session_id;
      int tran_index;
      std::uint32_t client_ip;
      char broker_name[BROKER_NAME_MAX];
      char client_type[16];
      char client_version[20];
      char db_user[32];
      std::int64_t num_requests;
      std::int64_t num_transactions;
      std::int64_t num_queries;
      std::int64_t num_selects;
      std::int64_t num_inserts;
      std::int64_t num_updates;
      std::int64_t num_deletes;
      std::int64_t num_errors;
      std::int64_t num_long_queries;
      std::int64_t num_long_transactions;
      std::int64_t num_query_replace_prepare;
      std::int64_t num_query_replace_execute;
      std::int64_t num_query_replace_fallback;
      char last_activity[64];
    };
    /* the session's thread, after its connect reply and after every request: row is its statistics of now */
    void registry_publish_stats (const session_stat_row &row);
    /* a reader: copies up to max_rows rows, each as one publication left it; returns the number copied */
    std::size_t registry_stats_snapshot (session_stat_row *rows, std::size_t max_rows);

    /* an administrative kill of tran_index: its driver session, if it is one, reads the end of its connection
     * and ends as when its client leaves.  False when no driver session has the transaction. */
    bool registry_shutdown_session (int tran_index);

    /* KEEP_CONNECTION=AUTO, called by the session thread only.  A session that waits for a request at a
     * boundary where it may leave its client (no transaction, no holdable cursor, no XA prepare, no callback
     * resource) is idle; a dispatcher out of slots or a broker reset yields an idle session, which then
     * closes the connection and keeps its server session for the client's next connection. */
    bool registry_auto_enable (std::uint32_t token, int wake_fd);
    /* the session waits for a request at such a boundary */
    void registry_auto_ready (void);
    /* a request header arrived; false when the session was yielded first, and the request must not run */
    bool registry_auto_begin_request (void);
    bool registry_auto_is_yielded (void);
    /* a broker reset asked this session to yield */
    bool registry_auto_is_reset (void);
    /* the connection ends; true when a yield ended it */
    bool registry_auto_finish (void);
#endif
  }				/* namespace adoption */
}				/* namespace cubconn */

#endif /* _ADOPTION_HPP_ */
