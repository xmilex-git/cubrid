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
 * driver_session.cpp - adopted driver connection session thread
 *
 * The CAS per-connection sequence (cas_common_main.c main loop body and the
 * cas.c connect reply) on a server thread: the thread registers with the
 * thread manager, takes a client session context and a socketless conn
 * entry, boots the in-process client half, serves the driver's requests and
 * tears the session down.
 */

#if defined (SERVER_MODE)

#include "driver_session.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <cstring>

#include "client_session_context.hpp"
#include "connection_defs.h"
#include "connection_sr.h"
#include "db.h"
#include "db_client_type.hpp"
#include "error_manager.h"
#include "network_interface_cl.h"	// boot_unregister_client
#include "session.h"		// session_adopt_client_context
#include "storage_common.h"	// NULL_TRAN_INDEX
#include "system_parameter.h"	// PRM_ID_ORACLE_COMPAT_NUMBER_BEHAVIOR
#include "thread_entry.hpp"
#include "thread_manager.hpp"
#include "transaction_cl.h"	// tm_Tran_index

/* page_buffer.h (via the engine headers above) and cas_common.h both define
 * FREE; neither macro is used in this TU */
#undef FREE

#include "cas_common_execute.h"	// cas_log_error_handler_begin/end
#include "cas_common_vars.h"	// req_info (thread_local CAS globals)
#include "cas_dispatch.h"	// cas_process_request + server-support API
#include "boot_cl.h"		// boot_resume_client
#include "broker_config.h"	// KEEP_CON_AUTO
#include "cas.h"		// is_xa_prepared
#include "cas_error.h"
#include "cas_execute.h"	// ux_get_default_setting
#include "cas_handle.h"		// hm_srv_handle_free_all
#include "cas_log.h"		// the session's SQL, slow and access logs
#include "cas_net_buf.h"
#include "cas_protocol.h"
#include "cas_ssl.h"
#include "broker_util.h"	// ut_get_ipv4_string
#include "ddl_log.h"
#include "query_replace.h"
#include "trigger_manager.h"	// tr_set_execution_state

extern thread_local unsigned int db_on_server;	/* network_interface_sr.cpp */

namespace cubconn
{
  namespace adoption
  {
    /* the adoption protocol mirrors these cas_protocol.h facts; a driver
     * dialect change must be a deliberate protocol bump, not a drift */
    static_assert (DRIVER_HEADER_SIZE == SRV_CON_CLIENT_INFO_SIZE, "driver header size drift");
    static_assert (DRIVER_DB_INFO_SIZE == SRV_CON_DB_INFO_SIZE, "db_info size drift");
    static_assert (DRIVER_BROKER_INFO_SIZE == BROKER_INFO_SIZE, "broker_info size drift");

    static const std::size_t CONNECT_REPLY_BUF_SIZE =
	    sizeof (int) + CAS_INFO_SIZE + CAS_CONNECTION_REPLY_SIZE;
    static_assert (CAS_CONNECTION_REPLY_SIZE == 36, "V4 connect reply layout drift");

    /* request bodies larger than this are a protocol violation, not a query */
    static const std::uint32_t REQUEST_BODY_MAX = 16 * 1024 * 1024;

    /* ------------------------------------------------------------------ */
    /* blocking wire helpers (the session thread owns the fd)             */
    /* ------------------------------------------------------------------ */

    static int
    write_full (int fd, const void *buf, std::size_t len)
    {
      const char *p = static_cast<const char *> (buf);
      while (len > 0)
	{
	  /* after cas_init_ssl the connection's bytes go through its TLS channel, as in cas_network.c */
	  ssize_t n = ssl_client ? cas_ssl_write (fd, p, (int) len) : send (fd, p, len, MSG_NOSIGNAL);
	  if (n < 0)
	    {
	      if (errno == EINTR && !ssl_client)
		{
		  continue;
		}
	      return ER_FAILED;
	    }
	  if (n == 0 && ssl_client)
	    {
	      return ER_FAILED;
	    }
	  p += n;
	  len -= (std::size_t) n;
	}
      return NO_ERROR;
    }

    static int
    read_full (int fd, void *buf, std::size_t len)
    {
      char *p = static_cast<char *> (buf);
      while (len > 0)
	{
	  ssize_t n = ssl_client ? cas_ssl_read (fd, p, (int) len) : recv (fd, p, len, 0);
	  if (n == 0)
	    {
	      return ER_FAILED;	/* peer closed */
	    }
	  if (n < 0 && ssl_client)
	    {
	      return ER_FAILED;
	    }
	  if (n < 0)
	    {
	      if (errno == EINTR)
		{
		  continue;
		}
	      return ER_FAILED;
	    }
	  p += n;
	  len -= (std::size_t) n;
	}
      return NO_ERROR;
    }

    /* ------------------------------------------------------------------ */
    /* pure helpers (unit-tested)                                         */
    /* ------------------------------------------------------------------ */

    static void
    copy_field (char *dst, std::size_t dst_size, const char *src, std::size_t src_size)
    {
      /* the driver is not trusted to NUL-terminate (default stance) */
      std::size_t n = strnlen (src, src_size);
      assert (dst_size > src_size);
      std::memcpy (dst, src, n);
      dst[n] = '\0';
    }

    int
    parse_db_info (const char *buf, std::size_t len, driver_conn_info &out)
    {
      if (buf == NULL || len != DRIVER_DB_INFO_SIZE)
	{
	  return ER_FAILED;
	}

      /* layout per cas_parse_db_info (cas_common_main.c): dbname[32],
       * user[32], passwd[32], url[512], session_id[20] */
      const char *p = buf;
      copy_field (out.db_name, sizeof (out.db_name), p, SRV_CON_DBNAME_SIZE);
      p += SRV_CON_DBNAME_SIZE;
      copy_field (out.db_user, sizeof (out.db_user), p, SRV_CON_DBUSER_SIZE);
      p += SRV_CON_DBUSER_SIZE;
      copy_field (out.db_passwd, sizeof (out.db_passwd), p, SRV_CON_DBPASSWD_SIZE);
      p += SRV_CON_DBPASSWD_SIZE;
      copy_field (out.url, sizeof (out.url), p, SRV_CON_URL_SIZE);
      /* a V5+ driver puts its version behind the url's NUL, with its length in front */
      out.driver_version[0] = '\0';
      const std::size_t url_size = std::strlen (out.url);
      if (url_size + 2 <= SRV_CON_URL_SIZE)
	{
	  const std::size_t version_size = (unsigned char) p[url_size + 1];
	  if (version_size > 0 && version_size < sizeof (out.driver_version)
	      && version_size <= SRV_CON_URL_SIZE - url_size - 2)
	    {
	      std::memcpy (out.driver_version, p + url_size + 2, version_size);
	      out.driver_version[version_size] = '\0';
	    }
	}
      p += SRV_CON_URL_SIZE;
      std::memcpy (out.session_id, p, SRV_CON_DBSESS_ID_SIZE);

      if (out.db_user[0] == '\0')
	{
	  std::strcpy (out.db_user, "PUBLIC");
	}
      out.is_health_check = (std::strcmp (out.db_name, HEALTH_CHECK_DUMMY_DB) == 0);
      return NO_ERROR;
    }

    int
    parse_driver_protocol (const char (&driver_header)[DRIVER_HEADER_SIZE])
    {
      unsigned char ver = (unsigned char) driver_header[SRV_CON_MSG_IDX_PROTO_VERSION];
      if ((ver & CAS_PROTO_INDICATOR) == 0)
	{
	  /* pre-9.0 major/minor/patch dialect — not carried over (#116 D3) */
	  return -1;
	}
      return (int) (ver & CAS_PROTO_VER_MASK);
    }

    std::size_t
    build_connect_reply (std::uint32_t token, std::int32_t slot_idx,
			 const char (&broker_info)[DRIVER_BROKER_INFO_SIZE],
			 const char *session_20b, char *out, std::size_t out_size)
    {
      /* cas_send_connect_reply_to_driver, V4+ layout only (V12-single wire):
       * len | cas_info | token (pid slot, #117 D4) | broker_info |
       * slot_idx + 1 (shm_as_index + 1 slot) | 20-byte driver session */
      if (out_size < CONNECT_REPLY_BUF_SIZE)
	{
	  assert (false);
	  return 0;
	}

      char *p = out;
      int v = htonl (CAS_CONNECTION_REPLY_SIZE);
      std::memcpy (p, &v, sizeof (int));
      p += sizeof (int);

      char cas_info[CAS_INFO_SIZE] =
      { CAS_INFO_STATUS_ACTIVE, CAS_INFO_RESERVED_DEFAULT, CAS_INFO_RESERVED_DEFAULT, CAS_INFO_RESERVED_DEFAULT };
      std::memcpy (p, cas_info, CAS_INFO_SIZE);
      p += CAS_INFO_SIZE;

      v = htonl (token);
      std::memcpy (p, &v, CAS_PID_SIZE);
      p += CAS_PID_SIZE;
      std::memcpy (p, broker_info, DRIVER_BROKER_INFO_SIZE);
      p += DRIVER_BROKER_INFO_SIZE;
      v = htonl (slot_idx + 1);
      std::memcpy (p, &v, CAS_PID_SIZE);
      p += CAS_PID_SIZE;
      std::memcpy (p, session_20b, DRIVER_SESSION_SIZE);
      p += DRIVER_SESSION_SIZE;

      return (std::size_t) (p - out);
    }

    /* ------------------------------------------------------------------ */
    /* driver-facing replies                                              */
    /* ------------------------------------------------------------------ */

    /* net_write_error translation, V12-single: length-prefixed
     * cas_info | indicator | code | message.  No pre-V2 code conversion —
     * every V12 driver understands renewed error codes. */
    static void
    send_error_reply (int fd, char status, int indicator, int code, const char *msg)
    {
      assert (code != NO_ERROR);

      std::size_t msg_len = (msg != NULL && msg[0] != '\0') ? std::strlen (msg) + 1 : 0;
      int len = htonl ((int) (2 * sizeof (int) + msg_len));
      char cas_info[CAS_INFO_SIZE] =
      { status, CAS_INFO_RESERVED_DEFAULT, CAS_INFO_RESERVED_DEFAULT, CAS_INFO_RESERVED_DEFAULT };

      if (write_full (fd, &len, sizeof (int)) != NO_ERROR || write_full (fd, cas_info, CAS_INFO_SIZE) != NO_ERROR)
	{
	  return;
	}
      int v = htonl (indicator);
      if (write_full (fd, &v, sizeof (int)) != NO_ERROR)
	{
	  return;
	}
      v = htonl (code);
      if (write_full (fd, &v, sizeof (int)) != NO_ERROR)
	{
	  return;
	}
      if (msg_len > 0)
	{
	  (void) write_full (fd, msg, msg_len);
	}
    }

    /* connection == session (#116 D5; the CAS session-reattach protocol is
     * retired, B1-D12): every adopted connection gets a FRESH server session,
     * whatever session key the driver sent.  Reattaching would land a second
     * client context on a session slot that still owns the previous
     * connection's one (session_adopt_client_context invariant). */
    static void
    apply_driver_session_id (const char (&session_20b)[20])
    {
      static const char empty_key[SERVER_SESSION_KEY_SIZE] =
      { (char) 0xFF, (char) 0xFF, (char) 0xFF, (char) 0xFF, (char) 0xFF, (char) 0xFF, (char) 0xFF, (char) 0xFF };

      (void) session_20b;
      db_set_server_session_key (empty_key);
      db_set_session_id (DB_EMPTY_SESSION);
    }

    /* cas_make_session_for_driver translation.  Only an AUTO session is given its secret: it is what lets
     * the client take the session up again after a yield; any other connection is its session. */
    static void
    make_session_for_driver (char *out, bool with_secret)
    {
      std::size_t size = 0;
      SESSION_ID session;

      std::memcpy (out + size, db_get_server_session_key (), SERVER_SESSION_KEY_SIZE);
      size += SERVER_SESSION_KEY_SIZE;
      session = db_get_session_id ();
      session = htonl (session);
      std::memcpy (out + size, &session, sizeof (SESSION_ID));
      size += sizeof (SESSION_ID);
      std::memset (out + size, 0, DRIVER_SESSION_SIZE - size);
      if (with_secret)
	{
	  std::memcpy (out + size, db_get_session_secret (), SESSION_SECRET_SIZE);
	}
    }

    /* ------------------------------------------------------------------ */
    /* request loop: the folded CAS speaker (B1-D9)                       */
    /* ------------------------------------------------------------------ */

    /* cas_common_main.c's inner request loop, translated: the CAS globals
     * (req_info, as_info, srv handles, net_buf scratch) are thread-local in
     * SERVER_MODE, so this thread IS the CAS process for its connection */
    /* SHOW SESSION STATUS's Client_type */
    static const char *
    driver_client_name (int kind)
    {
      switch (kind)
	{
	case CAS_CLIENT_CCI:
	  return "CCI";
	case CAS_CLIENT_ODBC:
	  return "ODBC";
	case CAS_CLIENT_JDBC:
	  return "JDBC";
	case CAS_CLIENT_PHP:
	  return "PHP";
	case CAS_CLIENT_OLEDB:
	  return "OLEDB";
	case CAS_CLIENT_SERVER_SIDE_JDBC:
	  return "INTERNAL_JDBC";
	case CAS_CLIENT_GATEWAY:
	  return "GATEWAY";
	default:
	  return "UNKNOWN";
	}
    }

    /* the session's SHOW SESSION STATUS row: its statistics of now, published as one row */
    static void
    publish_stats (session_stat_row &row)
    {
      row.session_id = as_info->session_id;
      row.tran_index = tm_Tran_index;
      row.num_requests = as_info->num_requests_received;
      row.num_transactions = as_info->num_transactions_processed;
      row.num_queries = as_info->num_queries_processed;
      row.num_selects = as_info->num_select_queries;
      row.num_inserts = as_info->num_insert_queries;
      row.num_updates = as_info->num_update_queries;
      row.num_deletes = as_info->num_delete_queries;
      row.num_errors = as_info->num_error_queries;
      row.num_long_queries = as_info->num_long_queries;
      row.num_long_transactions = as_info->num_long_transactions;
      row.num_query_replace_prepare = as_info->num_query_replace_prepare;
      row.num_query_replace_execute = as_info->num_query_replace_execute;
      row.num_query_replace_fallback = as_info->num_query_replace_fallback;
      snprintf (row.db_user, sizeof (row.db_user), "%s", as_info->database_user);
      snprintf (row.last_activity, sizeof (row.last_activity), "%.*s", (int) sizeof (row.last_activity) - 1,
		as_info->log_msg);
      registry_publish_stats (row);
    }

    static void
    request_loop (int fd, int client_version, const char (&driver_header)[DRIVER_HEADER_SIZE], int wake_fd,
		  session_stat_row &stats)
    {
      T_NET_BUF net_buf;

      std::memset (&net_buf, 0, sizeof (net_buf));
      net_buf_init (&net_buf, client_version);
      net_buf.data = (char *) malloc (NET_BUF_ALLOC_SIZE);
      if (net_buf.data == NULL)
	{
	  send_error_reply (fd, CAS_INFO_STATUS_INACTIVE, CAS_ERROR_INDICATOR, CAS_ER_NO_MORE_MEMORY, NULL);
	  return;
	}
      net_buf.alloc_size = NET_BUF_ALLOC_SIZE;

      std::memset (&req_info, 0, sizeof (req_info));
      req_info.client_version = client_version;
      std::memcpy (req_info.driver_info, driver_header, DRIVER_HEADER_SIZE);
      req_info.need_rollback = TRUE;

      cas_log_error_handler_begin ();

      FN_RETURN fn_ret = FN_KEEP_CONN;
      while (fn_ret == FN_KEEP_CONN)
	{
	  /* the SIGUSR1 (re)arming of the CAS loop is retired: cancel arrives
	   * as a tran interrupt via the control channel (#117 D4) */
	  if (wake_fd >= 0 && as_info->con_status == CON_STATUS_OUT_TRAN && as_info->num_holdable_results == 0
	      && as_info->cas_change_mode == CAS_CHANGE_MODE_AUTO && !is_xa_prepared ()
	      && !csc_has_method_callback_state ())
	    {
	      /* nothing of this connection outlives a detach here: the session may be yielded while it waits */
	      registry_auto_ready ();
	    }
	  fn_ret = cas_process_request (fd, &net_buf, &req_info, wake_fd);
	  /* as the CAS main loop: only the connection's first request is answered as out of a transaction
	   * whatever it found; later a request that starts none (a cursor close) leaves the status as it was */
	  is_first_request = false;
	  cas_log_error_handler_clear ();
	  publish_stats (stats);
	}

      cas_log_error_handler_end ();
      net_buf_destroy (&net_buf);
    }

    /* ------------------------------------------------------------------ */
    /* the session thread                                                 */
    /* ------------------------------------------------------------------ */

    void
    driver_session_run (session_params params)
    {
      int err;
      bool registered = false;
      bool adopted = false;
      bool resumed = false;
      bool yielded = false;
      int wake_fd = -1;
      CSS_CONN_ENTRY *conn = NULL;
      client_session_context *ctx = NULL;
      driver_conn_info info;
      char session_blob[DRIVER_SESSION_SIZE];
      char reply[CONNECT_REPLY_BUF_SIZE];
      std::size_t reply_size;
      const bool is_ssl = IS_SSL_CLIENT (params.driver_header);
      const unsigned char *const client_ip = (const unsigned char *) &params.client_ip;
      char client_ip_str[16];
      struct timeval session_start;
      session_stat_row stats;

      gettimeofday (&session_start, NULL);	/* the access log's time of the connection */
      ut_get_ipv4_string (client_ip_str, sizeof (client_ip_str), client_ip);

      /* the broker's peek engine set O_NONBLOCK, and an SCM_RIGHTS-passed fd
       * shares the open file description — restore blocking mode, this
       * thread's loop owns the fd exclusively */
      {
	int flags = fcntl (params.client_fd, F_GETFL, 0);
	if (flags >= 0)
	  {
	    (void) fcntl (params.client_fd, F_SETFL, flags & ~O_NONBLOCK);
	  }
      }

      /* register this foreign thread with the thread manager (same ritual as
       * connection_worker.cpp) */
      cubthread::entry *entry_p = cubthread::get_manager ()->claim_entry ();
      if (entry_p == NULL)
	{
	  registry_begin_session_cleanup (params.token);
	  css_decrement_num_conn (DB_CLIENT_TYPE_DEFAULT);	/* the adoption channel's increment */
	  registry_session_finished (params.token);
	  close (params.client_fd);
	  return;
	}
      entry_p->register_id ();
      entry_p->type = TT_SERVER;
      entry_p->tran_index = -1;
      entry_p->m_status = cubthread::entry::status::TS_RUN;
      entry_p->shutdown = false;
      entry_p->get_error_context ().register_thread_local ();

      /* session-scoped client context; adopted by the server session after
       * registration, freed with it (session_state_uninit) */
      ctx = new client_session_context ();
      csc_activate (ctx);

      /* socketless conn entry: the CAS wire lives on params.client_fd, the
       * in-process client half anchors on thread_p->conn_entry */
      conn = css_make_conn (INVALID_SOCKET);
      if (conn == NULL)
	{
	  send_error_reply (params.client_fd, CAS_INFO_STATUS_INACTIVE, CAS_ERROR_INDICATOR, CAS_ER_FREE_SERVER, NULL);
	  goto retire;
	}
      /* pairs with the adoption channel's css_increment_num_conn: css_free_conn
       * decrements by conn->client_type on every exit path */
      conn->client_type = DB_CLIENT_TYPE_DEFAULT;
      css_insert_into_active_conn_list (conn);
      entry_p->conn_entry = conn;

      /* this thread now acts as the in-process client (D5) */
      db_on_server = 0;

      /* The session's CAS slot with its broker's conf, and its log files, first: what goes wrong from here
       * is written to the session's SQL log, as a CAS wrote it to its own. */
      (void) registry_session_begin (params.token);
      cas_server_session_slot_begin (params.driver_header[SRV_CON_MSG_IDX_CLIENT_TYPE],
				     CAS_MAKE_PROTO_VER (params.driver_header), params.driver_header,
				     params.broker_name.c_str (), &params.config);
      std::memcpy (as_info->cas_clt_ip, client_ip, sizeof (as_info->cas_clt_ip));
      as_info->cas_clt_port = params.client_port;
      if (cas_server_session_log_begin () != 0)
	{
	  if (!is_ssl)
	    {
	      send_error_reply (params.client_fd, CAS_INFO_STATUS_INACTIVE, CAS_ERROR_INDICATOR, CAS_ER_INTERNAL,
				"Cannot take a session log index");
	    }
	  goto retire;
	}
      cas_log_open (broker_name);
      cas_slow_log_open (broker_name);
      cas_log_write_client_ip (client_ip);

      if (is_ssl)
	{
	  /* the broker could not read the encrypted db_info: TLS ends here and the packet is read here.
	   * cas_init_ssl writes a certificate or handshake problem to the SQL log. */
	  if (cas_init_ssl (params.client_fd) < 0
	      || read_full (params.client_fd, params.db_info, sizeof (params.db_info)) != NO_ERROR)
	    {
	      goto retire;
	    }
	}

      if (parse_db_info (params.db_info, sizeof (params.db_info), info) != NO_ERROR)
	{
	  /* the broker validates a cleartext db_info: a malformed handoff, or a malformed encrypted packet */
	  send_error_reply (params.client_fd, CAS_INFO_STATUS_INACTIVE, CAS_ERROR_INDICATOR, CAS_ER_COMMUNICATION,
			    NULL);
	  goto retire;
	}
      if (info.is_health_check)
	{
	  if (is_ssl)
	    {
	      /* the broker answers a cleartext health check; an encrypted one arrives here */
	      const int zero = 0;
	      const char cas_info[CAS_INFO_SIZE] =
	      { CAS_INFO_STATUS_ACTIVE, CAS_INFO_RESERVED_DEFAULT, CAS_INFO_RESERVED_DEFAULT, CAS_INFO_RESERVED_DEFAULT };

	      cas_log_write_and_end (0, false, "Incoming health check request from client.");
	      if (write_full (params.client_fd, &zero, sizeof (zero)) == NO_ERROR)
		{
		  (void) write_full (params.client_fd, cas_info, sizeof (cas_info));
		}
	    }
	  else
	    {
	      send_error_reply (params.client_fd, CAS_INFO_STATUS_INACTIVE, CAS_ERROR_INDICATOR, CAS_ER_COMMUNICATION,
				NULL);
	    }
	  goto retire;
	}

      if (parse_driver_protocol (params.driver_header) < PROTOCOL_V12)
	{
	  /* V12-single wire (#116 D3): older dialects are not spoken here */
	  send_error_reply (params.client_fd, CAS_INFO_STATUS_INACTIVE, CAS_ERROR_INDICATOR, CAS_ER_NOT_IMPLEMENTED,
			    "this server accepts protocol V12 or later drivers");
	  goto retire;
	}

      /* The broker routed a cleartext client by its dbname: a mismatch is the broker's bug.  It sent an SSL
       * client here without reading the name (DIRECT_HANDOFF_SSL_DB), so that client may ask for another. */
      if (params.server_name != info.db_name)
	{
	  assert (is_ssl);
	  send_error_reply (params.client_fd, CAS_INFO_STATUS_INACTIVE, CAS_ERROR_INDICATOR, CAS_ER_FREE_SERVER,
			    "this broker sends SSL connections to another database");
	  goto retire;
	}

      if (info.driver_version[0] != '\0')
	{
	  snprintf (as_info->driver_version, sizeof (as_info->driver_version), "%s", info.driver_version);
	}
      else
	{
	  snprintf (as_info->driver_version, sizeof (as_info->driver_version), "PROTOCOL V%d",
		    parse_driver_protocol (params.driver_header));
	}
      cas_log_write_and_end (0, false, "CLIENT VERSION %s", as_info->driver_version);
      {
	/* cas_set_session_id's line: the session the driver asks for, 0 for a new one */
	SESSION_ID asked;

	std::memcpy (&asked, info.session_id + SERVER_SESSION_KEY_SIZE, sizeof (asked));
	cas_log_write_and_end (0, false, "session id for connection %u", ntohl (asked));
      }

      /* the db:dbuser:ip check a CAS made before it connected to the database */
      if (cas_server_acl_check (broker_name, info.db_name, info.db_user, client_ip) < 0)
	{
	  as_info->num_connect_rejected++;
	  send_error_reply (params.client_fd, CAS_INFO_STATUS_INACTIVE, DBMS_ERROR_INDICATOR,
			    CAS_ER_NOT_AUTHORIZED_CLIENT, "Authorization error.(Address is rejected)");
	  cas_log_write_and_end (0, false, "connect db %s user %s url %s - rejected", info.db_name, info.db_user,
				 info.url);
	  if (CAS_SHM_CFG (access_log) == ON)
	    {
	      (void) cas_server_access_log (&session_start, (int) params.client_ip, info.db_name, info.db_user,
					    ACL_REJECTED);
	    }
	  goto retire;
	}

      logddl_init (APP_NAME_CAS);
      logddl_check_ddl_audit_param ();
      logddl_set_broker_info (shm_as_index, broker_name);
      logddl_set_ip (client_ip_str);

      if (params.keep_con_auto)
	{
	  SESSION_ID session_id;

	  wake_fd = eventfd (0, EFD_CLOEXEC | EFD_NONBLOCK);
	  if (wake_fd < 0 || !registry_auto_enable (params.token, wake_fd))
	    {
	      send_error_reply (params.client_fd, CAS_INFO_STATUS_INACTIVE, CAS_ERROR_INDICATOR, CAS_ER_FREE_SERVER,
				NULL);
	      goto retire;
	    }
	  as_info->cur_keep_con = KEEP_CON_AUTO;

	  /* the driver session the client was given before a yield: server key | session id | secret */
	  std::memcpy (&session_id, info.session_id + SERVER_SESSION_KEY_SIZE, sizeof (SESSION_ID));
	  session_id = ntohl (session_id);
	  if (session_id != DB_EMPTY_SESSION)
	    {
	      client_session_context *kept = NULL;

	      err = session_claim_kept_context (entry_p, session_id, info.session_id,
						info.session_id + SERVER_SESSION_KEY_SIZE + sizeof (SESSION_ID),
						&kept);
	      if (err == NO_ERROR)
		{
		  /* this thread goes on in the context the session kept, once its previous connection left it */
		  csc_deactivate ();
		  csc_retire_and_delete (ctx);
		  ctx = kept;
		  csc_activate (ctx);
		  adopted = true;
		  resumed = true;
		}
	      else if (err != ER_SES_SESSION_EXPIRED)
		{
		  send_error_reply (params.client_fd, CAS_INFO_STATUS_INACTIVE, DBMS_ERROR_INDICATOR, err,
				    er_msg () != NULL ? er_msg () : "");
		  goto retire;
		}
	      /* else the session is gone: a new one, as for a client whose session expired */
	    }
	}

      if (resumed)
	{
	  BOOT_CLIENT_CREDENTIAL credential;

	  credential.client_type = DB_CLIENT_TYPE_DEFAULT;
	  credential.db_name = info.db_name;
	  credential.db_user = info.db_user;
	  credential.program_name = "driver_session";
	  credential.process_id = -1;
	  err = boot_resume_client (&credential, info.db_passwd);
	}
      else
	{
	  apply_driver_session_id (info.session_id);

	  /* client-half boot with the driver's credentials; serialization is the
	   * engine's own (boot_restart_client) since A5 */
	  err = db_restart_ex ("driver_session", info.db_name, info.db_user, info.db_passwd, NULL,
			       DB_CLIENT_TYPE_DEFAULT);
	}
      if (err != NO_ERROR)
	{
	  /* cas_db_connect failure path: DBMS error straight to the driver, and its line in the SQL and slow
	   * logs */
	  const char *const message = er_msg () != NULL ? er_msg () : "";
	  char msg_buf[LINE_MAX];

	  send_error_reply (params.client_fd, CAS_INFO_STATUS_INACTIVE, DBMS_ERROR_INDICATOR, err, message);
	  snprintf (msg_buf, sizeof (msg_buf), "connect db %s user %s url %s, error:%d%s%s", info.db_name,
		    info.db_user, info.url, err, message[0] != '\0' ? ", " : ".", message);
	  cas_log_write_and_end (0, false, "%s", msg_buf);
	  cas_slow_log_write_and_end (NULL, 0, "%s", msg_buf);
	  goto retire;
	}
      registered = true;
      db_set_client_ip_addr (client_ip_str);

      /* replay ux_database_connect's connected-identity bookkeeping the
       * direct db_restart_ex boot skipped, so CHECK_CAS answers correctly
       * on this OUT_TRAN adopted connection (B2-D11) */
      ux_adopted_identity_record (info.db_name, info.db_user, info.db_passwd);

      /* the session created during registration becomes the durable owner of
       * the client context (#123 D3); a resumed session owns it already */
      if (resumed || session_adopt_client_context (entry_p, ctx) == NO_ERROR)
	{
	  adopted = true;
	}
      else
	{
	  send_error_reply (params.client_fd, CAS_INFO_STATUS_INACTIVE, CAS_ERROR_INDICATOR, CAS_ER_FREE_SERVER, NULL);
	  goto retire;
	}

      /* cancel arrives on the control channel as a tran interrupt (#117 D4) */
      registry_set_tran_index (params.token, tm_Tran_index);

      /* the new-connection defaults capture ux_database_connect performs
       * (cas_execute.c:493): isolation/lock-timeout baselines + sys params */
      ux_get_default_setting ();
      (void) tr_set_execution_state (CAS_SHM_CFG (trigger_action_flag) != 0);

      /* the broker's query replace rules: the segment is the broker's, the lookup state the session's */
      (void) qr_init_key (CAS_SHM_CFG (query_replace_shm_key));
      qr_load_dbuser_has_rules (as_info->database_name, info.db_user);

      /* cas_post_db_connect: the access log's line and the SQL log's.  A resume goes on with a connection
       * the access log has, as a CAS's CHANGE CLIENT did. */
      as_info->session_id = db_get_session_id ();
      if (CAS_SHM_CFG (access_log) == ON && !resumed)
	{
	  (void) cas_server_access_log (&session_start, (int) params.client_ip, info.db_name, info.db_user,
					NEW_CONNECTION);
	}
      cas_log_write_and_end (0, false, "connect db %s@%s user %s url %s session id %u", as_info->database_name,
			     as_info->database_host, info.db_user, info.url, as_info->session_id);
      cas_log_write_and_end (0, false, "DEFAULT isolation_level %d, lock_timeout %d", cas_default_isolation_level,
			     cas_default_lock_timeout);

      /* the broker filled its own connect-reply facts (bytes 0-3:
       * dbms/keep_con/statement pooling/pconnect); the server owns the
       * protocol bytes (cas_bi_make_broker_info split, B1-D5) */
      params.broker_info[BROKER_INFO_PROTO_VERSION] = CAS_PROTO_PACK_CURRENT_NET_VER;
      params.broker_info[BROKER_INFO_FUNCTION_FLAG] = (char) (BROKER_RENEWED_ERROR_CODE | BROKER_SUPPORT_HOLDABLE_RESULT);
      params.broker_info[BROKER_INFO_SYSTEM_PARAM] =
	      prm_get_bool_value (PRM_ID_ORACLE_COMPAT_NUMBER_BEHAVIOR) ? MASK_ORACLE_COMPAT_NUMBER_BEHAVIOR : 0;
      params.broker_info[BROKER_INFO_RESERVED3] = 0;

      make_session_for_driver (session_blob, params.keep_con_auto);
      reply_size = build_connect_reply (params.token, shm_as_index, params.broker_info, session_blob,
					reply, sizeof (reply));
      if (write_full (params.client_fd, reply, reply_size) != NO_ERROR)
	{
	  goto retire;
	}

      std::memset (&stats, 0, sizeof (stats));
      stats.token = params.token;
      stats.log_index = shm_as_index + 1;
      stats.client_ip = params.client_ip;
      snprintf (stats.broker_name, sizeof (stats.broker_name), "%.*s", (int) sizeof (stats.broker_name) - 1, broker_name);
      snprintf (stats.client_type, sizeof (stats.client_type), "%s",
		driver_client_name ((unsigned char) params.driver_header[SRV_CON_MSG_IDX_CLIENT_TYPE]));
      snprintf (stats.client_version, sizeof (stats.client_version), "%s", as_info->driver_version);
      publish_stats (stats);

      request_loop (params.client_fd, CAS_MAKE_PROTO_VER (params.driver_header), params.driver_header, wake_fd,
		    stats);

retire:
      /* cas.c's end of a connection: a yield (CHANGE CLIENT, a broker reset) keeps the session for the client's
       * next connection and session_state_timeout runs from here; any other end rolls back and ends it */
      yielded = registry_auto_finish ();
      if (registered)
	{
	  if (!yielded && !is_xa_prepared ())
	    {
	      (void) ux_end_tran (CCI_TRAN_ROLLBACK, false, true);
	    }
	  db_set_keep_session (yielded);
	  (void) ux_end_session ();
	  if (!yielded)
	    {
	      /* the server session, and with it any stream session it held, is gone */
	      ux_stream_reset ();
	    }
	}
      else if (resumed)
	{
	  /* the login on the claimed session failed: it stays as its client left it */
	  session_rekeep_client_context (entry_p);
	}
      registry_begin_session_cleanup (params.token);
      if (wake_fd >= 0)
	{
	  close (wake_fd);	/* after the cleanup: no YIELD_TRY or reset still holds it */
	}
      qr_final ();
      if (as_info != NULL)
	{
	  hm_srv_handle_table_final ();
	  /* a CAS closed its logs when its client left; the session's files are its own until then.  A
	   * CON_CLOSE request wrote the line and closed the SQL log before its reply. */
	  if (cas_log_is_open ())
	    {
	      cas_log_write_and_end (0, true, "disconnect");
	    }
	  logddl_destroy ();
	  cas_log_close (true);
	  cas_slow_log_close ();
	}
      cas_server_session_slot_end ();
      /* teardown order: unregister the tran,
       * scrub the conn entry, close the bracket, then retire the thread */
      if (registered)
	{
	  (void) boot_unregister_client (tm_Tran_index);
	  tm_Tran_index = NULL_TRAN_INDEX;
	}
      if (conn != NULL)
	{
	  entry_p->conn_entry = NULL;
	  css_free_conn (conn);	/* decrements the conn-rule counter by client_type */
	}
      else
	{
	  /* never got a conn entry: pair the adoption channel's increment here */
	  css_decrement_num_conn (DB_CLIENT_TYPE_DEFAULT);
	}
      entry_p->tran_index = NULL_TRAN_INDEX;
      entry_p->m_status = cubthread::entry::status::TS_DEAD;
      csc_deactivate ();
      if (!adopted)
	{
	  csc_retire_and_delete (ctx);
	}
      entry_p->get_error_context ().deregister_thread_local ();
      entry_p->unregister_id ();
      cubthread::get_manager ()->retire_entry (*entry_p);

      /* drop the registry entry BEFORE closing the fd: stop() shuts down the
       * fds it finds in the registry, and a closed (possibly reused) number
       * must never be visible there */
      registry_session_finished (params.token);
      if (ssl_client)
	{
	  cas_ssl_close (params.client_fd);
	  ssl_client = false;
	}
      close (params.client_fd);
    }
  }				/* namespace adoption */
}				/* namespace cubconn */

#endif /* SERVER_MODE */
