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
 * adoption.cpp - broker->server connection adoption endpoint (stage B1, #117)
 *
 * The server end of the handoff contract: a per-database UNIX listen socket,
 * one thread per broker control channel, a token registry of adopted
 * sessions, and one detached session thread per adopted driver connection
 * (driver_session.cpp).  All of this is connection-setup cold path; the data
 * path is the session thread's own fd.
 */

#if defined (SERVER_MODE)

#include "adoption.hpp"
#include "driver_session.hpp"

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cas_dispatch.h"	// cas_server_speaker_boot_init
#include "connection_defs.h"
#include "connection_sr.h"	// css_increment_num_conn
#include "db_client_type.hpp"
#include "environment_variable.h"
#include "error_manager.h"
#include "log_impl.h"		// logtb_set_tran_index_interrupt
#include "porting.h"
#include "system_parameter.h"
#include "thread_entry.hpp"
#include "thread_manager.hpp"

namespace cubconn
{
  namespace adoption
  {
    /* fn_status vocabulary of the "ST" probe (cas_common.h); the broker
     * forwards the raw int to the driver */
    static const std::int32_t FN_STATUS_PROBE_NONE = -2;
    static const std::int32_t FN_STATUS_PROBE_BUSY = 1;

    struct channel
    {
      std::uint64_t id;
      int fd;			/* -1 once invalidated; guarded by send_mutex */
      char broker_name[BROKER_NAME_MAX];
      std::mutex send_mutex;	/* replies and async SESSION_ENDs interleave; also guards fd close */
      std::thread thread;	/* joinable: stop()/the accept reaper joins it */
      std::atomic<bool> dead { false };

      channel () : id (0), fd (-1)
      {
	broker_name[0] = '\0';
      }
    };

    /* what a cancel or stop() reaches of a session: pinned under its mutex,
     * so the session's cleanup (registry_begin_session_cleanup) waits until no
     * cancel still holds its transaction index before releasing it */
    struct session_binding
    {
      std::mutex mutex;
      std::condition_variable drained;
      int users = 0;
      bool closing = false;
      int tran_index = NULL_TRAN_INDEX;
      int client_fd = -1;
    };

    class binding_pin
    {
      public:
	explicit binding_pin (const std::shared_ptr<session_binding> &binding)
	  : m_binding (binding), tran_index (NULL_TRAN_INDEX), client_fd (-1)
	{
	  if (m_binding != nullptr)
	    {
	      std::lock_guard<std::mutex> guard (m_binding->mutex);
	      if (!m_binding->closing)
		{
		  m_binding->users++;
		  tran_index = m_binding->tran_index;
		  client_fd = m_binding->client_fd;
		  m_active = true;
		}
	    }
	}

	~binding_pin ()
	{
	  if (m_active)
	    {
	      std::lock_guard<std::mutex> guard (m_binding->mutex);
	      if (--m_binding->users == 0)
		{
		  m_binding->drained.notify_all ();
		}
	    }
	}

	binding_pin (const binding_pin &) = delete;
	binding_pin &operator= (const binding_pin &) = delete;

      private:
	std::shared_ptr<session_binding> m_binding;
	bool m_active = false;

      public:
	int tran_index;
	int client_fd;
    };

    struct session_entry
    {
      std::uint32_t token;
      char broker_name[BROKER_NAME_MAX];
      std::shared_ptr<session_binding> binding;
      std::uint32_t client_ip;
      std::uint16_t client_port;
      std::int32_t fn_status;
    };

    struct manager
    {
      std::string db_name;
      std::string socket_path;
      int listen_fd = -1;
      std::thread accept_thread;
      std::atomic<bool> stopping { false };	/* read again under registry_mutex before a registration */

      std::mutex channels_mutex;
      std::unordered_map<std::uint64_t, std::shared_ptr<channel>> channels;
      std::uint64_t next_channel_id = 1;
      /* per broker name, its newest channel (accept order) that completed
       * HELLO: the one SESSION_ENDs go to.  A broker dials a channel only
       * after its previous one to this database died, so every token it
       * still holds for this server was admitted on this channel or is
       * dropped with an older one */
      std::unordered_map<std::string, std::uint64_t> latest_channel;

      std::mutex registry_mutex;
      std::condition_variable registry_cv;	/* signaled when a session signs off */
      std::unordered_map<std::uint32_t, session_entry> registry;
      std::uint32_t next_token = 1;

      /* sign-off callbacks still touching this manager after their registry
       * erase; stop() must not free the manager under them (codex review F2) */
      std::atomic<int> active_finishers { 0 };
    };

    static manager *adoption_Manager = NULL;

    /* ------------------------------------------------------------------ */
    /* wire helpers                                                       */
    /* ------------------------------------------------------------------ */

    static int
    send_all (int fd, const void *buf, std::size_t len)
    {
      const char *p = static_cast<const char *> (buf);
      while (len > 0)
	{
	  ssize_t n = send (fd, p, len, MSG_NOSIGNAL);
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

    /* one framed control message; the caller holds ch.send_mutex */
    static int
    send_message_locked (channel &ch, msg_op op, const void *body, std::size_t body_len)
    {
      msg_header header;
      header.magic = PROTO_MAGIC;
      header.op = (std::uint32_t) op;
      header.length = (std::uint32_t) body_len;

      if (ch.fd < 0)
	{
	  return ER_FAILED;	/* invalidated under this mutex; never write a reused fd */
	}
      if (send_all (ch.fd, &header, sizeof (header)) != NO_ERROR)
	{
	  return ER_FAILED;
	}
      if (body_len > 0 && send_all (ch.fd, body, body_len) != NO_ERROR)
	{
	  return ER_FAILED;
	}
      return NO_ERROR;
    }

    /* one framed control message under the channel's send mutex */
    static int
    send_message (channel &ch, msg_op op, const void *body, std::size_t body_len)
    {
      std::lock_guard<std::mutex> guard (ch.send_mutex);
      return send_message_locked (ch, op, body, body_len);
    }

    /* receive len bytes; SCM_RIGHTS fds may ride on any segment (the broker
     * sends header+body+fd in one sendmsg, but the stream may fragment) */
    static int
    recv_with_fd (int fd, void *buf, std::size_t len, int *received_fd)
    {
      char *p = static_cast<char *> (buf);
      while (len > 0)
	{
	  struct iovec iov;
	  iov.iov_base = p;
	  iov.iov_len = len;

	  char control[CMSG_SPACE (sizeof (int))];
	  struct msghdr msg;
	  std::memset (&msg, 0, sizeof (msg));
	  msg.msg_iov = &iov;
	  msg.msg_iovlen = 1;
	  msg.msg_control = control;
	  msg.msg_controllen = sizeof (control);

	  ssize_t n = recvmsg (fd, &msg, MSG_CMSG_CLOEXEC);
	  if (n == 0)
	    {
	      return ER_FAILED;	/* peer closed */
	    }
	  if (n < 0)
	    {
	      if (errno == EINTR)
		{
		  continue;
		}
	      return ER_FAILED;
	    }

	  for (struct cmsghdr *cmsg = CMSG_FIRSTHDR (&msg); cmsg != NULL; cmsg = CMSG_NXTHDR (&msg, cmsg))
	    {
	      if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_RIGHTS
		  && cmsg->cmsg_len >= CMSG_LEN (sizeof (int)))
		{
		  int new_fd;
		  std::memcpy (&new_fd, CMSG_DATA (cmsg), sizeof (int));
		  if (received_fd != NULL && *received_fd < 0)
		    {
		      *received_fd = new_fd;
		    }
		  else
		    {
		      close (new_fd);	/* unexpected extra fd: don't leak it */
		    }
		}
	    }

	  p += n;
	  len -= (std::size_t) n;
	}
      return NO_ERROR;
    }

    /* ------------------------------------------------------------------ */
    /* registry                                                           */
    /* ------------------------------------------------------------------ */

    void
    registry_set_tran_index (std::uint32_t token, int tran_index)
    {
      manager *m = adoption_Manager;
      if (m == NULL)
	{
	  return;
	}
      std::lock_guard<std::mutex> guard (m->registry_mutex);
      auto it = m->registry.find (token);
      if (it != m->registry.end ())
	{
	  std::lock_guard<std::mutex> binding_guard (it->second.binding->mutex);
	  it->second.binding->tran_index = tran_index;
	}
    }

    void
    registry_set_fn_status (std::uint32_t token, int fn_status)
    {
      manager *m = adoption_Manager;
      if (m == NULL)
	{
	  return;
	}
      std::lock_guard<std::mutex> guard (m->registry_mutex);
      auto it = m->registry.find (token);
      if (it != m->registry.end ())
	{
	  it->second.fn_status = fn_status;
	}
    }

    /* the session is about to release its transaction: no new cancel or
     * stop() reaches it, and one that already pinned it finishes first */
    void
    registry_begin_session_cleanup (std::uint32_t token)
    {
      manager *m = adoption_Manager;
      if (m == NULL)
	{
	  return;
	}
      std::shared_ptr<session_binding> binding;
      {
	std::lock_guard<std::mutex> guard (m->registry_mutex);
	auto it = m->registry.find (token);
	if (it != m->registry.end ())
	  {
	    binding = it->second.binding;
	  }
      }
      if (binding != nullptr)
	{
	  std::unique_lock<std::mutex> guard (binding->mutex);
	  binding->closing = true;
	  binding->drained.wait (guard, [&binding] { return binding->users == 0; });
	  binding->tran_index = NULL_TRAN_INDEX;
	  binding->client_fd = -1;
	}
    }

    void
    registry_session_finished (std::uint32_t token)
    {
      manager *m = adoption_Manager;
      if (m == NULL)
	{
	  return;
	}
      m->active_finishers.fetch_add (1);

      char owner_broker[BROKER_NAME_MAX];
      bool found = false;
      {
	std::lock_guard<std::mutex> guard (m->registry_mutex);
	auto it = m->registry.find (token);
	if (it != m->registry.end ())
	  {
	    std::memcpy (owner_broker, it->second.broker_name, sizeof (owner_broker));
	    m->registry.erase (it);
	    found = true;
	  }
      }
      m->registry_cv.notify_all ();
      if (!found)
	{
	  m->active_finishers.fetch_sub (1);
	  return;
	}

      /* frees the broker's slot (#117 D3) on the broker's newest channel: the
       * one whose admissions (its HANDOFF_ACKs, its RESYNC snapshot) can hold
       * this token.  The channel the session was handed off on may be dead
       * while its broker's successor re-synced the token. */
      std::shared_ptr<channel> ch;
      {
	std::lock_guard<std::mutex> guard (m->channels_mutex);
	auto latest = m->latest_channel.find (owner_broker);
	if (latest != m->latest_channel.end ())
	  {
	    auto it = m->channels.find (latest->second);
	    if (it != m->channels.end ())
	      {
		ch = it->second;
	      }
	  }
      }
      if (ch != NULL)
	{
	  token_body body;
	  body.token = token;
	  (void) send_message (*ch, msg_op::SESSION_END, &body, sizeof (body));
	}
      m->active_finishers.fetch_sub (1);
    }

    /* ------------------------------------------------------------------ */
    /* control-channel message handlers                                   */
    /* ------------------------------------------------------------------ */

    static void
    handle_handoff (manager &m, channel &ch, const handoff_body &body, int client_fd)
    {
      reject_body reject;
      if (client_fd < 0)
	{
	  reject.reason = (std::int32_t) reject_reason::MALFORMED;
	  (void) send_message (ch, msg_op::HANDOFF_REJECT, &reject, sizeof (reject));
	  return;
	}

      if (m.stopping.load ())
	{
	  reject.reason = (std::int32_t) reject_reason::SHUTDOWN;
	  (void) send_message (ch, msg_op::HANDOFF_REJECT, &reject, sizeof (reject));
	  close (client_fd);
	  return;
	}

      /* the broker routed by the db_info dbname; verify before booting */
      char db_name[33];
      std::size_t name_len = strnlen (body.db_info, 32);
      std::memcpy (db_name, body.db_info, name_len);
      db_name[name_len] = '\0';
      if (m.db_name != db_name)
	{
	  reject.reason = (std::int32_t) reject_reason::DBNAME_MISMATCH;
	  (void) send_message (ch, msg_op::HANDOFF_REJECT, &reject, sizeof (reject));
	  close (client_fd);
	  return;
	}

      std::shared_ptr<session_binding> binding;
      try
	{
	  binding = std::make_shared<session_binding> ();
	}
      catch (const std::bad_alloc &)
	{
	  reject.reason = (std::int32_t) reject_reason::NO_RESOURCES;
	  (void) send_message (ch, msg_op::HANDOFF_REJECT, &reject, sizeof (reject));
	  close (client_fd);
	  return;
	}
      binding->client_fd = client_fd;

      /* adopted connections take normal seats (#117 D5/D6); the decrement
       * runs in the session thread's css_free_conn */
      if (css_increment_num_conn (DB_CLIENT_TYPE_DEFAULT) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CSS_CLIENTS_EXCEEDED, 1, NUM_NORMAL_TRANS);
	  reject.reason = (std::int32_t) reject_reason::CLIENTS_EXCEEDED;
	  (void) send_message (ch, msg_op::HANDOFF_REJECT, &reject, sizeof (reject));
	  close (client_fd);
	  return;
	}

      session_params params;
      params.client_fd = client_fd;
      params.slot_idx = body.slot_idx;
      params.client_ip = body.client_ip;
      params.client_port = body.client_port;
      std::memcpy (params.broker_info, body.broker_info, sizeof (params.broker_info));
      std::memcpy (params.driver_header, body.driver_header, sizeof (params.driver_header));
      std::memcpy (params.db_info, body.db_info, sizeof (params.db_info));
      params.server_name = m.db_name;

      /* register under registry_mutex, reading stopping again there: stop()
       * sets it before it walks the registry under the same mutex, so a
       * session is either refused here or seen (and shut down) by stop() */
      std::uint32_t token = 0;
      bool refused = false;
      {
	std::lock_guard<std::mutex> guard (m.registry_mutex);
	if (m.stopping.load ())
	  {
	    refused = true;
	  }
	else
	  {
	    /* the driver's PID field is signed; never recycle a live token */
	    do
	      {
		token = m.next_token++;
		if (m.next_token > INT_MAX)
		  {
		    m.next_token = 1;
		  }
	      }
	    while (token == 0 || m.registry.count (token) > 0);

	    session_entry entry;
	    entry.token = token;
	    std::memcpy (entry.broker_name, ch.broker_name, sizeof (entry.broker_name));
	    entry.binding = binding;
	    entry.client_ip = body.client_ip;
	    entry.client_port = body.client_port;
	    entry.fn_status = FN_STATUS_PROBE_BUSY;
	    m.registry.emplace (token, entry);
	  }
      }
      if (refused)
	{
	  css_decrement_num_conn (DB_CLIENT_TYPE_DEFAULT);
	  reject.reason = (std::int32_t) reject_reason::SHUTDOWN;
	  (void) send_message (ch, msg_op::HANDOFF_REJECT, &reject, sizeof (reject));
	  close (client_fd);
	  return;
	}
      params.token = token;

      /* ACK strictly before the session may exist: a session that dies fast
       * would otherwise emit SESSION_END ahead of the ACK on this channel
       * (codex review F1).  It carries the registered peer so the broker's
       * channel reader admits the token in wire order.  An ACK the broker no
       * longer reads means it gave the handoff up and counts no slot for it:
       * undo the adoption the way a session ends — its SESSION_END reaches a
       * successor channel that re-synced the token meanwhile */
      resync_token_body ack = {};
      ack.token = token;
      ack.client_ip = params.client_ip;
      ack.client_port = params.client_port;
      if (send_message (ch, msg_op::HANDOFF_ACK, &ack, sizeof (ack)) == NO_ERROR)
	{
	  try
	    {
	      std::thread session_thread (driver_session_run, std::move (params));
	      session_thread.detach ();
	      return;
	    }
	  catch (const std::system_error &)
	    {
	      /* no thread: sign the token off like a session would */
	    }
	}
      registry_begin_session_cleanup (token);
      css_decrement_num_conn (DB_CLIENT_TYPE_DEFAULT);
      registry_session_finished (token);
      close (client_fd);
    }

    static void
    handle_cancel (manager &m, const token_body &body)
    {
      std::shared_ptr<session_binding> binding;
      {
	std::lock_guard<std::mutex> guard (m.registry_mutex);
	auto it = m.registry.find (body.token);
	if (it != m.registry.end ())
	  {
	    binding = it->second.binding;
	  }
      }
      /* the pin keeps the session from releasing this transaction index (and
       * another session from taking it) until the interrupt is set */
      binding_pin pin (binding);
      if (pin.tran_index != NULL_TRAN_INDEX)
	{
	  /* the query_cancel(SIGUSR1) successor (#117 D4): interrupt the
	   * session's transaction directly */
	  (void) logtb_set_tran_index_interrupt (NULL, pin.tran_index, true);
	}
    }

    static void
    handle_status (manager &m, channel &ch, const token_body &body)
    {
      status_reply_body reply;
      reply.fn_status = FN_STATUS_PROBE_NONE;
      {
	std::lock_guard<std::mutex> guard (m.registry_mutex);
	auto it = m.registry.find (body.token);
	if (it != m.registry.end ())
	  {
	    reply.fn_status = it->second.fn_status;
	  }
      }
      (void) send_message (ch, msg_op::STATUS_REPLY, &reply, sizeof (reply));
    }

    static void
    handle_resync (manager &m, channel &ch)
    {
      /* reply = resync_reply_body + live_count resync_token_body entries.  The
       * snapshot is taken and written while holding this channel's send_mutex,
       * so a SESSION_END of a token in it can only follow the reply on the wire
       * (a finisher erases under registry_mutex, then sends under send_mutex,
       * never holding both); a token erased before the snapshot is absent and
       * its END is unknown to the broker.  Lock order send_mutex ->
       * registry_mutex; nobody takes them the other way round. */
      std::vector<char> buf (sizeof (resync_reply_body));
      std::uint32_t live_count = 0;
      std::lock_guard<std::mutex> send_guard (ch.send_mutex);
      {
	std::lock_guard<std::mutex> guard (m.registry_mutex);
	for (const auto &pair : m.registry)
	  {
	    if (std::strncmp (pair.second.broker_name, ch.broker_name, BROKER_NAME_MAX) == 0)
	      {
		resync_token_body t = {};
		t.token = pair.first;
		t.client_ip = pair.second.client_ip;
		t.client_port = pair.second.client_port;
		const char *p = reinterpret_cast<const char *> (&t);
		buf.insert (buf.end (), p, p + sizeof (t));
		live_count++;
	      }
	  }
      }
      resync_reply_body reply;
      reply.live_count = live_count;
      std::memcpy (buf.data (), &reply, sizeof (reply));
      (void) send_message_locked (ch, msg_op::RESYNC_REPLY, buf.data (), buf.size ());
    }

    /* ------------------------------------------------------------------ */
    /* channel and accept threads                                         */
    /* ------------------------------------------------------------------ */

    static void
    channel_thread_run (manager *m, std::shared_ptr<channel> ch)
    {
      /* CANCEL handling lands in engine code that uses the thread-local
       * error context (logtb interrupt notification) — register this foreign
       * thread like the session threads do */
      cubthread::entry *entry_p = cubthread::get_manager ()->claim_entry ();
      if (entry_p != NULL)
	{
	  entry_p->register_id ();
	  entry_p->type = TT_SERVER;
	  entry_p->tran_index = -1;
	  entry_p->m_status = cubthread::entry::status::TS_RUN;
	  entry_p->shutdown = false;
	  entry_p->get_error_context ().register_thread_local ();
	}

      for (;;)
	{
	  msg_header header;
	  int handoff_fd = -1;

	  if (recv_with_fd (ch->fd, &header, sizeof (header), &handoff_fd) != NO_ERROR)
	    {
	      break;
	    }
	  if (header.magic != PROTO_MAGIC || header.length > sizeof (handoff_body))
	    {
	      if (handoff_fd >= 0)
		{
		  close (handoff_fd);
		}
	      break;		/* protocol violation: drop the channel */
	    }

	  char payload[sizeof (handoff_body)];
	  if (header.length > 0 && recv_with_fd (ch->fd, payload, header.length, &handoff_fd) != NO_ERROR)
	    {
	      if (handoff_fd >= 0)
		{
		  close (handoff_fd);
		}
	      break;
	    }

	  switch ((msg_op) header.op)
	    {
	    case msg_op::HELLO:
	      if (header.length == sizeof (hello_body))
		{
		  const hello_body *hello = reinterpret_cast<const hello_body *> (payload);
		  std::memcpy (ch->broker_name, hello->broker_name, BROKER_NAME_MAX);
		  ch->broker_name[BROKER_NAME_MAX - 1] = '\0';
		  {
		    std::lock_guard<std::mutex> guard (m->channels_mutex);
		    std::uint64_t &latest = m->latest_channel[ch->broker_name];
		    latest = std::max (latest, ch->id);
		  }
		  hello_ack_body ack;
		  ack.proto_version = PROTO_VERSION;
		  (void) send_message (*ch, msg_op::HELLO_ACK, &ack, sizeof (ack));
		}
	      break;
	    case msg_op::HANDOFF:
	      if (header.length == sizeof (handoff_body))
		{
		  handoff_body body;
		  std::memcpy (&body, payload, sizeof (body));
		  handle_handoff (*m, *ch, body, handoff_fd);
		  handoff_fd = -1;	/* ownership transferred */
		}
	      else if (handoff_fd >= 0)
		{
		  close (handoff_fd);
		  handoff_fd = -1;
		}
	      break;
	    case msg_op::CANCEL:
	      if (header.length == sizeof (token_body))
		{
		  token_body body;
		  std::memcpy (&body, payload, sizeof (body));
		  handle_cancel (*m, body);
		}
	      break;
	    case msg_op::STATUS:
	      if (header.length == sizeof (token_body))
		{
		  token_body body;
		  std::memcpy (&body, payload, sizeof (body));
		  handle_status (*m, *ch, body);
		}
	      break;
	    case msg_op::RESYNC:
	      handle_resync (*m, *ch);
	      break;
	    default:
	      break;		/* unknown ops are skipped for forward compatibility */
	    }

	  if (handoff_fd >= 0)
	    {
	      close (handoff_fd);	/* fd arrived on a non-handoff message */
	    }
	}

      /* invalidate under send_mutex so no concurrent SESSION_END/reply can
       * write a closed (possibly reused) descriptor (codex review F4); the
       * map entry stays — the accept loop reaps dead channels, stop() joins
       * them (F3: the thread is joinable, never detached) */
      {
	std::lock_guard<std::mutex> guard (ch->send_mutex);
	close (ch->fd);
	ch->fd = -1;
      }
      ch->dead.store (true);

      if (entry_p != NULL)
	{
	  entry_p->tran_index = NULL_TRAN_INDEX;
	  entry_p->m_status = cubthread::entry::status::TS_DEAD;
	  entry_p->get_error_context ().deregister_thread_local ();
	  entry_p->unregister_id ();
	  cubthread::get_manager ()->retire_entry (*entry_p);
	}
    }

    static void
    accept_thread_run (manager *m)
    {
      for (;;)
	{
	  int fd = accept (m->listen_fd, NULL, NULL);
	  if (fd < 0)
	    {
	      if (errno == EINTR)
		{
		  continue;
		}
	      break;		/* listen socket closed: shutting down */
	    }
	  if (m->stopping.load ())
	    {
	      close (fd);
	      break;
	    }

	  auto ch = std::make_shared<channel> ();
	  ch->fd = fd;
	  bool admitted = true;
	  {
	    std::lock_guard<std::mutex> guard (m->channels_mutex);
	    /* reap finished channel threads (a dead entry appears once per
	     * broker restart, so this stays tiny) */
	    for (auto it = m->channels.begin (); it != m->channels.end ();)
	      {
		if (it->second->dead.load ())
		  {
		    if (it->second->thread.joinable ())
		      {
			it->second->thread.join ();
		      }
		    it = m->channels.erase (it);
		  }
		else
		  {
		    ++it;
		  }
	      }
	    /* an unauthenticated local peer can connect repeatedly; bound the
	     * pre-admission channels so thread exhaustion cannot take the
	     * server down (reviewed: PR 7837) */
	    if (m->channels.size () >= (std::size_t) NUM_NORMAL_TRANS + 16)
	      {
		admitted = false;
	      }
	    else
	      {
		ch->id = m->next_channel_id++;
		m->channels.emplace (ch->id, ch);
	      }
	  }
	  if (!admitted)
	    {
	      close (fd);
	      ch->fd = -1;
	      continue;
	    }
	  try
	    {
	      ch->thread = std::thread (channel_thread_run, m, ch);
	    }
	  catch (const std::system_error &)
	    {
	      /* out of threads is a refused connection, not std::terminate */
	      {
		std::lock_guard<std::mutex> guard (m->channels_mutex);
		m->channels.erase (ch->id);
	      }
	      std::lock_guard<std::mutex> send_guard (ch->send_mutex);
	      close (ch->fd);
	      ch->fd = -1;
	    }
	}
    }

    /* ------------------------------------------------------------------ */
    /* lifecycle                                                          */
    /* ------------------------------------------------------------------ */

    /* <TMP>/<prefix>_adopt_<dbname> — same directory convention as the
     * master's well-known socket (css_get_master_domain_path) */
    static std::string
    get_adoption_domain_path (const char *db_name)
    {
      const char *cubrid_tmp = envvar_get ("TMP");
      if (cubrid_tmp == NULL || cubrid_tmp[0] == '\0')
	{
	  cubrid_tmp = "/tmp";
	}
      std::string path (cubrid_tmp);
      path += "/";
      path += envvar_prefix ();
      path += "_adopt_";
      path += db_name;
      return path;
    }

    int
    start (const char *db_name)
    {
      assert (adoption_Manager == NULL);

      manager *m = new manager ();
      m->db_name = db_name;
      m->socket_path = get_adoption_domain_path (db_name);

      struct sockaddr_un addr;
      if (m->socket_path.length () >= sizeof (addr.sun_path))
	{
	  er_log_debug (ARG_FILE_LINE, "adoption: socket path too long: %s\n", m->socket_path.c_str ());
	  delete m;
	  return ER_FAILED;
	}

      /* stale socket from a crashed server: remove it (tcp.c precedent) */
      struct stat stat_buf;
      if (stat (m->socket_path.c_str (), &stat_buf) == 0 && S_ISSOCK (stat_buf.st_mode))
	{
	  unlink (m->socket_path.c_str ());
	}

      m->listen_fd = socket (AF_UNIX, SOCK_STREAM, 0);
      if (m->listen_fd < 0)
	{
	  delete m;
	  return ER_FAILED;
	}

      std::memset (&addr, 0, sizeof (addr));
      addr.sun_family = AF_UNIX;
      std::strcpy (addr.sun_path, m->socket_path.c_str ());
      if (bind (m->listen_fd, (struct sockaddr *) &addr, sizeof (addr)) < 0
	  || listen (m->listen_fd, 8) < 0)
	{
	  close (m->listen_fd);
	  delete m;
	  return ER_FAILED;
	}

      /* the folded CAS speaker's process-wide config stub (cas_server_support) */
      cas_server_speaker_boot_init (db_name);

      m->accept_thread = std::thread (accept_thread_run, m);
      adoption_Manager = m;
      return NO_ERROR;
    }

    void
    stop (void)
    {
      manager *m = adoption_Manager;
      if (m == NULL)
	{
	  return;
	}
      m->stopping.store (true);

      /* wake the accept thread */
      shutdown (m->listen_fd, SHUT_RDWR);
      close (m->listen_fd);
      if (m->accept_thread.joinable ())
	{
	  m->accept_thread.join ();
	}

      /* wake the control-channel readers; their (joinable) threads mark
       * themselves dead and are joined below (codex review F3) */
      {
	std::lock_guard<std::mutex> guard (m->channels_mutex);
	for (auto &pair : m->channels)
	  {
	    std::lock_guard<std::mutex> send_guard (pair.second->send_mutex);
	    if (pair.second->fd >= 0)
	      {
		shutdown (pair.second->fd, SHUT_RDWR);
	      }
	  }
      }

      /* wake every adopted session (their loops block on the client fd) and
       * wait for the sign-offs — sessions must unregister their trans while
       * the server infrastructure is still up.  A handoff registering
       * concurrently re-reads stopping under registry_mutex, so every session
       * that will ever exist is in the registry now. */
      const int wait_secs = prm_get_integer_value (PRM_ID_SHUTDOWN_WAIT_TIME_IN_SECS);
      bool drained = false;
      {
	std::unique_lock<std::mutex> lock (m->registry_mutex);
	for (auto &pair : m->registry)
	  {
	    binding_pin pin (pair.second.binding);
	    if (pin.client_fd >= 0)
	      {
		shutdown (pin.client_fd, SHUT_RDWR);
	      }
	    if (pin.tran_index != NULL_TRAN_INDEX)
	      {
		/* a session mid-request won't see the fd close until it
		 * returns to the wire; interrupt its transaction too */
		(void) logtb_set_tran_index_interrupt (NULL, pin.tran_index, true);
	      }
	  }
	drained = m->registry_cv.wait_for (lock, std::chrono::seconds (wait_secs),
					   [m] { return m->registry.empty (); });
      }

      if (!drained)
	{
	  /* a stuck session still references the manager; freeing it here
	   * would be a use-after-free.  The process is shutting down — leak the
	   * manager instead of racing it. */
	  er_log_debug (ARG_FILE_LINE, "adoption: sessions still registered after %d s; leaking manager\n", wait_secs);
	  unlink (m->socket_path.c_str ());
	  adoption_Manager = NULL;
	  return;
	}

      /* a sign-off may still be inside the manager after its registry erase
       * (SESSION_END notify); don't free the manager under it (codex F2) */
      while (m->active_finishers.load () > 0)
	{
	  std::this_thread::sleep_for (std::chrono::milliseconds (10));
	}

      /* join the channel threads outside channels_mutex: a handler that is
       * still running (HELLO, an undone handoff's sign-off) takes it */
      std::unordered_map<std::uint64_t, std::shared_ptr<channel>> channels;
      {
	std::lock_guard<std::mutex> guard (m->channels_mutex);
	channels.swap (m->channels);
      }
      for (auto &pair : channels)
	{
	  if (pair.second->thread.joinable ())
	    {
	      pair.second->thread.join ();
	    }
	}
      channels.clear ();

      unlink (m->socket_path.c_str ());
      adoption_Manager = NULL;
      delete m;
    }
  }				/* namespace adoption */
}				/* namespace cubconn */

#endif /* SERVER_MODE */
