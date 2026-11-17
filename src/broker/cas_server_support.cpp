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
 * cas_server_support.cpp - what the CAS request code had from its broker and its process, given by cub_server
 *
 * A CAS read its configuration from broker shared memory (shm_appl) and kept its per-connection scratch in a
 * CAS slot (as_info).  In the server a session is a thread: as_info is the thread's slot, the configuration is
 * the session's copy of what its broker sent with the handoff (cas_session_cfg), and shm_appl is one read-only
 * stub for the fields no broker configures here.  The session's SQL, slow and DDL logs are named after its
 * broker and an index it holds while it writes them; the access log and the ACCESS_CONTROL check of a CAS are
 * here too.  sql_log2 stays off: it redirects fd 1, which is the process's.
 */

#if defined (SERVER_MODE)

#include <semaphore.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cctype>
#include <cerrno>
#include <climits>
#include <cstring>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "broker_config.h"
#include "broker_filename.h"	/* make_abs_path */
#include "broker_shm.h"		/* uw_sem_* declarations */
#include "broker_util.h"	/* trim */
#include "cas_common.h"
#include "cas_common_main.h"	/* cas_shutdown_signo */
#include "cas_common_vars.h"
#include "cas_dispatch.h"	/* the server-support API declarations */
#include "cas_log.h"
#include "cas_net_buf.h"
#include "cas_sql_log2.h"
#include "environment_variable.h"
#include "system_parameter.h"
#include "trigger_manager.h"
// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

/* ------------------------------------------------------------------ */
/* shm stubs                                                          */
/* ------------------------------------------------------------------ */

static T_SHM_APPL_SERVER cas_Shm_stub;

/* cub_cas sets it from SIGTERM/SIGINT to leave its loops; a folded session is stopped through its transport instead */
volatile sig_atomic_t cas_shutdown_signo = 0;
static thread_local T_APPL_SERVER_INFO cas_As_slot;
static thread_local int cas_Log_lease_fd = -1;

void
cas_server_speaker_boot_init (const char *db_name)
{
  T_SHM_APPL_SERVER *shm = &cas_Shm_stub;
  struct sigaction act;

  (void) db_name;

  std::memset (shm, 0, sizeof (*shm));

  /* what no broker configures for a session of the server; the rest is cas_session_cfg */
  std::strncpy (shm->broker_name, "cub_server", BROKER_NAME_LEN - 1);
  shm->keep_connection = KEEP_CON_ON;
  shm->sql_log2 = 0;
  shm->access_mode = READ_WRITE_ACCESS_MODE;
  shm->replica_only_flag = OFF;
  shm->cas_rctime = 0;
  shm->job_queue_size = 0;

  shm_appl = shm;
  program_name = "cub_server";
  cas_shard_flag = OFF;

  /* the SQL log's flush timer of a session signals the session's thread (cas_log.c); the handler tells its
   * own signals by their value and returns on any other */
  std::memset (&act, 0, sizeof (act));
  act.sa_sigaction = cas_log_sigusr2_handler;
  sigemptyset (&act.sa_mask);
  act.sa_flags = SA_SIGINFO | SA_RESTART;
  (void) sigaction (SIGUSR2, &act, NULL);
}

/* the session takes the configuration its broker sent: at its start, and at a request boundary after
 * broker_changer changed it.  Only the session's thread reads and writes cas_session_cfg and its slot. */
static void
cas_server_session_config_apply (const cubconn::adoption::session_config *config, bool at_start)
{
  T_APPL_SERVER_INFO *slot = &cas_As_slot;

  if (!at_start)
    {
      /* as broker_changer told every CAS: the logs are opened again at the mode and directory of now */
      if (slot->cur_sql_log_mode != config->sql_log_mode
	  || std::strcmp (cas_session_cfg.log_dir, config->log_dir) != 0)
	{
	  slot->cas_log_reset = CAS_LOG_RESET_REOPEN;
	}
      if (slot->cur_slow_log_mode != config->slow_log_mode
	  || std::strcmp (cas_session_cfg.slow_log_dir, config->slow_log_dir) != 0)
	{
	  slot->cas_slow_log_reset = CAS_LOG_RESET_REOPEN;
	}
      if (cas_session_cfg.trigger_action_flag != config->trigger_action_flag)
	{
	  (void) tr_set_execution_state (config->trigger_action_flag != 0);
	}
    }

  cas_session_cfg = *config;
  slot->cur_sql_log_mode = (char) config->sql_log_mode;
  slot->cur_slow_log_mode = (char) config->slow_log_mode;
  slot->cur_statement_pooling = config->statement_pooling ? ON : OFF;
  slot->cci_default_autocommit = (char) config->cci_default_autocommit;
  stripped_column_name = (char) config->stripped_column_name;
  set_net_buf_size ();
}

void
cas_server_session_config_take (void)
{
  cubconn::adoption::session_config config;

  if (cubconn::adoption::registry_take_config (config))
    {
      cas_server_session_config_apply (&config, false);
    }
}

/* per adopted session: point the CAS globals at this thread's slot */
void
cas_server_session_slot_begin (int client_type, int client_version, const char *driver_info, const char *broker,
			       const cubconn::adoption::session_config *config)
{
  T_APPL_SERVER_INFO *slot = &cas_As_slot;

  std::memset (slot, 0, sizeof (*slot));
  CON_STATUS_LOCK_INIT (slot);

  slot->service_flag = ON;
  slot->con_status = CON_STATUS_IN_TRAN;	/* connect starts in-tran (cas_common_main.c:136) */
  slot->cur_keep_con = KEEP_CON_ON;	/* driver_session raises it to AUTO for an AUTO broker's session */
  slot->cas_change_mode = CAS_CHANGE_MODE_DEFAULT;
  slot->auto_commit_mode = FALSE;
  slot->cur_sql_log2 = 0;
  slot->isolation_level = CAS_USE_DEFAULT_DB_PARAM;
  slot->lock_timeout = CAS_USE_DEFAULT_DB_PARAM;
  slot->clt_version = client_version;
  slot->cas_client_type = (char) client_type;
  std::memcpy (slot->driver_info, driver_info, SRV_CON_CLIENT_INFO_SIZE);
  slot->fn_status = FN_STATUS_CONN;

  as_info = slot;
  snprintf (broker_name, sizeof (broker_name), "%s", broker);
  shm_as_index = 0;
  cas_server_session_config_apply (config, true);

  /* per-connection CAS globals that cas_common_main.c's session setup used
   * to (re)initialize */
  cas_client_type = (char) client_type;
  errors_in_transaction = 0;
  is_first_request = true;
  con_status_before_check_cas = -1;
  query_cancel_flag = 0;
  cas_info_size = CAS_INFO_SIZE;
}

/* A CAS wrote <broker>_<its number>.sql.log; a session writes <broker>_<N>.sql.log, N the lowest index no
 * session of that broker holds.  One broker hands sessions to several servers and its sessions outlive its
 * restart, so the index is held as a lock on a file of the host: flock belongs to the open file description,
 * which excludes the threads of one server as well as other servers, and the kernel drops it when a server
 * dies.  The files are never removed: a new inode would stand beside a held lock. */
int
cas_server_session_log_begin (void)
{
  char directory[BROKER_PATH_MAX];
  char path[BROKER_PATH_MAX];

  assert (cas_Log_lease_fd < 0);
  if (broker_name[0] == '\0' || std::strchr (broker_name, '/') != NULL)
    {
      return -1;
    }
  envvar_vardir_file (directory, sizeof (directory), "cas_log_slots");
  if (mkdir (directory, 0770) != 0 && errno != EEXIST)
    {
      return -1;
    }

  for (int index = 0; index < INT_MAX; index++)
    {
      int result;

      if ((std::size_t) snprintf (path, sizeof (path), "%s/%s_%d.lock", directory, broker_name, index + 1)
	  >= sizeof (path))
	{
	  return -1;
	}
      int fd = open (path, O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0660);
      if (fd < 0)
	{
	  return -1;
	}
      do
	{
	  result = flock (fd, LOCK_EX | LOCK_NB);
	}
      while (result < 0 && errno == EINTR);
      if (result == 0)
	{
	  cas_Log_lease_fd = fd;
	  shm_as_index = index;
	  return 0;
	}
      const int lock_errno = errno;
      close (fd);
      if (lock_errno != EWOULDBLOCK)
	{
	  return -1;
	}
    }
  return -1;
}

/* the session closed its logs: its index is free for the next session of the broker */
void
cas_server_session_slot_end (void)
{
  if (as_info == &cas_As_slot)
    {
      cas_log_timer_final ();
      CON_STATUS_LOCK_DESTROY (&cas_As_slot);
      as_info = NULL;
      if (cas_Log_lease_fd >= 0)
	{
	  close (cas_Log_lease_fd);
	  cas_Log_lease_fd = -1;
	}
      shm_as_index = 0;
    }
}

/* ------------------------------------------------------------------ */
/* ACCESS_CONTROL: the db:dbuser:ip check a CAS made, at session        */
/* establishment.  The file is a broker's ACCESS_CONTROL_FILE:          */
/* [%BROKER] sections of "dbname:dbuser:ipfile[,ipfile]" lines, ip      */
/* files under $CUBRID/conf.  Names match without case, '*' is any,     */
/* an ip matches by its leading octets, and 127.0.0.1 always passes.    */
/* A file that is missing or cannot be read in full allows nobody       */
/* else.                                                                */
/* ------------------------------------------------------------------ */

struct cas_acl_rule
{
  std::string dbname;
  std::string dbuser;
  std::vector<unsigned char> addresses;	/* IP_BYTE_COUNT bytes per entry */
};

typedef std::map<std::string, std::vector<cas_acl_rule>> cas_acl_table;	/* by broker section, lower case */

/* A check reads the table under cas_Acl_mutex.  A load reads the file with only cas_Acl_load_mutex held and
 * puts its table in under cas_Acl_mutex: a check is decided by one table as a whole and does not wait for
 * the file, and the loads take turns, so a later load's table is not replaced by an earlier load's. */
static std::mutex cas_Acl_load_mutex;
static std::mutex cas_Acl_mutex;
static cas_acl_table cas_Acl_table;
static bool cas_Acl_loaded = false;
static bool cas_Acl_load_failed = false;

static std::string
cas_acl_lower (const char *s)
{
  std::string out (s);
  for (char &c : out)
    {
      c = (char) tolower ((unsigned char) c);
    }
  return out;
}

/* one ip list file, same accept rules as access_control_read_ip_info */
static int
cas_acl_read_ip_file (const char *path, std::vector<unsigned char> &addresses)
{
  FILE *fp = fopen (path, "r");
  char buf[LINE_MAX];

  if (fp == NULL)
    {
      return -1;
    }

  while (fgets (buf, (int) sizeof (buf), fp) != NULL)
    {
      char *p = strchr (buf, '#');
      char *save = NULL;
      unsigned char entry[IP_BYTE_COUNT] = { 0, 0, 0, 0, 0 };
      int i;

      if (p != NULL)
	{
	  *p = '\0';
	}
      trim (buf);
      if (buf[0] == '\0')
	{
	  continue;
	}

      char *token = strtok_r (buf, ".", &save);
      for (i = 0; i < 4; i++)
	{
	  if (token == NULL)
	    {
	      fclose (fp);
	      return -1;
	    }
	  if (strcmp (token, "*") == 0)
	    {
	      break;
	    }
	  char *end = NULL;
	  long adr = strtol (token, &end, 10);
	  if (end == token || *end != '\0' || adr < 0 || adr > 255)
	    {
	      fclose (fp);
	      return -1;
	    }
	  entry[1 + i] = (unsigned char) adr;
	  token = strtok_r (NULL, ".", &save);
	}
      entry[0] = (unsigned char) i;	/* number of significant octets; 0 = any */

      addresses.insert (addresses.end (), entry, entry + IP_BYTE_COUNT);
    }

  fclose (fp);
  return 0;
}

/* read the whole ACCESS_CONTROL_FILE; false when it is missing or any part of it cannot be read */
static bool
cas_acl_parse (cas_acl_table &table)
{
  const char *file = prm_get_string_value (PRM_ID_CAS_ACCESS_CONTROL_FILE);
  char buf[1024];
  char path_buf[BROKER_PATH_MAX];
  std::string current_section;

  if (file == NULL || file[0] == '\0')
    {
      return false;
    }
  FILE *fp = fopen (file, "r");
  if (fp == NULL)
    {
      return false;
    }

  bool ok = true;
  while (ok && fgets (buf, (int) sizeof (buf), fp) != NULL)
    {
      char *p = strchr (buf, '#');
      if (p != NULL)
	{
	  *p = '\0';
	}
      trim (buf);
      if (buf[0] == '\0')
	{
	  continue;
	}

      size_t len = strlen (buf);
      if (strncmp (buf, "[%", 2) == 0 && buf[len - 1] == ']')
	{
	  buf[len - 1] = '\0';
	  current_section = cas_acl_lower (buf + 2);
	  continue;
	}

      char *save = NULL;
      char *dbname = strtok_r (buf, ":", &save);
      char *dbuser = (dbname != NULL) ? strtok_r (NULL, ":", &save) : NULL;
      char *ip_files = save;

      /* an entry before any [%BROKER] section is an error, as is one without its three parts */
      if (current_section.empty () || dbname == NULL || dbuser == NULL || ip_files == NULL || ip_files[0] == '\0')
	{
	  ok = false;
	  break;
	}

      cas_acl_rule rule;
      rule.dbname = dbname;
      rule.dbuser = dbuser;

      char *fsave = NULL;
      for (char *token = strtok_r (ip_files, ",", &fsave); token != NULL; token = strtok_r (NULL, ",", &fsave))
	{
	  trim (token);
	  if (make_abs_path (path_buf, "conf", token, BROKER_PATH_MAX) < 0
	      || cas_acl_read_ip_file (path_buf, rule.addresses) < 0)
	    {
	      ok = false;
	      break;
	    }
	}
      table[current_section].push_back (std::move (rule));
    }

  fclose (fp);
  return ok;
}

void
cas_server_acl_reload (void)
{
  cas_acl_table table;
  bool ok = false;
  std::lock_guard<std::mutex> load_guard (cas_Acl_load_mutex);

  try
    {
      ok = cas_acl_parse (table);
    }
  catch (const std::bad_alloc &)
    {
    }

  std::lock_guard<std::mutex> guard (cas_Acl_mutex);
  if (ok)
    {
      cas_Acl_table.swap (table);
    }
  else
    {
      cas_Acl_table.clear ();
    }
  cas_Acl_load_failed = !ok;
  cas_Acl_loaded = true;
}

/* 0: allowed, -1: rejected.  address is the client's 4 bytes in network order */
int
cas_server_acl_check (const char *broker, const char *dbname, const char *dbuser, const unsigned char *address)
{
  const bool local_ip = (address[0] == 127 && address[1] == 0 && address[2] == 0 && address[3] == 1);

  if (!prm_get_bool_value (PRM_ID_CAS_ACCESS_CONTROL) || local_ip)
    {
      return 0;
    }

  std::string section;
  try
    {
      section = cas_acl_lower (broker);
    }
  catch (const std::bad_alloc &)
    {
      return -1;
    }

  std::unique_lock<std::mutex> guard (cas_Acl_mutex);
  if (!cas_Acl_loaded)
    {
      guard.unlock ();
      cas_server_acl_reload ();
      guard.lock ();
    }
  if (cas_Acl_load_failed)
    {
      return -1;
    }

  auto it = cas_Acl_table.find (section);
  if (it == cas_Acl_table.end () || it->second.empty ())
    {
      return prm_get_bool_value (PRM_ID_CAS_ACCESS_CONTROL_DEFAULT_ALLOW) ? 0 : -1;
    }

  /* dbname may carry an @host suffix; match only the name part */
  const char *at = strchr (dbname, '@');
  const size_t dbname_len = (at != NULL) ? (size_t) (at - dbname) : strlen (dbname);

  for (const cas_acl_rule &rule : it->second)
    {
      if (rule.dbname != "*"
	  && (rule.dbname.size () != dbname_len || strncasecmp (rule.dbname.c_str (), dbname, dbname_len) != 0))
	{
	  continue;
	}
      if (rule.dbuser != "*" && strcasecmp (rule.dbuser.c_str (), dbuser) != 0)
	{
	  continue;
	}
      for (size_t i = 0; i + IP_BYTE_COUNT <= rule.addresses.size (); i += IP_BYTE_COUNT)
	{
	  const unsigned char significant = rule.addresses[i];
	  if (significant == 0 || memcmp (&rule.addresses[i + 1], address, significant) == 0)
	    {
	      return 0;
	    }
	}
    }
  return -1;
}

/* ------------------------------------------------------------------ */
/* access log: the sessions of a broker append to its one file; its     */
/* size check, rename and reopen are not one step, so the writers of    */
/* the server take turns                                                */
/* ------------------------------------------------------------------ */

static std::mutex cas_Access_log_mutex;

int
cas_server_access_log (struct timeval *start_time, int client_ip_addr, char *dbname, char *dbuser, int log_type)
{
  std::lock_guard<std::mutex> guard (cas_Access_log_mutex);

  return cas_access_log (start_time, shm_as_index, client_ip_addr, dbname, dbuser, (ACCESS_LOG_TYPE) log_type);
}

/* ------------------------------------------------------------------ */
/* uw_sem wrappers (CON_STATUS_LOCK); broker_shm.c is not folded      */
/* ------------------------------------------------------------------ */

int
uw_sem_init (sem_t *sem)
{
  /* pshared=0: both sides of this lock live in cub_server now */
  return sem_init (sem, 0, 1);
}

int
uw_sem_wait (sem_t *sem)
{
  return sem_wait (sem);
}

int
uw_sem_post (sem_t *sem)
{
  return sem_post (sem);
}

int
uw_sem_destroy (sem_t *sem)
{
  return sem_destroy (sem);
}

/* ------------------------------------------------------------------ */
/* sql_log2: dup2 on fd 1 cannot be per-session — structurally off    */
/* (#B1 fold note R3); the stubs keep the folded call sites linkable  */
/* ------------------------------------------------------------------ */

void
sql_log2_init (char *, int, int, bool)
{
}

char *
sql_log2_get_filename (void)
{
  static char empty[] = "";
  return empty;
}

void
sql_log2_dup_stdout (void)
{
}

void
sql_log2_restore_stdout (void)
{
}

void
sql_log2_end (bool)
{
}

void
sql_log2_write (const char *, ...)
{
}

void
sql_log2_append_file (char *)
{
}

void
sql_log2_flush (void)
{
}

#endif /* SERVER_MODE */
