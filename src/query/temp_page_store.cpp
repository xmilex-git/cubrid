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
 * temp_page_store.cpp - temporary query page backing and work-memory accounting
 */

#include "temp_page_store.hpp"

#include "dbtype.h"
#include "error_manager.h"
#include "boot_sr.h"
#include "file_manager.h"
#include "list_file.h"
#include "log_impl.h"
#include "object_domain.h"
#include "object_representation.h"
#include "page_buffer.h"
#include "perf_monitor.h"
#include "qfile_page_spill.hpp"	/* (c′) PAGE_SPILL_OVERFLOW backing (#132) */
#include "qfile_spill_file.hpp"	/* full_pwrite/full_pread (shared substrate, #132) */
#include "query_manager.h"
#include "system_parameter.h"
#include "tde.h"
#include "file_io.h"

#include <cerrno>
#include <chrono>
#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <deque>
#include <atomic>
#include <cstdio>
#include <dirent.h>
#include <fcntl.h>
#include <functional>
#include <immintrin.h>
#include <limits>
#include <mutex>
#include <new>
#include <set>
#include <sched.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unordered_map>
#include <unistd.h>
#include <vector>

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace
{
  constexpr std::size_t WORKMEM_SHARD_COUNT = 64;
  constexpr std::size_t WORKMEM_REFILL_QUANTUM_BYTES = 256ULL * 1024ULL;
  constexpr std::size_t WORKMEM_MIN_CAP_BYTES = 64ULL * 1024ULL * 1024ULL;
  constexpr std::size_t WORKMEM_MAX_CAP_BYTES = 4ULL * 1024ULL * 1024ULL * 1024ULL;
  constexpr std::size_t WORKMEM_MIN_TEMP_FILE_BYTES = 4ULL * 1024ULL * 1024ULL;
  constexpr std::size_t WORKMEM_MAX_TEMP_FILE_BYTES = 256ULL * 1024ULL * 1024ULL;
  constexpr std::size_t WORKMEM_POSITION_SAVED_SCAN_BUDGET = 64;
  constexpr std::size_t WORKMEM_POSITION_HASH_ENTRY_BUDGET = 4096;
  constexpr std::size_t WORKMEM_CONNECT_BY_PARENT_BUDGET = 1024;

  /* LEADER-VERIFIED P5 PRODUCTION FLIP -- now TRUE.  The earlier large-data regression (connect_list-based
   * merges require homogeneous real-VPID disk lists; raw-fd-backed NULL_VOLID lists could not be relinked ->
   * server abort) was root-caused to VPID-only cross-file identity and fixed by the segment-native merge
   * (qmgr per-segment copy into a single-owner real-VPID destination) and the segment-native parallel
   * hash-aggregate partial-list consumption; the parallel-hash-GBY scope-limit was removed once that
   * consumption became segment-native.  A large-data guard-reflip campaign on a fresh 1,048,576-row TDE DB
   * then passed with raw-fd LIVE: hash join / external sort / hash aggregate / DISTINCT / ORDER BY all
   * correct and parallel==serial md5-parity at parallelism 4 and 1; TDE raw-fd pages encrypted (no
   * plaintext, fresh nonce per write/physical-page); orphan-zero across SIGKILL-mid-spill + boot full-sweep.
   * Raw-fd writes remain runtime-gated on (master && boot_sweep_complete && reaper_active), so raw-fd activates
   * after the full safety net is live.  Revert this line to false to
   * return to the develop overflow path.  See .not_git_tracking/scratch/p6-design.md + bench/harness/results/g003. */
  constexpr bool LEADER_VERIFIED_ENABLE_RAW_FD_WRITES = true;

  struct alignas (64) workmem_shard
  {
    std::atomic<int64_t> reserved { 0 };
  };

  struct workmem_accountant
  {
    std::once_flag init_once;
    std::atomic<int64_t> cap { 0 };
    workmem_shard shards[WORKMEM_SHARD_COUNT];
  };

  workmem_accountant g_accountant;
  void put_page_header (PAGE_PTR page_p, QFILE_PAGE_HEADER *header_p);

  constexpr std::size_t RAWFD_READ_CACHE_PAGES = 32;
  constexpr int RAWFD_REAPER_GRACE_SECONDS = 30;
  constexpr const char *RAWFD_ROOT_SUFFIX = "cubrid-rawfd-workmem";
  constexpr const char *RAWFD_SERVER_ID_FILE = ".server-id";

  struct rawfd_registry_entry
  {
    temp_page_store::raw_fd_key key {};
    std::string path;
    QUERY_ID query_id { NULL_QUERY_ID };
    int owner_tran_index { NULL_TRAN_INDEX };
    unsigned int worker_id { 0 };
    int fd { -1 };
    temp_page_store::raw_fd_file *owner { nullptr };
    QMGR_TEMP_FILE *tfile_owner { nullptr };
    std::chrono::steady_clock::time_point created_at;
  };

  struct rawfd_cached_page
  {
    temp_page_store::raw_fd_key key {};
    PAGEID page_index { NULL_PAGEID };
    std::vector<char> io_page;
  };

  struct rawfd_fixed_page
  {
    temp_page_store::raw_fd_file *file { nullptr };
    PAGEID page_index { NULL_PAGEID };
    PAGE_PTR page_p { NULL };
    int ref_count { 1 };
    bool dirty { false };
  };
  using rawfd_sec_map = std::unordered_map<temp_page_store::rawfd_sec_key, PAGE_PTR>;
  constexpr std::size_t RAWFD_FIXED_PAGE_SHARD_COUNT = 64;
  constexpr std::uint64_t RAWFD_SHARD_GOLDEN = 0x9E3779B97F4A7C15ULL;

  struct alignas (64) rawfd_primary_shard
  {
    std::mutex m;
    std::unordered_map<PAGE_PTR, rawfd_fixed_page> map;
  };

  struct alignas (64) rawfd_secondary_shard
  {
    std::mutex m;
    rawfd_sec_map map;
  };

  std::uint64_t
  rawfd_sec_key_hash (const temp_page_store::rawfd_sec_key &key) noexcept
  {
    return static_cast<std::uint64_t> (std::hash<temp_page_store::rawfd_sec_key> {} (key));
  }

  std::size_t
  primary_shard_of (PAGE_PTR page_p) noexcept
  {
    return (reinterpret_cast<std::uintptr_t> (page_p) >> 6) & (RAWFD_FIXED_PAGE_SHARD_COUNT - 1);
  }

  std::size_t
  secondary_shard_of (const temp_page_store::rawfd_sec_key &key) noexcept
  {
    return ((rawfd_sec_key_hash (key) * RAWFD_SHARD_GOLDEN) >> 58) & (RAWFD_FIXED_PAGE_SHARD_COUNT - 1);
  }

  constexpr int RAWFD_ALLOC_MAX_RETRIES = 16;

  temp_page_store::rawfd_sec_key
  rawfd_make_sec_key (const temp_page_store::raw_fd_key &file_key, PAGEID page_index) noexcept
  {
    return temp_page_store::rawfd_sec_key { file_key.file_seq, page_index };
  }

  bool
  rawfd_make_sec_key (const rawfd_fixed_page &entry, temp_page_store::rawfd_sec_key &sec_key) noexcept
  {
    if (entry.file == nullptr)
      {
	return false;
      }

    sec_key = rawfd_make_sec_key (entry.file->key (), entry.page_index);
    return true;
  }

  bool
  rawfd_fixed_page_matches_sec_key (const rawfd_fixed_page &entry, const temp_page_store::rawfd_sec_key &sec_key) noexcept
  {
    if (entry.file == nullptr || entry.page_index != sec_key.page_index)
      {
	return false;
      }

    return entry.file->key ().file_seq == sec_key.file_seq;
  }

  struct rawfd_file_snapshot
  {
    temp_page_store::raw_fd_key key {};
    std::string path;
    QUERY_ID query_id { NULL_QUERY_ID };
    int owner_tran_index { NULL_TRAN_INDEX };
    int fd { -1 };
    temp_page_store::raw_fd_file *owner { nullptr };
  };

  struct rawfd_disk_candidate
  {
    temp_page_store::raw_fd_key key {};
    std::string path;
    QUERY_ID query_id { NULL_QUERY_ID };
    int owner_tran_index { NULL_TRAN_INDEX };
    std::chrono::system_clock::time_point modified_at;
  };

  struct rawfd_state
  {
    std::once_flag init_once;
    std::mutex registry_mutex;
    std::mutex read_cache_mutex;
    std::uint64_t boot_incarnation { 0 };
    std::atomic<std::uint64_t> file_seq { 0 };
    std::string scratch_root;
    std::string db_name;
    std::string server_id;
    std::string server_subtree;
    bool boot_sweep_complete { false };
    bool tde_wired { false };
    bool reaper_active { false };
    std::unordered_map<std::uint64_t, rawfd_registry_entry> registry;
    rawfd_primary_shard fixed_primary_shards[RAWFD_FIXED_PAGE_SHARD_COUNT];
    rawfd_secondary_shard fixed_secondary_shards[RAWFD_FIXED_PAGE_SHARD_COUNT];
    std::deque<rawfd_cached_page> read_cache;
  };

  rawfd_state g_rawfd_state;
#ifdef RAWFD_UNIT_TEST
  std::atomic<int> g_rawfd_test_alloc_calls { 0 };
  std::atomic<int> g_rawfd_test_alloc_retries { 0 };
  std::atomic<bool> g_rawfd_test_force_persistent_primary_absent { false };
  std::vector<int> g_rawfd_test_purge_events;
  constexpr int RAWFD_TEST_EVENT_SECONDARY_ERASE = 1;
  constexpr int RAWFD_TEST_EVENT_FREE_BUFFER = 2;
#endif

#if !defined (NDEBUG)
  std::atomic<int64_t> g_rawfd_fault_inject_spill_writes { 0 };
#endif /* !NDEBUG */

  std::string
  sanitize_path_component (const char *input)
  {
    std::string out;
    if (input == NULL || input[0] == '\0')
      {
	return "unknown";
      }

    for (const char *p = input; *p != '\0'; p++)
      {
	const unsigned char ch = static_cast<unsigned char> (*p);
	if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-')
	  {
	    out.push_back (static_cast<char> (ch));
	  }
	else
	  {
	    out.push_back ('_');
	  }
      }

    return out.empty () ? std::string ("unknown") : out;
  }

  bool
  mkdir_if_needed (const std::string &path)
  {
    if (path.empty ())
      {
	return false;
      }

    if (mkdir (path.c_str (), 0700) == 0 || errno == EEXIST)
      {
	return true;
      }

    return false;
  }

  bool
  mkdir_recursive (const std::string &path)
  {
    if (path.empty ())
      {
	return false;
      }

    std::string partial;
    if (path[0] == '/')
      {
	partial = "/";
      }

    std::size_t start = (path[0] == '/') ? 1 : 0;
    while (start <= path.size ())
      {
	const std::size_t slash = path.find ('/', start);
	const std::string part = path.substr (start, slash == std::string::npos ? std::string::npos : slash - start);
	if (!part.empty ())
	  {
	    if (!partial.empty () && partial[partial.size () - 1] != '/')
	      {
		partial += '/';
	      }
	    partial += part;
	    if (!mkdir_if_needed (partial))
	      {
		return false;
	      }
	  }

	if (slash == std::string::npos)
	  {
	    break;
	  }
	start = slash + 1;
      }

    return true;
  }

  std::string
  read_small_file (const std::string &path)
  {
    FILE *fp = fopen (path.c_str (), "r");
    if (fp == NULL)
      {
	return std::string ();
      }

    char buffer[128];
    const size_t nread = fread (buffer, 1, sizeof (buffer) - 1, fp);
    fclose (fp);
    buffer[nread] = '\0';

    std::string value (buffer);
    while (!value.empty () && (value[value.size () - 1] == '\n' || value[value.size () - 1] == '\r'))
      {
	value.erase (value.size () - 1);
      }

    return value;
  }

  bool
  write_small_file (const std::string &path, const std::string &value)
  {
    FILE *fp = fopen (path.c_str (), "w");
    if (fp == NULL)
      {
	return false;
      }

    const bool ok = fwrite (value.c_str (), 1, value.size (), fp) == value.size () && fputc ('\n', fp) != EOF;
    fclose (fp);
    return ok;
  }

  bool
  unlink_tree_files_only (const std::string &dir)
  {
    DIR *dp = opendir (dir.c_str ());
    if (dp == NULL)
      {
	return false;
      }

    bool ok = true;
    struct dirent *entry = NULL;
    while ((entry = readdir (dp)) != NULL)
      {
	if (strcmp (entry->d_name, ".") == 0 || strcmp (entry->d_name, "..") == 0)
	  {
	    continue;
	  }

	const std::string child = dir + "/" + entry->d_name;
	struct stat st;
	if (lstat (child.c_str (), &st) != 0)
	  {
	    ok = false;
	    continue;
	  }

	if (S_ISDIR (st.st_mode))
	  {
	    ok = unlink_tree_files_only (child) && ok;
	    if (rmdir (child.c_str ()) != 0 && errno != ENOENT)
	      {
		ok = false;
	      }
	  }
	else if (unlink (child.c_str ()) != 0 && errno != ENOENT)
	  {
	    ok = false;
	  }
      }

    closedir (dp);
    return ok;
  }

  std::uint64_t
  make_boot_incarnation () noexcept
  {
    const std::uint64_t now =
      static_cast<std::uint64_t> (std::chrono::system_clock::now ().time_since_epoch ().count ());
    const std::uint64_t pid = static_cast<std::uint64_t> (getpid ());
    return (now << 16) ^ pid;
  }

  void
  init_rawfd_state ()
  {
    /* scratch_root resolution (DISK-BACKED, never tmpfs): raw-fd overflow files are real spill storage;
     * placing them on tmpfs (/tmp) would defeat the spill and risk OOM. Priority:
     *   1) $CUBRID_TMP (operator-configured scratch dir),
     *   2) the database volume directory (disk-backed; where the DB .db volumes live),
     *   3) $TMP / /tmp as a last-resort fallback. */
    std::string scratch_base;
    const char *cubrid_tmp_env = getenv ("CUBRID_TMP");
    if (cubrid_tmp_env != NULL && cubrid_tmp_env[0] != '\0')
      {
	scratch_base = cubrid_tmp_env;
      }
    else
      {
	const char *db_full = boot_db_full_name ();
	if (db_full != NULL && db_full[0] != '\0')
	  {
	    char dir_buf[PATH_MAX];
	    const char *dir = fileio_get_directory_path (dir_buf, db_full);
	    if (dir != NULL && dir[0] != '\0')
	      {
		scratch_base = dir;
	      }
	  }
      }
    if (scratch_base.empty ())
      {
	const char *tmp_env = getenv ("TMP");
	scratch_base = (tmp_env != NULL && tmp_env[0] != '\0') ? std::string (tmp_env) : std::string ("/tmp");
      }
    g_rawfd_state.scratch_root = scratch_base + "/" + RAWFD_ROOT_SUFFIX;

    g_rawfd_state.db_name = sanitize_path_component (boot_db_name ());
    const std::string db_root = g_rawfd_state.scratch_root + "/" + g_rawfd_state.db_name;
    bool boot_sweep_ok = mkdir_recursive (db_root);

    const std::string server_id_path = db_root + "/" + RAWFD_SERVER_ID_FILE;
    g_rawfd_state.server_id = sanitize_path_component (read_small_file (server_id_path).c_str ());
    if (g_rawfd_state.server_id == "unknown")
      {
	g_rawfd_state.server_id = "sid_" + std::to_string (make_boot_incarnation ());
	boot_sweep_ok = write_small_file (server_id_path, g_rawfd_state.server_id) && boot_sweep_ok;
      }

    g_rawfd_state.server_subtree = db_root + "/" + g_rawfd_state.server_id;
    boot_sweep_ok = mkdir_recursive (g_rawfd_state.server_subtree) && boot_sweep_ok;
    if (boot_sweep_ok)
      {
	boot_sweep_ok = unlink_tree_files_only (g_rawfd_state.server_subtree);
      }

    g_rawfd_state.boot_incarnation = make_boot_incarnation ();
    g_rawfd_state.boot_sweep_complete = boot_sweep_ok;
    g_rawfd_state.tde_wired = tde_is_loaded ()
      && ((TDE_ALGORITHM) prm_get_integer_value (PRM_ID_TDE_DEFAULT_ALGORITHM) != TDE_ALGORITHM_NONE);
    g_rawfd_state.reaper_active = boot_sweep_ok;
  }

  void
  ensure_rawfd_state ()
  {
    std::call_once (g_rawfd_state.init_once, init_rawfd_state);
  }

  std::uint64_t
  registry_map_key (const temp_page_store::raw_fd_key &key) noexcept
  {
    return (key.boot_incarnation << 32) ^ key.file_seq;
  }

  bool
  rawfd_key_equals (const temp_page_store::raw_fd_key &lhs, const temp_page_store::raw_fd_key &rhs) noexcept
  {
    return lhs.boot_incarnation == rhs.boot_incarnation && lhs.file_seq == rhs.file_seq;
  }
  void free_db_page_buffer (PAGE_PTR page_p) noexcept;

  struct rawfd_purge_item
  {
    temp_page_store::rawfd_sec_key sec_key {};
    PAGE_PTR page_p { NULL };
  };


  void
  purge_cached_pages_locked (const temp_page_store::raw_fd_key &key) noexcept
  {
    g_rawfd_state.read_cache.erase (std::remove_if (g_rawfd_state.read_cache.begin (), g_rawfd_state.read_cache.end (),
						    [&key] (const rawfd_cached_page &entry)
    {
      return rawfd_key_equals (entry.key, key);
    }),
				    g_rawfd_state.read_cache.end ());
  }
  void
  purge_fixed_pages_for_key (const temp_page_store::raw_fd_key &key) noexcept
  {
    std::vector<rawfd_purge_item> purge_items;

    for (std::size_t shard_index = 0; shard_index < RAWFD_FIXED_PAGE_SHARD_COUNT; shard_index++)
      {
	rawfd_primary_shard &primary_shard = g_rawfd_state.fixed_primary_shards[shard_index];
	std::lock_guard<std::mutex> primary_guard (primary_shard.m);

	for (const auto &fixed : primary_shard.map)
	  {
	    const rawfd_fixed_page &entry = fixed.second;
	    if (entry.file == NULL || !rawfd_key_equals (entry.file->key (), key))
	      {
		continue;
	      }

	    if (entry.ref_count > 0)
	      {
		er_log_debug (ARG_FILE_LINE, "RAWFD: skipping fixed page purge for referenced page %d (ref_count=%d)",
			      entry.page_index, entry.ref_count);
		continue;
	      }

	    if (entry.ref_count == 0)
	      {
		purge_items.push_back (rawfd_purge_item { rawfd_make_sec_key (key, entry.page_index), entry.page_p });
	      }
	  }
      }

    for (const rawfd_purge_item &item : purge_items)
      {
	rawfd_secondary_shard &secondary_shard = g_rawfd_state.fixed_secondary_shards[secondary_shard_of (item.sec_key)];
	std::lock_guard<std::mutex> secondary_guard (secondary_shard.m);
	const auto sec_it = secondary_shard.map.find (item.sec_key);
	if (sec_it != secondary_shard.map.end () && sec_it->second == item.page_p)
	  {
	    secondary_shard.map.erase (sec_it);
#ifdef RAWFD_UNIT_TEST
	    g_rawfd_test_purge_events.push_back (RAWFD_TEST_EVENT_SECONDARY_ERASE);
#endif
	  }
      }

    for (const rawfd_purge_item &item : purge_items)
      {
	rawfd_primary_shard &primary_shard = g_rawfd_state.fixed_primary_shards[primary_shard_of (item.page_p)];
	{
	  std::lock_guard<std::mutex> primary_guard (primary_shard.m);
	  primary_shard.map.erase (item.page_p);
	}

#ifdef RAWFD_UNIT_TEST
	g_rawfd_test_purge_events.push_back (RAWFD_TEST_EVENT_FREE_BUFFER);
#endif
	free_db_page_buffer (item.page_p);
      }
  }

  void
  purge_fixed_and_cached_pages_locked (const temp_page_store::raw_fd_key &key) noexcept
  {
    purge_fixed_pages_for_key (key);

    std::lock_guard<std::mutex> read_cache_guard (g_rawfd_state.read_cache_mutex);
    purge_cached_pages_locked (key);
  }



  rawfd_file_snapshot
  detach_registry_entry_locked (rawfd_registry_entry &entry) noexcept
  {
    rawfd_file_snapshot snapshot;
    snapshot.key = entry.key;
    snapshot.query_id = entry.query_id;
    snapshot.owner_tran_index = entry.owner_tran_index;
    snapshot.owner = entry.owner;

    if (entry.owner != NULL)
      {
	snapshot.fd = entry.owner->detach_for_unlink (snapshot.path);
      }

    if (snapshot.fd < 0 && entry.fd >= 0)
      {
	snapshot.fd = entry.fd;
      }
    if (snapshot.path.empty ())
      {
	snapshot.path = entry.path;
      }

    if (entry.tfile_owner != NULL && entry.tfile_owner->raw_fd_handle == entry.owner)
      {
	entry.tfile_owner->raw_fd_handle = NULL;
	entry.tfile_owner->raw_fd_next_pageid = 0;
      }

    entry.fd = -1;
    entry.owner = NULL;
    entry.tfile_owner = NULL;
    entry.path.clear ();
    return snapshot;
  }

  void
  close_unlink_snapshot (rawfd_file_snapshot &snapshot) noexcept
  {
    if (snapshot.owner != NULL)
      {
	snapshot.owner->mark_destroyed ();
      }

    if (snapshot.fd >= 0)
      {
	(void) close (snapshot.fd);
	snapshot.fd = -1;
      }

    purge_fixed_and_cached_pages_locked (snapshot.key);

    if (!snapshot.path.empty ())
      {
	(void) unlink (snapshot.path.c_str ());
	snapshot.path.clear ();
      }

    delete snapshot.owner;
    snapshot.owner = NULL;
  }

  bool
  parse_rawfd_tmp_filename (const char *name, rawfd_disk_candidate &candidate) noexcept
  {
    if (name == NULL || name[0] == '\0')
      {
	return false;
      }

    char *end = NULL;
    errno = 0;
    const unsigned long long boot = std::strtoull (name, &end, 10);
    if (errno != 0 || end == name || *end != '.')
      {
	return false;
      }

    const char *p = end + 1;
    errno = 0;
    const unsigned long long file_seq = std::strtoull (p, &end, 10);
    if (errno != 0 || end == p || *end != '.')
      {
	return false;
      }

    p = end + 1;
    errno = 0;
    const long long query_id = std::strtoll (p, &end, 10);
    if (errno != 0 || end == p || *end != '.')
      {
	return false;
      }

    p = end + 1;
    errno = 0;
    const long long owner_tran_index = std::strtoll (p, &end, 10);
    if (errno != 0 || end == p || *end != '.')
      {
	return false;
      }

    p = end + 1;
    errno = 0;
    (void) std::strtoull (p, &end, 10);
    if (errno != 0 || end == p || strcmp (end, ".tmp") != 0)
      {
	return false;
      }

    candidate.key.boot_incarnation = static_cast<std::uint64_t> (boot);
    candidate.key.file_seq = static_cast<std::uint64_t> (file_seq);
    candidate.query_id = static_cast<QUERY_ID> (query_id);
    candidate.owner_tran_index = static_cast<int> (owner_tran_index);
    return true;
  }

  std::vector<rawfd_disk_candidate>
  scan_rawfd_disk_candidates ()
  {
    std::vector<rawfd_disk_candidate> candidates;
    DIR *dp = opendir (g_rawfd_state.server_subtree.c_str ());
    if (dp == NULL)
      {
	return candidates;
      }

    struct dirent *entry = NULL;
    while ((entry = readdir (dp)) != NULL)
      {
	rawfd_disk_candidate candidate;
	if (!parse_rawfd_tmp_filename (entry->d_name, candidate))
	  {
	    continue;
	  }

	candidate.path = g_rawfd_state.server_subtree + "/" + entry->d_name;
	struct stat st;
	if (lstat (candidate.path.c_str (), &st) != 0 || !S_ISREG (st.st_mode))
	  {
	    continue;
	  }

	candidate.modified_at = std::chrono::system_clock::from_time_t (st.st_mtime);
	candidates.push_back (candidate);
      }

    closedir (dp);
    return candidates;
  }

  bool
  is_fd_or_space_error (int os_error) noexcept
  {
    return os_error == EMFILE || os_error == ENFILE || os_error == ENOSPC || os_error == EDQUOT;
  }

  std::string
  make_rawfd_path (const temp_page_store::raw_fd_key &key, QUERY_ID query_id, int owner_tran_index,
		   unsigned int worker_id)
  {
    return g_rawfd_state.server_subtree + "/" + std::to_string (key.boot_incarnation) + "."
      + std::to_string (key.file_seq) + "." + std::to_string (static_cast<long long> (query_id)) + "."
      + std::to_string (owner_tran_index) + "." + std::to_string (worker_id) + ".tmp";
  }

  bool
  reserve_registry_slot (const rawfd_registry_entry &entry)
  {
    std::lock_guard<std::mutex> guard (g_rawfd_state.registry_mutex);
    return g_rawfd_state.registry.emplace (registry_map_key (entry.key), entry).second;
  }

  void
  install_registry_fd (temp_page_store::raw_fd_file *owner, int fd)
  {
    std::lock_guard<std::mutex> guard (g_rawfd_state.registry_mutex);
    const auto it = g_rawfd_state.registry.find (registry_map_key (owner->key ()));
    if (it != g_rawfd_state.registry.end ())
      {
	it->second.fd = fd;
	it->second.owner = owner;
      }
  }

  void
  forget_registry_slot (const temp_page_store::raw_fd_key &key)
  {
    std::lock_guard<std::mutex> guard (g_rawfd_state.registry_mutex);
    g_rawfd_state.registry.erase (registry_map_key (key));
  }

  PAGE_PTR
  alloc_db_page_buffer () noexcept
  {
    return static_cast<PAGE_PTR> (malloc (DB_PAGESIZE));
  }

  void
  free_db_page_buffer (PAGE_PTR page_p) noexcept
  {
    free (page_p);
  }

  struct aligned_io_page
  {
    char *raw { NULL };
    FILEIO_PAGE *page { NULL };

    aligned_io_page ()
    {
      raw = static_cast<char *> (malloc (IO_PAGESIZE + MAX_ALIGNMENT));
      if (raw != NULL)
	{
	  page = reinterpret_cast<FILEIO_PAGE *> (PTR_ALIGN (raw, MAX_ALIGNMENT));
	  memset (page, 0, IO_PAGESIZE);
	}
    }

    ~aligned_io_page ()
    {
      free (raw);
    }

    bool valid () const noexcept
    {
      return page != NULL;
    }
  };

  /* full_pwrite/full_pread duplicates deleted -- the shared spill-file
   * substrate's copies are used instead (qfile_spill_file.hpp, #132). */
  using qfile::full_pread;
  using qfile::full_pwrite;

  bool
  rawfd_use_read_cache (const temp_page_store::raw_fd_file &file) noexcept
  {
    return file.access_hint () == temp_page_store::raw_fd_access_hint::RANDOM_REACCESS;
  }

  void
  cache_insert_decrypted_page (const temp_page_store::raw_fd_key &key, PAGEID page_index, const FILEIO_PAGE *plain)
  {
    std::lock_guard<std::mutex> guard (g_rawfd_state.read_cache_mutex);
    const auto same_page = [&key, page_index] (const rawfd_cached_page &entry)
    {
      return entry.key.boot_incarnation == key.boot_incarnation && entry.key.file_seq == key.file_seq
	&& entry.page_index == page_index;
    };

    g_rawfd_state.read_cache.erase (std::remove_if (g_rawfd_state.read_cache.begin (), g_rawfd_state.read_cache.end (),
						    same_page),
				    g_rawfd_state.read_cache.end ());

    rawfd_cached_page entry;
    entry.key = key;
    entry.page_index = page_index;
    entry.io_page.assign (reinterpret_cast<const char *> (plain), reinterpret_cast<const char *> (plain) + IO_PAGESIZE);
    g_rawfd_state.read_cache.push_front (entry);

    while (g_rawfd_state.read_cache.size () > RAWFD_READ_CACHE_PAGES)
      {
	g_rawfd_state.read_cache.pop_back ();
      }
  }

  bool
  cache_lookup_decrypted_page (const temp_page_store::raw_fd_key &key, PAGEID page_index, PAGE_PTR out_page)
  {
    std::lock_guard<std::mutex> guard (g_rawfd_state.read_cache_mutex);
    for (auto it = g_rawfd_state.read_cache.begin (); it != g_rawfd_state.read_cache.end (); ++it)
      {
	if (it->key.boot_incarnation == key.boot_incarnation && it->key.file_seq == key.file_seq
	    && it->page_index == page_index)
	  {
	    memcpy (out_page, reinterpret_cast<const FILEIO_PAGE *> (it->io_page.data ())->page, DB_PAGESIZE);
	    const rawfd_cached_page copy = *it;
	    g_rawfd_state.read_cache.erase (it);
	    g_rawfd_state.read_cache.push_front (copy);
	    perfmon_inc_stat_to_global (PSTAT_RAWFD_READ_CACHE_HIT);
	    return true;
	  }
      }

    return false;
  }

  bool
  rawfd_alloc_retry (int &retry_count) noexcept
  {
    if (retry_count >= RAWFD_ALLOC_MAX_RETRIES)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	return false;
      }

    retry_count++;
    perfmon_inc_stat_to_global (PSTAT_RAWFD_ALLOC_RETRY);
#ifdef RAWFD_UNIT_TEST
    g_rawfd_test_alloc_retries.fetch_add (1, std::memory_order_relaxed);
#endif
    if (retry_count <= 8)
      {
	_mm_pause ();
      }
    else
      {
	(void) sched_yield ();
      }

    return true;
  }

  PAGE_PTR
  rawfd_alloc_fixed_page (temp_page_store::raw_fd_file *file, PAGEID page_index, bool zero_page,
			  bool *reused_out = nullptr)
  {
    perfmon_inc_stat_to_global (PSTAT_RAWFD_ALLOC_CALLS);
#ifdef RAWFD_UNIT_TEST
    g_rawfd_test_alloc_calls.fetch_add (1, std::memory_order_relaxed);
#endif

    if (reused_out != nullptr)
      {
	*reused_out = false;
      }

    if (file == NULL || page_index < 0 || file->destroyed ())
      {
	return NULL;
      }

    const temp_page_store::rawfd_sec_key sec_key = rawfd_make_sec_key (file->key (), page_index);
    PAGE_PTR allocated_page = NULL;
    int retry_count = 0;

    for (;;)
      {
	if (file->destroyed ())
	  {
	    if (allocated_page != NULL)
	      {
		free_db_page_buffer (allocated_page);
	      }
	    return NULL;
	  }

	PAGE_PTR candidate_page = NULL;
	bool found_secondary = false;
	rawfd_secondary_shard &secondary_shard = g_rawfd_state.fixed_secondary_shards[secondary_shard_of (sec_key)];
	{
	  std::lock_guard<std::mutex> secondary_guard (secondary_shard.m);
#ifdef RAWFD_UNIT_TEST
	  if (g_rawfd_test_force_persistent_primary_absent.load (std::memory_order_acquire))
	    {
	      secondary_shard.map[sec_key] = reinterpret_cast<PAGE_PTR> (static_cast<uintptr_t> (1));
	    }
#endif
	  const auto sec_it = secondary_shard.map.find (sec_key);
	  if (sec_it != secondary_shard.map.end ())
	    {
	      candidate_page = sec_it->second;
	      found_secondary = true;
	    }
	}

	if (found_secondary)
	  {
	    bool erase_secondary = false;
	    {
	      rawfd_primary_shard &primary_shard = g_rawfd_state.fixed_primary_shards[primary_shard_of (candidate_page)];
	      std::lock_guard<std::mutex> primary_guard (primary_shard.m);
	      const auto primary_it = primary_shard.map.find (candidate_page);
	      if (primary_it != primary_shard.map.end ())
		{
		  if (rawfd_fixed_page_matches_sec_key (primary_it->second, sec_key)
		      && primary_it->second.ref_count > 0)
		    {
		      if (primary_it->second.file != NULL && primary_it->second.file->destroyed ())
			{
			  if (allocated_page != NULL)
			    {
			      free_db_page_buffer (allocated_page);
			    }
			  return NULL;
			}

		      primary_it->second.ref_count++;
		      if (reused_out != nullptr)
			{
			  *reused_out = true;
			}
		      if (allocated_page != NULL)
			{
			  free_db_page_buffer (allocated_page);
			}
		      return primary_it->second.page_p;
		    }

		  erase_secondary = true;
		}
	    }

	    if (erase_secondary)
	      {
		std::lock_guard<std::mutex> secondary_guard (secondary_shard.m);
		const auto sec_it = secondary_shard.map.find (sec_key);
		if (sec_it != secondary_shard.map.end () && sec_it->second == candidate_page)
		  {
		    secondary_shard.map.erase (sec_it);
		  }
	      }

	    if (!rawfd_alloc_retry (retry_count))
	      {
		if (allocated_page != NULL)
		  {
		    free_db_page_buffer (allocated_page);
		  }
		return NULL;
	      }
	    continue;
	  }

	if (allocated_page != NULL)
	  {
	    bool inserted_secondary = false;
	    {
	      std::lock_guard<std::mutex> secondary_guard (secondary_shard.m);
	      inserted_secondary = secondary_shard.map.emplace (sec_key, allocated_page).second;
	    }

	    if (!inserted_secondary)
	      {
		free_db_page_buffer (allocated_page);
		allocated_page = NULL;
		if (!rawfd_alloc_retry (retry_count))
		  {
		    return NULL;
		  }
		continue;
	      }

	    if (file->destroyed ())
	      {
		{
		  std::lock_guard<std::mutex> secondary_guard (secondary_shard.m);
		  const auto sec_it = secondary_shard.map.find (sec_key);
		  if (sec_it != secondary_shard.map.end () && sec_it->second == allocated_page)
		    {
		      secondary_shard.map.erase (sec_it);
		    }
		}
		free_db_page_buffer (allocated_page);
		return NULL;
	      }

	    rawfd_primary_shard &primary_shard = g_rawfd_state.fixed_primary_shards[primary_shard_of (allocated_page)];
	    {
	      std::lock_guard<std::mutex> primary_guard (primary_shard.m);
	      if (file->destroyed ())
		{
		  primary_shard.map.erase (allocated_page);
		}
	      else
		{
		  primary_shard.map[allocated_page] = rawfd_fixed_page { file, page_index, allocated_page, 1 };
		  return allocated_page;
		}
	    }

	    {
	      std::lock_guard<std::mutex> secondary_guard (secondary_shard.m);
	      const auto sec_it = secondary_shard.map.find (sec_key);
	      if (sec_it != secondary_shard.map.end () && sec_it->second == allocated_page)
		{
		  secondary_shard.map.erase (sec_it);
		}
	    }
	    free_db_page_buffer (allocated_page);
	    return NULL;
	  }

	allocated_page = alloc_db_page_buffer ();
	if (allocated_page == NULL)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) DB_PAGESIZE);
	    return NULL;
	  }

	if (zero_page)
	  {
	    memset (allocated_page, 0, DB_PAGESIZE);
	    QFILE_PAGE_HEADER page_header = QFILE_PAGE_HEADER_INITIALIZER;
	    put_page_header (allocated_page, &page_header);
	  }
      }
  }

  rawfd_fixed_page
  rawfd_take_fixed_page (PAGE_PTR page_p)
  {
    rawfd_primary_shard &primary_shard = g_rawfd_state.fixed_primary_shards[primary_shard_of (page_p)];
    rawfd_fixed_page entry;
    temp_page_store::rawfd_sec_key sec_key {};
    bool has_sec_key = false;

    {
      std::lock_guard<std::mutex> primary_guard (primary_shard.m);
      const auto it = primary_shard.map.find (page_p);
      if (it == primary_shard.map.end ())
	{
	  return rawfd_fixed_page {};
	}

      if (it->second.ref_count > 1)
	{
	  it->second.ref_count--;
	  entry = it->second;
	  entry.page_p = NULL;
	  return entry;
	}

      entry = it->second;
      has_sec_key = rawfd_make_sec_key (entry, sec_key);
      it->second.ref_count = -1;
    }

    if (has_sec_key)
      {
	rawfd_secondary_shard &secondary_shard = g_rawfd_state.fixed_secondary_shards[secondary_shard_of (sec_key)];
	std::lock_guard<std::mutex> secondary_guard (secondary_shard.m);
	const auto sec_it = secondary_shard.map.find (sec_key);
	if (sec_it != secondary_shard.map.end () && sec_it->second == entry.page_p)
	  {
	    secondary_shard.map.erase (sec_it);
	  }
      }

    {
      std::lock_guard<std::mutex> primary_guard (primary_shard.m);
      const auto it = primary_shard.map.find (page_p);
      if (it != primary_shard.map.end () && it->second.ref_count < 1
	  && (!has_sec_key || rawfd_fixed_page_matches_sec_key (it->second, sec_key)))
	{
	  primary_shard.map.erase (it);
	}
    }

    return entry;
  }

  bool
  rawfd_find_and_mark_dirty (PAGE_PTR page_p)
  {
    rawfd_primary_shard &primary_shard = g_rawfd_state.fixed_primary_shards[primary_shard_of (page_p)];
    std::lock_guard<std::mutex> primary_guard (primary_shard.m);
    const auto it = primary_shard.map.find (page_p);
    if (it == primary_shard.map.end ())
      {
	return false;
      }
    if (it->second.file != NULL && it->second.file->destroyed ())
      {
	return false;
      }

    it->second.dirty = true;
    return true;
  }


  int64_t
  clamp_to_accounting_bytes (std::size_t bytes) noexcept
  {
    if (bytes > static_cast<std::size_t> (std::numeric_limits<int64_t>::max ()))
      {
        return std::numeric_limits<int64_t>::max ();
      }

    return static_cast<int64_t> (bytes);
  }

  std::size_t
  checked_add_bytes (std::size_t lhs, std::size_t rhs) noexcept
  {
    if (lhs > std::numeric_limits<std::size_t>::max () - rhs)
      {
        return std::numeric_limits<std::size_t>::max ();
      }

    return lhs + rhs;
  }

  std::size_t
  checked_pages_to_bytes (std::size_t pages) noexcept
  {
    if (pages > (std::numeric_limits<std::size_t>::max () / DB_PAGESIZE))
      {
        return std::numeric_limits<std::size_t>::max ();
      }

    return pages * DB_PAGESIZE;
  }

  std::size_t
  checked_bytes_to_pages (std::size_t bytes) noexcept
  {
    return (bytes + DB_PAGESIZE - 1) / DB_PAGESIZE;
  }

  void
  init_accountant () noexcept
  {
    /* The P1a cap is intentionally internal: use a conservative fraction of the data buffer so work_mem cannot grow
     * with concurrency to the size of the whole buffer pool; keep a fixed ceiling for large installations and a small
     * floor for default test databases.  This is not a GUC and is surfaced only through perf peek statistics. */
    const int page_buffer_pages = std::max (prm_get_integer_value (PRM_ID_PAGE_BUFFER_SIZE), 0);
    const std::size_t data_buffer_bytes = checked_pages_to_bytes (static_cast<std::size_t> (page_buffer_pages));
    const std::size_t fraction = (data_buffer_bytes > 0) ? data_buffer_bytes / 8 : WORKMEM_MIN_CAP_BYTES;
    const std::size_t capped = std::min (std::max (fraction, WORKMEM_MIN_CAP_BYTES), WORKMEM_MAX_CAP_BYTES);

    g_accountant.cap.store (clamp_to_accounting_bytes (capped), std::memory_order_release);
  }

  void
  ensure_init () noexcept
  {
    std::call_once (g_accountant.init_once, init_accountant);
  }

  std::size_t
  exact_reserved_bytes () noexcept
  {
    ensure_init ();

    std::size_t sum = 0;
    for (std::size_t i = 0; i < WORKMEM_SHARD_COUNT; i++)
      {
        const int64_t shard_reserved = g_accountant.shards[i].reserved.load (std::memory_order_acquire);
        if (shard_reserved > 0)
          {
            sum = checked_add_bytes (sum, static_cast<std::size_t> (shard_reserved));
          }
      }

    return sum;
  }

  int
  choose_shard () noexcept
  {
    const std::size_t hash = std::hash<std::thread::id> {} (std::this_thread::get_id ());
    return static_cast<int> (hash & (WORKMEM_SHARD_COUNT - 1));
  }

  void
  put_page_header (PAGE_PTR page_p, QFILE_PAGE_HEADER *header_p)
  {
    OR_PUT_INT ((page_p) + QFILE_TUPLE_COUNT_OFFSET, (header_p)->pg_tplcnt);
    OR_PUT_INT ((page_p) + QFILE_PREV_PAGE_ID_OFFSET, (header_p)->prev_pgid);
    OR_PUT_INT ((page_p) + QFILE_NEXT_PAGE_ID_OFFSET, (header_p)->next_pgid);
    OR_PUT_INT ((page_p) + QFILE_LAST_TUPLE_OFFSET, (header_p)->lasttpl_off);
    OR_PUT_INT ((page_p) + QFILE_OVERFLOW_PAGE_ID_OFFSET, (header_p)->ovfl_pgid);
    OR_PUT_SHORT ((page_p) + QFILE_PREV_VOL_ID_OFFSET, (header_p)->prev_volid);
    OR_PUT_SHORT ((page_p) + QFILE_NEXT_VOL_ID_OFFSET, (header_p)->next_volid);
    OR_PUT_SHORT ((page_p) + QFILE_OVERFLOW_VOL_ID_OFFSET, (header_p)->ovfl_volid);
#if !defined(NDEBUG)
    std::memset (page_p + QFILE_RESERVED_OFFSET, 0, QFILE_PAGE_HEADER_SIZE - QFILE_RESERVED_OFFSET);
#endif
  }

  int
  init_external_file_page (THREAD_ENTRY * thread_p, PAGE_PTR page, void *args)
  {
    QFILE_PAGE_HEADER page_header = QFILE_PAGE_HEADER_INITIALIZER;

    pgbuf_set_page_ptype (thread_p, page, PAGE_QRESULT);
    put_page_header (page, &page_header);
    pgbuf_set_dirty (thread_p, page, DONT_FREE);

    return NO_ERROR;
  }

  PAGE_PTR
  alloc_private_spill_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, VPID * vpid_p)
  {
    PAGE_PTR page_p = NULL;

    if (VFID_ISNULL (&tfile_p->temp_vfid))
      {
        TDE_ALGORITHM tde_algo = TDE_ALGORITHM_NONE;
        if (file_create_temp (thread_p, 1, &tfile_p->temp_vfid) != NO_ERROR)
          {
            ASSERT_ERROR ();
            return NULL;
          }
        tfile_p->temp_file_type = FILE_TEMP;

        if (tfile_p->tde_encrypted)
          {
            tde_algo = (TDE_ALGORITHM) prm_get_integer_value (PRM_ID_TDE_DEFAULT_ALGORITHM);

            if (file_apply_tde_algorithm (thread_p, &tfile_p->temp_vfid, tde_algo) != NO_ERROR)
              {
                ASSERT_ERROR ();
                file_temp_retire (thread_p, &tfile_p->temp_vfid);
                VFID_SET_NULL (&tfile_p->temp_vfid);
                return NULL;
              }
          }
      }

    VPID_SET_NULL (vpid_p);
    if (file_alloc (thread_p, &tfile_p->temp_vfid, init_external_file_page, NULL, vpid_p, &page_p) != NO_ERROR)
      {
        ASSERT_ERROR ();
        if (er_errid () == ER_FILE_NOT_ENOUGH_PAGES_IN_VOLUME)
          {
            er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OUT_OF_TEMP_SPACE, 0);
          }
        vpid_p->pageid = NULL_PAGEID;
        return NULL;
      }

    assert (page_p != NULL);
    assert (pgbuf_get_page_ptype (thread_p, page_p) == PAGE_QRESULT);
    return page_p;
  }

  PAGE_PTR
  fix_membuf_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, VPID * vpid_p)
  {
    PAGE_PTR page_p = NULL;
    int tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
#if defined(SERVER_MODE)
    bool dummy;
#endif /* SERVER_MODE */

    if (vpid_p->pageid >= 0 && vpid_p->pageid <= tfile_p->membuf_last)
      {
        page_p = tfile_p->membuf[vpid_p->pageid];

#if defined (SERVER_MODE)
        if (logtb_get_check_interrupt (thread_p) == true
            && logtb_is_interrupted_tran (thread_p, true, &dummy, tran_index) == true)
          {
            er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_INTERRUPTED, 0);
            page_p = NULL;
          }
#endif
      }
    else
      {
        er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TEMP_FILE, 1, tran_index);
      }

    return page_p;
  }

  PAGE_PTR
  fix_private_spill_page (THREAD_ENTRY * thread_p, VPID * vpid_p)
  {
    PAGE_PTR page_p = pgbuf_fix (thread_p, vpid_p, OLD_PAGE, PGBUF_LATCH_WRITE, PGBUF_UNCONDITIONAL_LATCH);

    if (page_p != NULL)
      {
#if !defined (NDEBUG)
        (void) pgbuf_check_page_ptype (thread_p, page_p, PAGE_QRESULT);
#endif /* !NDEBUG */
      }

    return page_p;
  }

  PAGE_PTR
  unreachable_backing_page (THREAD_ENTRY * thread_p)
  {
    assert_release_error (false);
    if (er_errid () == NO_ERROR)
      {
        er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TEMP_FILE, 1, LOG_FIND_THREAD_TRAN_INDEX (thread_p));
      }
    return NULL;
  }

#if !defined (NDEBUG)
  void
  qmgr_temp_file_move_selftest_init (QMGR_TEMP_FILE * tfile_p) noexcept
  {
    memset (tfile_p, 0, sizeof (*tfile_p));
    tfile_p->temp_file_type = FILE_TEMP;
    VFID_SET_NULL (&tfile_p->temp_vfid);
    tfile_p->membuf_last = -1;
    tfile_p->membuf_type = TEMP_FILE_MEMBUF_NONE;
    tfile_p->backing = qmgr_temp_backing::MEMBUF;
    tfile_p->wm_reserved_shard = -1;
    tfile_p->raw_fd_query_id = NULL_QUERY_ID;
    tfile_p->raw_fd_owner_tran_index = NULL_TRAN_INDEX;
    tfile_p->raw_fd_hint = temp_page_store::raw_fd_access_hint::RANDOM_REACCESS;
  }

  int
  qmgr_temp_file_move_selftest_destroy (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p) noexcept
  {
    int error = NO_ERROR;

    if (tfile_p->raw_fd_handle != NULL)
      {
	temp_page_store::destroy_raw_fd_file (tfile_p->raw_fd_handle);
	tfile_p->raw_fd_handle = NULL;
	tfile_p->raw_fd_next_pageid = 0;
      }
    if (tfile_p->page_spill_handle != NULL)
      {
	delete tfile_p->page_spill_handle;
	tfile_p->page_spill_handle = NULL;
	tfile_p->raw_fd_next_pageid = 0;
      }

    if (!VFID_ISNULL (&tfile_p->temp_vfid))
      {
	if (file_temp_retire (thread_p, &tfile_p->temp_vfid) != NO_ERROR)
	  {
	    error = ER_FAILED;
	  }
	VFID_SET_NULL (&tfile_p->temp_vfid);
      }

    temp_page_store::release_held_reservation (tfile_p);

    if (tfile_p->membuf != NULL)
      {
	free (tfile_p->membuf);
	tfile_p->membuf = NULL;
      }

    return error;
  }
#endif /* !NDEBUG */

}

namespace temp_page_store
{
  raw_fd_file::raw_fd_file () noexcept
    : m_fd (-1)
    , m_path ()
    , m_query_id (NULL_QUERY_ID)
    , m_owner_tran_index (NULL_TRAN_INDEX)
    , m_worker_id (0)
    , m_key { 0, 0 }
    , m_segment_id (0)
    , m_tde_encrypted (false)
    , m_tfile_owner (NULL)
  {
  }

  raw_fd_file::~raw_fd_file () noexcept
  {
    close_and_unlink ();
  }


  int
  raw_fd_file::fd () const noexcept
  {
    return m_fd;
  }

  const std::string &
  raw_fd_file::path () const noexcept
  {
    return m_path;
  }

  QUERY_ID
  raw_fd_file::query_id () const noexcept
  {
    return m_query_id;
  }

  int
  raw_fd_file::owner_tran_index () const noexcept
  {
    return m_owner_tran_index;
  }

  unsigned int
  raw_fd_file::worker_id () const noexcept
  {
    return m_worker_id;
  }

  raw_fd_key
  raw_fd_file::key () const noexcept
  {
    return m_key;
  }

  std::uint64_t
  raw_fd_file::segment_id () const noexcept
  {
    return m_segment_id;
  }

  raw_fd_access_hint
  raw_fd_file::access_hint () const noexcept
  {
    return m_tfile_owner == NULL ? raw_fd_access_hint::RANDOM_REACCESS : m_tfile_owner->raw_fd_hint;
  }

  bool
  raw_fd_file::is_open () const noexcept
  {
    return m_fd >= 0;
  }
  bool
  raw_fd_file::destroyed () const noexcept
  {
    return m_destroyed.load (std::memory_order_acquire);
  }

  void
  raw_fd_file::mark_destroyed () noexcept
  {
    m_destroyed.store (true, std::memory_order_release);
  }

  void
  raw_fd_file::close_and_unlink () noexcept
  {
    m_destroyed.store (true, std::memory_order_release);
    if (m_fd >= 0)
      {
	(void) close (m_fd);
	m_fd = -1;
      }

    if (!m_path.empty ())
      {
	(void) unlink (m_path.c_str ());
	m_path.clear ();
      }

    m_tfile_owner = NULL;
  }

  void
  raw_fd_file::attach_temp_file (QMGR_TEMP_FILE * tfile_p) noexcept
  {
    m_tfile_owner = tfile_p;
    m_tde_encrypted = tfile_p != NULL && tfile_p->tde_encrypted;

    std::lock_guard<std::mutex> guard (g_rawfd_state.registry_mutex);
    const auto it = g_rawfd_state.registry.find (registry_map_key (m_key));
    if (it != g_rawfd_state.registry.end ())
      {
	it->second.tfile_owner = tfile_p;
      }
  }


  int
  raw_fd_file::detach_for_unlink (std::string &path_out) noexcept
  {
    const int fd = m_fd;
    path_out = m_path;

    m_fd = -1;
    m_path.clear ();
    m_tfile_owner = NULL;
    return fd;
  }
  raw_fd_file *
  create_raw_fd_file (THREAD_ENTRY * thread_p, QUERY_ID query_id, int owner_tran_index, unsigned int worker_id,
		      int *os_error_out) noexcept
  {
    (void) thread_p;
    ensure_rawfd_state ();

    if (os_error_out != NULL)
      {
	*os_error_out = 0;
      }

    if (!g_rawfd_state.boot_sweep_complete || !g_rawfd_state.reaper_active)
      {
	if (os_error_out != NULL)
	  {
	    *os_error_out = EAGAIN;
	  }
	return NULL;
      }

    raw_fd_file *file = new raw_fd_file ();
    if (file == NULL)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (raw_fd_file));
	return NULL;
      }

    file->m_query_id = query_id;
    file->m_owner_tran_index = owner_tran_index;
    file->m_worker_id = worker_id;
    file->m_key.boot_incarnation = g_rawfd_state.boot_incarnation;
    file->m_key.file_seq = g_rawfd_state.file_seq.fetch_add (1, std::memory_order_acq_rel) + 1;
    file->m_segment_id = file->m_key.file_seq;
    file->m_path = make_rawfd_path (file->m_key, query_id, owner_tran_index, worker_id);

    rawfd_registry_entry entry;
    entry.key = file->m_key;
    entry.path = file->m_path;
    entry.query_id = query_id;
    entry.owner_tran_index = owner_tran_index;
    entry.worker_id = worker_id;
    entry.created_at = std::chrono::steady_clock::now ();
    if (!reserve_registry_slot (entry))
      {
	delete file;
	if (os_error_out != NULL)
	  {
	    *os_error_out = EEXIST;
	  }
	return NULL;
      }

    const int fd = open (file->m_path.c_str (), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
    if (fd < 0)
      {
	const int saved_errno = errno;
	(void) unlink (file->m_path.c_str ());
	forget_registry_slot (file->m_key);
	delete file;
	if (os_error_out != NULL)
	  {
	    *os_error_out = saved_errno;
	  }
	if (!is_fd_or_space_error (saved_errno))
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	  }
	perfmon_inc_stat_to_global (PSTAT_RAWFD_CREATE_FAILURE);
	return NULL;
      }

    file->m_fd = fd;
    install_registry_fd (file, fd);
    return file;
  }

  void
  destroy_raw_fd_file (raw_fd_file *file_p) noexcept
  {
    if (file_p == NULL)
      {
	return;
      }

    ensure_rawfd_state ();

    rawfd_file_snapshot snapshot;
    const raw_fd_key key = file_p->key ();
    {
      std::lock_guard<std::mutex> guard (g_rawfd_state.registry_mutex);
      const auto it = g_rawfd_state.registry.find (registry_map_key (key));
      if (it != g_rawfd_state.registry.end ())
	{
	  snapshot = detach_registry_entry_locked (it->second);
	  g_rawfd_state.registry.erase (it);
	}
      else
	{
	  snapshot.key = key;
	  snapshot.query_id = file_p->query_id ();
	  snapshot.owner_tran_index = file_p->owner_tran_index ();
	  snapshot.owner = file_p;
	  snapshot.fd = file_p->detach_for_unlink (snapshot.path);
	}
    }

    close_unlink_snapshot (snapshot);
  }

  void
  reassign_raw_fd_owner (raw_fd_file *file_p, QMGR_TEMP_FILE * new_owner) noexcept
  {
    if (file_p == NULL)
      {
	return;
      }

    ensure_rawfd_state ();

    std::lock_guard<std::mutex> guard (g_rawfd_state.registry_mutex);
    file_p->m_tfile_owner = new_owner;
    file_p->m_tde_encrypted = new_owner != NULL && new_owner->tde_encrypted;

    const auto it = g_rawfd_state.registry.find (registry_map_key (file_p->key ()));
    if (it != g_rawfd_state.registry.end () && it->second.owner == file_p)
      {
	it->second.tfile_owner = new_owner;
      }
  }

  void
  cleanup_query_raw_fd_files (int owner_tran_index, QUERY_ID query_id) noexcept
  {
    ensure_rawfd_state ();

    std::vector<rawfd_file_snapshot> snapshots;
    {
      std::lock_guard<std::mutex> guard (g_rawfd_state.registry_mutex);
      for (auto it = g_rawfd_state.registry.begin (); it != g_rawfd_state.registry.end ();)
	{
	  if (it->second.owner_tran_index == owner_tran_index && it->second.query_id == query_id)
	    {
	      snapshots.push_back (detach_registry_entry_locked (it->second));
	      it = g_rawfd_state.registry.erase (it);
	    }
	  else
	    {
	      ++it;
	    }
	}
    }

    for (rawfd_file_snapshot &snapshot : snapshots)
      {
	close_unlink_snapshot (snapshot);
      }

    for (const rawfd_disk_candidate &candidate : scan_rawfd_disk_candidates ())
      {
	if (candidate.owner_tran_index == owner_tran_index && candidate.query_id == query_id)
	  {
	    (void) unlink (candidate.path.c_str ());
	  }
      }

    reap_raw_fd_files ();
  }

  void
  reap_raw_fd_files () noexcept
  {
    ensure_rawfd_state ();

    if (!g_rawfd_state.reaper_active)
      {
	return;
      }

    const auto steady_now = std::chrono::steady_clock::now ();
    const auto wall_now = std::chrono::system_clock::now ();
    const std::chrono::seconds grace (RAWFD_REAPER_GRACE_SECONDS);

    std::vector<rawfd_registry_entry> registry_candidates;
    std::set<std::uint64_t> registered_keys;
    {
      std::lock_guard<std::mutex> guard (g_rawfd_state.registry_mutex);
      for (const auto &kv : g_rawfd_state.registry)
	{
	  registered_keys.insert (kv.first);

	  const bool grace_expired = steady_now - kv.second.created_at > grace;
	  if (grace_expired)
	    {
	      registry_candidates.push_back (kv.second);
	    }
	}
    }

    std::set<std::uint64_t> registry_keys_to_reap;
    for (const rawfd_registry_entry &entry : registry_candidates)
      {
	const bool old_incarnation = entry.key.boot_incarnation != g_rawfd_state.boot_incarnation;
	if (old_incarnation)
	  {
	    registry_keys_to_reap.insert (registry_map_key (entry.key));
	  }
      }

    std::vector<rawfd_file_snapshot> snapshots;
    {
      std::lock_guard<std::mutex> guard (g_rawfd_state.registry_mutex);
      for (const std::uint64_t map_key : registry_keys_to_reap)
	{
	  const auto it = g_rawfd_state.registry.find (map_key);
	  if (it != g_rawfd_state.registry.end ()
	      && steady_now - it->second.created_at > grace)
	    {
	      snapshots.push_back (detach_registry_entry_locked (it->second));
	      g_rawfd_state.registry.erase (it);
	    }
	}
    }

    for (rawfd_file_snapshot &snapshot : snapshots)
      {
	close_unlink_snapshot (snapshot);
      }

    for (const rawfd_disk_candidate &candidate : scan_rawfd_disk_candidates ())
      {
	if (registered_keys.find (registry_map_key (candidate.key)) != registered_keys.end ())
	  {
	    continue;
	  }

	if (wall_now < candidate.modified_at || wall_now - candidate.modified_at <= grace)
	  {
	    continue;
	  }

	const bool old_incarnation = candidate.key.boot_incarnation != g_rawfd_state.boot_incarnation;
	const bool query_not_live = old_incarnation || !qmgr_is_query_live (candidate.owner_tran_index, candidate.query_id);
	if (old_incarnation || query_not_live)
	  {
	    (void) unlink (candidate.path.c_str ());
	  }
      }
  }

  int
  rawfd_write_page (THREAD_ENTRY * thread_p, raw_fd_file &file, PAGEID page_index, PAGE_PTR page_p) noexcept
  {
    if (file.destroyed ())
      {
	return ER_FAILED;
      }

    if (!file.is_open () || page_p == NULL || page_index < 0)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TEMP_FILE, 1, LOG_FIND_THREAD_TRAN_INDEX (thread_p));
	return ER_FAILED;
      }

    const bool tde_encrypted = file.m_tde_encrypted;
    if (!tde_encrypted)
      {
	assert (!file.m_tde_encrypted);
	perfmon_inc_stat_to_global (PSTAT_RAWFD_PLAINTEXT_WRITE_COUNT);

	const off_t offset = static_cast<off_t> (page_index) * static_cast<off_t> (DB_PAGESIZE);
	rawfd_invalidate_cached_page (file, page_index);
	if (!full_pwrite (file.fd (), page_p, DB_PAGESIZE, offset))
	  {
	    const int saved_errno = errno;
	    if (saved_errno == ENOSPC || saved_errno == EDQUOT)
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OUT_OF_TEMP_SPACE, 0);
	      }
	    else
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	      }
	    return ER_FAILED;
	  }

	goto rawfd_write_success;
      }

    {
      aligned_io_page plain;
      aligned_io_page cipher;
      if (!plain.valid () || !cipher.valid ())
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) IO_PAGESIZE);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      fileio_initialize_res (thread_p, plain.page, IO_PAGESIZE);
      plain.page->prv.pageid = page_index;
      plain.page->prv.volid = NULL_VOLID;
      plain.page->prv.ptype = PAGE_QRESULT;
      memcpy (plain.page->page, page_p, DB_PAGESIZE);

      {
	if (!tde_is_loaded ())
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TDE_CIPHER_IS_NOT_LOADED, 0);
	    return ER_TDE_CIPHER_IS_NOT_LOADED;
	  }

	const TDE_ALGORITHM tde_algo = (TDE_ALGORITHM) prm_get_integer_value (PRM_ID_TDE_DEFAULT_ALGORITHM);
	if (tde_algo == TDE_ALGORITHM_NONE)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TDE_CIPHER_IS_NOT_LOADED, 0);
	    return ER_TDE_CIPHER_IS_NOT_LOADED;
	  }

	const int error = tde_encrypt_data_page (plain.page, tde_algo, true, cipher.page);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
      }

      const off_t offset = static_cast<off_t> (page_index) * static_cast<off_t> (IO_PAGESIZE);
      rawfd_invalidate_cached_page (file, page_index);
      if (!full_pwrite (file.fd (), cipher.page, IO_PAGESIZE, offset))
	{
	  const int saved_errno = errno;
	  if (saved_errno == ENOSPC || saved_errno == EDQUOT)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OUT_OF_TEMP_SPACE, 0);
	    }
	  else
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	    }
	  return ER_FAILED;
	}
      }

rawfd_write_success:
#if !defined (NDEBUG)
    {
      const char *kill_after_env = getenv ("CUBRID_RAWFD_FAULT_INJECT_SPILL_KILL_AFTER_PAGES");
      const int kill_after = (kill_after_env != NULL && kill_after_env[0] != '\0') ? atoi (kill_after_env) : 0;
      if (kill_after > 0
	  && g_rawfd_fault_inject_spill_writes.fetch_add (1, std::memory_order_acq_rel) + 1 == kill_after)
	{
	  er_log_debug (ARG_FILE_LINE, "RAWFD_FAULT_INJECT: pausing at spill");
	  pause ();
	}
    }
#endif /* !NDEBUG */

    rawfd_invalidate_cached_page (file, page_index);
    return NO_ERROR;
  }

  int
  rawfd_rewrite_page (THREAD_ENTRY * thread_p, raw_fd_file &file, PAGEID page_index, PAGE_PTR page_p) noexcept
  {
    return rawfd_write_page (thread_p, file, page_index, page_p);
  }

  PAGE_PTR
  rawfd_pos_read (THREAD_ENTRY * thread_p, raw_fd_file &file, const raw_fd_page_coordinate &coordinate) noexcept
  {
    if (file.destroyed ())
      {
	return NULL;
      }

    if (!file.is_open () || coordinate.raw_fd_segment_id != file.segment_id () || coordinate.page_index < 0)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TEMP_FILE, 1, LOG_FIND_THREAD_TRAN_INDEX (thread_p));
	return NULL;
      }

    bool reused_fixed_page = false;
    PAGE_PTR page_p = rawfd_alloc_fixed_page (&file, coordinate.page_index, false, &reused_fixed_page);
    if (page_p == NULL)
      {
	return NULL;
      }

    if (reused_fixed_page)
      {
	return page_p;
      }
    if (rawfd_use_read_cache (file) && cache_lookup_decrypted_page (file.key (), coordinate.page_index, page_p))
      {
	return page_p;
      }

    /* cache miss: a real pread + TDE decrypt of the addressed raw-fd page follows. */
    perfmon_inc_stat_to_global (PSTAT_RAWFD_READ_CACHE_MISS);

    const bool tde_encrypted = file.m_tde_encrypted;
    if (!tde_encrypted)
      {
	assert (!file.m_tde_encrypted);
	const off_t offset = static_cast<off_t> (coordinate.page_index) * static_cast<off_t> (DB_PAGESIZE);
	if (!full_pread (file.fd (), page_p, DB_PAGESIZE, offset))
	  {
	    rawfd_release_fixed_page (thread_p, NULL, page_p);
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	    return NULL;
	  }
	return page_p;
      }

    aligned_io_page stored;
    aligned_io_page plain;
    if (!stored.valid () || !plain.valid ())
      {
	rawfd_release_fixed_page (thread_p, NULL, page_p);
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) IO_PAGESIZE);
	return NULL;
      }

    const off_t offset = static_cast<off_t> (coordinate.page_index) * static_cast<off_t> (IO_PAGESIZE);
    if (!full_pread (file.fd (), stored.page, IO_PAGESIZE, offset))
      {
	rawfd_release_fixed_page (thread_p, NULL, page_p);
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	return NULL;
      }

    const TDE_ALGORITHM tde_algo = (TDE_ALGORITHM) prm_get_integer_value (PRM_ID_TDE_DEFAULT_ALGORITHM);
    if (tde_algo == TDE_ALGORITHM_NONE)
      {
	rawfd_release_fixed_page (thread_p, NULL, page_p);
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TDE_CIPHER_IS_NOT_LOADED, 0);
	return NULL;
      }

    const int error = tde_decrypt_data_page (stored.page, tde_algo, true, plain.page);
    if (error != NO_ERROR)
      {
	rawfd_release_fixed_page (thread_p, NULL, page_p);
	return NULL;
      }
    perfmon_inc_stat_to_global (PSTAT_RAWFD_DECRYPT_COUNT);

    memcpy (page_p, plain.page->page, DB_PAGESIZE);
    if (rawfd_use_read_cache (file))
      {
	cache_insert_decrypted_page (file.key (), coordinate.page_index, plain.page);
      }
    return page_p;
  }

  void
  rawfd_invalidate_cached_page (raw_fd_file &file, PAGEID page_index) noexcept
  {
    if (file.destroyed ())
      {
	return;
      }

    std::lock_guard<std::mutex> guard (g_rawfd_state.read_cache_mutex);
    g_rawfd_state.read_cache.erase (std::remove_if (g_rawfd_state.read_cache.begin (), g_rawfd_state.read_cache.end (),
						    [&file, page_index] (const rawfd_cached_page &entry)
    {
      return entry.key.boot_incarnation == file.key ().boot_incarnation && entry.key.file_seq == file.key ().file_seq
	&& entry.page_index == page_index;
    }),
				    g_rawfd_state.read_cache.end ());
  }

  int
  rawfd_flush_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, PAGE_PTR page_p, int free_page) noexcept
  {
    (void) tfile_p;
    if (!rawfd_find_and_mark_dirty (page_p))
      {
	return NO_ERROR;
      }

    if (free_page != (int) FREE)
      {
	return NO_ERROR;
      }

    return rawfd_release_fixed_page (thread_p, tfile_p, page_p);
  }

  int
  rawfd_release_fixed_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, PAGE_PTR page_p) noexcept
  {
    (void) tfile_p;
    rawfd_fixed_page fixed = rawfd_take_fixed_page (page_p);
    if (fixed.page_p != NULL)
      {
	if (fixed.dirty && fixed.file != NULL && !fixed.file->destroyed ())
	  {
	    const int error = rawfd_rewrite_page (thread_p, *fixed.file, fixed.page_index, fixed.page_p);
	    if (error != NO_ERROR)
	      {
		return error;
	      }
	  }
	free_db_page_buffer (fixed.page_p);
      }

    return NO_ERROR;
  }

  /* (c′) PAGE_SPILL_OVERFLOW consumer shims (#132) -- rawfd_flush_page /
   * rawfd_release_fixed_page contract parity: an unknown page (e.g. a membuf
   * page routed through the same qmgr call path) is a silent NO_ERROR. */
  int
  spill_flush_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, PAGE_PTR page_p, int free_page) noexcept
  {
    if (tfile_p == NULL || tfile_p->page_spill_handle == NULL
	|| !tfile_p->page_spill_handle->mark_dirty (page_p))
      {
	return NO_ERROR;
      }

    if (free_page != (int) FREE)
      {
	return NO_ERROR;
      }

    return spill_release_fixed_page (thread_p, tfile_p, page_p);
  }

  int
  spill_release_fixed_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, PAGE_PTR page_p) noexcept
  {
    if (tfile_p == NULL || tfile_p->page_spill_handle == NULL)
      {
	return NO_ERROR;
      }

    return tfile_p->page_spill_handle->release_page (thread_p, page_p);
  }

#ifdef RAWFD_UNIT_TEST
  void
  rawfd_test_reset_registry () noexcept
  {
    std::vector<PAGE_PTR> pages_to_free;
    for (std::size_t shard_index = 0; shard_index < RAWFD_FIXED_PAGE_SHARD_COUNT; shard_index++)
      {
	rawfd_primary_shard &primary_shard = g_rawfd_state.fixed_primary_shards[shard_index];
	std::lock_guard<std::mutex> primary_guard (primary_shard.m);
	for (const auto &fixed : primary_shard.map)
	  {
	    if (fixed.second.page_p != NULL)
	      {
		pages_to_free.push_back (fixed.second.page_p);
	      }
	  }
	primary_shard.map.clear ();
      }

    for (std::size_t shard_index = 0; shard_index < RAWFD_FIXED_PAGE_SHARD_COUNT; shard_index++)
      {
	rawfd_secondary_shard &secondary_shard = g_rawfd_state.fixed_secondary_shards[shard_index];
	std::lock_guard<std::mutex> secondary_guard (secondary_shard.m);
	secondary_shard.map.clear ();
      }

    g_rawfd_test_force_persistent_primary_absent.store (false, std::memory_order_release);
    g_rawfd_test_alloc_calls.store (0, std::memory_order_relaxed);
    g_rawfd_test_alloc_retries.store (0, std::memory_order_relaxed);
    g_rawfd_test_purge_events.clear ();

    for (PAGE_PTR page_p : pages_to_free)
      {
	free_db_page_buffer (page_p);
      }

    {
      std::lock_guard<std::mutex> cache_guard (g_rawfd_state.read_cache_mutex);
      g_rawfd_state.read_cache.clear ();
    }
  }

  void
  rawfd_test_init_file (raw_fd_file &file, std::uint64_t file_seq) noexcept
  {
    file.m_key.boot_incarnation = 1;
    file.m_key.file_seq = file_seq;
    file.m_segment_id = file_seq;
    file.m_destroyed.store (false, std::memory_order_release);
  }
  std::size_t
  rawfd_test_primary_size () noexcept
  {
    std::size_t size = 0;
    for (std::size_t shard_index = 0; shard_index < RAWFD_FIXED_PAGE_SHARD_COUNT; shard_index++)
      {
	rawfd_primary_shard &primary_shard = g_rawfd_state.fixed_primary_shards[shard_index];
	std::lock_guard<std::mutex> primary_guard (primary_shard.m);
	size += primary_shard.map.size ();
      }
    return size;
  }

  std::size_t
  rawfd_test_secondary_size () noexcept
  {
    std::size_t size = 0;
    for (std::size_t shard_index = 0; shard_index < RAWFD_FIXED_PAGE_SHARD_COUNT; shard_index++)
      {
	rawfd_secondary_shard &secondary_shard = g_rawfd_state.fixed_secondary_shards[shard_index];
	std::lock_guard<std::mutex> secondary_guard (secondary_shard.m);
	size += secondary_shard.map.size ();
      }
    return size;
  }

  bool
  rawfd_test_secondary_points_to (const temp_page_store::rawfd_sec_key &sec_key, PAGE_PTR page_p) noexcept
  {
    rawfd_secondary_shard &secondary_shard = g_rawfd_state.fixed_secondary_shards[secondary_shard_of (sec_key)];
    std::lock_guard<std::mutex> secondary_guard (secondary_shard.m);
    const auto sec_it = secondary_shard.map.find (sec_key);
    return sec_it != secondary_shard.map.end () && sec_it->second == page_p;
  }

  bool
  rawfd_test_primary_contains (PAGE_PTR page_p) noexcept
  {
    rawfd_primary_shard &primary_shard = g_rawfd_state.fixed_primary_shards[primary_shard_of (page_p)];
    std::lock_guard<std::mutex> primary_guard (primary_shard.m);
    return primary_shard.map.find (page_p) != primary_shard.map.end ();
  }

  void
  rawfd_test_insert_fixed_page (PAGE_PTR page_p, const rawfd_fixed_page &entry) noexcept
  {
    rawfd_primary_shard &primary_shard = g_rawfd_state.fixed_primary_shards[primary_shard_of (page_p)];
    std::lock_guard<std::mutex> primary_guard (primary_shard.m);
    primary_shard.map[page_p] = entry;
  }

  void
  rawfd_test_insert_secondary (const temp_page_store::rawfd_sec_key &sec_key, PAGE_PTR page_p) noexcept
  {
    rawfd_secondary_shard &secondary_shard = g_rawfd_state.fixed_secondary_shards[secondary_shard_of (sec_key)];
    std::lock_guard<std::mutex> secondary_guard (secondary_shard.m);
    secondary_shard.map[sec_key] = page_p;
  }

  void
  rawfd_test_set_ref_count (PAGE_PTR page_p, int ref_count) noexcept
  {
    rawfd_primary_shard &primary_shard = g_rawfd_state.fixed_primary_shards[primary_shard_of (page_p)];
    std::lock_guard<std::mutex> primary_guard (primary_shard.m);
    primary_shard.map[page_p].ref_count = ref_count;
  }

  int
  rawfd_test_t1_hash_collision () noexcept
  {
    rawfd_test_reset_registry ();

    raw_fd_file first;
    raw_fd_file second;
    rawfd_test_init_file (first, 0);
    rawfd_test_init_file (second, 0x9E3779B97F4A7C15ULL);

    const temp_page_store::rawfd_sec_key first_key = rawfd_make_sec_key (first.key (), 0);
    const temp_page_store::rawfd_sec_key second_key = rawfd_make_sec_key (second.key (), 1);
    if (secondary_shard_of (first_key) != secondary_shard_of (second_key)
	|| rawfd_sec_key_hash (first_key) != rawfd_sec_key_hash (second_key))
      {
	rawfd_test_reset_registry ();
	return ER_FAILED;
      }

    PAGE_PTR first_page = rawfd_alloc_fixed_page (&first, 0, true);
    PAGE_PTR second_page = rawfd_alloc_fixed_page (&second, 1, true);
    bool ok = first_page != NULL && second_page != NULL && first_page != second_page;

    ok = ok && rawfd_test_primary_size () == 2 && rawfd_test_secondary_size () == 2
	 && rawfd_test_secondary_points_to (first_key, first_page)
	 && rawfd_test_secondary_points_to (second_key, second_page);

    rawfd_test_reset_registry ();
    return ok ? NO_ERROR : ER_FAILED;
  }

  int
  rawfd_test_t2_aba_validation () noexcept
  {
    rawfd_test_reset_registry ();

    raw_fd_file request_file;
    raw_fd_file other_file;
    rawfd_test_init_file (request_file, 10);
    rawfd_test_init_file (other_file, 11);

    const PAGEID request_page = 20;
    const temp_page_store::rawfd_sec_key request_key = rawfd_make_sec_key (request_file.key (), request_page);
    PAGE_PTR stale_page = alloc_db_page_buffer ();
    if (stale_page == NULL)
      {
	return ER_FAILED;
      }

    rawfd_test_insert_fixed_page (stale_page, rawfd_fixed_page { &other_file, 21, stale_page, 1 });
    rawfd_test_insert_secondary (request_key, stale_page);

    PAGE_PTR fresh_page = rawfd_alloc_fixed_page (&request_file, request_page, true);
    bool ok = fresh_page != NULL && fresh_page != stale_page
	      && g_rawfd_test_alloc_retries.load (std::memory_order_relaxed) == 1;

    ok = ok && rawfd_test_secondary_points_to (request_key, fresh_page);

    rawfd_test_reset_registry ();
    return ok ? NO_ERROR : ER_FAILED;
  }

  int
  rawfd_test_t3_retry_bound () noexcept
  {
    rawfd_test_reset_registry ();

    raw_fd_file file;
    rawfd_test_init_file (file, 22);
    g_rawfd_test_force_persistent_primary_absent.store (true, std::memory_order_release);

    PAGE_PTR page_p = rawfd_alloc_fixed_page (&file, 23, true);
    const bool ok = page_p == NULL
		    && g_rawfd_test_alloc_calls.load (std::memory_order_relaxed) == 1
		    && g_rawfd_test_alloc_retries.load (std::memory_order_relaxed) == RAWFD_ALLOC_MAX_RETRIES;

    g_rawfd_test_force_persistent_primary_absent.store (false, std::memory_order_release);
    rawfd_test_reset_registry ();
    return ok ? NO_ERROR : ER_FAILED;
  }

  int
  rawfd_test_t4_destroyed_gate () noexcept
  {
    rawfd_test_reset_registry ();

    raw_fd_file file;
    rawfd_test_init_file (file, 24);
    file.m_destroyed.store (true, std::memory_order_release);

    PAGE_PTR page_p = rawfd_alloc_fixed_page (&file, 25, true);
    bool ok = page_p == NULL;

    PAGE_PTR fixed_page = alloc_db_page_buffer ();
    if (fixed_page == NULL)
      {
	rawfd_test_reset_registry ();
	return ER_FAILED;
      }

    const temp_page_store::rawfd_sec_key fixed_key = rawfd_make_sec_key (file.key (), 26);
    rawfd_test_insert_fixed_page (fixed_page, rawfd_fixed_page { &file, 26, fixed_page, 1 });
    rawfd_test_insert_secondary (fixed_key, fixed_page);

    ok = ok && !rawfd_find_and_mark_dirty (fixed_page)
	 && rawfd_release_fixed_page (NULL, NULL, fixed_page) == NO_ERROR
	 && rawfd_write_page (NULL, file, 26, fixed_page) == ER_FAILED
	 && rawfd_pos_read (NULL, file, raw_fd_page_coordinate { file.segment_id (), 26, 0 }) == NULL;

    ok = ok && !rawfd_test_primary_contains (fixed_page)
	 && !rawfd_test_secondary_points_to (fixed_key, fixed_page);

    rawfd_test_reset_registry ();
    return ok ? NO_ERROR : ER_FAILED;
  }

  int
  rawfd_test_t5_purge_order () noexcept
  {
    rawfd_test_reset_registry ();

    raw_fd_file file;
    rawfd_test_init_file (file, 30);
    PAGE_PTR page_p = rawfd_alloc_fixed_page (&file, 31, true);
    if (page_p == NULL)
      {
	rawfd_test_reset_registry ();
	return ER_FAILED;
      }

    rawfd_test_set_ref_count (page_p, 0);
    g_rawfd_test_purge_events.clear ();

    purge_fixed_pages_for_key (file.key ());

    bool ok = rawfd_test_primary_size () == 0 && rawfd_test_secondary_size () == 0
	      && g_rawfd_test_purge_events.size () == 2
	      && g_rawfd_test_purge_events[0] == RAWFD_TEST_EVENT_SECONDARY_ERASE
	      && g_rawfd_test_purge_events[1] == RAWFD_TEST_EVENT_FREE_BUFFER;

    rawfd_test_reset_registry ();
    return ok ? NO_ERROR : ER_FAILED;
  }
#endif /* RAWFD_UNIT_TEST */
  int
  rawfd_single_worker_tde_positioned_read_parity (THREAD_ENTRY * thread_p) noexcept
  {
    int os_error = 0;
    raw_fd_file *file = create_raw_fd_file (thread_p, static_cast<QUERY_ID> (-1), LOG_FIND_THREAD_TRAN_INDEX (thread_p),
					    0, &os_error);
    if (file == NULL)
      {
	return ER_FAILED;
      }

    constexpr PAGEID PAGE_COUNT = 257;
    std::vector<std::vector<char>> expected (PAGE_COUNT, std::vector<char> (DB_PAGESIZE));
    int error = NO_ERROR;
    std::set<INT64> seen_nonces;

    for (PAGEID page = 0; page < PAGE_COUNT && error == NO_ERROR; page++)
      {
	for (int offset = 0; offset < DB_PAGESIZE; offset++)
	  {
	    expected[page][offset] = static_cast<char> ((page * 131 + offset * 17 + (offset >> 3)) & 0xff);
	  }
	QFILE_PAGE_HEADER page_header = QFILE_PAGE_HEADER_INITIALIZER;
	put_page_header (expected[page].data (), &page_header);
	error = rawfd_write_page (thread_p, *file, page, expected[page].data ());
      }

    for (PAGEID step = 0; step < PAGE_COUNT && error == NO_ERROR; step++)
      {
	const PAGEID page = static_cast<PAGEID> ((step * 73) % PAGE_COUNT);
	raw_fd_page_coordinate coord { file->segment_id (), page, static_cast<std::size_t> ((page * 19) % DB_PAGESIZE) };
        aligned_io_page cipher_check;
        aligned_io_page plain_check;
        if (!cipher_check.valid () || !plain_check.valid ()
            || !full_pread (file->fd (), cipher_check.page, IO_PAGESIZE,
                            static_cast<off_t> (page) * static_cast<off_t> (IO_PAGESIZE)))
          {
            error = ER_FAILED;
            break;
          }
        const TDE_ALGORITHM tde_algo = (TDE_ALGORITHM) prm_get_integer_value (PRM_ID_TDE_DEFAULT_ALGORITHM);
        error = tde_decrypt_data_page (cipher_check.page, tde_algo, true, plain_check.page);
        if (error != NO_ERROR)
          {
            break;
          }
        if (memcmp (plain_check.page->page, expected[page].data (), DB_PAGESIZE) != 0
            || plain_check.page->prv.tde_nonce != cipher_check.page->prv.tde_nonce
            || plain_check.page->prv.tde_nonce == 0)
          {
            er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
            error = ER_FAILED;
            break;
          }
        seen_nonces.insert (cipher_check.page->prv.tde_nonce);
	PAGE_PTR actual = rawfd_pos_read (thread_p, *file, coord);
	if (actual == NULL)
	  {
	    error = ER_FAILED;
	    break;
	  }

	if (memcmp (actual, expected[page].data (), DB_PAGESIZE) != 0)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	    error = ER_FAILED;
	  }
	rawfd_release_fixed_page (thread_p, NULL, actual);
      }

    if (error == NO_ERROR && seen_nonces.size () != static_cast<std::size_t> (PAGE_COUNT))
      {
	/* TDE security gate (b): every raw write must take a FRESH global-counter nonce -> 257 distinct nonces. */
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	error = ER_FAILED;
      }

    destroy_raw_fd_file (file);
    return error;
  }

  int
  rawfd_positional_alias_mutation_selftest (THREAD_ENTRY * thread_p) noexcept
  {
    int os_error = 0;
    raw_fd_file *file = create_raw_fd_file (thread_p, static_cast<QUERY_ID> (-6), LOG_FIND_THREAD_TRAN_INDEX (thread_p),
					    0, &os_error);
    if (file == NULL)
      {
	return ER_FAILED;
      }

    QMGR_TEMP_FILE tfile = {};
    tfile.temp_file_type = FILE_TEMP;
    tfile.backing = qmgr_temp_backing::RAW_FD_OVERFLOW;
    tfile.raw_fd_query_id = static_cast<QUERY_ID> (-6);
    tfile.raw_fd_owner_tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
    tfile.raw_fd_handle = file;
    tfile.raw_fd_next_pageid = 1;
    tfile.membuf_last = -1;
    tfile.wm_reserved_shard = -1;
    tfile.tde_encrypted = true;
    file->attach_temp_file (&tfile);

    QFILE_LIST_ID list_id;
    QFILE_CLEAR_LIST_ID (&list_id);
    QFILE_LIST_ID_TFILE_VFID(&(list_id)) = &tfile;
    list_id.page_cnt = 1;
    QFILE_LIST_ID_FIRST_VPID(&(list_id)).volid = NULL_VOLID;
    QFILE_LIST_ID_FIRST_VPID(&(list_id)).pageid = 0;
    QFILE_LIST_ID_LAST_VPID(&(list_id)).volid = NULL_VOLID;
    QFILE_LIST_ID_LAST_VPID(&(list_id)).pageid = 0;
    list_id.last_offset = QFILE_PAGE_HEADER_SIZE;
    list_id.query_id = static_cast<QUERY_ID> (-6);

    PAGE_PTR held_page_p = rawfd_alloc_fixed_page (file, 0, true);
    int error = NO_ERROR;
    INT64 nonce_after_first = 0;
    INT64 nonce_after_mutation = 0;
    INT64 nonce_after_second = 0;

    auto make_int_tuple = [] (int value) -> std::vector<char>
    {
      const int tuple_length = QFILE_TUPLE_LENGTH_SIZE + QFILE_TUPLE_VALUE_HEADER_SIZE + OR_INT_SIZE;
      std::vector<char> tuple (tuple_length, 0);

      QFILE_PUT_TUPLE_LENGTH (tuple.data (), tuple_length);
      QFILE_PUT_PREV_TUPLE_LENGTH (tuple.data (), 0);

      char *value_p = tuple.data () + QFILE_TUPLE_LENGTH_SIZE;
      QFILE_PUT_TUPLE_VALUE_FLAG (value_p, V_BOUND);
      QFILE_PUT_TUPLE_VALUE_LENGTH (value_p, OR_INT_SIZE);

      DB_VALUE int_value;
      db_make_int (&int_value, value);
      OR_BUF int_buf;
      or_init (&int_buf, value_p + QFILE_TUPLE_VALUE_HEADER_SIZE, OR_INT_SIZE);
      if (tp_Integer_domain.type->data_writeval (&int_buf, &int_value) != NO_ERROR)
	{
	  tuple.clear ();
	}
      return tuple;
    };

    const TDE_ALGORITHM tde_algo = (TDE_ALGORITHM) prm_get_integer_value (PRM_ID_TDE_DEFAULT_ALGORITHM);
    auto read_decrypt_and_nonce = [&] (PAGE_PTR out_page, INT64 *nonce_out) -> int
    {
      aligned_io_page cipher_check;
      aligned_io_page plain_check;
      if (!cipher_check.valid () || !plain_check.valid ()
	  || !full_pread (file->fd (), cipher_check.page, IO_PAGESIZE, 0))
	{
	  return ER_FAILED;
	}

      int local_error = tde_decrypt_data_page (cipher_check.page, tde_algo, true, plain_check.page);
      if (local_error != NO_ERROR)
	{
	  return local_error;
	}

      memcpy (out_page, plain_check.page->page, DB_PAGESIZE);
      *nonce_out = cipher_check.page->prv.tde_nonce;
      return NO_ERROR;
    };

    if (held_page_p == NULL)
      {
	error = ER_FAILED;
      }
    else
      {
	VPID null_vpid = VPID_INITIALIZER;
	VPID_SET_NULL (&null_vpid);
	QFILE_PUT_TUPLE_COUNT (held_page_p, 0);
	QFILE_PUT_PREV_VPID (held_page_p, &null_vpid);
	QFILE_PUT_NEXT_VPID_NULL (held_page_p);
	QFILE_PUT_LAST_TUPLE_OFFSET (held_page_p, QFILE_PAGE_HEADER_SIZE);
	QFILE_PUT_OVERFLOW_VPID_NULL (held_page_p);
	list_id.last_pgptr = held_page_p;
      }

    std::vector<char> first_tuple = make_int_tuple (7);
    std::vector<char> second_tuple = make_int_tuple (11);
    std::vector<char> expected_first_tuple = make_int_tuple (99);
    if (error == NO_ERROR && (first_tuple.empty () || second_tuple.empty () || expected_first_tuple.empty ()))
      {
	error = ER_FAILED;
      }

    QFILE_TUPLE_POSITION first_pos = {};
    if (error == NO_ERROR)
      {
	error = qfile_add_tuple_get_pos_in_list (thread_p, &list_id, first_tuple.data (), &first_pos);
      }

    std::vector<char> decrypted (DB_PAGESIZE, 0);
    if (error == NO_ERROR)
      {
	error = read_decrypt_and_nonce (decrypted.data (), &nonce_after_first);
      }

    if (error == NO_ERROR)
      {
	DB_VALUE int_value;
	db_make_int (&int_value, 99);
	error = qfile_set_tuple_column_value_by_position (thread_p, &list_id, &first_pos, 0, &int_value,
							  &tp_Integer_domain);
      }

    if (error == NO_ERROR)
      {
	error = read_decrypt_and_nonce (decrypted.data (), &nonce_after_mutation);
      }

    if (error == NO_ERROR)
      {
	error = qfile_add_tuple_to_list (thread_p, &list_id, second_tuple.data ());
      }

    if (error == NO_ERROR)
      {
	error = read_decrypt_and_nonce (decrypted.data (), &nonce_after_second);
      }

    if (error == NO_ERROR)
      {
	const char *tuple_p = decrypted.data () + first_pos.tuple_offset;
	if (first_pos.tuple_offset != QFILE_PAGE_HEADER_SIZE
	    || memcmp (tuple_p, expected_first_tuple.data (), expected_first_tuple.size ()) != 0
	    || nonce_after_first == 0 || nonce_after_mutation == 0 || nonce_after_second == 0
	    || nonce_after_first == nonce_after_mutation || nonce_after_first == nonce_after_second
	    || nonce_after_mutation == nonce_after_second)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	    error = ER_FAILED;
	  }
      }

    if (list_id.last_pgptr != NULL)
      {
	rawfd_release_fixed_page (thread_p, &tfile, list_id.last_pgptr);
	list_id.last_pgptr = NULL;
      }
    tfile.raw_fd_handle = NULL;
    destroy_raw_fd_file (file);
    return error;
  }

  int
  rawfd_mutation_nonce_selftest (THREAD_ENTRY * thread_p) noexcept
  {
    int os_error = 0;
    raw_fd_file *file = create_raw_fd_file (thread_p, static_cast<QUERY_ID> (-5), LOG_FIND_THREAD_TRAN_INDEX (thread_p),
                                            0, &os_error);
    if (file == NULL)
      {
	return ER_FAILED;
      }

    QMGR_TEMP_FILE tfile = {};
    tfile.temp_file_type = FILE_TEMP;
    tfile.backing = qmgr_temp_backing::RAW_FD_OVERFLOW;
    tfile.raw_fd_query_id = static_cast<QUERY_ID> (-5);
    tfile.raw_fd_owner_tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
    tfile.raw_fd_handle = file;
    tfile.raw_fd_next_pageid = 2;
    tfile.membuf_last = -1;
    tfile.wm_reserved_shard = -1;
    tfile.tde_encrypted = true;
    file->attach_temp_file (&tfile);

    QFILE_LIST_ID list_id;
    QFILE_CLEAR_LIST_ID (&list_id);
    QFILE_LIST_ID_TFILE_VFID(&(list_id)) = &tfile;
    list_id.tuple_cnt = 1;
    list_id.page_cnt = 2;
    QFILE_LIST_ID_FIRST_VPID(&(list_id)).volid = NULL_VOLID;
    QFILE_LIST_ID_FIRST_VPID(&(list_id)).pageid = 0;
    QFILE_LIST_ID_LAST_VPID(&(list_id)).volid = NULL_VOLID;
    QFILE_LIST_ID_LAST_VPID(&(list_id)).pageid = 1;
    list_id.last_offset = QFILE_PAGE_HEADER_SIZE;
    list_id.query_id = static_cast<QUERY_ID> (-5);

    constexpr PAGEID PAGE_COUNT = 2;
    const int first_chunk_size = QFILE_MAX_TUPLE_SIZE_IN_PAGE;
    const int filler_len = QFILE_MAX_TUPLE_SIZE_IN_PAGE;
    const int int_value_len = OR_INT_SIZE;
    const int tuple_length = QFILE_TUPLE_LENGTH_SIZE + QFILE_TUPLE_VALUE_HEADER_SIZE + filler_len
                             + QFILE_TUPLE_VALUE_HEADER_SIZE + int_value_len;

    std::vector<char> tuple (tuple_length, 0);
    QFILE_PUT_TUPLE_LENGTH (tuple.data (), tuple_length);
    QFILE_PUT_PREV_TUPLE_LENGTH (tuple.data (), 0);

    char *value_p = tuple.data () + QFILE_TUPLE_LENGTH_SIZE;
    QFILE_PUT_TUPLE_VALUE_FLAG (value_p, V_BOUND);
    QFILE_PUT_TUPLE_VALUE_LENGTH (value_p, filler_len);
    memset (value_p + QFILE_TUPLE_VALUE_HEADER_SIZE, 0x5a, filler_len);

    value_p += QFILE_TUPLE_VALUE_HEADER_SIZE + filler_len;
    QFILE_PUT_TUPLE_VALUE_FLAG (value_p, V_BOUND);
    QFILE_PUT_TUPLE_VALUE_LENGTH (value_p, int_value_len);

    DB_VALUE int_value;
    db_make_int (&int_value, 7);
    OR_BUF int_buf;
    or_init (&int_buf, value_p + QFILE_TUPLE_VALUE_HEADER_SIZE, int_value_len);
    int error = tp_Integer_domain.type->data_writeval (&int_buf, &int_value);
    if (error != NO_ERROR)
      {
	destroy_raw_fd_file (file);
	return error;
      }

    std::vector<std::vector<char>> expected (PAGE_COUNT, std::vector<char> (DB_PAGESIZE, 0));
    for (PAGEID page = 0; page < PAGE_COUNT; page++)
      {
	QFILE_PAGE_HEADER page_header = QFILE_PAGE_HEADER_INITIALIZER;
	put_page_header (expected[page].data (), &page_header);
      }

    QFILE_PUT_TUPLE_COUNT (expected[0].data (), 1);
    QFILE_PUT_LAST_TUPLE_OFFSET (expected[0].data (), QFILE_PAGE_HEADER_SIZE);
    {
      VPID overflow_vpid = { 1, NULL_VOLID };
      QFILE_PUT_OVERFLOW_VPID (expected[0].data (), &overflow_vpid);
    }
    QFILE_PUT_TUPLE_COUNT (expected[1].data (), QFILE_OVERFLOW_TUPLE_COUNT_FLAG);
    QFILE_PUT_OVERFLOW_TUPLE_PAGE_SIZE (expected[1].data (), tuple_length - first_chunk_size);
    QFILE_PUT_OVERFLOW_VPID_NULL (expected[1].data ());

    memcpy (expected[0].data () + QFILE_PAGE_HEADER_SIZE, tuple.data (), first_chunk_size);
    memcpy (expected[1].data () + QFILE_PAGE_HEADER_SIZE, tuple.data () + first_chunk_size,
	    tuple_length - first_chunk_size);

    INT64 initial_nonces[PAGE_COUNT] = { 0, 0 };
    INT64 rewritten_nonces[PAGE_COUNT] = { 0, 0 };
    const TDE_ALGORITHM tde_algo = (TDE_ALGORITHM) prm_get_integer_value (PRM_ID_TDE_DEFAULT_ALGORITHM);

    auto read_decrypt_and_nonce = [&] (PAGEID page, PAGE_PTR out_page, INT64 *nonce_out) -> int
    {
      aligned_io_page cipher_check;
      aligned_io_page plain_check;
      if (!cipher_check.valid () || !plain_check.valid ()
	  || !full_pread (file->fd (), cipher_check.page, IO_PAGESIZE,
			  static_cast<off_t> (page) * static_cast<off_t> (IO_PAGESIZE)))
	{
	  return ER_FAILED;
	}

      int local_error = tde_decrypt_data_page (cipher_check.page, tde_algo, true, plain_check.page);
      if (local_error != NO_ERROR)
	{
	  return local_error;
	}

      memcpy (out_page, plain_check.page->page, DB_PAGESIZE);
      *nonce_out = cipher_check.page->prv.tde_nonce;
      return NO_ERROR;
    };

    for (PAGEID page = 0; page < PAGE_COUNT && error == NO_ERROR; page++)
      {
	error = rawfd_write_page (thread_p, *file, page, expected[page].data ());
	if (error == NO_ERROR)
	  {
	    std::vector<char> decrypted (DB_PAGESIZE, 0);
	    error = read_decrypt_and_nonce (page, decrypted.data (), &initial_nonces[page]);
	    if (error == NO_ERROR && memcmp (decrypted.data (), expected[page].data (), DB_PAGESIZE) != 0)
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
		error = ER_FAILED;
	      }
	  }
      }

    if (error == NO_ERROR)
      {
	raw_fd_page_coordinate coord { file->segment_id (), 0, QFILE_PAGE_HEADER_SIZE };
	PAGE_PTR cached_page_p = rawfd_pos_read (thread_p, *file, coord);
	if (cached_page_p == NULL)
	  {
	    error = ER_FAILED;
	  }
	else
	  {
	    rawfd_release_fixed_page (thread_p, &tfile, cached_page_p);
	  }
      }

    if (error == NO_ERROR)
      {
	db_make_int (&int_value, 99);
	VPID first_vpid = { 0, NULL_VOLID };
	error = qfile_set_tuple_column_value (thread_p, &list_id, NULL, &first_vpid,
					      expected[0].data () + QFILE_PAGE_HEADER_SIZE, 1, &int_value,
					      &tp_Integer_domain);
	if (error == NO_ERROR)
	  {
	    QFILE_TUPLE assembled_tuple = tuple.data ();
	    char *target_value_p = NULL;
	    int target_value_size = 0;
	    if (qfile_locate_tuple_value (assembled_tuple, 1, &target_value_p, &target_value_size) != V_BOUND)
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
		error = ER_FAILED;
	      }
	    else
	      {
		or_init (&int_buf, target_value_p, target_value_size);
		error = tp_Integer_domain.type->data_writeval (&int_buf, &int_value);
	      }
	  }
      }

    if (error == NO_ERROR)
      {
	memcpy (expected[0].data () + QFILE_PAGE_HEADER_SIZE, tuple.data (), first_chunk_size);
	memcpy (expected[1].data () + QFILE_PAGE_HEADER_SIZE, tuple.data () + first_chunk_size,
		tuple_length - first_chunk_size);

	for (PAGEID page = 0; page < PAGE_COUNT && error == NO_ERROR; page++)
	  {
	    std::vector<char> decrypted (DB_PAGESIZE, 0);
	    error = read_decrypt_and_nonce (page, decrypted.data (), &rewritten_nonces[page]);
	    if (error != NO_ERROR)
	      {
		break;
	      }

	    if (memcmp (decrypted.data (), expected[page].data (), DB_PAGESIZE) != 0
		|| rewritten_nonces[page] == 0 || rewritten_nonces[page] == initial_nonces[page])
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
		error = ER_FAILED;
	      }
	  }
      }

    if (error == NO_ERROR)
      {
	std::set<INT64> seen_nonces;
	for (PAGEID page = 0; page < PAGE_COUNT; page++)
	  {
	    seen_nonces.insert (initial_nonces[page]);
	    seen_nonces.insert (rewritten_nonces[page]);
	  }

	if (seen_nonces.size () != PAGE_COUNT * 2)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	    error = ER_FAILED;
	  }
      }

    if (error == NO_ERROR)
      {
	raw_fd_page_coordinate coord { file->segment_id (), 0, QFILE_PAGE_HEADER_SIZE };
	PAGE_PTR actual_page_p = rawfd_pos_read (thread_p, *file, coord);
	if (actual_page_p == NULL)
	  {
	    error = ER_FAILED;
	  }
	else
	  {
	    if (memcmp (actual_page_p, expected[0].data (), DB_PAGESIZE) != 0)
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
		error = ER_FAILED;
	      }
	    rawfd_release_fixed_page (thread_p, &tfile, actual_page_p);
	  }
      }

    if (error == NO_ERROR)
      {
	error = rawfd_positional_alias_mutation_selftest (thread_p);
      }
    tfile.raw_fd_handle = NULL;
    destroy_raw_fd_file (file);
    return error;
  }

  int
  qmgr_temp_file_move_selftest (THREAD_ENTRY * thread_p) noexcept
  {
#if !defined (NDEBUG)
    QMGR_TEMP_FILE src, dst;
    int error = NO_ERROR;

    qmgr_temp_file_move_selftest_init (&src);
    qmgr_temp_file_move_selftest_init (&dst);

    src.membuf = static_cast<PAGE_PTR *> (malloc (sizeof (PAGE_PTR) * 2));
    dst.membuf = static_cast<PAGE_PTR *> (malloc (sizeof (PAGE_PTR) * 2));
    if (src.membuf == NULL || dst.membuf == NULL)
      {
	free (src.membuf);
	free (dst.membuf);
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (PAGE_PTR) * 2);
	return ER_FAILED;
      }

    const std::size_t reserved_bytes = reservation_bytes_for_pages (1);
    if (!reserve_held (reserved_bytes, &src.wm_reserved_shard))
      {
	free (src.membuf);
	free (dst.membuf);
	return ER_FAILED;
      }
    src.wm_reserved_bytes = reserved_bytes;

    /* Use a SENTINEL temp-volume VFID (no real file): file_create_temp/file_temp_retire require a sysop/transaction
     * context that does not exist in qmgr_initialize at server boot (assert tdes->is_allowed_sysop()).  The move
     * primitive's temp_vfid handling is a pure value-transfer + VFID_SET_NULL(src), so a sentinel exercises it fully;
     * dst.temp_vfid is nulled before destroy so the sentinel is never retired. */
    src.temp_vfid.volid = 1;
    src.temp_vfid.fileid = 0x5A5A5A;

    src.temp_file_type = FILE_QUERY_AREA;
    src.membuf[0] = NULL;
    src.membuf[1] = NULL;
    src.membuf_last = 1;
    src.membuf_npages = 2;
    src.membuf_type = TEMP_FILE_MEMBUF_NORMAL;
    src.membuf_capacity_pages = 2;
    dst.membuf[0] = NULL;
    dst.membuf[1] = NULL;
    dst.membuf_last = 0;
    dst.membuf_npages = 2;
    dst.membuf_type = TEMP_FILE_MEMBUF_KEY_BUFFER;
    dst.membuf_capacity_pages = 2;
    src.backing = qmgr_temp_backing::PRIVATE_SPILL_FALLBACK;
    src.raw_fd_query_id = static_cast<QUERY_ID> (-2);
    src.raw_fd_owner_tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
    src.raw_fd_worker_id = 7;
    src.raw_fd_next_pageid = 2;
    src.tde_encrypted = true;
    src.preserved = true;

    PAGE_PTR *const src_membuf = src.membuf;
    PAGE_PTR *const dst_membuf = dst.membuf;
    const VFID temp_vfid = src.temp_vfid;

    qmgr_temp_file_move (&dst, &src);

    if (src.raw_fd_handle != NULL || !VFID_ISNULL (&src.temp_vfid) || src.membuf != src_membuf
	|| src.membuf_last != 1 || src.membuf_npages != 2 || src.membuf_type != TEMP_FILE_MEMBUF_NORMAL
	|| src.membuf_capacity_pages != 2 || src.wm_reserved_bytes != 0 || src.wm_reserved_shard != -1
	|| src.backing != qmgr_temp_backing::MEMBUF || src.raw_fd_query_id != NULL_QUERY_ID
	|| src.raw_fd_owner_tran_index != NULL_TRAN_INDEX || src.raw_fd_worker_id != 0 || src.raw_fd_next_pageid != 0
	|| src.temp_file_type != FILE_TEMP || src.preserved || src.tde_encrypted || dst.membuf != dst_membuf
	|| dst.membuf_last != 0 || dst.membuf_npages != 2 || dst.membuf_type != TEMP_FILE_MEMBUF_KEY_BUFFER
	|| dst.membuf_capacity_pages != 2 || !VFID_EQ (&dst.temp_vfid, &temp_vfid)
	|| dst.temp_file_type != FILE_QUERY_AREA || !dst.preserved || dst.wm_reserved_bytes != reserved_bytes
	|| dst.wm_reserved_shard < 0 || dst.backing != qmgr_temp_backing::PRIVATE_SPILL_FALLBACK
	|| dst.raw_fd_query_id != static_cast<QUERY_ID> (-2)
	|| dst.raw_fd_owner_tran_index != LOG_FIND_THREAD_TRAN_INDEX (thread_p) || dst.raw_fd_worker_id != 7
	|| dst.raw_fd_next_pageid != 2 || !dst.tde_encrypted)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	error = ER_FAILED;
      }

    /* Sentinel temp-volume VFID: clear it on dst so destroy does not file_temp_retire a non-existent file at boot. */
    VFID_SET_NULL (&dst.temp_vfid);
    if (qmgr_temp_file_move_selftest_destroy (thread_p, &src) != NO_ERROR)
      {
	error = ER_FAILED;
      }
    if (qmgr_temp_file_move_selftest_destroy (thread_p, &dst) != NO_ERROR)
      {
	error = ER_FAILED;
      }
    if (error != NO_ERROR)
      {
	return error;
      }

    qmgr_temp_file_move_selftest_init (&src);
    qmgr_temp_file_move_selftest_init (&dst);

    /* (c′) leg (#132, design §4 D5): the page-spill handle moves by plain
     * pointer transfer (containment ownership -- no registry reassign), and
     * src resets to pristine MEMBUF.  No gate/master dependency. */
    {
      int os_error = 0;
      qfile::page_spill_file *spill_p =
	qfile::page_spill_file::create (static_cast<QUERY_ID> (-11), LOG_FIND_THREAD_TRAN_INDEX (thread_p), 0,
					false, &os_error);
      if (spill_p == NULL)
	{
	  return ER_FAILED;
	}

      src.backing = qmgr_temp_backing::PAGE_SPILL_OVERFLOW;
      src.page_spill_handle = spill_p;
      src.raw_fd_query_id = static_cast<QUERY_ID> (-11);
      src.raw_fd_owner_tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
      src.raw_fd_next_pageid = 3;

      qmgr_temp_file_move (&dst, &src);

      if (src.page_spill_handle != NULL || src.backing != qmgr_temp_backing::MEMBUF || src.raw_fd_next_pageid != 0
	  || src.raw_fd_query_id != NULL_QUERY_ID || dst.page_spill_handle != spill_p
	  || dst.backing != qmgr_temp_backing::PAGE_SPILL_OVERFLOW || dst.raw_fd_next_pageid != 3
	  || dst.raw_fd_query_id != static_cast<QUERY_ID> (-11))
	{
	  delete spill_p;
	  src.page_spill_handle = NULL;
	  dst.page_spill_handle = NULL;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	  return ER_FAILED;
	}

      delete dst.page_spill_handle;
      dst.page_spill_handle = NULL;
      qmgr_temp_file_move_selftest_init (&src);
      qmgr_temp_file_move_selftest_init (&dst);
    }

    if (!raw_fd_writes_enabled ())
      {
	return NO_ERROR;
      }
    if (qfile_spill_new_backing_enabled ())
      {
	/* #134: this leg drives alloc_page () to manufacture a RAW_FD_OVERFLOW
	 * backing, but under the (c′) coexistence gate the membuf-overflow
	 * branch serves PAGE_SPILL instead -- the leg would leave a fixed
	 * spill page behind and trip the ~page_spill_file leak assert.  The
	 * raw-fd move transfer is covered by gate-off boots. */
	er_log_debug (ARG_FILE_LINE, "TEMPMOVE_SELFTEST: raw-fd leg skipped (CUBRID_WM_SPILL_NEW gate on)\n");
	return NO_ERROR;
      }
    src.membuf = static_cast<PAGE_PTR *> (malloc (sizeof (PAGE_PTR)));
    dst.membuf = static_cast<PAGE_PTR *> (malloc (sizeof (PAGE_PTR)));
    if (src.membuf == NULL || dst.membuf == NULL)
      {
	(void) qmgr_temp_file_move_selftest_destroy (thread_p, &src);
	(void) qmgr_temp_file_move_selftest_destroy (thread_p, &dst);
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (PAGE_PTR));
	return ER_FAILED;
      }

    src.membuf[0] = NULL;
    src.membuf_last = -1;
    src.membuf_npages = 0;
    src.membuf_type = TEMP_FILE_MEMBUF_NORMAL;
    src.membuf_capacity_pages = 1;
    dst.membuf[0] = NULL;
    dst.membuf_last = -1;
    dst.membuf_npages = 0;
    dst.membuf_type = TEMP_FILE_MEMBUF_KEY_BUFFER;
    dst.membuf_capacity_pages = 1;

    src.backing = qmgr_temp_backing::MEMBUF;
    src.raw_fd_query_id = static_cast<QUERY_ID> (-3);
    src.raw_fd_owner_tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
    src.raw_fd_worker_id = 11;

    VPID raw_vpid;
    VPID_SET_NULL (&raw_vpid);
    PAGE_PTR raw_page = alloc_page (thread_p, &src, &raw_vpid);
    if (raw_page == NULL || src.raw_fd_handle == NULL || src.backing != qmgr_temp_backing::RAW_FD_OVERFLOW)
      {
	if (raw_page != NULL)
	  {
	    rawfd_release_fixed_page (thread_p, &src, raw_page);
	  }
	(void) qmgr_temp_file_move_selftest_destroy (thread_p, &src);
	(void) qmgr_temp_file_move_selftest_destroy (thread_p, &dst);
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	return ER_FAILED;
      }
    rawfd_release_fixed_page (thread_p, &src, raw_page);

    PAGE_PTR *const raw_src_membuf = src.membuf;
    PAGE_PTR *const raw_dst_membuf = dst.membuf;
    raw_fd_file *const raw_fd_handle = src.raw_fd_handle;
    qmgr_temp_file_move (&dst, &src);

    if (src.raw_fd_handle != NULL || src.membuf != raw_src_membuf || src.membuf_last != -1
	|| src.membuf_npages != 0 || src.membuf_type != TEMP_FILE_MEMBUF_NORMAL || src.membuf_capacity_pages != 1
	|| src.raw_fd_query_id != NULL_QUERY_ID || src.raw_fd_owner_tran_index != NULL_TRAN_INDEX
	|| src.raw_fd_worker_id != 0 || src.raw_fd_next_pageid != 0 || src.backing != qmgr_temp_backing::MEMBUF
	|| dst.raw_fd_handle != raw_fd_handle || dst.raw_fd_next_pageid != 1
	|| dst.backing != qmgr_temp_backing::RAW_FD_OVERFLOW || dst.membuf != raw_dst_membuf
	|| dst.membuf_last != -1 || dst.membuf_npages != 0 || dst.membuf_type != TEMP_FILE_MEMBUF_KEY_BUFFER
	|| dst.membuf_capacity_pages != 1 || dst.raw_fd_query_id != static_cast<QUERY_ID> (-3)
	|| dst.raw_fd_owner_tran_index != LOG_FIND_THREAD_TRAN_INDEX (thread_p) || dst.raw_fd_worker_id != 11)
      {
	(void) qmgr_temp_file_move_selftest_destroy (thread_p, &src);
	(void) qmgr_temp_file_move_selftest_destroy (thread_p, &dst);
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	return ER_FAILED;
      }

    (void) qmgr_temp_file_move_selftest_destroy (thread_p, &src);
    cleanup_query_raw_fd_files (dst.raw_fd_owner_tran_index, dst.raw_fd_query_id);
    if (dst.raw_fd_handle != NULL)
      {
	dst.raw_fd_handle = NULL;
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	return ER_FAILED;
      }

    return qmgr_temp_file_move_selftest_destroy (thread_p, &dst);
#else /* !NDEBUG */
    (void) thread_p;
    return NO_ERROR;
#endif /* !NDEBUG */
  }

  bool
  raw_fd_master_enabled () noexcept
  {
    bool master_enable = LEADER_VERIFIED_ENABLE_RAW_FD_WRITES;

#if !defined (NDEBUG)
    /* Debug-only P1b end-to-end test affordance: exercise the raw-fd product path under the full safety net without
     * permanently flipping the committed master switch.  Never compiled into release (NDEBUG).  Default stays false. */
    if (getenv ("CUBRID_RAWFD_ENABLE_TEST") != NULL)
      {
	master_enable = true;
      }
#endif /* !NDEBUG */

    return master_enable;
  }

  void
  initialize_raw_fd_boot_sweep () noexcept
  {
    if (raw_fd_master_enabled ())
      {
	ensure_rawfd_state ();
      }
  }

  bool
  raw_fd_writes_enabled () noexcept
  {
    if (!raw_fd_master_enabled ())
      {
	return false;
      }

    ensure_rawfd_state ();

    return g_rawfd_state.boot_sweep_complete && g_rawfd_state.reaper_active;
  }

  PAGE_PTR
  alloc_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, VPID * vpid_p)
  {
    if (tfile_p == NULL)
      {
        er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TEMP_FILE, 1, LOG_FIND_THREAD_TRAN_INDEX (thread_p));
        return NULL;
      }

    switch (tfile_p->backing)
      {
      case qmgr_temp_backing::MEMBUF:
        {
          if (tfile_p->membuf != NULL && tfile_p->membuf_last < tfile_p->membuf_npages - 1)
            {
	      QFILE_PAGE_HEADER page_header = QFILE_PAGE_HEADER_INITIALIZER;

              vpid_p->volid = NULL_VOLID;
              vpid_p->pageid = ++(tfile_p->membuf_last);
	      put_page_header (tfile_p->membuf[tfile_p->membuf_last], &page_header);
              return tfile_p->membuf[tfile_p->membuf_last];
            }

          if (qfile_spill_new_backing_enabled ())
            {
              /* (c′) page-spill backing (#132; default ON since 커밋 A #135,
               * =0 opt-out).  The choice is made ONCE at the tfile's first
               * spill; the PAGE_SPILL_OVERFLOW tag pins it thereafter. */
              int os_error = 0;
              if (tfile_p->page_spill_handle == NULL)
                {
                  tfile_p->page_spill_handle =
                    qfile::page_spill_file::create (tfile_p->raw_fd_query_id, tfile_p->raw_fd_owner_tran_index,
                                                    tfile_p->raw_fd_worker_id, tfile_p->tde_encrypted, &os_error);
                }
              if (tfile_p->page_spill_handle == NULL)
                {
                  qfile::spill_file::set_os_error (os_error);
                  return NULL;
                }

              if (tfile_p->raw_fd_next_pageid <= tfile_p->membuf_last)
                {
                  tfile_p->raw_fd_next_pageid = tfile_p->membuf_last + 1;
                }
              vpid_p->volid = NULL_VOLID;
              vpid_p->pageid = tfile_p->raw_fd_next_pageid++;
              tfile_p->backing = qmgr_temp_backing::PAGE_SPILL_OVERFLOW;
              PAGE_PTR spill_page_p = tfile_p->page_spill_handle->alloc_new_page (thread_p, vpid_p->pageid);
              if (spill_page_p == NULL)
                {
                  delete tfile_p->page_spill_handle;
                  tfile_p->page_spill_handle = NULL;
                  tfile_p->raw_fd_next_pageid = 0;
                  return NULL;
                }
              QFILE_PAGE_HEADER page_header = QFILE_PAGE_HEADER_INITIALIZER;
              put_page_header (spill_page_p, &page_header);
              return spill_page_p;
            }

          if (!raw_fd_writes_enabled ())
            {
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OUT_OF_TEMP_SPACE, 0);
              return NULL;
            }

          int os_error = 0;
          if (tfile_p->raw_fd_handle == NULL)
            {
              tfile_p->raw_fd_handle = create_raw_fd_file (thread_p, tfile_p->raw_fd_query_id,
                                                           tfile_p->raw_fd_owner_tran_index,
                                                           tfile_p->raw_fd_worker_id, &os_error);
              if (tfile_p->raw_fd_handle != NULL)
                {
                  tfile_p->raw_fd_handle->attach_temp_file (tfile_p);
                }
            }
          if (tfile_p->raw_fd_handle == NULL)
            {
              if (is_fd_or_space_error (os_error))
                {
                  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OUT_OF_TEMP_SPACE, 0);
                }
              return NULL;
            }

          if (tfile_p->raw_fd_next_pageid <= tfile_p->membuf_last)
            {
              tfile_p->raw_fd_next_pageid = tfile_p->membuf_last + 1;
            }
          vpid_p->volid = NULL_VOLID;
          vpid_p->pageid = tfile_p->raw_fd_next_pageid++;
          tfile_p->backing = qmgr_temp_backing::RAW_FD_OVERFLOW;
          PAGE_PTR raw_page_p = rawfd_alloc_fixed_page (tfile_p->raw_fd_handle, vpid_p->pageid, true);
          if (raw_page_p == NULL)
            {
              destroy_raw_fd_file (tfile_p->raw_fd_handle);
              tfile_p->raw_fd_handle = NULL;
              tfile_p->raw_fd_next_pageid = 0;
              return NULL;
            }
          return raw_page_p;
        }

      case qmgr_temp_backing::PRIVATE_SPILL_FALLBACK:
        return alloc_private_spill_page (thread_p, tfile_p, vpid_p);

      case qmgr_temp_backing::RAW_FD_OVERFLOW:
        if (!raw_fd_writes_enabled () || tfile_p->raw_fd_handle == NULL)
          {
            er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TEMP_FILE, 1, LOG_FIND_THREAD_TRAN_INDEX (thread_p));
            return NULL;
          }
        vpid_p->volid = NULL_VOLID;
        vpid_p->pageid = tfile_p->raw_fd_next_pageid++;
        {
          PAGE_PTR raw_page_p = rawfd_alloc_fixed_page (tfile_p->raw_fd_handle, vpid_p->pageid, true);
          if (raw_page_p == NULL)
            {
              return NULL;
            }
          return raw_page_p;
        }

      case qmgr_temp_backing::PAGE_SPILL_OVERFLOW:
        /* mid-run gate flip cannot happen (env is boot-cached) but keep the
         * re-check shape of the RAW_FD_OVERFLOW case above (#132). */
        if (!qfile_spill_new_backing_enabled () || tfile_p->page_spill_handle == NULL)
          {
            er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TEMP_FILE, 1, LOG_FIND_THREAD_TRAN_INDEX (thread_p));
            return NULL;
          }
        vpid_p->volid = NULL_VOLID;
        vpid_p->pageid = tfile_p->raw_fd_next_pageid++;
        {
          PAGE_PTR spill_page_p = tfile_p->page_spill_handle->alloc_new_page (thread_p, vpid_p->pageid);
          if (spill_page_p == NULL)
            {
              return NULL;
            }
          QFILE_PAGE_HEADER page_header = QFILE_PAGE_HEADER_INITIALIZER;
          put_page_header (spill_page_p, &page_header);
          return spill_page_p;
        }

      case qmgr_temp_backing::PGBUF_PINNED:
      case qmgr_temp_backing::SHARED_SPILL:
        return unreachable_backing_page (thread_p);
      }

    return unreachable_backing_page (thread_p);
  }

  PAGE_PTR
  fix_old_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, VPID * vpid_p)
  {
    if (vpid_p->volid == NULL_VOLID && tfile_p == NULL)
      {
        er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TEMP_FILE, 1, LOG_FIND_THREAD_TRAN_INDEX (thread_p));
        return NULL;
      }

    if (tfile_p == NULL)
      {
        return fix_private_spill_page (thread_p, vpid_p);
      }

    switch (tfile_p->backing)
      {
      case qmgr_temp_backing::MEMBUF:
        if (vpid_p->volid == NULL_VOLID)
          {
            return fix_membuf_page (thread_p, tfile_p, vpid_p);
          }
        return fix_private_spill_page (thread_p, vpid_p);

      case qmgr_temp_backing::PRIVATE_SPILL_FALLBACK:
        if (vpid_p->volid == NULL_VOLID)
          {
            return fix_membuf_page (thread_p, tfile_p, vpid_p);
          }
        return fix_private_spill_page (thread_p, vpid_p);

      case qmgr_temp_backing::RAW_FD_OVERFLOW:
        if (vpid_p->volid == NULL_VOLID && vpid_p->pageid >= 0 && vpid_p->pageid <= tfile_p->membuf_last)
          {
            return fix_membuf_page (thread_p, tfile_p, vpid_p);
          }
        if (tfile_p->raw_fd_handle == NULL)
          {
            return unreachable_backing_page (thread_p);
          }
        return rawfd_pos_read (thread_p, *tfile_p->raw_fd_handle,
                               raw_fd_page_coordinate { tfile_p->raw_fd_handle->segment_id (), vpid_p->pageid, 0 });

      case qmgr_temp_backing::PAGE_SPILL_OVERFLOW:
        if (vpid_p->volid == NULL_VOLID && vpid_p->pageid >= 0 && vpid_p->pageid <= tfile_p->membuf_last)
          {
            return fix_membuf_page (thread_p, tfile_p, vpid_p);
          }
        if (tfile_p->page_spill_handle == NULL)
          {
            return unreachable_backing_page (thread_p);
          }
        return tfile_p->page_spill_handle->fix_page (thread_p, vpid_p->pageid);

      case qmgr_temp_backing::PGBUF_PINNED:
      case qmgr_temp_backing::SHARED_SPILL:
        return unreachable_backing_page (thread_p);
      }

    return unreachable_backing_page (thread_p);
  }

  budget_result
  reserve_membuf_budget (int requested_pages, std::size_t *reserved_bytes_out, int *reserved_shard_out) noexcept
  {
    const std::size_t cap = cap_bytes ();
    const std::size_t headroom = headroom_bytes ();
    const std::size_t requested_page_count = static_cast<std::size_t> (std::max (requested_pages, 0));
    const std::size_t per_file_target_bytes = std::min (std::max (cap / 8, WORKMEM_MIN_TEMP_FILE_BYTES),
							WORKMEM_MAX_TEMP_FILE_BYTES);
    const std::size_t target_pages = checked_bytes_to_pages (per_file_target_bytes);
    const std::size_t requested = std::max (requested_page_count, target_pages);
    std::size_t pages = requested;

    if (cap > 0 && reservation_bytes_for_pages (requested) > headroom)
      {
        const std::size_t requested_bytes = reservation_bytes_for_pages (requested);

        if (headroom == 0 || requested_bytes == 0)
          {
            pages = requested > 0 ? 1 : 0;
          }
        else
          {
            pages = std::max (static_cast<std::size_t> (1), requested * headroom / requested_bytes);
          }
      }

    const std::size_t bytes = reservation_bytes_for_pages (pages);
    int shard = -1;
    reserve_held_soft (bytes, &shard);
    if (shard >= 0)
      {
        const bool over_cap = pages != requested;
        if (over_cap)
          {
            record_degrade ();
          }
        *reserved_bytes_out = bytes;
        *reserved_shard_out = shard;
        return budget_result { static_cast<int> (std::min<std::size_t> (pages,
									 static_cast<std::size_t> (std::numeric_limits<int>::max ()))),
			       over_cap, false };
      }

    *reserved_bytes_out = 0;
    *reserved_shard_out = -1;
    return budget_result { 0, true, true };
  }

  void
  release_held_reservation (QMGR_TEMP_FILE * tfile_p) noexcept
  {
    if (tfile_p == NULL)
      {
        return;
      }

    if (tfile_p->raw_fd_handle != NULL)
      {
        destroy_raw_fd_file (tfile_p->raw_fd_handle);
        tfile_p->raw_fd_handle = NULL;
      }
    if (tfile_p->page_spill_handle != NULL)
      {
        /* containment ownership (D2, #132): delete = close + unlink + census */
        delete tfile_p->page_spill_handle;
        tfile_p->page_spill_handle = NULL;
      }

    if (tfile_p->wm_reserved_bytes == 0)
      {
        return;
      }

    release_held (tfile_p->wm_reserved_bytes, tfile_p->wm_reserved_shard);
    tfile_p->wm_reserved_bytes = 0;
    tfile_p->wm_reserved_shard = -1;
  }

  bool
  reserve_held (std::size_t bytes, int *shard_out) noexcept
  {
    ensure_init ();

    const int shard = choose_shard ();
    const int64_t accounting_bytes = clamp_to_accounting_bytes (bytes);
    g_accountant.shards[shard].reserved.fetch_add (accounting_bytes, std::memory_order_acq_rel);

    const std::size_t observed = exact_reserved_bytes ();
    if (observed <= overshoot_limit_bytes ())
      {
        *shard_out = shard;
        return true;
      }

    const std::size_t reconciled = exact_reserved_bytes ();
    if (reconciled <= cap_bytes ())
      {
        *shard_out = shard;
        return true;
      }

    g_accountant.shards[shard].reserved.fetch_sub (accounting_bytes, std::memory_order_acq_rel);
    *shard_out = -1;
    return false;
  }

  void
  reserve_held_soft (std::size_t bytes, int *shard_out) noexcept
  {
    ensure_init ();

    const int shard = choose_shard ();
    g_accountant.shards[shard].reserved.fetch_add (clamp_to_accounting_bytes (bytes), std::memory_order_acq_rel);
    *shard_out = shard;
  }

  void
  release_held (std::size_t bytes, int shard_index) noexcept
  {
    if (bytes == 0 || shard_index < 0 || shard_index >= static_cast<int> (WORKMEM_SHARD_COUNT))
      {
        return;
      }

    g_accountant.shards[shard_index].reserved.fetch_sub (clamp_to_accounting_bytes (bytes), std::memory_order_acq_rel);
  }

  std::size_t
  reservation_bytes_for_pages (std::size_t pages) noexcept
  {
    return checked_add_bytes (checked_pages_to_bytes (pages), position_budget_bytes ());
  }

  std::size_t
  reservation_bytes_for_degree (UINT32 degree, std::size_t pages_per_worker) noexcept
  {
    const std::size_t tier_pages = (pages_per_worker == 0)
      ? static_cast<std::size_t> (std::max<UINT64> (prm_get_bigint_value (PRM_ID_WORK_MEM) / DB_PAGESIZE, 1))
      : pages_per_worker;
    const std::size_t per_worker_bytes = reservation_bytes_for_pages (tier_pages);

    if (degree != 0 && per_worker_bytes > std::numeric_limits<std::size_t>::max () / degree)
      {
        return std::numeric_limits<std::size_t>::max ();
      }

    return per_worker_bytes * degree;
  }

  void
  record_degrade () noexcept
  {
    perfmon_inc_stat_to_global (PSTAT_WORKMEM_NUM_DEGRADES);
  }

  std::size_t
  cap_bytes () noexcept
  {
    ensure_init ();
    const int64_t cap = g_accountant.cap.load (std::memory_order_acquire);
    return cap > 0 ? static_cast<std::size_t> (cap) : 0;
  }

  std::size_t
  reserved_bytes () noexcept
  {
    return exact_reserved_bytes ();
  }

  std::size_t
  headroom_bytes () noexcept
  {
    const std::size_t cap = cap_bytes ();
    const std::size_t reserved = reserved_bytes ();

    return (reserved < cap) ? cap - reserved : 0;
  }

  std::size_t
  shard_count () noexcept
  {
    return WORKMEM_SHARD_COUNT;
  }

  std::size_t
  refill_quantum_bytes () noexcept
  {
    return WORKMEM_REFILL_QUANTUM_BYTES;
  }

  std::size_t
  worst_case_slack_bytes () noexcept
  {
    return shard_count () * refill_quantum_bytes ();
  }

  std::size_t
  overshoot_limit_bytes () noexcept
  {
    return checked_add_bytes (cap_bytes (), worst_case_slack_bytes ());
  }

  std::size_t
  position_budget_bytes () noexcept
  {
    /* P4 introduces fixed-size segment-aware tuple positions.  Charge the held reservation for widened saved-scan,
     * hash-list, connect-by parent-position bytes plus the raw-fd read-cache footprint.  reserve_held still enforces
     * reserved <= cap + worst_case_slack, with worst_case_slack = shard_count * refill_quantum. */
    constexpr bool POSITION_BUDGET_ACTIVE = true;
    if (!POSITION_BUDGET_ACTIVE)
      {
	return 0;
      }
    return WORKMEM_POSITION_SAVED_SCAN_BUDGET * projected_tuple_position_bytes
      + WORKMEM_POSITION_HASH_ENTRY_BUDGET * projected_tuple_simple_pos_bytes
      + WORKMEM_CONNECT_BY_PARENT_BUDGET * projected_tuple_position_db_bytes
      + future_read_cache_placeholder_bytes;
  }
}
