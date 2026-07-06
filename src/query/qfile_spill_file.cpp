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
 * qfile_spill_file.cpp - shared spill-file substrate.
 * See qfile_spill_file.hpp.
 */

#include "qfile_spill_file.hpp"

#include "boot_sr.h"		/* boot_db_full_name */
#include "error_manager.h"
#include "file_io.h"		/* FILEIO_PAGE / fileio_initialize_res / fileio_get_directory_path */
#include "query_workmem.hpp"	/* record_spill_read_bytes / record_spill_write_bytes (#146 T3 S4) */
#include "tde.h"

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <atomic>
#include <ctime>
#include <mutex>

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace
{
  /* Process-wide orphan-scan census.  Only the per-worker backing path
   * touches these. */
  std::atomic<long> g_census_open_files {0};
  std::atomic<long> g_census_held_prefix_pages {0};

  /* mkdir -p for the immediate scratch dir tree.  Tolerates existing dirs. */
  bool
  mkdir_p (const std::string &path) noexcept
  {
    if (path.empty ())
      {
	return false;
      }
    std::string acc;
    for (std::size_t i = 0; i < path.size (); i++)
      {
	acc.push_back (path[i]);
	if (path[i] == '/' && acc.size () > 1)
	  {
	    if (mkdir (acc.c_str (), 0700) != 0 && errno != EEXIST)
	      {
		return false;
	      }
	  }
      }
    if (mkdir (path.c_str (), 0700) != 0 && errno != EEXIST)
      {
	return false;
      }
    return true;
  }

  /* ------------------------------------------------------------------ */
  /* Boot-time orphan sweep for cubrid_buffile/.  A persistent per-database */
  /* server_id (survives restarts) namespaces this server's spill subtree, */
  /* so boot can safely wipe ONLY that subtree -- files left behind by a    */
  /* kill -9'd previous run of *this* server -- without touching another   */
  /* live server's spills (a different server_id / different db_root).     */
  /* ------------------------------------------------------------------ */

  constexpr const char *BUFFILE_SERVER_ID_FILE = ".server-id";

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

  std::string
  read_small_file (const std::string &path)
  {
    FILE *fp = fopen (path.c_str (), "r");
    if (fp == NULL)
      {
	return std::string ();
      }
    char buffer[128];
    const std::size_t nread = fread (buffer, 1, sizeof (buffer) - 1, fp);
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

  /* Recursively unlink every file/dir *under* `dir`, keeping `dir` itself. */
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
    const std::uint64_t now = static_cast<std::uint64_t> (time (NULL));
    const std::uint64_t pid = static_cast<std::uint64_t> (getpid ());
    return (now << 16) ^ pid;
  }

  struct buffile_dir_state
  {
    std::once_flag once;
    std::string server_subtree;	/* <root>/cubrid_buffile/<db_name>/<server_id> */
    bool sweep_ok { false };
  };

  buffile_dir_state g_buffile_dirs;

  /* Resolve the disk-backed root: unconditionally the database volume
   * directory (no $CUBRID_TMP override, no /tmp or $TMP fallback -- all of
   * those can point outside the DB volume or at tmpfs, defeating the point
   * of spilling and risking host OOM; see issue #147 D-SP1).  Empty return
   * means "no usable disk-backed base". */
  std::string
  buffile_root_base ()
  {
    const char *db_full = boot_db_full_name ();
    if (db_full != NULL && db_full[0] != '\0')
      {
	char dir_buf[PATH_MAX];
	const char *dir = fileio_get_directory_path (dir_buf, db_full);
	if (dir != NULL && dir[0] != '\0')
	  {
	    return dir;
	  }
      }
    return std::string ();
  }

  void
  init_buffile_dirs ()
  {
    const std::string root_base = buffile_root_base ();
    if (root_base.empty ())
      {
	g_buffile_dirs.sweep_ok = false;
	return;
      }

    const std::string db_name = sanitize_path_component (boot_db_name ());
    const std::string db_root = root_base + "/cubrid_buffile/" + db_name;
    bool ok = mkdir_p (db_root);

    const std::string server_id_path = db_root + "/" + BUFFILE_SERVER_ID_FILE;
    std::string server_id = sanitize_path_component (read_small_file (server_id_path).c_str ());
    if (server_id == "unknown")
      {
	server_id = "sid_" + std::to_string (make_boot_incarnation ());
	ok = write_small_file (server_id_path, server_id) && ok;
      }

    g_buffile_dirs.server_subtree = db_root + "/" + server_id;
    ok = mkdir_p (g_buffile_dirs.server_subtree) && ok;
    if (ok)
      {
	ok = unlink_tree_files_only (g_buffile_dirs.server_subtree);
      }
    g_buffile_dirs.sweep_ok = ok;
  }
}				/* anonymous namespace */

namespace qfile
{
  /* ------------------------------------------------------------------ */
  /* tape_backing_census (orphan-scan hook)                             */
  /* ------------------------------------------------------------------ */

  tape_backing_census_snapshot
  tape_backing_census ()
  {
    tape_backing_census_snapshot s;
    s.open_files = g_census_open_files.load (std::memory_order_relaxed);
    s.held_prefix_pages = g_census_held_prefix_pages.load (std::memory_order_relaxed);
    return s;
  }

  void
  tape_backing_census_file_opened ()
  {
    g_census_open_files.fetch_add (1, std::memory_order_relaxed);
  }

  void
  tape_backing_census_file_closed ()
  {
    g_census_open_files.fetch_sub (1, std::memory_order_relaxed);
  }

  void
  tape_backing_census_prefix_added (long pages)
  {
    if (pages > 0)
      {
	g_census_held_prefix_pages.fetch_add (pages, std::memory_order_relaxed);
      }
  }

  void
  tape_backing_census_prefix_removed (long pages)
  {
    if (pages > 0)
      {
	g_census_held_prefix_pages.fetch_sub (pages, std::memory_order_relaxed);
      }
  }

  /* ------------------------------------------------------------------ */
  /* EINTR-looped positional I/O                                        */
  /* ------------------------------------------------------------------ */

  bool
  full_pwrite (int fd, const void *buf, std::size_t len, off_t offset) noexcept
  {
    const std::size_t total_len = len;
    const char *ptr = static_cast<const char *> (buf);
    while (len > 0)
      {
	const ssize_t written = pwrite (fd, ptr, len, offset);
	if (written < 0)
	  {
	    if (errno == EINTR)
	      {
		continue;
	      }
	    return false;
	  }
	if (written == 0)
	  {
	    errno = ENOSPC;
	    return false;
	  }
	ptr += written;
	len -= static_cast<std::size_t> (written);
	offset += written;
      }
    /* #146 T3 S4 (§6, #141 VTune): this raw fd bypasses the page buffer, so
     * without this counter its I/O volume is invisible to statdump. */
    temp_page_store::record_spill_write_bytes (total_len);
    return true;
  }

  bool
  full_pread (int fd, void *buf, std::size_t len, off_t offset) noexcept
  {
    const std::size_t total_len = len;
    char *ptr = static_cast<char *> (buf);
    while (len > 0)
      {
	const ssize_t nread = pread (fd, ptr, len, offset);
	if (nread < 0)
	  {
	    if (errno == EINTR)
	      {
		continue;
	      }
	    return false;
	  }
	if (nread == 0)
	  {
	    errno = EIO;
	    return false;
	  }
	ptr += nread;
	len -= static_cast<std::size_t> (nread);
	offset += nread;
      }
    temp_page_store::record_spill_read_bytes (total_len);
    return true;
  }

  /* ------------------------------------------------------------------ */
  /* scratch tree + boot sweep                                          */
  /* ------------------------------------------------------------------ */

  void
  spill_scratch_boot_sweep ()
  {
    std::call_once (g_buffile_dirs.once, init_buffile_dirs);
  }

  bool
  spill_scratch_default_dir (std::string &out)
  {
    std::call_once (g_buffile_dirs.once, init_buffile_dirs);
    if (!g_buffile_dirs.sweep_ok || g_buffile_dirs.server_subtree.empty ())
      {
	return false;
      }
    out = g_buffile_dirs.server_subtree;
    return true;
  }

  /* ------------------------------------------------------------------ */
  /* spill_file                                                         */
  /* ------------------------------------------------------------------ */

  spill_file::spill_file () noexcept
    : m_fd (-1)
    , m_path ()
    , m_tde_algo (TDE_ALGORITHM_NONE)
    , m_disk_pagesize (DB_PAGESIZE)
  {
  }

  spill_file::~spill_file ()
  {
    if (m_fd >= 0)
      {
	tape_backing_census_file_closed ();
	::close (m_fd);
	m_fd = -1;
      }
    if (!m_path.empty ())
      {
	(void) ::unlink (m_path.c_str ());
      }
  }

  int
  spill_file::create (const char *dir, const char *filename, TDE_ALGORITHM tde_algo, int *os_error_out)
  {
    assert (m_fd < 0);
    if (os_error_out != NULL)
      {
	*os_error_out = 0;
      }
    if (dir == NULL || dir[0] == '\0' || filename == NULL || filename[0] == '\0')
      {
	if (os_error_out != NULL)
	  {
	    *os_error_out = EINVAL;
	  }
	return ER_FAILED;
      }

    std::string dirstr (dir);
    if (!mkdir_p (dirstr))
      {
	if (os_error_out != NULL)
	  {
	    *os_error_out = errno;
	  }
	return ER_FAILED;
      }

    const std::string path = dirstr + "/" + filename;
    const int fd = open (path.c_str (), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
    if (fd < 0)
      {
	const int saved = errno;
	if (os_error_out != NULL)
	  {
	    *os_error_out = saved;
	  }
	return ER_FAILED;
      }

    m_fd = fd;
    m_path = path;
    m_tde_algo = tde_algo;
    m_disk_pagesize = (tde_algo != TDE_ALGORITHM_NONE) ? IO_PAGESIZE : DB_PAGESIZE;
    tape_backing_census_file_opened ();
    return NO_ERROR;
  }

  void
  spill_file::set_os_error (int os_error)
  {
    if (os_error == EMFILE || os_error == ENFILE || os_error == ENOSPC || os_error == EDQUOT)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OUT_OF_TEMP_SPACE, 0);
      }
    else
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
      }
  }

  /* ------------------------------------------------------------------ */
  /* per-page TDE staging (fresh nonce per encrypt)                     */
  /* ------------------------------------------------------------------ */

  int
  spill_file::tde_stage_encrypt (const PAGE_PTR list_page, int page_index, TDE_ALGORITHM tde_algo,
				 FILEIO_PAGE *plain_scratch, FILEIO_PAGE *cipher_out)
  {
    if (!tde_is_loaded ())
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TDE_CIPHER_IS_NOT_LOADED, 0);
	return ER_TDE_CIPHER_IS_NOT_LOADED;
      }

    fileio_initialize_res (NULL, plain_scratch, IO_PAGESIZE);
    plain_scratch->prv.pageid = page_index;
    plain_scratch->prv.volid = NULL_VOLID;
    plain_scratch->prv.ptype = PAGE_QRESULT;
    std::memcpy (plain_scratch->page, list_page, DB_PAGESIZE);

    return tde_encrypt_data_page (plain_scratch, tde_algo, true, cipher_out);
  }

  int
  spill_file::tde_read_decrypt (FILEIO_PAGE *cipher, TDE_ALGORITHM tde_algo, FILEIO_PAGE *plain_scratch,
				PAGE_PTR dest)
  {
    const int error = tde_decrypt_data_page (cipher, tde_algo, true, plain_scratch);
    if (error != NO_ERROR)
      {
	return error;
      }
    std::memcpy (dest, plain_scratch->page, DB_PAGESIZE);
    return NO_ERROR;
  }
}				/* namespace qfile */
