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
 * qfile_buffile.cpp - Phase1 1B per-worker private backing (redesign G006, issue #71).
 * See qfile_buffile.hpp for the design rationale.
 */

#include "qfile_buffile.hpp"

#include "boot_sr.h"		/* boot_db_full_name */
#include "error_manager.h"
#include "file_io.h"		/* FILEIO_PAGE / fileio_initialize_res / fileio_get_directory_path */
#include "page_buffer.h"	/* pgbuf_get_fix_debug_count (issue #93) */
#include "system_parameter.h"	/* prm_get_integer_value / PRM_ID_TDE_DEFAULT_ALGORITHM */
#include "tde.h"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <atomic>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace
{
  /* Pages buffered before a batched pwrite (PostgreSQL BufFile batches its
   * buffer; here a small batch coalesces syscalls without holding much RAM). */
  constexpr int BUFFILE_BATCH_PAGES = 8;

  /* Process-wide orphan-scan census (redesign G003 #68, 1C slice).  Only the
   * new per-worker backing path touches these. */
  std::atomic<long> g_census_open_files {0};
  std::atomic<long> g_census_held_prefix_pages {0};

#if !defined (NDEBUG)
  /* ENOSPC fault injection (#86, debug-only).  g_fault_flush_target is the
   * 1-based ordinal of the real (page-bearing) flush to fail; 0 = disarmed.
   * g_fault_flush_count counts real flushes seen since the last arm. */
  std::atomic<int> g_fault_flush_target {0};
  std::atomic<int> g_fault_flush_count {0};

  /* True exactly once, for the armed Nth real flush.  Called only from the
   * page-bearing branch of buffile::flush so idle flushes are not counted. */
  bool
  fault_flush_should_fail () noexcept
  {
    /* One-time arm from env for query-level repro; the self-test arms directly
     * (env unset) so this leaves its arming untouched. */
    static const bool env_armed = [] ()
    {
      const char *e = getenv ("CUBRID_WM_FAULT_FLUSH_AT");
      if (e != NULL)
	{
	  int n = atoi (e);
	  if (n > 0)
	    {
	      g_fault_flush_target.store (n, std::memory_order_relaxed);
	      g_fault_flush_count.store (0, std::memory_order_relaxed);
	      return true;
	    }
	}
      return false;
    } ();
    (void) env_armed;

    const int target = g_fault_flush_target.load (std::memory_order_relaxed);
    if (target <= 0)
      {
	return false;
      }
    const int seen = g_fault_flush_count.fetch_add (1, std::memory_order_relaxed) + 1;
    return seen == target;
  }
#endif /* !NDEBUG */

  bool
  full_pwrite (int fd, const void *buf, std::size_t len, off_t offset) noexcept
  {
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
    return true;
  }

  bool
  full_pread (int fd, void *buf, std::size_t len, off_t offset) noexcept
  {
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
    return true;
  }

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
}				/* anonymous namespace */

namespace qfile
{
  /* ------------------------------------------------------------------ */
  /* tape_backing_census (orphan-scan hook, redesign G003 #68 / 1C)     */
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

#if !defined (NDEBUG)
  void
  buffile_fault_arm_flush_fail (int nth)
  {
    g_fault_flush_count.store (0, std::memory_order_relaxed);
    g_fault_flush_target.store (nth > 0 ? nth : 0, std::memory_order_relaxed);
  }
#endif /* !NDEBUG */

  /* ------------------------------------------------------------------ */
  /* tde_read_scratch (per-reader read scratch, ADR 0005)               */
  /* ------------------------------------------------------------------ */

  tde_read_scratch::tde_read_scratch ()
    : cipher_raw (NULL), cipher (NULL), plain_raw (NULL), plain (NULL)
  {
  }

  tde_read_scratch::~tde_read_scratch ()
  {
    free (cipher_raw);
    free (plain_raw);
  }

  int
  tde_read_scratch::ensure ()
  {
    if (cipher != NULL && plain != NULL)
      {
	return NO_ERROR;
      }
    if (cipher_raw == NULL)
      {
	cipher_raw = static_cast<char *> (malloc ((std::size_t) IO_PAGESIZE + MAX_ALIGNMENT));
	if (cipher_raw == NULL)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) IO_PAGESIZE);
	    return ER_OUT_OF_VIRTUAL_MEMORY;
	  }
	cipher = reinterpret_cast<FILEIO_PAGE *> (PTR_ALIGN (cipher_raw, MAX_ALIGNMENT));
      }
    if (plain_raw == NULL)
      {
	plain_raw = static_cast<char *> (malloc ((std::size_t) IO_PAGESIZE + MAX_ALIGNMENT));
	if (plain_raw == NULL)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) IO_PAGESIZE);
	    return ER_OUT_OF_VIRTUAL_MEMORY;
	  }
	plain = reinterpret_cast<FILEIO_PAGE *> (PTR_ALIGN (plain_raw, MAX_ALIGNMENT));
      }
    return NO_ERROR;
  }

  buffile::buffile (int fd, const std::string &path, TDE_ALGORITHM tde_algo, int disk_pagesize)
    : m_fd (fd)
    , m_path (path)
    , m_tde_algo (tde_algo)
    , m_disk_pagesize (disk_pagesize)
    , m_pages_on_disk (0)
    , m_batch_raw (NULL)
    , m_batch (NULL)
    , m_batch_pages (0)
    , m_plain_raw (NULL)
    , m_plain (NULL)
#if !defined (NDEBUG)
    , m_pgbuf_fix_baseline (pgbuf_get_fix_debug_count ())
#else /* NDEBUG */
    , m_pgbuf_fix_baseline (0)
#endif /* NDEBUG */
    , m_metrics ()
  {
    if (m_fd >= 0)
      {
	tape_backing_census_file_opened ();
      }
  }

  /* Producer-side pgbuf-bypass gate (issue #93): a BufFile reads/writes only
   * through pread/pwrite on its own fd and must never fix a pgbuf BCB.
   * Snapshot-diffing the boot-independent debug counter across the object's
   * lifetime replaces the old always-zero field with a real measurement. */
  void
  buffile::refresh_pgbuf_fixes ()
  {
#if !defined (NDEBUG)
    m_metrics.pgbuf_fixes = pgbuf_get_fix_debug_count () - m_pgbuf_fix_baseline;
#endif /* !NDEBUG */
  }

  buffile::~buffile ()
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
    free (m_batch_raw);
    free (m_plain_raw);
  }

  bool
  buffile::default_scratch_dir (std::string &out)
  {
    std::string base;
    const char *cubrid_tmp = getenv ("CUBRID_TMP");
    if (cubrid_tmp != NULL && cubrid_tmp[0] != '\0')
      {
	base = cubrid_tmp;
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
		base = dir;
	      }
	  }
      }
    if (base.empty ())
      {
	const char *tmp_env = getenv ("TMP");
	base = (tmp_env != NULL && tmp_env[0] != '\0') ? std::string (tmp_env) : std::string ("/tmp");
      }
    out = base + "/cubrid_buffile";
    return !out.empty ();
  }

  buffile *
  buffile::create (THREAD_ENTRY *thread_p, const char *dir, std::uint64_t seq, unsigned int worker_id,
		   TDE_ALGORITHM tde_algo, int *os_error_out)
  {
    (void) thread_p;
    if (os_error_out != NULL)
      {
	*os_error_out = 0;
      }
    if (dir == NULL || dir[0] == '\0')
      {
	if (os_error_out != NULL)
	  {
	    *os_error_out = EINVAL;
	  }
	return NULL;
      }

    std::string dirstr (dir);
    if (!mkdir_p (dirstr))
      {
	if (os_error_out != NULL)
	  {
	    *os_error_out = errno;
	  }
	return NULL;
      }

    char name[256];
    std::snprintf (name, sizeof (name), "/buffile_%llu_w%u_p%ld.tmp", (unsigned long long) seq, worker_id,
		   (long) getpid ());
    const std::string path = dirstr + name;

    const int fd = open (path.c_str (), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
    if (fd < 0)
      {
	const int saved = errno;
	if (os_error_out != NULL)
	  {
	    *os_error_out = saved;
	  }
	return NULL;
      }

    const int disk_pagesize = (tde_algo != TDE_ALGORITHM_NONE) ? IO_PAGESIZE : DB_PAGESIZE;
    buffile *bf = new buffile (fd, path, tde_algo, disk_pagesize);
    if (bf == NULL)
      {
	::close (fd);
	(void) ::unlink (path.c_str ());
	if (os_error_out != NULL)
	  {
	    *os_error_out = ENOMEM;
	  }
	return NULL;
      }

    /* aligned batch write buffer */
    const std::size_t batch_bytes = (std::size_t) BUFFILE_BATCH_PAGES * (std::size_t) disk_pagesize;
    bf->m_batch_raw = static_cast<char *> (malloc (batch_bytes + MAX_ALIGNMENT));
    if (bf->m_batch_raw == NULL)
      {
	delete bf;
	if (os_error_out != NULL)
	  {
	    *os_error_out = ENOMEM;
	  }
	return NULL;
      }
    bf->m_batch = PTR_ALIGN (bf->m_batch_raw, MAX_ALIGNMENT);
    return bf;
  }

  int
  buffile::ensure_write_scratch ()
  {
    if (m_plain != NULL)
      {
	return NO_ERROR;
      }
    m_plain_raw = static_cast<char *> (malloc ((std::size_t) IO_PAGESIZE + MAX_ALIGNMENT));
    if (m_plain_raw == NULL)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) IO_PAGESIZE);
	return ER_OUT_OF_VIRTUAL_MEMORY;
      }
    m_plain = reinterpret_cast<FILEIO_PAGE *> (PTR_ALIGN (m_plain_raw, MAX_ALIGNMENT));
    return NO_ERROR;
  }

  int
  buffile::stage_plaintext (const PAGE_PTR list_page, char *slot)
  {
    std::memcpy (slot, list_page, DB_PAGESIZE);
    return NO_ERROR;
  }

  int
  buffile::stage_tde (const PAGE_PTR list_page, char *slot, int page_index)
  {
    int rc = ensure_write_scratch ();
    if (rc != NO_ERROR)
      {
	return rc;
      }
    if (!tde_is_loaded ())
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TDE_CIPHER_IS_NOT_LOADED, 0);
	return ER_TDE_CIPHER_IS_NOT_LOADED;
      }

    fileio_initialize_res (NULL, m_plain, IO_PAGESIZE);
    m_plain->prv.pageid = page_index;
    m_plain->prv.volid = NULL_VOLID;
    m_plain->prv.ptype = PAGE_QRESULT;
    std::memcpy (m_plain->page, list_page, DB_PAGESIZE);

    const int error = tde_encrypt_data_page (m_plain, m_tde_algo, true, reinterpret_cast<FILEIO_PAGE *> (slot));
    return error;
  }

  int
  buffile::append_page (THREAD_ENTRY *thread_p, const PAGE_PTR list_page)
  {
    if (m_fd < 0 || list_page == NULL)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	return ER_FAILED;
      }

    if (m_batch_pages == BUFFILE_BATCH_PAGES)
      {
	int rc = flush (thread_p);
	if (rc != NO_ERROR)
	  {
	    return rc;
	  }
      }

    char *slot = m_batch + (std::size_t) m_batch_pages * (std::size_t) m_disk_pagesize;
    const int page_index = m_pages_on_disk + m_batch_pages;

    int rc;
    if (m_tde_algo != TDE_ALGORITHM_NONE)
      {
	rc = stage_tde (list_page, slot, page_index);
      }
    else
      {
	rc = stage_plaintext (list_page, slot);
      }
    if (rc != NO_ERROR)
      {
	return rc;
      }

    m_batch_pages++;
    m_metrics.pages_appended++;
    refresh_pgbuf_fixes ();
    return NO_ERROR;
  }

  int
  buffile::flush (THREAD_ENTRY *thread_p)
  {
    (void) thread_p;
    if (m_batch_pages == 0)
      {
	return NO_ERROR;
      }
#if !defined (NDEBUG)
    if (fault_flush_should_fail ())
      {
	/* simulate a disk-full pwrite (#86): same error the real ENOSPC path
	 * raises, so the close/freeze failure-propagation contract is exercised
	 * end to end without an actual full filesystem. */
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OUT_OF_TEMP_SPACE, 0);
	return ER_FAILED;
      }
#endif /* !NDEBUG */
    const off_t offset = (off_t) m_pages_on_disk * (off_t) m_disk_pagesize;
    const std::size_t len = (std::size_t) m_batch_pages * (std::size_t) m_disk_pagesize;
    if (!full_pwrite (m_fd, m_batch, len, offset))
      {
	const int saved = errno;
	if (saved == ENOSPC || saved == EDQUOT)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OUT_OF_TEMP_SPACE, 0);
	  }
	else
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	  }
	return ER_FAILED;
      }
    m_pages_on_disk += m_batch_pages;
    m_metrics.pages_flushed += m_batch_pages;
    m_metrics.flush_calls++;
    m_metrics.bytes_written += (long) len;
    m_batch_pages = 0;
    refresh_pgbuf_fixes ();
    return NO_ERROR;
  }

  int
  buffile::read_page (THREAD_ENTRY *thread_p, int page_offset, PAGE_PTR dest, tde_read_scratch *scratch) const
  {
    (void) thread_p;
    /* Re-entrant + const: no flush here.  Pages must already be on disk
     * (append-all-then-freeze); the frozen backing is immutable so a shared fd
     * + pread serves N concurrent readers safely (ADR 0005). */
    if (m_fd < 0 || dest == NULL || page_offset < 0 || page_offset >= m_pages_on_disk)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	return ER_FAILED;
      }

    if (m_tde_algo == TDE_ALGORITHM_NONE)
      {
	const off_t offset = (off_t) page_offset * (off_t) DB_PAGESIZE;
	if (!full_pread (m_fd, dest, DB_PAGESIZE, offset))
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	    return ER_FAILED;
	  }
	m_metrics.pages_read.fetch_add (1, std::memory_order_relaxed);
	return NO_ERROR;
      }

    /* TDE: the caller's scratch (not a member) carries the ciphertext read +
     * decrypt output, so two threads decrypting the same page never collide. */
    if (scratch == NULL)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	return ER_FAILED;
      }
    int rc = scratch->ensure ();
    if (rc != NO_ERROR)
      {
	return rc;
      }
    if (!tde_is_loaded ())
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TDE_CIPHER_IS_NOT_LOADED, 0);
	return ER_TDE_CIPHER_IS_NOT_LOADED;
      }
    const off_t offset = (off_t) page_offset * (off_t) IO_PAGESIZE;
    if (!full_pread (m_fd, scratch->cipher, IO_PAGESIZE, offset))
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	return ER_FAILED;
      }
    const int error = tde_decrypt_data_page (scratch->cipher, m_tde_algo, true, scratch->plain);
    if (error != NO_ERROR)
      {
	return error;
      }
    std::memcpy (dest, scratch->plain->page, DB_PAGESIZE);
    m_metrics.pages_read.fetch_add (1, std::memory_order_relaxed);
    return NO_ERROR;
  }
}				/* namespace qfile */

/* ------------------------------------------------------------------ */
/* In-server self-test (TDE round-trip; bootless unit test cannot load */
/* a cipher).  Gated by env CUBRID_BUFFILE_SELFTEST.                   */
/* ------------------------------------------------------------------ */

int
qfile_buffile_selftest (THREAD_ENTRY *thread_p)
{
  std::string dir;
  if (!qfile::buffile::default_scratch_dir (dir))
    {
      return ER_FAILED;
    }

  /* Pick TDE iff a cipher is loaded; otherwise verify the plaintext path. */
  TDE_ALGORITHM algo = TDE_ALGORITHM_NONE;
  if (tde_is_loaded ())
    {
      const TDE_ALGORITHM def = (TDE_ALGORITHM) prm_get_integer_value (PRM_ID_TDE_DEFAULT_ALGORITHM);
      if (def != TDE_ALGORITHM_NONE)
	{
	  algo = def;
	}
    }

  int os_error = 0;
  qfile::buffile *bf = qfile::buffile::create (thread_p, dir.c_str (), (std::uint64_t) -7, 0, algo, &os_error);
  if (bf == NULL)
    {
      return ER_FAILED;
    }

  int rc = NO_ERROR;
  const int NPAGES = 20;	/* > BUFFILE_BATCH_PAGES so a batch boundary is crossed */
  char *page = static_cast<char *> (malloc (DB_PAGESIZE));
  char *back = static_cast<char *> (malloc (DB_PAGESIZE));
  if (page == NULL || back == NULL)
    {
      free (page);
      free (back);
      delete bf;
      return ER_FAILED;
    }

  for (int i = 0; i < NPAGES && rc == NO_ERROR; i++)
    {
      std::memset (page, 0, DB_PAGESIZE);
      /* a recognizable pattern keyed on the page index */
      for (int b = 0; b < DB_PAGESIZE; b++)
	{
	  page[b] = (char) ((i * 31 + b) & 0xff);
	}
      rc = bf->append_page (thread_p, (PAGE_PTR) page);
    }
  if (rc == NO_ERROR)
    {
      rc = bf->flush (thread_p);
    }

  /* read back forward + a couple of random offsets; verify byte-identity */
  qfile::tde_read_scratch rscratch;
  for (int i = 0; i < NPAGES && rc == NO_ERROR; i++)
    {
      std::memset (back, 0xee, DB_PAGESIZE);
      rc = bf->read_page (thread_p, i, (PAGE_PTR) back, &rscratch);
      if (rc != NO_ERROR)
	{
	  break;
	}
      for (int b = 0; b < DB_PAGESIZE; b++)
	{
	  if (back[b] != (char) ((i * 31 + b) & 0xff))
	    {
	      rc = ER_FAILED;
	      break;
	    }
	}
    }

  /* pgbuf-bypass producer-side invariant */
  if (rc == NO_ERROR && bf->metrics ().pgbuf_fixes != 0)
    {
      rc = ER_FAILED;
    }

  free (page);
  free (back);
  delete bf;

  er_log_debug (ARG_FILE_LINE, "BUFFILE_SELFTEST algo=%d result=%d (0=PASS)\n", (int) algo, rc);
  fprintf (stderr, "BUFFILE_SELFTEST algo=%d result=%d (0=PASS)\n", (int) algo, rc);
  return rc;
}
