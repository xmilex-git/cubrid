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

#include "error_manager.h"
#include "file_io.h"		/* FILEIO_PAGE */
#include "page_buffer.h"	/* pgbuf_get_fix_debug_count (issue #93) */
#include "system_parameter.h"	/* prm_get_integer_value / PRM_ID_TDE_DEFAULT_ALGORITHM */
#include "tde.h"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <atomic>

#include <sys/types.h>
#include <unistd.h>

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace
{
  /* Pages buffered before a batched pwrite (PostgreSQL BufFile batches its
   * buffer; here a small batch coalesces syscalls without holding much RAM). */
  constexpr int BUFFILE_BATCH_PAGES = 8;

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

  /* BufFile create() fault injection (#125, debug-only).  When non-zero, the
   * next buffile::create () short-circuits its open () and reports this errno,
   * proving the ensure_buffile os_error mapping (EMFILE/ENFILE -> temp-space)
   * without exhausting the real process fd table.  0 = disarmed. */
  std::atomic<int> g_fault_create_errno {0};

  int
  fault_create_injected_errno () noexcept
  {
    return g_fault_create_errno.load (std::memory_order_relaxed);
  }
#endif /* !NDEBUG */

}				/* anonymous namespace */

namespace qfile
{
  /* tape_backing_census, full_pwrite/full_pread, mkdir_p and the boot-sweep
   * scratch-tree machinery moved to the shared spill-file substrate
   * (qfile_spill_file.cpp; Phase3 (c′) extraction, #132). */

#if !defined (NDEBUG)
  void
  buffile_fault_arm_flush_fail (int nth)
  {
    g_fault_flush_count.store (0, std::memory_order_relaxed);
    g_fault_flush_target.store (nth > 0 ? nth : 0, std::memory_order_relaxed);
  }

  void
  buffile_fault_arm_create_fail (int os_errno)
  {
    g_fault_create_errno.store (os_errno, std::memory_order_relaxed);
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

  buffile::buffile ()
    : m_file ()
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
    /* fd close + unlink + census are the substrate dtor's (m_file). */
    free (m_batch_raw);
    free (m_plain_raw);
  }

  void
  buffile::boot_sweep ()
  {
    spill_scratch_boot_sweep ();
  }

  bool
  buffile::default_scratch_dir (std::string &out)
  {
    return spill_scratch_default_dir (out);
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

    char name[256];
    std::snprintf (name, sizeof (name), "buffile_%llu_w%u_p%ld.tmp", (unsigned long long) seq, worker_id,
		   (long) getpid ());

#if !defined (NDEBUG)
    /* #125: injected fd-exhaustion (EMFILE/ENFILE) before touching the real fd
     * table.  No file was created, so nothing to unlink. */
    const int injected = fault_create_injected_errno ();
    if (injected != 0)
      {
	if (os_error_out != NULL)
	  {
	    *os_error_out = injected;
	  }
	return NULL;
      }
#endif /* !NDEBUG */

    buffile *bf = new buffile ();
    if (bf == NULL)
      {
	if (os_error_out != NULL)
	  {
	    *os_error_out = ENOMEM;
	  }
	return NULL;
      }

    if (bf->m_file.create (dir, name, tde_algo, os_error_out) != NO_ERROR)
      {
	delete bf;
	return NULL;
      }

    /* aligned batch write buffer */
    const std::size_t batch_bytes = (std::size_t) BUFFILE_BATCH_PAGES * (std::size_t) bf->m_file.stride ();
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
    return spill_file::tde_stage_encrypt (list_page, page_index, m_file.tde_algo (), m_plain,
					  reinterpret_cast<FILEIO_PAGE *> (slot));
  }

  int
  buffile::append_page (THREAD_ENTRY *thread_p, const PAGE_PTR list_page)
  {
    if (!m_file.is_open () || list_page == NULL)
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

    char *slot = m_batch + (std::size_t) m_batch_pages * (std::size_t) m_file.stride ();
    const int page_index = m_pages_on_disk + m_batch_pages;

    int rc;
    if (m_file.tde_encrypted ())
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
    const off_t offset = (off_t) m_pages_on_disk * (off_t) m_file.stride ();
    const std::size_t len = (std::size_t) m_batch_pages * (std::size_t) m_file.stride ();
    if (!m_file.pwrite_full (m_batch, len, offset))
      {
	/* disk-full class errno -> ER_QPROC_OUT_OF_TEMP_SPACE, else ER_FAILED
	 * (promoted mapping, #125/#132) */
	spill_file::set_os_error (errno);
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
    if (!m_file.is_open () || dest == NULL || page_offset < 0 || page_offset >= m_pages_on_disk)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	return ER_FAILED;
      }

    if (!m_file.tde_encrypted ())
      {
	const off_t offset = (off_t) page_offset * (off_t) DB_PAGESIZE;
	if (!m_file.pread_full (dest, DB_PAGESIZE, offset))
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
    if (!m_file.pread_full (scratch->cipher, IO_PAGESIZE, offset))
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	return ER_FAILED;
      }
    const int error = spill_file::tde_read_decrypt (scratch->cipher, m_file.tde_algo (), scratch->plain, dest);
    if (error != NO_ERROR)
      {
	return error;
      }
    m_metrics.pages_read.fetch_add (1, std::memory_order_relaxed);
    return NO_ERROR;
  }
}				/* namespace qfile */

/* ------------------------------------------------------------------ */
/* In-server self-test (TDE round-trip; bootless unit test cannot load */
/* a cipher).  Gated by env CUBRID_WM_BUFFILE_SELFTEST.                   */
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
