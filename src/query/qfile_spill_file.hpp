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
 * qfile_spill_file.hpp - shared spill-file substrate.
 *
 * The pieces every private spill backing needs, shared by the append-only
 * buffile and the random-page page_spill_file so the two variants use ONE
 * file substrate instead of each carrying its own copy:
 *   - fd/path/TDE-algo/stride ownership + the create factory core
 *     (open (O_CREAT|O_EXCL|O_RDWR|O_CLOEXEC, 0600), os_error out),
 *   - dtor close+unlink+census,
 *   - EINTR-looped positional I/O (full_pwrite / full_pread),
 *   - the per-server scratch tree + boot orphan sweep,
 *   - the tape_backing_census orphan-scan hooks,
 *   - per-page TDE staging helpers (fresh nonce per physical write --
 *     guaranteed by the tde_encrypt_data_page primitive itself),
 *   - the fd-exhaustion/disk-full errno -> ER_QPROC_OUT_OF_TEMP_SPACE mapping.
 *
 * Composition, not inheritance (no virtuals, no indirect calls): each backing
 * owns a spill_file by value, keeping its public API and hot-path shape intact.
 */

#ifndef _QFILE_SPILL_FILE_HPP_
#define _QFILE_SPILL_FILE_HPP_

#include "storage_common.h"	/* PAGE_PTR / DB_PAGESIZE / IO_PAGESIZE */
#include "thread_compat.hpp"	/* THREAD_ENTRY (required before tde.h) */
#include "tde.h"		/* TDE_ALGORITHM */

#include <cstddef>
#include <string>

#include <sys/types.h>		/* off_t */

struct fileio_page;
typedef struct fileio_page FILEIO_PAGE;

namespace qfile
{
  /*
   * tape_backing_census - process-wide orphan-scan hook.  Counts the two
   * backing resources a holdable result owns: open private-file handles and
   * RAM membuf-prefix pages held by frozen Tapes.  Two invariants are asserted
   * against it:
   *   - holdable reparent (a zero-copy ownership MOVE, tran -> session) leaves
   *     BOTH counters unchanged (no copy, no flush);
   *   - session teardown drives BOTH back to the pre-result baseline
   *     (orphan-zero: file handles AND RAM prefix, not just files).
   * Counters are atomic because per-worker backings open/close concurrently;
   * they only move on the per-worker backing path, so single-backing queries
   * pay nothing.
   */
  struct tape_backing_census_snapshot
  {
    long open_files;		/* live private-file handles (spill-file fds) */
    long held_prefix_pages;	/* RAM membuf-prefix pages owned by frozen Tapes */
  };

  /* Read the current census (the orphan scan). */
  tape_backing_census_snapshot tape_backing_census ();

  /* Ownership-boundary hooks (called by spill_file open/close and tape ctors/dtors). */
  void tape_backing_census_file_opened ();
  void tape_backing_census_file_closed ();
  void tape_backing_census_prefix_added (long pages);
  void tape_backing_census_prefix_removed (long pages);

  /* EINTR-looped positional I/O.  false => errno carries the OS error (a
   * 0-byte pwrite is reported as ENOSPC, a short pread as EIO). */
  bool full_pwrite (int fd, const void *buf, std::size_t len, off_t offset) noexcept;
  bool full_pread (int fd, void *buf, std::size_t len, off_t offset) noexcept;

  /* One-shot boot sweep of this server's cubrid_buffile spill subtree: wipes
   * files orphaned by a kill -9'd previous run.  Idempotent (std::call_once).
   * spill_scratch_default_dir () runs it lazily on first use; calling it at
   * boot keeps the sweep off the query hot path. */
  void spill_scratch_boot_sweep ();

  /* Resolve the per-server default scratch directory: $CUBRID_TMP, else the
   * database volume directory, then <base>/cubrid_buffile/<db>/<server_id>
   * (server_id persists across restarts in a per-db marker file, so boot
   * sweeps only this server's own subtree).  No /tmp or $TMP fallback -- both
   * can be tmpfs, which would defeat spilling.  Returns false if no disk-backed
   * base can be formed. */
  bool spill_scratch_default_dir (std::string &out);

  /*
   * spill_file - one private, unlinked-on-destroy temp file: the fd/path/
   * TDE-algo/stride quartet plus its lifecycle (create factory, census,
   * close+unlink dtor).  Owned by value (composition) by each backing
   * variant; it carries NO paging policy -- batching/EOF arithmetic stay in
   * buffile, the resident-page cache stays in page_spill_file.
   */
  class spill_file
  {
    public:
      spill_file () noexcept;
      ~spill_file ();		/* closes fd, unlinks the file, census file_closed */

      /* Create factory core: mkdir -p `dir`, then open (dir/filename,
       * O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600) and census file_opened.
       * tde_algo == TDE_ALGORITHM_NONE => plaintext (stride DB_PAGESIZE);
       * otherwise pages are encrypted on disk (stride IO_PAGESIZE).
       * Returns NO_ERROR or ER_FAILED with *os_error_out = errno (no er_set;
       * callers map through set_os_error ()). */
      int create (const char *dir, const char *filename, TDE_ALGORITHM tde_algo, int *os_error_out);

      bool is_open () const
      {
	return m_fd >= 0;
      }
      int fd () const
      {
	return m_fd;
      }
      const std::string &path () const
      {
	return m_path;
      }
      TDE_ALGORITHM tde_algo () const
      {
	return m_tde_algo;
      }
      bool tde_encrypted () const
      {
	return m_tde_algo != TDE_ALGORITHM_NONE;
      }
      /* on-disk bytes per logical page: DB_PAGESIZE (plaintext) or IO_PAGESIZE (TDE) */
      int stride () const
      {
	return m_disk_pagesize;
      }

      bool pwrite_full (const void *buf, std::size_t len, off_t offset) const noexcept
      {
	return full_pwrite (m_fd, buf, len, offset);
      }
      bool pread_full (void *buf, std::size_t len, off_t offset) const noexcept
      {
	return full_pread (m_fd, buf, len, offset);
      }

      /* os_error -> er_set mapping: fd exhaustion / disk-full class errno
       * (EMFILE/ENFILE/ENOSPC/EDQUOT) is diagnosed as ER_QPROC_OUT_OF_TEMP_SPACE
       * -- an actionable temp-space error -- anything else as generic ER_FAILED. */
      static void set_os_error (int os_error);

      /* Stage one DB_PAGESIZE list page for an encrypted write: wrap it in an
       * IO_PAGESIZE FILEIO_PAGE (plain_scratch) and encrypt into cipher_out.
       * tde_encrypt_data_page (…, is_temp=true, …) takes a FRESH nonce on
       * every call, so each physical (re)write is uniquely keyed. */
      static int tde_stage_encrypt (const PAGE_PTR list_page, int page_index, TDE_ALGORITHM tde_algo,
				    FILEIO_PAGE *plain_scratch, FILEIO_PAGE *cipher_out);

      /* Decrypt one on-disk IO_PAGESIZE page (cipher) through plain_scratch
       * and copy the DB_PAGESIZE payload into dest. */
      static int tde_read_decrypt (FILEIO_PAGE *cipher, TDE_ALGORITHM tde_algo, FILEIO_PAGE *plain_scratch,
				   PAGE_PTR dest);

      spill_file (const spill_file &) = delete;
      spill_file &operator= (const spill_file &) = delete;

    private:
      int m_fd;
      std::string m_path;
      TDE_ALGORITHM m_tde_algo;
      int m_disk_pagesize;	/* DB_PAGESIZE (plaintext) or IO_PAGESIZE (TDE) on disk */
  };
}				/* namespace qfile */

#endif /* _QFILE_SPILL_FILE_HPP_ */
