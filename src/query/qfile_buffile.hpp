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
 * qfile_buffile.hpp - per-worker private backing.
 *
 * Backing model: a Tape's spilled pages live in a per-worker PRIVATE file
 * (PostgreSQL BufFile model) -- its own write buffer + fd, owner-only append,
 * batched flush.  It BYPASSES the shared buffer pool entirely: pages never
 * enter a pgbuf BCB, there is no server-global page registry and no per-page
 * lock.
 *
 * Addressing is pure page-offset arithmetic: logical file page N is at byte
 * offset N * stride.  No directory, no sector list, no occupancy bitmap --
 * justified by "no mid-life dealloc + private flat file" (the file holds only
 * this Tape's pages, no FTAB interleaving).
 *
 * TDE: the BufFile encrypts on disk only when created with a real algorithm
 * (includes_tde_class), reusing tde_encrypt_data_page / tde_decrypt_data_page
 * (fresh-nonce-per-page).  The membuf prefix that precedes the BufFile is
 * plaintext RAM and is NOT this class's concern (see qfile::buffile_tape /
 * tape_writer).
 */

#ifndef _QFILE_BUFFILE_HPP_
#define _QFILE_BUFFILE_HPP_

#include "qfile_spill_file.hpp"	/* spill_file substrate + tape_backing_census */
#include "storage_common.h"	/* PAGE_PTR / DB_PAGESIZE / IO_PAGESIZE */
#include "thread_compat.hpp"	/* THREAD_ENTRY */
#include "tde.h"		/* TDE_ALGORITHM */

#include <cstdint>
#include <string>
#include <atomic>

struct fileio_page;
typedef struct fileio_page FILEIO_PAGE;

namespace qfile
{
  /*
   * buffile_metrics - measurement hooks for the per-worker private backing
   * (producer side of the pgbuf-bypass hard gate).  pgbuf_fixes is the
   * load-bearing counter: a BufFile reads/writes exclusively through
   * pread/pwrite on its own fd and NEVER fixes a pgbuf BCB, so it MUST stay 0.
   */
  struct buffile_metrics
  {
    long pages_appended;	/* append_page() calls (= logical pages produced) */
    long pages_flushed;		/* pages written to disk by flush() */
    long flush_calls;		/* pwrite batches issued */
    std::atomic<long> pages_read;	/* read_page() disk reads (atomic: re-entrant concurrent readers) */
    long bytes_written;		/* total bytes pwritten */
    long pgbuf_fixes;		/* pgbuf BCB fixes -- MUST stay 0 (bypass gate, producer side); see
				 * pgbuf_get_fix_debug_count() snapshot-diff in buffile::refresh_pgbuf_fixes() */

    buffile_metrics ()
      : pages_appended (0), pages_flushed (0), flush_calls (0), pages_read (0), bytes_written (0), pgbuf_fixes (0)
    {
    }
  };

  /* tape_backing_census (orphan-scan hook) lives in the shared spill-file
   * substrate -- see qfile_spill_file.hpp. */

#if !defined (NDEBUG)
  /* ENOSPC fault injection (debug-only).  Arm to make the Nth subsequent
   * buffile::flush that actually writes fail as if the disk hit ENOSPC (er_set
   * ER_QPROC_OUT_OF_TEMP_SPACE, no pwrite).  nth <= 0 disarms.  Also armed at
   * boot from env CUBRID_WM_FAULT_FLUSH_AT for query-level repro.  Exists only
   * to mechanically exercise the close/freeze failure-propagation contract;
   * compiled out of release (NDEBUG). */
  void buffile_fault_arm_flush_fail (int nth);

  /* fd-exhaustion fault injection (debug-only).  Arm with an errno
   * (EMFILE/ENFILE) to make the next buffile::create () short-circuit its
   * open () and report that errno, so the ensure_buffile os_error -> temp-space
   * mapping can be exercised without draining the real process fd table.
   * 0 disarms.  Compiled out of release (NDEBUG). */
  void buffile_fault_arm_create_fail (int os_errno);
#endif /* !NDEBUG */

  /*
   * tde_read_scratch - per-reader cipher/plain page buffers for the re-entrant
   * read path.  A concurrent reader owns one of these so two threads reading
   * the same frozen TDE BufFile never share decrypt state.  Never allocated on
   * the plaintext path.
   */
  struct tde_read_scratch
  {
    char *cipher_raw;
    FILEIO_PAGE *cipher;	/* IO_PAGESIZE ciphertext read buffer */
    char *plain_raw;
    FILEIO_PAGE *plain;		/* IO_PAGESIZE decrypt-output buffer */

    tde_read_scratch ();
    ~tde_read_scratch ();
    int ensure ();		/* lazily allocate both buffers; NO_ERROR or error */

    tde_read_scratch (const tde_read_scratch &) = delete;
    tde_read_scratch &operator= (const tde_read_scratch &) = delete;
  };

  /*
   * buffile - one worker's private append-only temp file backing a Tape's
   * spilled pages.  Owner-only writes; addressed by page offset; pgbuf-bypassed.
   *
   * Contract: append all pages, then flush()/freeze, then read.  Reads after
   * flush serve from disk; a defensive flush precedes any read so an unflushed
   * tail is never missed.
   */
  class buffile
  {
    public:
      /* Create a private file under `dir` (created if missing).  tde_algo ==
       * TDE_ALGORITHM_NONE => plaintext; otherwise pages are encrypted on disk.
       * Returns NULL on failure and sets *os_error_out to errno (0 on success). */
      static buffile *create (THREAD_ENTRY *thread_p, const char *dir, std::uint64_t seq, unsigned int worker_id,
			      TDE_ALGORITHM tde_algo, int *os_error_out);

      /* One-shot boot sweep: wipes this server's cubrid_buffile spill subtree
       * of any files orphaned by a kill -9'd previous run.  Idempotent
       * (std::call_once); call once at server boot.  Safe to skip -- if never
       * called explicitly, default_scratch_dir() below runs it lazily on first
       * use, but calling it at boot keeps the sweep off the query hot path. */
      static void boot_sweep ();

      /* Resolve the per-server default scratch directory: unconditionally
       * the database volume directory, then <base>/cubrid_buffile/<db>/<server_id>
       * (server_id persists across restarts in a per-db marker file, so boot
       * sweeps only this server's own subtree).  No $CUBRID_TMP override, no
       * /tmp or $TMP fallback (issue #147 D-SP1) -- all of those can point
       * outside the DB volume or at tmpfs, which would defeat spilling.
       * Returns false if no disk-backed base can be formed. */
      static bool default_scratch_dir (std::string &out);

      ~buffile ();		/* closes fd and unlinks the file */

      /* Owner-only append of one DB_PAGESIZE list page.  Buffered; flushed in
       * batches.  Returns NO_ERROR or an error code. */
      int append_page (THREAD_ENTRY *thread_p, const PAGE_PTR list_page);

      /* Force any buffered pages to disk. */
      int flush (THREAD_ENTRY *thread_p);

      /* Re-entrant read of logical file page `page_offset` into `dest`
       * (DB_PAGESIZE), decrypting if TDE.  After freeze the backing is
       * immutable, so a shared fd + pread is concurrency-safe and all mutable
       * read state is caller-supplied: `scratch` MUST be non-NULL for a TDE
       * BufFile (its cipher/plain buffers carry the pread + decrypt) and is
       * ignored when plaintext.  const + safe for N concurrent readers.  Pages
       * must already be flushed (append-all-then-freeze). */
      int read_page (THREAD_ENTRY *thread_p, int page_offset, PAGE_PTR dest, tde_read_scratch *scratch) const;

      int page_count () const
      {
	return m_pages_on_disk + m_batch_pages;
      }
      bool tde_encrypted () const
      {
	return m_file.tde_encrypted ();
      }
      const std::string &path () const
      {
	return m_file.path ();
      }
      const buffile_metrics &metrics () const
      {
	return m_metrics;
      }

    private:
      buffile ();

      int ensure_write_scratch ();
      int stage_plaintext (const PAGE_PTR list_page, char *slot);
      int stage_tde (const PAGE_PTR list_page, char *slot, int page_index);
      void refresh_pgbuf_fixes ();	/* producer-side pgbuf-bypass gate */

      spill_file m_file;	/* fd/path/TDE-algo/stride + create/close+unlink+census (substrate) */
      int m_pages_on_disk;	/* pages already pwritten */

      char *m_batch_raw;	/* aligned batch write buffer (BATCH_PAGES * m_disk_pagesize) */
      char *m_batch;
      int m_batch_pages;	/* pages currently staged in m_batch (unflushed) */

      /* TDE write-staging scratch (encrypt path only; allocated lazily, NULL
       * when plaintext).  The read path keeps no member scratch -- it is
       * caller-supplied (tde_read_scratch) so N readers stay re-entrant. */
      char *m_plain_raw;
      FILEIO_PAGE *m_plain;	/* encrypt staging wrap buffer (write path only) */

      long m_pgbuf_fix_baseline;	/* pgbuf_get_fix_debug_count() at construction */

      mutable buffile_metrics m_metrics;	/* mutable: pages_read is atomic-updated from const read_page() */

      buffile (const buffile &) = delete;
      buffile &operator= (const buffile &) = delete;
  };
}				/* namespace qfile */

/*
 * In-server self-test of the BufFile write/flush/read round-trip, including the
 * TDE encrypt -> pwrite -> pread -> decrypt path which cannot run in the
 * bootless unit test (no loaded cipher).  Gated by env CUBRID_WM_BUFFILE_SELFTEST
 * in qmgr_initialize (debug-only).  Returns 0 on PASS.
 */
int qfile_buffile_selftest (THREAD_ENTRY *thread_p);

#endif /* _QFILE_BUFFILE_HPP_ */
