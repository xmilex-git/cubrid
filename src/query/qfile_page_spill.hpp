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
 * qfile_page_spill.hpp - per-tfile random-page spill backing.
 *
 * One page_spill_file per QMGR_TEMP_FILE (lazy, created at the tfile's first
 * spill), owned by CONTAINMENT (the QMGR_TEMP_FILE member pointer is the only
 * owner; no global registry, no reaper: crash leftovers are reclaimed by the
 * shared boot sweep, the file lives in the same cubrid_buffile tree as buffile
 * with a `spillpg_` name prefix).  Pages are addressed by the dispatcher's
 * dense pageid (membuf_last+1 ...) at byte offset pageid * stride.
 *
 * Cache + coherence invariants (each preserves the existing consumer contract,
 * so consumers are unmodified):
 *   INV-1 (visibility, resident-first): fix () returns the SAME buffer
 *     pointer for a resident page (ref++), so a writer's un-flushed dirty
 *     bytes are visible to every subsequent fixer by pointer identity.
 *   INV-2 (flush point): mark_dirty () only sets a flag; write-back happens
 *     at the LAST unfix only, under the per-tfile mutex (positional pwrite,
 *     fresh TDE nonce per physical write).  A resident slot becomes
 *     non-resident only after its write-back completed, so a pread can never
 *     observe a half-written image.
 *   INV-3 (errors): the last-unfix write-back failure returns an error and
 *     the caller poisons the query (existing qmgr contract).
 *   INV-4 (sparse): write order != pageid order is allowed (the file is
 *     inherently sparse -- pageids 0..membuf_last are served by the permanent
 *     membuf).  A fix of a page that is neither resident nor written is a
 *     FAULT: debug assert + ER_QPROC_INVALID_TEMP_FILE + NULL (the
 *     random-page equivalent of buffile's freeze check -- it turns silent
 *     zero-page corruption into noise).
 *
 * Locking: ONE std::mutex per file.  The guard system keeps concurrent
 * same-tfile fixers at 0~1 threads (serial sort / connect-by copy /
 * materialize), so the lock is uncontended; a single lock domain makes
 * lookup->refcount atomic.  There is NO global read cache -- the OS page cache
 * absorbs re-reads of the unlinked temp file.
 */

#ifndef _QFILE_PAGE_SPILL_HPP_
#define _QFILE_PAGE_SPILL_HPP_

#include "qfile_spill_file.hpp"
#include "storage_common.h"	/* PAGE_PTR / PAGEID / DB_PAGESIZE */
#include "thread_compat.hpp"	/* THREAD_ENTRY */

#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

struct fileio_page;
typedef struct fileio_page FILEIO_PAGE;

namespace qfile
{
#if !defined (NDEBUG)
  /* write-back fault injection (debug-only): the Nth subsequent page_spill
   * write-back fails as if the disk hit ENOSPC (er_set
   * ER_QPROC_OUT_OF_TEMP_SPACE, no pwrite).  nth <= 0 disarms. */
  void page_spill_fault_arm_flush_fail (int nth);

  /* sparse-fault expectation (selftest-only): while armed, the INV-4 fault
   * keeps its er_set + NULL contract but skips the debug assert so the
   * selftest can exercise it without aborting boot. */
  void page_spill_fault_expect_sparse (bool expected);
#endif /* !NDEBUG */

  class page_spill_file
  {
    public:
      /* Create the backing file under the shared spill scratch tree
       * (spillpg_<seq>_q<qid>_t<tran>_p<pid>.tmp).  Never er_sets: on failure
       * returns NULL with *os_error_out = errno-class error; the caller maps
       * it once (spill_file::set_os_error).  m_seq comes from a process-wide
       * atomic counter and doubles as the tuple-position segment_id (stale
       * cross-file coordinates can never match another file's id). */
      static page_spill_file *create (QUERY_ID query_id, int owner_tran_index, unsigned int worker_id,
				      bool tde_encrypted, int *os_error_out);

      /* Closes + unlinks the file (census-tracked).  Remaining resident slots
       * are freed; a slot still referenced is a fix leak -- er_log + debug
       * assert. */
      ~page_spill_file ();

      std::uint64_t segment_id () const
      {
	return m_seq;
      }

      /* Allocate a brand-new zeroed page for `pageid` (dense, dispatcher
       * issued): enters the cache with ref=1.  The caller stamps the qfile
       * page header.  A pageid that is already resident or written is a
       * dispatcher bug (assert + NULL). */
      PAGE_PTR alloc_new_page (THREAD_ENTRY *thread_p, PAGEID pageid);

      /* INV-1 fix: resident -> same pointer, ref++; else written -> pread
       * (+TDE decrypt) into a fresh buffer, ref=1; else INV-4 sparse fault. */
      PAGE_PTR fix_page (THREAD_ENTRY *thread_p, PAGEID pageid);

      /* INV-2 set_dirty(DONT_FREE): flag only.  false when page_p is not a
       * resident slot of this file (e.g. a membuf page -- caller no-ops). */
      bool mark_dirty (PAGE_PTR page_p);

      /* Unfix: ref--.  On the LAST unfix, write back if dirty (mutex held;
       * fresh TDE nonce), then drop the now clean+unreferenced slot.  A
       * write-back failure frees the slot and returns the error (INV-3).
       * Unknown page_p is a silent NO_ERROR (membuf pages share this call
       * path).  */
      int release_page (THREAD_ENTRY *thread_p, PAGE_PTR page_p);

      page_spill_file (const page_spill_file &) = delete;
      page_spill_file &operator= (const page_spill_file &) = delete;

#if !defined (NDEBUG)
      /* In-server selftests (env-gated in qmgr_initialize, debug-only).
       * Member functions for full access to the cache internals.  Return 0
       * on PASS. */
      static int selftest (THREAD_ENTRY *thread_p);	/* CUBRID_WM_SPILL_SELFTEST: 257-page random-order
							 * parity + sparse fault + TDE nonce distinctness */
      static int coherence_selftest (THREAD_ENTRY *thread_p);	/* CUBRID_WM_SPILL_COHERENCE_SELFTEST:
								 * INV-1~3 direct evidence + flush-fault injection */
#endif /* !NDEBUG */

    private:
      page_spill_file () = default;

      /* write one resident page to disk (m_mutex held); sets m_written */
      int write_back_locked (THREAD_ENTRY *thread_p, PAGEID pageid, PAGE_PTR buf);
      int ensure_tde_scratch_locked ();

      struct slot
      {
	PAGEID pageid;
	int ref;
	bool dirty;
      };

      spill_file m_file;
      std::uint64_t m_seq = 0;	/* segment_id (global atomic issue) */
      std::mutex m_mutex;	/* per-tfile single lock */
      std::unordered_map<PAGEID, PAGE_PTR> m_resident;	/* pageid -> shared buffer (INV-1) */
      std::unordered_map<PAGE_PTR, slot> m_slots;	/* buffer -> {pageid, ref, dirty}; file-local,
							 * so cross-file pointer aliasing cannot occur */
      std::vector<bool> m_written;	/* pageid -> reached disk at least once (INV-4) */

      /* TDE write/read scratch; only touched under m_mutex, so members are safe */
      char *m_plain_raw = NULL;
      FILEIO_PAGE *m_plain = NULL;
      char *m_cipher_raw = NULL;
      FILEIO_PAGE *m_cipher = NULL;
  };
}				/* namespace qfile */

#endif /* _QFILE_PAGE_SPILL_HPP_ */
