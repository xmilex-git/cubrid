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
 * qfile_page_spill.cpp - per-tfile random-page spill backing (Phase3 (c′),
 * issue #74/#132).  See qfile_page_spill.hpp for the invariants.
 */

#include "qfile_page_spill.hpp"

#include "error_manager.h"
#include "file_io.h"		/* FILEIO_PAGE */
#include "log_impl.h"		/* LOG_FIND_THREAD_TRAN_INDEX */
#include "system_parameter.h"	/* prm_get_integer_value / PRM_ID_TDE_DEFAULT_ALGORITHM */
#include "tde.h"

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <atomic>
#include <set>
#include <vector>

#include <unistd.h>		/* getpid */

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace
{
  /* process-wide segment_id issue; shared coordinate namespace with the
   * tuple-position spill_segment_id (a stale position can never match a
   * different file: ids are never reused within a boot). */
  std::atomic<std::uint64_t> g_page_spill_seq {0};

#if !defined (NDEBUG)
  /* #86-idiom write-back fault: 1-based ordinal of the write-back to fail;
   * 0 = disarmed. */
  std::atomic<int> g_fault_flush_target {0};
  std::atomic<int> g_fault_flush_count {0};

  bool
  fault_flush_should_fail () noexcept
  {
    const int target = g_fault_flush_target.load (std::memory_order_relaxed);
    if (target <= 0)
      {
	return false;
      }
    const int seen = g_fault_flush_count.fetch_add (1, std::memory_order_relaxed) + 1;
    return seen == target;
  }

  /* selftest-armed: an INV-4 sparse fault is expected -- skip the assert */
  std::atomic<bool> g_expect_sparse_fault {false};
#endif /* !NDEBUG */
}				/* anonymous namespace */

namespace qfile
{
#if !defined (NDEBUG)
  void
  page_spill_fault_arm_flush_fail (int nth)
  {
    g_fault_flush_count.store (0, std::memory_order_relaxed);
    g_fault_flush_target.store (nth > 0 ? nth : 0, std::memory_order_relaxed);
  }

  void
  page_spill_fault_expect_sparse (bool expected)
  {
    g_expect_sparse_fault.store (expected, std::memory_order_relaxed);
  }
#endif /* !NDEBUG */

  page_spill_file *
  page_spill_file::create (QUERY_ID query_id, int owner_tran_index, unsigned int worker_id, bool tde_encrypted,
			   int *os_error_out)
  {
    if (os_error_out != NULL)
      {
	*os_error_out = 0;
      }

    std::string dir;
    if (!spill_scratch_default_dir (dir))
      {
	if (os_error_out != NULL)
	  {
	    *os_error_out = ENOENT;	/* no disk-backed scratch base */
	  }
	return NULL;
      }

    const std::uint64_t seq = g_page_spill_seq.fetch_add (1, std::memory_order_acq_rel) + 1;
    char name[256];
    std::snprintf (name, sizeof (name), "spillpg_%llu_q%lld_t%d_p%ld.tmp", (unsigned long long) seq,
		   (long long) query_id, owner_tran_index, (long) getpid ());

    TDE_ALGORITHM tde_algo = TDE_ALGORITHM_NONE;
    if (tde_encrypted)
      {
	tde_algo = (TDE_ALGORITHM) prm_get_integer_value (PRM_ID_TDE_DEFAULT_ALGORITHM);
      }

    page_spill_file *psf = new page_spill_file ();
    if (psf == NULL)
      {
	if (os_error_out != NULL)
	  {
	    *os_error_out = ENOMEM;
	  }
	return NULL;
      }
    psf->m_seq = seq;
    (void) worker_id;		/* dispatcher context only; not part of the file name (per-tfile, not per-worker) */

    if (psf->m_file.create (dir.c_str (), name, tde_algo, os_error_out) != NO_ERROR)
      {
	delete psf;
	return NULL;
      }
    return psf;
  }

  page_spill_file::~page_spill_file ()
  {
    /* Containment ownership (D2): the QMGR_TEMP_FILE destructor path is the
     * only caller.  Any still-referenced slot is a consumer fix leak. */
    for (auto &entry : m_slots)
      {
	if (entry.second.ref > 0)
	  {
	    er_log_debug (ARG_FILE_LINE, "PAGE_SPILL: leaked fixed page %d (ref=%d) at file destroy\n",
			  entry.second.pageid, entry.second.ref);
	    assert (false);
	  }
	free (entry.first);
      }
    free (m_plain_raw);
    free (m_cipher_raw);
    /* m_file dtor closes + unlinks + census */
  }

  int
  page_spill_file::ensure_tde_scratch_locked ()
  {
    if (m_plain != NULL && m_cipher != NULL)
      {
	return NO_ERROR;
      }
    if (m_plain_raw == NULL)
      {
	m_plain_raw = static_cast<char *> (malloc ((std::size_t) IO_PAGESIZE + MAX_ALIGNMENT));
	if (m_plain_raw == NULL)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) IO_PAGESIZE);
	    return ER_OUT_OF_VIRTUAL_MEMORY;
	  }
	m_plain = reinterpret_cast<FILEIO_PAGE *> (PTR_ALIGN (m_plain_raw, MAX_ALIGNMENT));
      }
    if (m_cipher_raw == NULL)
      {
	m_cipher_raw = static_cast<char *> (malloc ((std::size_t) IO_PAGESIZE + MAX_ALIGNMENT));
	if (m_cipher_raw == NULL)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) IO_PAGESIZE);
	    return ER_OUT_OF_VIRTUAL_MEMORY;
	  }
	m_cipher = reinterpret_cast<FILEIO_PAGE *> (PTR_ALIGN (m_cipher_raw, MAX_ALIGNMENT));
      }
    return NO_ERROR;
  }

  PAGE_PTR
  page_spill_file::alloc_new_page (THREAD_ENTRY *thread_p, PAGEID pageid)
  {
    if (pageid < 0)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TEMP_FILE, 1,
		LOG_FIND_THREAD_TRAN_INDEX (thread_p));
	return NULL;
      }

    std::lock_guard<std::mutex> guard (m_mutex);

    /* dense pageids are dispatcher-issued and monotonic: a collision is a bug */
    if (m_resident.find (pageid) != m_resident.end ()
	|| (static_cast<std::size_t> (pageid) < m_written.size () && m_written[pageid]))
      {
	assert (false);
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TEMP_FILE, 1,
		LOG_FIND_THREAD_TRAN_INDEX (thread_p));
	return NULL;
      }

    PAGE_PTR buf = static_cast<PAGE_PTR> (malloc (DB_PAGESIZE));
    if (buf == NULL)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) DB_PAGESIZE);
	return NULL;
      }
    std::memset (buf, 0, DB_PAGESIZE);

    m_resident.emplace (pageid, buf);
    m_slots.emplace (buf, slot { pageid, 1, false });
    return buf;
  }

  PAGE_PTR
  page_spill_file::fix_page (THREAD_ENTRY *thread_p, PAGEID pageid)
  {
    if (pageid < 0 || !m_file.is_open ())
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TEMP_FILE, 1,
		LOG_FIND_THREAD_TRAN_INDEX (thread_p));
	return NULL;
      }

    std::lock_guard<std::mutex> guard (m_mutex);

    /* INV-1: resident-first, pointer identity (un-flushed dirty bytes are
     * immediately visible to every fixer; single lock domain makes the
     * lookup->ref++ atomic). */
    const auto res_it = m_resident.find (pageid);
    if (res_it != m_resident.end ())
      {
	const auto slot_it = m_slots.find (res_it->second);
	assert (slot_it != m_slots.end ());
	slot_it->second.ref++;
	return res_it->second;
      }

    /* INV-4: only pages that reached disk may be pread; anything else is a
     * sparse fault (the random-page equivalent of buffile's freeze check). */
    if (static_cast<std::size_t> (pageid) >= m_written.size () || !m_written[pageid])
      {
#if !defined (NDEBUG)
	assert (g_expect_sparse_fault.load (std::memory_order_relaxed));
#endif /* !NDEBUG */
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TEMP_FILE, 1,
		LOG_FIND_THREAD_TRAN_INDEX (thread_p));
	return NULL;
      }

    PAGE_PTR buf = static_cast<PAGE_PTR> (malloc (DB_PAGESIZE));
    if (buf == NULL)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) DB_PAGESIZE);
	return NULL;
      }

    if (!m_file.tde_encrypted ())
      {
	if (!m_file.pread_full (buf, DB_PAGESIZE, (off_t) pageid * (off_t) DB_PAGESIZE))
	  {
	    free (buf);
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	    return NULL;
	  }
      }
    else
      {
	if (ensure_tde_scratch_locked () != NO_ERROR)
	  {
	    free (buf);
	    return NULL;
	  }
	if (!m_file.pread_full (m_cipher, IO_PAGESIZE, (off_t) pageid * (off_t) IO_PAGESIZE))
	  {
	    free (buf);
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	    return NULL;
	  }
	if (spill_file::tde_read_decrypt (m_cipher, m_file.tde_algo (), m_plain, buf) != NO_ERROR)
	  {
	    free (buf);
	    return NULL;
	  }
      }

    m_resident.emplace (pageid, buf);
    m_slots.emplace (buf, slot { pageid, 1, false });
    return buf;
  }

  bool
  page_spill_file::mark_dirty (PAGE_PTR page_p)
  {
    std::lock_guard<std::mutex> guard (m_mutex);
    const auto slot_it = m_slots.find (page_p);
    if (slot_it == m_slots.end ())
      {
	return false;
      }
    slot_it->second.dirty = true;
    return true;
  }

  int
  page_spill_file::write_back_locked (THREAD_ENTRY *thread_p, PAGEID pageid, PAGE_PTR buf)
  {
    (void) thread_p;
    if (!m_file.is_open ())
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TEMP_FILE, 1,
		LOG_FIND_THREAD_TRAN_INDEX (thread_p));
	return ER_FAILED;
      }

#if !defined (NDEBUG)
    if (fault_flush_should_fail ())
      {
	/* simulated disk-full write-back (#86 idiom): same error the real
	 * ENOSPC path raises, exercising INV-3 end to end. */
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OUT_OF_TEMP_SPACE, 0);
	return ER_FAILED;
      }
#endif /* !NDEBUG */

    if (!m_file.tde_encrypted ())
      {
	if (!m_file.pwrite_full (buf, DB_PAGESIZE, (off_t) pageid * (off_t) DB_PAGESIZE))
	  {
	    spill_file::set_os_error (errno);
	    return ER_FAILED;
	  }
      }
    else
      {
	int rc = ensure_tde_scratch_locked ();
	if (rc != NO_ERROR)
	  {
	    return rc;
	  }
	/* fresh nonce per physical (re)write -- guaranteed by the
	 * tde_encrypt_data_page primitive (design §6) */
	rc = spill_file::tde_stage_encrypt (buf, pageid, m_file.tde_algo (), m_plain, m_cipher);
	if (rc != NO_ERROR)
	  {
	    return rc;
	  }
	if (!m_file.pwrite_full (m_cipher, IO_PAGESIZE, (off_t) pageid * (off_t) IO_PAGESIZE))
	  {
	    spill_file::set_os_error (errno);
	    return ER_FAILED;
	  }
      }

    if (static_cast<std::size_t> (pageid) >= m_written.size ())
      {
	m_written.resize (static_cast<std::size_t> (pageid) + 1, false);
      }
    m_written[pageid] = true;
    return NO_ERROR;
  }

  int
  page_spill_file::release_page (THREAD_ENTRY *thread_p, PAGE_PTR page_p)
  {
    std::lock_guard<std::mutex> guard (m_mutex);

    const auto slot_it = m_slots.find (page_p);
    if (slot_it == m_slots.end ())
      {
	/* not ours (e.g. a membuf page routed through the same call path) */
	return NO_ERROR;
      }

    if (slot_it->second.ref > 1)
      {
	slot_it->second.ref--;
	return NO_ERROR;
      }

    /* last unfix (INV-2): write back while STILL resident and under the
     * mutex, so the slot's resident -> non-resident transition is atomic and
     * a concurrent fixer can never pread a half-written image. */
    const PAGEID pageid = slot_it->second.pageid;
    int error = NO_ERROR;
    if (slot_it->second.dirty)
      {
	error = write_back_locked (thread_p, pageid, page_p);
      }

    /* clean & unreferenced -> drop the slot (resident set stays == fix set);
     * on write-back failure the slot is dropped too -- ref is 0, nobody holds
     * it, and the caller poisons the query (INV-3). */
    m_slots.erase (slot_it);
    m_resident.erase (pageid);
    free (page_p);
    return error;
  }

#if !defined (NDEBUG)
  /* ------------------------------------------------------------------ */
  /* in-server selftests (#132, design §7)                              */
  /* ------------------------------------------------------------------ */

  namespace
  {
    struct selftest_io_page
    {
      char *raw = NULL;
      FILEIO_PAGE *page = NULL;

      selftest_io_page ()
      {
	raw = static_cast<char *> (malloc ((std::size_t) IO_PAGESIZE + MAX_ALIGNMENT));
	if (raw != NULL)
	  {
	    page = reinterpret_cast<FILEIO_PAGE *> (PTR_ALIGN (raw, MAX_ALIGNMENT));
	  }
      }
      ~selftest_io_page ()
      {
	free (raw);
      }
    };

    bool
    selftest_use_tde ()
    {
      return tde_is_loaded ()
	&& (TDE_ALGORITHM) prm_get_integer_value (PRM_ID_TDE_DEFAULT_ALGORITHM) != TDE_ALGORITHM_NONE;
    }

    void
    selftest_fill_pattern (PAGE_PTR buf, PAGEID pageid)
    {
      for (int offset = 0; offset < DB_PAGESIZE; offset++)
	{
	  buf[offset] = static_cast<char> ((pageid * 131 + offset * 17 + (offset >> 3)) & 0xff);
	}
    }

    bool
    selftest_check_pattern (const PAGE_PTR buf, PAGEID pageid)
    {
      for (int offset = 0; offset < DB_PAGESIZE; offset++)
	{
	  if (buf[offset] != static_cast<char> ((pageid * 131 + offset * 17 + (offset >> 3)) & 0xff))
	    {
	      return false;
	    }
	}
      return true;
    }
  }				/* anonymous namespace */

  /* read one page image straight from disk (bypassing the cache), decrypting
   * if TDE; *nonce_out receives the on-disk nonce (0 when plaintext). */
  static int
  selftest_read_disk (qfile::spill_file &file, PAGEID pageid, PAGE_PTR dest, INT64 *nonce_out)
  {
    if (nonce_out != NULL)
      {
	*nonce_out = 0;
      }
    if (!file.tde_encrypted ())
      {
	return file.pread_full (dest, DB_PAGESIZE, (off_t) pageid * (off_t) DB_PAGESIZE) ? NO_ERROR : ER_FAILED;
      }

    selftest_io_page cipher;
    selftest_io_page plain;
    if (cipher.page == NULL || plain.page == NULL
	|| !file.pread_full (cipher.page, IO_PAGESIZE, (off_t) pageid * (off_t) IO_PAGESIZE))
      {
	return ER_FAILED;
      }
    if (spill_file::tde_read_decrypt (cipher.page, file.tde_algo (), plain.page, dest) != NO_ERROR)
      {
	return ER_FAILED;
      }
    if (nonce_out != NULL)
      {
	*nonce_out = cipher.page->prv.tde_nonce;
      }
    return NO_ERROR;
  }

  /*
   * CUBRID_WM_SPILL_SELFTEST: 257-page random-order write/read parity +
   * INV-4 sparse fault + TDE nonce distinctness incl. rewrite freshness
   * (the rawfd positioned-read-parity port, design §6/§7).
   */
  int
  page_spill_file::selftest (THREAD_ENTRY *thread_p)
  {
    const bool tde = selftest_use_tde ();
    int os_error = 0;
    page_spill_file *psf = create (static_cast<QUERY_ID> (-8), 0, 0, tde, &os_error);
    if (psf == NULL)
      {
	return ER_FAILED;
      }

    constexpr PAGEID PAGE_COUNT = 257;
    int rc = NO_ERROR;
    std::vector<PAGE_PTR> bufs (PAGE_COUNT, NULL);

    /* produce all pages resident, then release (= write-back) in a permuted
     * order: write order != pageid order is a §3 invariant. */
    for (PAGEID page = 0; page < PAGE_COUNT && rc == NO_ERROR; page++)
      {
	bufs[page] = psf->alloc_new_page (thread_p, page);
	if (bufs[page] == NULL)
	  {
	    rc = ER_FAILED;
	    break;
	  }
	selftest_fill_pattern (bufs[page], page);
	if (!psf->mark_dirty (bufs[page]))
	  {
	    rc = ER_FAILED;
	  }
      }
    for (PAGEID step = 0; step < PAGE_COUNT && rc == NO_ERROR; step++)
      {
	const PAGEID page = static_cast<PAGEID> ((step * 73) % PAGE_COUNT);
	rc = psf->release_page (thread_p, bufs[page]);
      }

    /* random-order re-fix parity */
    for (PAGEID step = 0; step < PAGE_COUNT && rc == NO_ERROR; step++)
      {
	const PAGEID page = static_cast<PAGEID> ((step * 101) % PAGE_COUNT);
	PAGE_PTR fixed = psf->fix_page (thread_p, page);
	if (fixed == NULL || !selftest_check_pattern (fixed, page))
	  {
	    rc = ER_FAILED;
	  }
	if (fixed != NULL && psf->release_page (thread_p, fixed) != NO_ERROR)
	  {
	    rc = ER_FAILED;
	  }
      }

    /* INV-4 sparse fault: an unwritten pageid must fault, not read zeroes */
    if (rc == NO_ERROR)
      {
	page_spill_fault_expect_sparse (true);
	PAGE_PTR faulted = psf->fix_page (thread_p, PAGE_COUNT + 5);
	page_spill_fault_expect_sparse (false);
	if (faulted != NULL || er_errid () != ER_QPROC_INVALID_TEMP_FILE)
	  {
	    rc = ER_FAILED;
	  }
      }

    /* TDE: every physical write took a fresh nonce -> 257 distinct; an
     * in-place rewrite takes yet another fresh one (mutation contract, §6) */
    if (rc == NO_ERROR && tde)
      {
	std::set<INT64> seen_nonces;
	std::vector<char> disk (DB_PAGESIZE);
	INT64 nonce = 0;
	for (PAGEID page = 0; page < PAGE_COUNT && rc == NO_ERROR; page++)
	  {
	    rc = selftest_read_disk (psf->m_file, page, disk.data (), &nonce);
	    if (rc == NO_ERROR && (nonce == 0 || !selftest_check_pattern (disk.data (), page)))
	      {
		rc = ER_FAILED;
	      }
	    seen_nonces.insert (nonce);
	  }
	if (rc == NO_ERROR && seen_nonces.size () != static_cast<std::size_t> (PAGE_COUNT))
	  {
	    rc = ER_FAILED;
	  }

	if (rc == NO_ERROR)
	  {
	    INT64 nonce_before = 0;
	    INT64 nonce_after = 0;
	    rc = selftest_read_disk (psf->m_file, 0, disk.data (), &nonce_before);
	    PAGE_PTR fixed = (rc == NO_ERROR) ? psf->fix_page (thread_p, 0) : NULL;
	    if (fixed == NULL)
	      {
		rc = ER_FAILED;
	      }
	    else
	      {
		fixed[0] = static_cast<char> (fixed[0] ^ 0x5a);
		psf->mark_dirty (fixed);
		rc = psf->release_page (thread_p, fixed);
		if (rc == NO_ERROR)
		  {
		    rc = selftest_read_disk (psf->m_file, 0, disk.data (), &nonce_after);
		  }
		if (rc == NO_ERROR
		    && (nonce_after == 0 || nonce_after == nonce_before
			|| disk[0] != static_cast<char> (((0 * 131 + 0 * 17 + 0) & 0xff) ^ 0x5a)))
		  {
		    rc = ER_FAILED;
		  }
	      }
	  }
      }

    delete psf;
    return rc;
  }

  /*
   * CUBRID_WM_SPILL_COHERENCE_SELFTEST: direct evidence for INV-1 (pointer
   * identity + ref while dirty), INV-2 (last-unfix write-back; stale disk
   * image while resident), INV-3 (write-back failure propagates), plus TDE
   * rewrite nonce freshness across the dirty cycle.
   */
  int
  page_spill_file::coherence_selftest (THREAD_ENTRY *thread_p)
  {
    const bool tde = selftest_use_tde ();
    int os_error = 0;
    page_spill_file *psf = create (static_cast<QUERY_ID> (-9), 0, 0, tde, &os_error);
    if (psf == NULL)
      {
	return ER_FAILED;
      }

    int rc = NO_ERROR;
    std::vector<char> disk (DB_PAGESIZE);
    INT64 nonce_first = 0;
    INT64 nonce_second = 0;

    auto ref_of = [psf] (PAGE_PTR buf) -> int
    {
      std::lock_guard<std::mutex> guard (psf->m_mutex);
      const auto it = psf->m_slots.find (buf);
      return (it == psf->m_slots.end ()) ? -1 : it->second.ref;
    };
    auto written = [psf] (PAGEID pageid) -> bool
    {
      std::lock_guard<std::mutex> guard (psf->m_mutex);
      return static_cast<std::size_t> (pageid) < psf->m_written.size () && psf->m_written[pageid];
    };

    /* INV-1: dirty 보유 중 재fix = 동일 buf, ref==2 */
    PAGE_PTR first = psf->alloc_new_page (thread_p, 0);
    if (first == NULL)
      {
	rc = ER_FAILED;
      }
    else
      {
	selftest_fill_pattern (first, 0);
	psf->mark_dirty (first);
	PAGE_PTR refixed = psf->fix_page (thread_p, 0);
	if (refixed != first || ref_of (first) != 2)
	  {
	    rc = ER_FAILED;
	  }
	/* INV-2: 첫 unfix는 write-back하지 않는다 (dirty ⊆ fixed 유지) */
	if (rc == NO_ERROR && (psf->release_page (thread_p, refixed) != NO_ERROR || written (0)))
	  {
	    rc = ER_FAILED;
	  }
	/* 마지막 unfix에서만 write-back */
	if (rc == NO_ERROR && (psf->release_page (thread_p, first) != NO_ERROR || !written (0)))
	  {
	    rc = ER_FAILED;
	  }
	if (rc == NO_ERROR)
	  {
	    rc = selftest_read_disk (psf->m_file, 0, disk.data (), &nonce_first);
	    if (rc == NO_ERROR && !selftest_check_pattern (disk.data (), 0))
	      {
		rc = ER_FAILED;
	      }
	  }
      }

    /* INV-2: DONT_FREE dirty 동안 디스크 이미지는 stale, fix는 신본을 본다 */
    if (rc == NO_ERROR)
      {
	PAGE_PTR fixed = psf->fix_page (thread_p, 0);
	if (fixed == NULL)
	  {
	    rc = ER_FAILED;
	  }
	else
	  {
	    fixed[1] = static_cast<char> (fixed[1] ^ 0x77);	/* mutate */
	    psf->mark_dirty (fixed);

	    /* disk must still hold the OLD image... */
	    std::vector<char> stale (DB_PAGESIZE);
	    if (selftest_read_disk (psf->m_file, 0, stale.data (), NULL) != NO_ERROR
		|| !selftest_check_pattern (stale.data (), 0))
	      {
		rc = ER_FAILED;
	      }
	    /* ...while a concurrent fixer sees the NEW bytes by pointer identity */
	    PAGE_PTR observer = psf->fix_page (thread_p, 0);
	    if (observer != fixed || observer[1] != fixed[1])
	      {
		rc = ER_FAILED;
	      }
	    if (observer != NULL && psf->release_page (thread_p, observer) != NO_ERROR)
	      {
		rc = ER_FAILED;
	      }
	    if (psf->release_page (thread_p, fixed) != NO_ERROR)	/* last unfix -> write-back */
	      {
		rc = ER_FAILED;
	      }
	    if (rc == NO_ERROR)
	      {
		if (selftest_read_disk (psf->m_file, 0, disk.data (), &nonce_second) != NO_ERROR
		    || disk[1] != static_cast<char> (((0 * 131 + 1 * 17 + 0) & 0xff) ^ 0x77))
		  {
		    rc = ER_FAILED;
		  }
		if (rc == NO_ERROR && tde && (nonce_second == 0 || nonce_second == nonce_first))
		  {
		    rc = ER_FAILED;
		  }
	      }
	  }
      }

    /* INV-3: write-back 실패 주입 -> 오류 전파 (#86 관용구) */
    if (rc == NO_ERROR)
      {
	PAGE_PTR fixed = psf->fix_page (thread_p, 0);
	if (fixed == NULL)
	  {
	    rc = ER_FAILED;
	  }
	else
	  {
	    fixed[2] = static_cast<char> (fixed[2] ^ 0x11);
	    psf->mark_dirty (fixed);
	    page_spill_fault_arm_flush_fail (1);
	    const int release_rc = psf->release_page (thread_p, fixed);
	    page_spill_fault_arm_flush_fail (0);
	    if (release_rc == NO_ERROR || er_errid () != ER_QPROC_OUT_OF_TEMP_SPACE || ref_of (fixed) != -1)
	      {
		rc = ER_FAILED;
	      }
	  }
      }

    delete psf;
    return rc;
  }
#endif /* !NDEBUG */
}				/* namespace qfile */
