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

#include <unistd.h>		/* getpid */

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace
{
  /* process-wide segment_id issue; shared coordinate namespace with the
   * tuple-position raw_fd_segment_id (a stale position can never match a
   * different file: ids are never reused within a boot). */
  std::atomic<std::uint64_t> g_page_spill_seq {0};
}				/* anonymous namespace */

namespace qfile
{
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
}				/* namespace qfile */
