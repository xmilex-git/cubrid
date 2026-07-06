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
 * qfile_tape.cpp - Tape/Tapeset scan implementation.
 * See qfile_tape.hpp for the design rationale.
 */

#include "qfile_tape.hpp"

#include "error_manager.h"
#include "memory_alloc.h"
#include "object_representation.h"	/* OR_GET_INT used by the QFILE_GET_* page macros */
#include "list_file.h"		/* qfile_copy_list_id / qfile_clear_list_id / QFILE_MOVE_DEPENDENT */
#include "page_buffer.h"	/* pgbuf_get_fix_debug_count */
#include "system_parameter.h"	/* prm_get_integer_value / PRM_ID_TDE_DEFAULT_ALGORITHM */
#include "file_io.h"		/* PEEK */
#include "query_workmem.hpp"	/* work_mem accountant: reserve_held / release_held */

#include <cassert>
#include <cerrno>		/* ENOSPC/EDQUOT (ensure_buffile os_error mapping) */
#include <cstdlib>
#include <cstring>
#include <sys/stat.h>		/* stat (orphan-zero on-disk check) */
#include <thread>		/* N-reader concurrent selftest */
#include <algorithm>		/* std::sort (coverage check) */
#include <atomic>		/* process-unique producer BufFile sequence */

#include "memory_wrapper.hpp"

namespace qfile
{
#if !defined (NDEBUG)
  namespace
  {
    /* freeze() allocation fault injection (debug-only).  Simulates the
     * SERVER_MODE noexcept-new returning NULL at a tape allocation without
     * exhausting real memory, so the OOM ownership-recovery path is exercised
     * deterministically.  g_fault_alloc_target is the 1-based ordinal of the
     * tape allocation to fail (0 = disarmed); g_fault_alloc_count counts tape
     * allocations seen since the last arm. */
    std::atomic<int> g_fault_alloc_target {0};
    std::atomic<int> g_fault_alloc_count {0};

    bool
    fault_alloc_should_fail () noexcept
    {
      /* one-time arm from env for query-level repro; the self-test arms
       * directly (env unset), leaving its arming untouched. */
      static const bool env_armed = [] ()
      {
	const char *e = getenv ("CUBRID_WM_FAULT_ALLOC_AT");
	if (e != NULL)
	  {
	    int n = atoi (e);
	    if (n > 0)
	      {
		g_fault_alloc_target.store (n, std::memory_order_relaxed);
		g_fault_alloc_count.store (0, std::memory_order_relaxed);
		return true;
	      }
	  }
	return false;
      } ();
      (void) env_armed;

      const int target = g_fault_alloc_target.load (std::memory_order_relaxed);
      if (target <= 0)
	{
	  return false;
	}
      const int seen = g_fault_alloc_count.fetch_add (1, std::memory_order_relaxed) + 1;
      return seen == target;
    }
  }				/* anonymous namespace */

  void
  tape_fault_arm_alloc_fail (int nth)
  {
    g_fault_alloc_count.store (0, std::memory_order_relaxed);
    g_fault_alloc_target.store (nth > 0 ? nth : 0, std::memory_order_relaxed);
  }
#endif /* !NDEBUG */

  /* ------------------------------------------------------------------ */
  /* tape (work_mem charge lifetime)                                    */
  /* ------------------------------------------------------------------ */

  tape::~tape ()
  {
    for (const std::pair<std::size_t, int> &charge : m_wm_charges)
      {
	temp_page_store::release_held (charge.first, charge.second);
      }
  }

  void
  tape::adopt_wm_charges (std::vector<std::pair<std::size_t, int>> &&charges)
  {
    assert (m_wm_charges.empty ());
    m_wm_charges = std::move (charges);
  }

  /* ------------------------------------------------------------------ */
  /* memory_tape                                                        */
  /* ------------------------------------------------------------------ */

  memory_tape::memory_tape ()
    : m_pages (), m_owns (false)
  {
  }

  memory_tape::memory_tape (bool owns)
    : m_pages (), m_owns (owns)
  {
  }

  memory_tape::~memory_tape ()
  {
    if (m_owns)
      {
	tape_backing_census_prefix_removed ((long) m_pages.size ());
	for (char *page : m_pages)
	  {
	    free (page);
	  }
      }
    m_pages.clear ();
  }

  void
  memory_tape::append_page (char *page)
  {
    m_pages.push_back (page);
    if (m_owns)
      {
	tape_backing_census_prefix_added (1);
      }
  }

  PAGE_PTR
  memory_tape::read_page_into (THREAD_ENTRY *thread_p, int page_offset, char *page_dest, tde_read_scratch *tde) const
  {
    /* All pages are RAM; return them directly (caller scratch unused). */
    (void) thread_p;
    (void) page_dest;
    (void) tde;
    if (page_offset < 0 || page_offset >= (int) m_pages.size ())
      {
	return NULL;
      }
    return (PAGE_PTR) m_pages[page_offset];
  }

  /* ------------------------------------------------------------------ */
  /* buffile_tape                                                       */
  /* ------------------------------------------------------------------ */

  buffile_tape::buffile_tape (std::vector<char *> &&prefix_pages, bool owns_prefix, buffile *bf, bool owns_buffile)
    : m_prefix (std::move (prefix_pages))
    , m_owns_prefix (owns_prefix)
    , m_buffile (bf)
    , m_owns_buffile (owns_buffile)
  {
    if (m_owns_prefix)
      {
	tape_backing_census_prefix_added ((long) m_prefix.size ());
      }
  }

  buffile_tape::~buffile_tape ()
  {
    if (m_owns_prefix)
      {
	tape_backing_census_prefix_removed ((long) m_prefix.size ());
	for (char *page : m_prefix)
	  {
	    free (page);
	  }
      }
    m_prefix.clear ();
    if (m_owns_buffile)
      {
	delete m_buffile;	/* closes + unlinks */
      }
    m_buffile = NULL;
  }

  PAGE_PTR
  buffile_tape::read_page_into (THREAD_ENTRY *thread_p, int page_offset, char *page_dest, tde_read_scratch *tde) const
  {
    const int prefix = (int) m_prefix.size ();
    if (page_offset < 0 || page_offset >= total_page_count ())
      {
	return NULL;
      }
    if (page_offset < prefix)
      {
	return (PAGE_PTR) m_prefix[page_offset];	/* RAM prefix: direct, scratch unused */
      }
    if (m_buffile == NULL || page_dest == NULL)
      {
	/* a spilled offset with no backing / no caller scratch is a caller bug,
	 * not a silent S_END */
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	return NULL;
      }
    if (m_buffile->read_page (thread_p, page_offset - prefix, (PAGE_PTR) page_dest, tde) != NO_ERROR)
      {
	return NULL;
      }
    return (PAGE_PTR) page_dest;
  }

  int
  buffile_tape::total_page_count () const
  {
    return (int) m_prefix.size () + (m_buffile != NULL ? m_buffile->page_count () : 0);
  }

  /* ------------------------------------------------------------------ */
  /* tape_writer (membuf producer)                                      */
  /* ------------------------------------------------------------------ */

  tape_writer::tape_writer (int prefix_budget_pages, TDE_ALGORITHM tde_algo, const std::string &dir,
			    std::uint64_t seq, unsigned int worker_id)
    : m_prefix_budget (prefix_budget_pages < 0 ? 0 : prefix_budget_pages)
    , m_tde_algo (tde_algo)
    , m_dir (dir)
    , m_seq (seq)
    , m_worker_id (worker_id)
    , m_prefix ()
    , m_buffile (NULL)
    , m_frozen (false)
    , m_failed (false)
    , m_wm_charges ()
    , m_wm_reserved_pages (0)
  {
  }

  tape_writer::~tape_writer ()
  {
    /* Not frozen (error path): the writer still owns prefix pages + BufFile. */
    for (char *page : m_prefix)
      {
	free (page);
      }
    m_prefix.clear ();
    wm_release_all ();
    if (m_buffile != NULL)
      {
	delete m_buffile;	/* closes + unlinks the partial spill */
	m_buffile = NULL;
      }
  }

  /* Prefix pages are charged to the work_mem accountant in batches (one
   * atomic reservation per WM_PREFIX_RESERVE_BATCH_PAGES pages, not per
   * page).  A failed reservation is a soft degrade, never a hard OOM: the
   * prefix budget shrinks to what is already reserved and subsequent pages
   * spill to the BufFile early. */
  static const int WM_PREFIX_RESERVE_BATCH_PAGES = 64;

  bool
  tape_writer::wm_reserve_batch ()
  {
    int want = m_prefix_budget - m_wm_reserved_pages;
    if (want > WM_PREFIX_RESERVE_BATCH_PAGES)
      {
	want = WM_PREFIX_RESERVE_BATCH_PAGES;
      }
    if (want <= 0)
      {
	return false;
      }
    const std::size_t bytes = (std::size_t) want * DB_PAGESIZE;
    int shard = -1;
    if (!temp_page_store::reserve_held (bytes, &shard))
      {
	/* cap reached: shrink the budget to what is reserved so the writer
	 * spills from here on (degrade once, no per-page retry). */
	m_prefix_budget = (int) m_prefix.size ();
	/* #146 T3 S4: record_degrade() retired -- this is a layer-2 cap
	 * rejection (reserve_held failed), i.e. cap pressure. */
	temp_page_store::record_cap_pressure_spill ();
	return false;
      }
    m_wm_charges.emplace_back (bytes, shard);
    m_wm_reserved_pages += want;
    return true;
  }

  void
  tape_writer::wm_trim_excess ()
  {
    /* Give back the unused tail of the last batch so steady-state accounting
     * is exact (reserved == actual prefix pages). */
    int excess = m_wm_reserved_pages - (int) m_prefix.size ();
    if (excess <= 0 || m_wm_charges.empty ())
      {
	return;
      }
    std::size_t bytes = (std::size_t) excess * DB_PAGESIZE;
    if (bytes > m_wm_charges.back ().first)
      {
	bytes = m_wm_charges.back ().first;	/* excess never spans batches */
      }
    temp_page_store::release_held (bytes, m_wm_charges.back ().second);
    m_wm_charges.back ().first -= bytes;
    m_wm_reserved_pages = (int) m_prefix.size ();
    if (m_wm_charges.back ().first == 0)
      {
	m_wm_charges.pop_back ();
      }
  }

  void
  tape_writer::wm_release_all ()
  {
    for (const std::pair<std::size_t, int> &charge : m_wm_charges)
      {
	temp_page_store::release_held (charge.first, charge.second);
      }
    m_wm_charges.clear ();
    m_wm_reserved_pages = 0;
  }

  int
  tape_writer::ensure_buffile (THREAD_ENTRY *thread_p)
  {
    if (m_buffile != NULL)
      {
	return NO_ERROR;
      }
    int os_error = 0;
    m_buffile = buffile::create (thread_p, m_dir.c_str (), m_seq, m_worker_id, m_tde_algo, &os_error);
    if (m_buffile == NULL)
      {
	/* fd exhaustion (EMFILE/ENFILE) is diagnosed as out-of-temp-space, not
	 * a generic ER_FAILED, so fd starvation surfaces as an actionable
	 * error. */
	spill_file::set_os_error (os_error);
	return ER_FAILED;
      }
    return NO_ERROR;
  }

  int
  tape_writer::append_page (THREAD_ENTRY *thread_p, const PAGE_PTR list_page)
  {
    /* Single latch point: any failed append -- lost prefix page, spill flush
     * ENOSPC, buffile-create error -- sets the sticky flag so freeze () cannot
     * later hand back a silently truncated Tape.  Only the error path writes
     * the flag, so the steady-state append costs nothing extra. */
    const int rc = append_page_impl (thread_p, list_page);
    if (rc != NO_ERROR)
      {
	m_failed = true;
      }
    return rc;
  }

  int
  tape_writer::append_page_impl (THREAD_ENTRY *thread_p, const PAGE_PTR list_page)
  {
    if (m_frozen || list_page == NULL)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	return ER_FAILED;
      }

    if ((int) m_prefix.size () < m_prefix_budget
	&& ((int) m_prefix.size () < m_wm_reserved_pages || wm_reserve_batch ()))
      {
	char *copy = (char *) malloc (DB_PAGESIZE);
	if (copy == NULL)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) DB_PAGESIZE);
	    return ER_OUT_OF_VIRTUAL_MEMORY;
	  }
	std::memcpy (copy, list_page, DB_PAGESIZE);
	m_prefix.push_back (copy);
	return NO_ERROR;
      }

    int rc = ensure_buffile (thread_p);
    if (rc != NO_ERROR)
      {
	return rc;
      }
    return m_buffile->append_page (thread_p, list_page);
  }

  tape *
  tape_writer::freeze (THREAD_ENTRY *thread_p)
  {
    if (m_frozen)
      {
	return NULL;
      }

    /* Sticky-error gate: a prior append lost a page, so any Tape built here
     * would be silently short.  Refuse -- return NULL before touching
     * ownership so the caller's teardown (delete w / ~tape_writer) reclaims
     * the partial spill exactly as on a freeze-flush failure. */
    if (m_failed)
      {
	return NULL;
      }

    wm_trim_excess ();

    if (m_buffile == NULL)
      {
	/* tiny / no spill: an all-RAM Tape, no disk touch. */
	memory_tape *mt;
#if !defined (NDEBUG)
	if (fault_alloc_should_fail ())
	  {
	    mt = NULL;		/* simulate noexcept-new OOM */
	  }
	else
#endif
	  {
	    mt = new memory_tape (true);
	  }
	if (mt == NULL)
	  {
	    /* SERVER_MODE new is noexcept and returns NULL on OOM: the prefix
	     * pages are still ours and untouched.  Latch + return NULL WITHOUT
	     * transferring ownership -- caller's delete w -> ~tape_writer frees
	     * the prefix.  (No NULL-deref, no lost pages.) */
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (memory_tape));
	    m_failed = true;
	    return NULL;
	  }
	for (char *page : m_prefix)
	  {
	    mt->append_page (page);
	  }
	m_prefix.clear ();	/* ownership transferred to the memory_tape */
	mt->adopt_wm_charges (std::move (m_wm_charges));
	m_wm_charges.clear ();
	m_wm_reserved_pages = 0;
	m_frozen = true;
	return mt;
      }

    if (m_buffile->flush (thread_p) != NO_ERROR)
      {
	return NULL;
      }
    buffile *bf = m_buffile;
    buffile_tape *bt;
#if !defined (NDEBUG)
    if (fault_alloc_should_fail ())
      {
	bt = NULL;		/* simulate noexcept-new OOM */
      }
    else
#endif
      {
	bt = new buffile_tape (std::move (m_prefix), true, bf, true);
      }
    if (bt == NULL)
      {
	/* SERVER_MODE new is noexcept and returns NULL on OOM: the ctor never
	 * ran, so m_prefix was NOT moved and m_buffile is still ours.  Latch +
	 * return NULL BEFORE touching ownership -- caller's delete w ->
	 * ~tape_writer frees the prefix and closes/unlinks the spill file.
	 * (No NULL-deref, no orphaned fd/file, no lost pages.) */
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (buffile_tape));
	m_failed = true;
	return NULL;
      }
    m_buffile = NULL;		/* allocation succeeded: ownership is now the Tape's
				 * (NULL-check above keeps bf owned by the writer on OOM) */
    m_prefix.clear ();
    bt->adopt_wm_charges (std::move (m_wm_charges));
    m_wm_charges.clear ();
    m_wm_reserved_pages = 0;
    m_frozen = true;
    return bt;
  }

  int
  tape_writer::file_pages () const
  {
    return m_buffile != NULL ? m_buffile->page_count () : 0;
  }

  const buffile_metrics *
  tape_writer::file_metrics () const
  {
    return m_buffile != NULL ? &m_buffile->metrics () : NULL;
  }

  /* ------------------------------------------------------------------ */
  /* tapeset                                                            */
  /* ------------------------------------------------------------------ */

  tapeset::tapeset ()
    : m_tapes (), m_owns_tapes (false), m_open_scan_cell (std::make_shared<std::atomic<int>> (0))
  {
  }

  tapeset::~tapeset ()
  {
    if (m_owns_tapes)
      {
	for (tape *tape_p : m_tapes)
	  {
	    delete tape_p;
	  }
      }
    m_tapes.clear ();
  }

  void
  tapeset::append_tape (tape *tape_p)
  {
    m_tapes.push_back (tape_p);
  }

  void
  tapeset::transfer_tapes_from (tapeset *src)
  {
    if (src == NULL || src == this)
      {
	return;
      }
    const size_t n = src->m_tapes.size ();
    m_tapes.reserve (m_tapes.size () + n);	/* may throw; nothing moved yet */
    for (size_t i = 0; i < n; i++)
      {
	tape *tape_p = src->m_tapes[i];
	src->m_tapes[i] = nullptr;	/* transfer ownership immediately */
	if (tape_p != NULL)
	  {
	    m_tapes.push_back (tape_p);	/* reserve()'d above: cannot throw/realloc */
	  }
      }
    src->m_tapes.clear ();
  }

  /* ------------------------------------------------------------------ */
  /* tapeset_scan                                                       */
  /* ------------------------------------------------------------------ */

  /* Scan-side pgbuf-bypass gate: tapeset_scan/tapeset_reader read pages only
   * via tape::read_page_into and must never fix a pgbuf BCB.  Snapshot-diff
   * the boot-independent debug counter to measure it. */
  static void
  refresh_pgbuf_fixes (tapeset_scan_metrics &metrics, long baseline)
  {
#if !defined (NDEBUG)
    metrics.pgbuf_fixes = pgbuf_get_fix_debug_count () - baseline;
#endif /* !NDEBUG */
  }

  tapeset_scan::tapeset_scan (tapeset *ts)
    : m_tapeset (ts)
    , m_position (S_BEFORE)
    , m_status (S_OPENED)
    , m_tape_idx (-1)
    , m_page_offset (-1)
    , m_page (NULL)
    , m_offset (0)
    , m_tplno (0)
    , m_curr_tpl (NULL)
    , m_curr_overflow (false)
    , m_overflow_run_end (-1)
    , m_cache_tick (0)
    , m_reasm_raw (NULL)
    , m_reasm (NULL)
    , m_peek_reasm_raw (NULL)
    , m_peek_reasm_cap (0)
#if !defined (NDEBUG)
    , m_pgbuf_fix_baseline (pgbuf_get_fix_debug_count ())
#else /* NDEBUG */
    , m_pgbuf_fix_baseline (0)
#endif /* NDEBUG */
    , m_open_scan_cell (ts ? ts->open_scan_cell () : nullptr)
  {
    for (int i = 0; i < READ_CACHE_SLOTS; i++)
      {
	m_cache[i].buf = NULL;
	m_cache[i].tape_p = NULL;
	m_cache[i].page_offset = -1;
	m_cache[i].valid = false;
	m_cache[i].tick = 0;
      }
    if (m_open_scan_cell)
      {
	m_open_scan_cell->fetch_add (1, std::memory_order_relaxed);
      }
  }

  tapeset_scan::~tapeset_scan ()
  {
    /* A held page needs no release call: a file page lives in a scan-owned
     * read-cache slot freed here, a prefix page is Tape-owned RAM. */
    /* Drop this scan's reference on its OWN copy of the shared cell -- never
     * touches m_tapeset, which may already be freed (see class comment in
     * qfile_tape.hpp). */
    if (m_open_scan_cell)
      {
	m_open_scan_cell->fetch_sub (1, std::memory_order_relaxed);
      }
    for (int i = 0; i < READ_CACHE_SLOTS; i++)
      {
	free (m_cache[i].buf);
	m_cache[i].buf = NULL;
      }
    free (m_reasm_raw);
    m_reasm_raw = NULL;
    m_reasm = NULL;
    free (m_peek_reasm_raw);
    m_peek_reasm_raw = NULL;
    m_peek_reasm_cap = 0;
  }

  void
  tapeset_scan::release_page (THREAD_ENTRY *thread_p)
  {
    /* Per-scan scratch: a held file page lives in a scan-owned read-cache slot
     * and a prefix page is Tape-owned RAM -- dropping the reference needs no Tape
     * call, so close() does not depend on the Tapeset (or its Tapes) being
     * alive.  The cache buffers are NOT freed here (kept for the scan's life,
     * freed in ~tapeset_scan); only the borrowed reference is dropped. */
    (void) thread_p;
    m_page = NULL;
    m_curr_tpl = NULL;
  }

  PAGE_PTR
  tapeset_scan::fetch_page (THREAD_ENTRY *thread_p, tape *tape_p, int page_offset)
  {
    /* RAM pages (memory Tape, or the buffile's in-RAM prefix) are returned by
     * read_page_into as a direct pointer -- no pread, no copy -- so they are
     * never cached (caching them would waste a slot on a zero-cost read). */
    if (page_offset < tape_p->prefix_page_count ())
      {
	return tape_p->read_page_into (thread_p, page_offset, NULL, &m_read_scratch);
      }

    /* Spilled (file-backed) page: serve from the per-scan LRU read cache.  The
     * backing is append-all-then-freeze immutable, so a cached page can never go
     * stale -- no invalidation.  The cache is per-reader (scan-owned): no sharing
     * across scans (#126/K-12 shared-cache hazard avoided). */
    m_cache_tick++;
    for (int i = 0; i < READ_CACHE_SLOTS; i++)
      {
	if (m_cache[i].valid && m_cache[i].tape_p == tape_p && m_cache[i].page_offset == page_offset)
	  {
	    m_cache[i].tick = m_cache_tick;
	    m_metrics.cache_hits++;
	    return (PAGE_PTR) m_cache[i].buf;
	  }
      }

    /* miss: evict the least-recently-used slot (or the first empty one) */
    int victim = 0;
    for (int i = 1; i < READ_CACHE_SLOTS; i++)
      {
	if (!m_cache[i].valid)
	  {
	    victim = i;
	    break;
	  }
	if (m_cache[i].tick < m_cache[victim].tick)
	  {
	    victim = i;
	  }
      }
    if (m_cache[victim].buf == NULL)
      {
	m_cache[victim].buf = (char *) malloc (DB_PAGESIZE);
	if (m_cache[victim].buf == NULL)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) DB_PAGESIZE);
	    return NULL;
	  }
      }
    /* For a file page read_page_into decrypts/reads into page_dest and returns it
     * (== buf); the prefix-RAM early-return above is never taken here. */
    PAGE_PTR pg = tape_p->read_page_into (thread_p, page_offset, m_cache[victim].buf, &m_read_scratch);
    if (pg == NULL)
      {
	m_cache[victim].valid = false;
	return NULL;
      }
    m_cache[victim].tape_p = tape_p;
    m_cache[victim].page_offset = page_offset;
    m_cache[victim].valid = true;
    m_cache[victim].tick = m_cache_tick;
    m_metrics.cache_misses++;
    return (PAGE_PTR) m_cache[victim].buf;
  }

  void
  tapeset_scan::set_on (int tape_idx, int page_offset, PAGE_PTR page, int byte_offset, int tplno)
  {
    if (m_tape_idx >= 0 && m_tape_idx != tape_idx)
      {
	m_metrics.tape_advances++;
      }
    m_tape_idx = tape_idx;
    m_page_offset = page_offset;
    m_page = page;
    m_offset = byte_offset;
    m_tplno = tplno;
    m_curr_tpl = page + byte_offset;
    m_position = S_ON;
    m_status = S_STARTED;
  }

  SCAN_CODE
  tapeset_scan::retrieve (THREAD_ENTRY *thread_p, QFILE_TUPLE_RECORD *tuple_record_p, int peek)
  {
    /* Overflow START page: reassemble the contiguous run as one tuple.  We
     * must be positioned ON the run's first page (forward / backward / jump
     * all land there); a continuation page here is a bug. */
    if (qfile_overflow_is_overflow_page (m_page))
      {
	if (qfile_overflow_first_page (m_page) != m_page_offset)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
	    return S_ERROR;
	  }
	const int tuple_len = QFILE_GET_TUPLE_LENGTH ((char *) m_page + QFILE_PAGE_HEADER_SIZE);
	tape *tape_p = m_tapeset->get_tape (m_tape_idx);
	if (tape_p == NULL || tuple_len <= 0)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
	    return S_ERROR;
	  }
	const int run_pages = qfile_overflow_run_pages (tuple_len);
	const int run_end = m_page_offset + run_pages - 1;
	if (m_reasm == NULL)
	  {
	    m_reasm_raw = (char *) malloc (DB_PAGESIZE);
	    if (m_reasm_raw == NULL)
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) DB_PAGESIZE);
		return S_ERROR;
	      }
	    m_reasm = (PAGE_PTR) m_reasm_raw;
	  }
	char *dest;
	if (peek)
	  {
	    /* Overflow-PEEK reassembles into a scan-owned buffer (freed by
	     * close()/~tapeset_scan()), never into the caller's record: PEEK
	     * callers don't free tuple_record_p->tpl, and a prior normal-tuple
	     * PEEK may have left tuple_record_p->tpl pointing at borrowed page
	     * memory with size == 0 -- reallocating THAT would corrupt the
	     * private heap. */
	    if (tuple_len > m_peek_reasm_cap)
	      {
		char *area = (char *) realloc (m_peek_reasm_raw, tuple_len);
		if (area == NULL)
		  {
		    return S_ERROR;
		  }
		m_peek_reasm_raw = area;
		m_peek_reasm_cap = tuple_len;
	      }
	    dest = m_peek_reasm_raw;
	  }
	else
	  {
	    if (tuple_record_p->size < tuple_len)
	      {
		if (qfile_reallocate_tuple (tuple_record_p, tuple_len) != NO_ERROR)
		  {
		    return S_ERROR;
		  }
	      }
	    dest = tuple_record_p->tpl;
	  }
	int copied = 0;
	for (int p = m_page_offset; p <= run_end && copied < tuple_len; p++)
	  {
	    PAGE_PTR pg = tape_p->read_page_into (thread_p, p, (char *) m_reasm, &m_reasm_tde);
	    if (pg == NULL)
	      {
		return S_ERROR;
	      }
	    int csz = tuple_len - copied;
	    if (csz > QFILE_MAX_TUPLE_SIZE_IN_PAGE)
	      {
		csz = QFILE_MAX_TUPLE_SIZE_IN_PAGE;
	      }
	    std::memcpy (dest + copied, (char *) pg + QFILE_PAGE_HEADER_SIZE, csz);
	    copied += csz;
	  }
	m_curr_overflow = true;
	m_overflow_run_end = run_end;
	if (peek)
	  {
	    tuple_record_p->tpl = dest;	/* borrowed; scan-owned, size left untouched */
	  }
	m_curr_tpl = dest;	/* peek points at the assembled buffer */
	m_metrics.tuple_reads++;
	if (peek)
	  {
	    m_metrics.peeks++;
	  }
	else
	  {
	    m_metrics.copies++;
	  }
	return S_SUCCESS;
      }
    m_curr_overflow = false;

    if (peek)
      {
	tuple_record_p->tpl = m_curr_tpl;
	m_metrics.peeks++;
	m_metrics.tuple_reads++;
	return S_SUCCESS;
      }

    int tuple_size = QFILE_GET_TUPLE_LENGTH (m_curr_tpl);
    if (tuple_record_p->size < tuple_size)
      {
	if (qfile_reallocate_tuple (tuple_record_p, tuple_size) != NO_ERROR)
	  {
	    return S_ERROR;
	  }
      }
    std::memcpy (tuple_record_p->tpl, m_curr_tpl, tuple_size);
    m_metrics.copies++;
    m_metrics.tuple_reads++;
    return S_SUCCESS;
  }

  /* Scan forward from (start_tape, start_page) for the next tuple-bearing page.
   * The start tape begins at start_page; every later tape begins at page 0.
   * Zero-tuple pages and empty/missing tapes are skipped.  S_END (with
   * m_position = S_AFTER) only when the last tape is exhausted. */
  SCAN_CODE
  tapeset_scan::forward (THREAD_ENTRY *thread_p, QFILE_TUPLE_RECORD *tuple_record_p, int peek)
  {
    if (m_tapeset == NULL)
      {
	m_position = S_AFTER;
	return S_END;
      }

    int start_tape;
    int start_page;

    if (m_position == S_BEFORE)
      {
	start_tape = 0;
	start_page = 0;
      }
    else if (m_position == S_ON)
      {
	if (m_curr_overflow)
	  {
	    start_tape = m_tape_idx;
	    start_page = m_overflow_run_end + 1;
	    release_page (thread_p);
	  }
	else
	  {
	    int count = QFILE_GET_TUPLE_COUNT (m_page);
	    if (m_tplno < count - 1)
	      {
		int len = QFILE_GET_TUPLE_LENGTH (m_curr_tpl);
		m_offset += len;
		m_curr_tpl += len;
		m_tplno++;
		return retrieve (thread_p, tuple_record_p, peek);
	      }
	    start_tape = m_tape_idx;
	    start_page = m_page_offset + 1;
	    release_page (thread_p);
	  }
      }
    else if (m_position == S_AFTER)
      {
	return S_END;
      }
    else
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
	return S_ERROR;
      }

    int ntapes = m_tapeset->tape_count ();
    int page_offset = start_page;
    for (int ti = start_tape; ti < ntapes; ti++, page_offset = 0)
      {
	tape *tape_p = m_tapeset->get_tape (ti);
	if (tape_p == NULL)
	  {
	    continue;
	  }
	int npages = tape_p->total_page_count ();
	for (; page_offset < npages; page_offset++)
	  {
	    PAGE_PTR page = fetch_page (thread_p, tape_p, page_offset);
	    if (page == NULL)
	      {
		return S_ERROR;
	      }
	    m_metrics.page_reads++;
	    refresh_pgbuf_fixes (m_metrics, m_pgbuf_fix_baseline);
	    int count = QFILE_GET_TUPLE_COUNT (page);
	    if (count > 0)
	      {
		set_on (ti, page_offset, page, QFILE_PAGE_HEADER_SIZE, 0);
		return retrieve (thread_p, tuple_record_p, peek);
	      }
	    if (qfile_overflow_is_overflow_page (page) && qfile_overflow_first_page (page) == page_offset)
	      {
		/* overflow START -- this page owns the run; retrieve reassembles. */
		set_on (ti, page_offset, page, QFILE_PAGE_HEADER_SIZE, 0);
		return retrieve (thread_p, tuple_record_p, peek);
	      }
	    /* zero-tuple page or overflow continuation (start already consumed in
	     * the sequential scan) -- skip; nothing to release (scan-owned scratch). */
	  }
      }

    m_position = S_AFTER;
    m_page = NULL;
    m_curr_tpl = NULL;
    return S_END;
  }

  /* Scan backward from (start_tape, start_page) for the previous tuple-bearing
   * page; position at that page's LAST tuple.  start_page < 0 means "no pages
   * left in start_tape" (move to the previous tape).  Each earlier tape starts
   * at its last page.  S_END (with m_position = S_BEFORE) when the first tape
   * is exhausted. */
  SCAN_CODE
  tapeset_scan::backward (THREAD_ENTRY *thread_p, QFILE_TUPLE_RECORD *tuple_record_p, int peek)
  {
    if (m_tapeset == NULL)
      {
	m_position = S_BEFORE;
	return S_END;
      }

    int start_tape;
    int start_page;

    if (m_position == S_BEFORE)
      {
	return S_END;
      }
    else if (m_position == S_ON)
      {
	if (m_tplno > 0)
	  {
	    int prev_len = QFILE_GET_PREV_TUPLE_LENGTH (m_curr_tpl);
	    m_offset -= prev_len;
	    m_curr_tpl -= prev_len;
	    m_tplno--;
	    return retrieve (thread_p, tuple_record_p, peek);
	  }
	start_tape = m_tape_idx;
	start_page = m_page_offset - 1;
	release_page (thread_p);
      }
    else if (m_position == S_AFTER)
      {
	start_tape = m_tapeset->tape_count () - 1;
	tape *last_tape = m_tapeset->get_tape (start_tape);
	start_page = (last_tape != NULL) ? last_tape->total_page_count () - 1 : -1;
      }
    else
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
	return S_ERROR;
      }

    for (int ti = start_tape; ti >= 0; ti--)
      {
	tape *tape_p = m_tapeset->get_tape (ti);
	if (tape_p == NULL)
	  {
	    continue;
	  }
	int page_offset = (ti == start_tape) ? start_page : tape_p->total_page_count () - 1;
	for (; page_offset >= 0; page_offset--)
	  {
	    PAGE_PTR page = fetch_page (thread_p, tape_p, page_offset);
	    if (page == NULL)
	      {
		return S_ERROR;
	      }
	    m_metrics.page_reads++;
	    refresh_pgbuf_fixes (m_metrics, m_pgbuf_fix_baseline);
	    int count = QFILE_GET_TUPLE_COUNT (page);
	    if (count > 0)
	      {
		set_on (ti, page_offset, page, QFILE_GET_LAST_TUPLE_OFFSET (page), count - 1);
		return retrieve (thread_p, tuple_record_p, peek);
	      }
	    if (qfile_overflow_is_overflow_page (page))
	      {
		/* Backward reaches the run's last (continuation) page first;
		 * reposition to its START page and reassemble the whole run as one
		 * tuple.  The next backward step then skips to start-1. */
		const int first = qfile_overflow_first_page (page);
		PAGE_PTR start_pg = fetch_page (thread_p, tape_p, first);
		if (start_pg == NULL)
		  {
		    return S_ERROR;
		  }
		m_metrics.page_reads++;
		refresh_pgbuf_fixes (m_metrics, m_pgbuf_fix_baseline);
		set_on (ti, first, start_pg, QFILE_PAGE_HEADER_SIZE, 0);
		return retrieve (thread_p, tuple_record_p, peek);
	      }
	  }
      }

    m_position = S_BEFORE;
    m_page = NULL;
    m_curr_tpl = NULL;
    return S_END;
  }

  void
  tapeset_scan::save_position (QFILE_TUPLE_POSITION *tuple_position_p) const
  {
    tuple_position_p->status = m_status;
    tuple_position_p->position = m_position;
    qfile_tuple_position_set_tape (tuple_position_p, m_tape_idx, m_page_offset, m_offset);
    tuple_position_p->tpl = m_curr_tpl;
    tuple_position_p->tplno = m_tplno;
  }

  SCAN_CODE
  tapeset_scan::jump (THREAD_ENTRY *thread_p, const QFILE_TUPLE_POSITION *tuple_position_p,
		      QFILE_TUPLE_RECORD *tuple_record_p, int peek)
  {
    if (m_tapeset == NULL)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
	return S_ERROR;
      }

    if (tuple_position_p->position == S_ON)
      {
	tape *tape_p = m_tapeset->get_tape (tuple_position_p->tape_idx);
	if (tape_p == NULL)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
	    return S_ERROR;
	  }
	PAGE_PTR page = fetch_page (thread_p, tape_p, tuple_position_p->tape_page_offset);
	if (page == NULL)
	  {
	    return S_ERROR;
	  }
	m_metrics.page_reads++;
	refresh_pgbuf_fixes (m_metrics, m_pgbuf_fix_baseline);
	release_page (thread_p);
	set_on (tuple_position_p->tape_idx, tuple_position_p->tape_page_offset, page,
		tuple_position_p->tape_byte_offset, tuple_position_p->tplno);
	m_status = tuple_position_p->status;
	m_metrics.jumps++;
	return retrieve (thread_p, tuple_record_p, peek);
      }
    else if (tuple_position_p->position == S_BEFORE || tuple_position_p->position == S_AFTER)
      {
	release_page (thread_p);
	m_status = tuple_position_p->status;
	m_position = tuple_position_p->position;
	return S_END;
      }

    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
    return S_ERROR;
  }

  void
  tapeset_scan::close (THREAD_ENTRY *thread_p)
  {
    release_page (thread_p);
  }

  /* ------------------------------------------------------------------ */
  /* tapeset_reader (per-participant concurrent read)                   */
  /* ------------------------------------------------------------------ */

  tapeset_reader::tapeset_reader (tapeset *ts, chunk_distributor *dist, int reader_id)
    : m_tapeset (ts)
    , m_dist (dist)
    , m_reader_id (reader_id)
    , m_range ()
    , m_have_chunk (false)
    , m_cur_page (0)
    , m_page (NULL)
    , m_count (0)
    , m_offset (0)
    , m_tplno (0)
    , m_page_raw (NULL)
    , m_page_buf (NULL)
    , m_tde ()
    , m_peek_reasm_raw (NULL)
    , m_peek_reasm_cap (0)
    , m_metrics ()
#if !defined (NDEBUG)
    , m_pgbuf_fix_baseline (pgbuf_get_fix_debug_count ())
#else /* NDEBUG */
    , m_pgbuf_fix_baseline (0)
#endif /* NDEBUG */
  {
    m_page_raw = (char *) malloc (DB_PAGESIZE);
    m_page_buf = (PAGE_PTR) m_page_raw;
  }

  tapeset_reader::~tapeset_reader ()
  {
    free (m_page_raw);
    m_page_raw = NULL;
    m_page_buf = NULL;
    free (m_peek_reasm_raw);
    m_peek_reasm_raw = NULL;
    m_peek_reasm_cap = 0;
  }

  SCAN_CODE
  tapeset_reader::emit_in_page (THREAD_ENTRY *thread_p, QFILE_TUPLE_RECORD *tuple_record_p, int peek)
  {
    m_metrics.tuple_reads++;
    if (peek)
      {
	tuple_record_p->tpl = (QFILE_TUPLE) (m_page + m_offset);
	m_metrics.peeks++;
	return S_SUCCESS;
      }
    int tuple_size = QFILE_GET_TUPLE_LENGTH (m_page + m_offset);
    if (tuple_record_p->size < tuple_size)
      {
	if (qfile_reallocate_tuple (tuple_record_p, tuple_size) != NO_ERROR)
	  {
	    return S_ERROR;
	  }
      }
    std::memcpy (tuple_record_p->tpl, m_page + m_offset, tuple_size);
    m_metrics.copies++;
    return S_SUCCESS;
  }

  SCAN_CODE
  tapeset_reader::reassemble (THREAD_ENTRY *thread_p, tape *tp, int first_page, int run_end, int tuple_len,
			     QFILE_TUPLE_RECORD *tuple_record_p, int peek)
  {
    char *dest;
    if (peek)
      {
	/* Overflow-PEEK reassembles into a reader-owned buffer (freed by
	 * ~tapeset_reader()), never into the caller's record -- same rationale
	 * as tapeset_scan::retrieve's overflow-PEEK branch. */
	if (tuple_len > m_peek_reasm_cap)
	  {
	    char *area = (char *) realloc (m_peek_reasm_raw, tuple_len);
	    if (area == NULL)
	      {
		return S_ERROR;
	      }
	    m_peek_reasm_raw = area;
	    m_peek_reasm_cap = tuple_len;
	  }
	dest = m_peek_reasm_raw;
      }
    else
      {
	if (tuple_record_p->size < tuple_len)
	  {
	    if (qfile_reallocate_tuple (tuple_record_p, tuple_len) != NO_ERROR)
	      {
		return S_ERROR;
	      }
	  }
	dest = tuple_record_p->tpl;
      }
    int copied = 0;
    for (int p = first_page; p <= run_end && copied < tuple_len; p++)
      {
	/* reuse m_page_buf as read scratch -- `page` is no longer needed once we
	 * commit to reassembling, and the frozen Tape is read by offset pread. */
	PAGE_PTR pg = tp->read_page_into (thread_p, p, (char *) m_page_buf, &m_tde);
	if (pg == NULL)
	  {
	    return S_ERROR;
	  }
	int csz = tuple_len - copied;
	if (csz > QFILE_MAX_TUPLE_SIZE_IN_PAGE)
	  {
	    csz = QFILE_MAX_TUPLE_SIZE_IN_PAGE;
	  }
	std::memcpy (dest + copied, (char *) pg + QFILE_PAGE_HEADER_SIZE, csz);
	copied += csz;
      }
    if (peek)
      {
	tuple_record_p->tpl = dest;	/* borrowed; reader-owned, size left untouched */
      }
    return S_SUCCESS;
  }

  SCAN_CODE
  tapeset_reader::next (THREAD_ENTRY *thread_p, QFILE_TUPLE_RECORD *tuple_record_p, int peek)
  {
    if (m_tapeset == NULL || m_dist == NULL)
      {
	return S_END;
      }

    for (;;)
      {
	/* still walking tuples within the current normal page? */
	if (m_page != NULL && m_tplno + 1 < m_count)
	  {
	    int len = QFILE_GET_TUPLE_LENGTH (m_page + m_offset);
	    m_offset += len;
	    m_tplno++;
	    return emit_in_page (thread_p, tuple_record_p, peek);
	  }
	m_page = NULL;

	if (!m_have_chunk)
	  {
	    if (!m_dist->next_chunk (m_reader_id, m_range))
	      {
		return S_END;
	      }
	    m_have_chunk = true;
	    m_cur_page = m_range.start_page;
	  }
	if (m_cur_page >= m_range.start_page + m_range.page_count)
	  {
	    m_have_chunk = false;
	    continue;
	  }

	tape *tp = m_tapeset->get_tape (m_range.tape_idx);
	if (tp == NULL)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
	    return S_ERROR;
	  }
	const int this_page = m_cur_page;
	PAGE_PTR page = tp->read_page_into (thread_p, this_page, (char *) m_page_buf, &m_tde);
	if (page == NULL)
	  {
	    return S_ERROR;
	  }
	m_metrics.page_reads++;
	refresh_pgbuf_fixes (m_metrics, m_pgbuf_fix_baseline);
	int count = QFILE_GET_TUPLE_COUNT (page);

	if (count > 0)
	  {
	    m_page = page;
	    m_count = count;
	    m_offset = QFILE_PAGE_HEADER_SIZE;
	    m_tplno = 0;
	    m_cur_page = this_page + 1;
	    return emit_in_page (thread_p, tuple_record_p, peek);
	  }

	if (qfile_overflow_is_overflow_page (page))
	  {
	    const int first = qfile_overflow_first_page (page);
	    if (first == this_page)
	      {
		/* first-page owner: reassemble the run forward past my chunk. */
		int tuple_len = QFILE_GET_TUPLE_LENGTH ((char *) page + QFILE_PAGE_HEADER_SIZE);
		if (tuple_len <= 0)
		  {
		    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
		    return S_ERROR;
		  }
		const int run_pages = qfile_overflow_run_pages (tuple_len);
		const int run_end = first + run_pages - 1;
		SCAN_CODE rc = reassemble (thread_p, tp, first, run_end, tuple_len, tuple_record_p, peek);
		if (rc != S_SUCCESS)
		  {
		    return rc;
		  }
		m_cur_page = run_end + 1;
		m_dist->skip_to_after (m_range.tape_idx, run_end);
		m_metrics.tuple_reads++;
		if (peek)
		  {
		    m_metrics.peeks++;
		  }
		else
		  {
		    m_metrics.copies++;
		  }
		return S_SUCCESS;
	      }
	    /* continuation whose start precedes my chunk: skip the run O(1) and
	     * bump the shared cursor past it. */
	    int run_end = qfile_overflow_run_end (page);
	    if (run_end < this_page)
	      {
		run_end = this_page;	/* defensive */
	      }
	    m_cur_page = run_end + 1;
	    m_dist->skip_to_after (m_range.tape_idx, run_end);
	    continue;
	  }

	/* zero-tuple page -- skip. */
	m_cur_page = this_page + 1;
      }
  }
}				/* namespace qfile */

/* ------------------------------------------------------------------ */
/* Overflow-continuation page-header helpers.                         */
/* ------------------------------------------------------------------ */

void
qfile_overflow_set_start (PAGE_PTR page, int self_page_offset)
{
  QFILE_PUT_TUPLE_COUNT (page, QFILE_OVERFLOW_TUPLE_COUNT_FLAG);
  /* first-page offset == self marks the START; stored where the old overflow
   * page-id VPID field used to live (now a logical offset, never a VPID). */
  OR_PUT_INT ((char *) page + QFILE_OVERFLOW_PAGE_ID_OFFSET, self_page_offset);
}

void
qfile_overflow_set_continuation (PAGE_PTR page, int first_page_offset, int run_end_offset)
{
  QFILE_PUT_TUPLE_COUNT (page, QFILE_OVERFLOW_TUPLE_COUNT_FLAG);
  OR_PUT_INT ((char *) page + QFILE_OVERFLOW_PAGE_ID_OFFSET, first_page_offset);
  /* run-end stored in the last-tuple-offset field (free on a continuation). */
  OR_PUT_INT ((char *) page + QFILE_LAST_TUPLE_OFFSET, run_end_offset);
}

bool
qfile_overflow_is_overflow_page (const PAGE_PTR page)
{
  return QFILE_GET_TUPLE_COUNT (page) == QFILE_OVERFLOW_TUPLE_COUNT_FLAG;
}

int
qfile_overflow_first_page (const PAGE_PTR page)
{
  return (int) OR_GET_INT ((char *) page + QFILE_OVERFLOW_PAGE_ID_OFFSET);
}

int
qfile_overflow_run_end (const PAGE_PTR page)
{
  return (int) OR_GET_INT ((char *) page + QFILE_LAST_TUPLE_OFFSET);
}

int
qfile_overflow_run_pages (int tuple_length)
{
  if (tuple_length <= 0)
    {
      return 0;
    }
  const int per = QFILE_MAX_TUPLE_SIZE_IN_PAGE;
  return (tuple_length + per - 1) / per;
}

/* ------------------------------------------------------------------ */
/* C++-linkage bridge (list_file.c + unit test)                       */
/* ------------------------------------------------------------------ */

static void
qfile_tapeset_mirror (qfile::tapeset_scan *scan, QFILE_LIST_SCAN_ID *scan_id_p)
{
  /* Mirror the multi-Tape scan state into the outer QFILE_LIST_SCAN_ID so that
   * code reading scan_id->curr_* / position / status after a scan step sees a
   * consistent view.  curr_vpid is synthetic (volid = NULL_VOLID, pageid =
   * logical page offset within the current Tape) -- there is no physical VPID. */
  scan_id_p->status = scan->status ();
  scan_id_p->position = scan->position ();
  scan_id_p->curr_pgptr = scan->curr_page ();
  scan_id_p->curr_tpl = scan->curr_tpl ();
  scan_id_p->curr_offset = scan->curr_offset ();
  scan_id_p->curr_tplno = scan->curr_tplno ();
  scan_id_p->curr_vpid.volid = NULL_VOLID;
  scan_id_p->curr_vpid.pageid = scan->page_offset ();
}

int
qfile_tapeset_scan_open (QFILE_LIST_SCAN_ID *scan_id_p)
{
  qfile::tapeset *ts = (qfile::tapeset *) QFILE_LIST_ID_TAPESET (&scan_id_p->list_id);
  if (ts == NULL)
    {
      scan_id_p->tapeset_scan_ = NULL;
      return NO_ERROR;
    }
  /* backing-kind entry guard (production-hard): a tapeset scan never runs over
   * a list that also carries pgbuf backing. */
  {
    int guard_rc = QFILE_GUARD_TAPESET_MECHANISM (&scan_id_p->list_id);
    if (guard_rc != NO_ERROR)
      {
	return guard_rc;
      }
  }
  qfile::tapeset_scan *scan = new qfile::tapeset_scan (ts);
  if (scan == NULL)
    {
      scan_id_p->tapeset_scan_ = NULL;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (qfile::tapeset_scan));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  scan_id_p->tapeset_scan_ = scan;
  return NO_ERROR;
}

void
qfile_tapeset_scan_close (THREAD_ENTRY *thread_p, QFILE_LIST_SCAN_ID *scan_id_p)
{
  qfile::tapeset_scan *scan = (qfile::tapeset_scan *) scan_id_p->tapeset_scan_;
  if (scan != NULL)
    {
      scan->close (thread_p);
      delete scan;
      scan_id_p->tapeset_scan_ = NULL;
    }
}

/* Shared cast-to-tapeset_scan -> method call -> mirror sequence for the
 * forward/backward/jump bridges; each bridge supplies only its method call. */
template <typename F>
static SCAN_CODE
qfile_tapeset_scan_step (QFILE_LIST_SCAN_ID *scan_id_p, F &&step)
{
  qfile::tapeset_scan *scan = (qfile::tapeset_scan *) scan_id_p->tapeset_scan_;
  assert (scan != NULL);
  SCAN_CODE code = step (scan);
  qfile_tapeset_mirror (scan, scan_id_p);
  return code;
}

SCAN_CODE
qfile_tapeset_scan_forward (THREAD_ENTRY *thread_p, QFILE_LIST_SCAN_ID *scan_id_p, QFILE_TUPLE_RECORD *tuple_record_p,
			    int peek)
{
  return qfile_tapeset_scan_step (scan_id_p, [&] (qfile::tapeset_scan *scan)
    {
      return scan->forward (thread_p, tuple_record_p, peek);
    });
}

SCAN_CODE
qfile_tapeset_scan_backward (THREAD_ENTRY *thread_p, QFILE_LIST_SCAN_ID *scan_id_p, QFILE_TUPLE_RECORD *tuple_record_p,
			     int peek)
{
  return qfile_tapeset_scan_step (scan_id_p, [&] (qfile::tapeset_scan *scan)
    {
      return scan->backward (thread_p, tuple_record_p, peek);
    });
}

SCAN_CODE
qfile_tapeset_scan_jump (THREAD_ENTRY *thread_p, QFILE_LIST_SCAN_ID *scan_id_p,
			 const QFILE_TUPLE_POSITION *tuple_position_p, QFILE_TUPLE_RECORD *tuple_record_p, int peek)
{
  return qfile_tapeset_scan_step (scan_id_p, [&] (qfile::tapeset_scan *scan)
    {
      return scan->jump (thread_p, tuple_position_p, tuple_record_p, peek);
    });
}

void
qfile_tapeset_scan_save_position (QFILE_LIST_SCAN_ID *scan_id_p, QFILE_TUPLE_POSITION *tuple_position_p)
{
  qfile::tapeset_scan *scan = (qfile::tapeset_scan *) scan_id_p->tapeset_scan_;
  assert (scan != NULL);
  scan->save_position (tuple_position_p);
}

void
qfile_tapeset_destroy (void *tapeset_ptr)
{
  delete (qfile::tapeset *) tapeset_ptr;
}

/* Client-fetch-over-Tapeset bridges.  Serve a tapeset-backed top-level result
 * straight from its frozen Tapes for the VPID-page client fetch protocol,
 * without the pgbuf materialize (full pgbuf copy).  The Tapeset's ordered
 * Tapes form one dense logical page sequence 0..page_count()-1 (offset
 * arithmetic); the client addresses pages by that global index (see
 * QFILE_TAPESET_FETCH_VOLID).  Overflow-free lists only -- the caller keeps
 * materialize for the overflow-containing case. */
int
qfile_tapeset_page_count (const QFILE_LIST_ID *list_id_p)
{
  qfile::tapeset *ts = (list_id_p != NULL) ? (qfile::tapeset *) QFILE_LIST_ID_TAPESET (list_id_p) : NULL;
  if (ts == NULL)
    {
      return 0;
    }
  int total = 0;
  for (int t = 0; t < ts->tape_count (); ++t)
    {
      qfile::tape *tp = ts->get_tape (t);
      if (tp != NULL)
	{
	  total += tp->total_page_count ();
	}
    }
  return total;
}

int
qfile_tapeset_read_global_page (THREAD_ENTRY *thread_p, const QFILE_LIST_ID *list_id_p, int global_index,
				char *page_dest, int *local_offset_out)
{
  qfile::tapeset *ts = (list_id_p != NULL) ? (qfile::tapeset *) QFILE_LIST_ID_TAPESET (list_id_p) : NULL;
  if (ts == NULL || global_index < 0 || page_dest == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  /* Map the global logical page index across the ordered Tapes (no built-in
   * mapper -- accumulate total_page_count per Tape). */
  int remaining = global_index;
  for (int t = 0; t < ts->tape_count (); ++t)
    {
      qfile::tape *tp = ts->get_tape (t);
      if (tp == NULL)
	{
	  continue;
	}
      const int n = tp->total_page_count ();
      if (remaining < n)
	{
	  /* Re-entrant read into caller scratch; a RAM prefix page is returned
	   * in place (pg != page_dest), a file page is read into page_dest.  TDE
	   * decrypt uses the local scratch (freed on return). */
	  qfile::tde_read_scratch tde;
	  PAGE_PTR pg = tp->read_page_into (thread_p, remaining, page_dest, &tde);
	  if (pg == NULL)
	    {
	      return ER_FAILED;	/* read_page_into already er_set */
	    }
	  if (pg != page_dest)
	    {
	      std::memcpy (page_dest, pg, DB_PAGESIZE);
	    }
	  if (local_offset_out != NULL)
	    {
	      *local_offset_out = remaining;	/* tape-local logical offset */
	    }
	  return NO_ERROR;
	}
      remaining -= n;
    }

  assert (false);		/* global_index >= page_count: caller bug */
  return ER_FAILED;
}

/* ------------------------------------------------------------------ */
/* producer bridge                                                    */
/* ------------------------------------------------------------------ */

void *
qfile_producer_create_for_list (THREAD_ENTRY *thread_p, bool tde_encrypted)
{
  /* process-unique sequence so concurrent producers never collide on a BufFile
   * name (open is O_EXCL); base well above the selftest's fixed seqs. */
  static std::atomic<unsigned long long> seq_gen { 0x100000000ULL };
  TDE_ALGORITHM algo = TDE_ALGORITHM_NONE;
  int budget;
  std::string dir;

  (void) thread_p;
  if (tde_encrypted)
    {
      algo = (TDE_ALGORITHM) prm_get_integer_value (PRM_ID_TDE_DEFAULT_ALGORITHM);
    }
  /* membuf prefix budget = work_mem in pages (tiny sorts stay in RAM, no spill) */
  budget = (int) (prm_get_bigint_value (PRM_ID_WORK_MEM) / DB_PAGESIZE);
  if (budget < 4)
    {
      budget = 4;
    }
  if (!qfile::buffile::default_scratch_dir (dir))
    {
      return NULL;
    }
  return new qfile::tape_writer (budget, algo, dir, (std::uint64_t) seq_gen.fetch_add (1), 0);
}

/* Import all Tapes of src's frozen Tapeset into dest's Tapeset, in order
 * (parallel fan-in).  Ownership of the Tapes transfers to dest; src's Tapeset
 * keeps the (now-unowned) vector so destroying src frees only its container,
 * not the moved Tapes.  dest's tuple/page counts accumulate src's.  dest must
 * be a frozen Tapeset list. */
int
qfile_tapeset_import (THREAD_ENTRY *thread_p, QFILE_LIST_ID *dest, QFILE_LIST_ID *src)
{
  (void) thread_p;
  if (dest == src)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
      return ER_FAILED;
    }
  qfile::tapeset *dts = (qfile::tapeset *) QFILE_LIST_ID_TAPESET (dest);
  qfile::tapeset *sts = (qfile::tapeset *) QFILE_LIST_ID_TAPESET (src);
  if (dts == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
      return ER_FAILED;
    }
  if (sts != NULL)
    {
      dts->transfer_tapes_from (sts);	/* per-tape move-and-null; src left empty */
      if (QFILE_LIST_ID_TAPESET_CONTAINS_OVERFLOW (src))
	{
	  QFILE_LIST_ID_TAPESET_CONTAINS_OVERFLOW (dest) = true;
	}
    }
  dest->tuple_cnt += src->tuple_cnt;
  dest->page_cnt += src->page_cnt;
  return NO_ERROR;
}

int
qfile_producer_append (THREAD_ENTRY *thread_p, void *writer, const PAGE_PTR full_page)
{
  if (writer == NULL || full_page == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
      return ER_FAILED;
    }
  return ((qfile::tape_writer *) writer)->append_page (thread_p, full_page);
}

void *
qfile_producer_freeze_tapeset (THREAD_ENTRY *thread_p, void *writer)
{
  if (writer == NULL)
    {
      return NULL;
    }
  qfile::tape_writer *w = (qfile::tape_writer *) writer;
  qfile::tape *t = w->freeze (thread_p);
  delete w;			/* spent: prefix + buffile ownership moved to the Tape */
  if (t == NULL)
    {
      return NULL;
    }
  qfile::tapeset *ts = new qfile::tapeset ();
  if (ts == NULL)
    {
      delete t;		/* nothing else references the freshly-frozen Tape */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (qfile::tapeset));
      return NULL;
    }
  ts->set_owns_tapes (true);
  ts->append_tape (t);
  return ts;
}

