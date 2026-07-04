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

#include "error_manager.h"
#include "file_manager.h"
#include "list_file.h"
#include "log_impl.h"
#include "object_representation.h"
#include "page_buffer.h"
#include "perf_monitor.h"
#include "qfile_page_spill.hpp"	/* (c′) SPILL_OVERFLOW backing (#132) */
#include "qfile_spill_file.hpp"	/* qfile::spill_file::set_os_error (shared substrate, #132) */
#include "query_manager.h"
#include "system_parameter.h"
#include "tde.h"

#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <atomic>
#include <functional>
#include <limits>
#include <mutex>
#include <thread>

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
    tfile_p->spill_query_id = NULL_QUERY_ID;
    tfile_p->spill_owner_tran_index = NULL_TRAN_INDEX;
  }

  int
  qmgr_temp_file_move_selftest_destroy (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p) noexcept
  {
    int error = NO_ERROR;

    if (tfile_p->page_spill_handle != NULL)
      {
	delete tfile_p->page_spill_handle;
	tfile_p->page_spill_handle = NULL;
	tfile_p->spill_next_pageid = 0;
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
  /* (c′) SPILL_OVERFLOW consumer shims (#132): an unknown page (e.g. a membuf
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
    src.spill_query_id = static_cast<QUERY_ID> (-2);
    src.spill_owner_tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
    src.spill_worker_id = 7;
    src.spill_next_pageid = 2;
    src.tde_encrypted = true;
    src.preserved = true;

    PAGE_PTR *const src_membuf = src.membuf;
    PAGE_PTR *const dst_membuf = dst.membuf;
    const VFID temp_vfid = src.temp_vfid;

    qmgr_temp_file_move (&dst, &src);

    if (!VFID_ISNULL (&src.temp_vfid) || src.membuf != src_membuf
	|| src.membuf_last != 1 || src.membuf_npages != 2 || src.membuf_type != TEMP_FILE_MEMBUF_NORMAL
	|| src.membuf_capacity_pages != 2 || src.wm_reserved_bytes != 0 || src.wm_reserved_shard != -1
	|| src.backing != qmgr_temp_backing::MEMBUF || src.spill_query_id != NULL_QUERY_ID
	|| src.spill_owner_tran_index != NULL_TRAN_INDEX || src.spill_worker_id != 0 || src.spill_next_pageid != 0
	|| src.temp_file_type != FILE_TEMP || src.preserved || src.tde_encrypted || dst.membuf != dst_membuf
	|| dst.membuf_last != 0 || dst.membuf_npages != 2 || dst.membuf_type != TEMP_FILE_MEMBUF_KEY_BUFFER
	|| dst.membuf_capacity_pages != 2 || !VFID_EQ (&dst.temp_vfid, &temp_vfid)
	|| dst.temp_file_type != FILE_QUERY_AREA || !dst.preserved || dst.wm_reserved_bytes != reserved_bytes
	|| dst.wm_reserved_shard < 0 || dst.backing != qmgr_temp_backing::PRIVATE_SPILL_FALLBACK
	|| dst.spill_query_id != static_cast<QUERY_ID> (-2)
	|| dst.spill_owner_tran_index != LOG_FIND_THREAD_TRAN_INDEX (thread_p) || dst.spill_worker_id != 7
	|| dst.spill_next_pageid != 2 || !dst.tde_encrypted)
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

      src.backing = qmgr_temp_backing::SPILL_OVERFLOW;
      src.page_spill_handle = spill_p;
      src.spill_query_id = static_cast<QUERY_ID> (-11);
      src.spill_owner_tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
      src.spill_next_pageid = 3;

      qmgr_temp_file_move (&dst, &src);

      if (src.page_spill_handle != NULL || src.backing != qmgr_temp_backing::MEMBUF || src.spill_next_pageid != 0
	  || src.spill_query_id != NULL_QUERY_ID || dst.page_spill_handle != spill_p
	  || dst.backing != qmgr_temp_backing::SPILL_OVERFLOW || dst.spill_next_pageid != 3
	  || dst.spill_query_id != static_cast<QUERY_ID> (-11))
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

    return NO_ERROR;
#else /* !NDEBUG */
    (void) thread_p;
    return NO_ERROR;
#endif /* !NDEBUG */
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

          /* (c′) page-spill backing (#132; the sole membuf-overflow backing
           * since 커밋 B #137).  The choice is made ONCE at the tfile's first
           * spill; the SPILL_OVERFLOW tag pins it thereafter. */
          int os_error = 0;
          if (tfile_p->page_spill_handle == NULL)
            {
              tfile_p->page_spill_handle =
                qfile::page_spill_file::create (tfile_p->spill_query_id, tfile_p->spill_owner_tran_index,
                                                tfile_p->spill_worker_id, tfile_p->tde_encrypted, &os_error);
            }
          if (tfile_p->page_spill_handle == NULL)
            {
              qfile::spill_file::set_os_error (os_error);
              return NULL;
            }

          if (tfile_p->spill_next_pageid <= tfile_p->membuf_last)
            {
              tfile_p->spill_next_pageid = tfile_p->membuf_last + 1;
            }
          vpid_p->volid = NULL_VOLID;
          vpid_p->pageid = tfile_p->spill_next_pageid++;
          tfile_p->backing = qmgr_temp_backing::SPILL_OVERFLOW;
          PAGE_PTR spill_page_p = tfile_p->page_spill_handle->alloc_new_page (thread_p, vpid_p->pageid);
          if (spill_page_p == NULL)
            {
              delete tfile_p->page_spill_handle;
              tfile_p->page_spill_handle = NULL;
              tfile_p->spill_next_pageid = 0;
              return NULL;
            }
          QFILE_PAGE_HEADER page_header = QFILE_PAGE_HEADER_INITIALIZER;
          put_page_header (spill_page_p, &page_header);
          return spill_page_p;
        }

      case qmgr_temp_backing::PRIVATE_SPILL_FALLBACK:
        return alloc_private_spill_page (thread_p, tfile_p, vpid_p);

      case qmgr_temp_backing::SPILL_OVERFLOW:
        if (tfile_p->page_spill_handle == NULL)
          {
            er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TEMP_FILE, 1, LOG_FIND_THREAD_TRAN_INDEX (thread_p));
            return NULL;
          }
        vpid_p->volid = NULL_VOLID;
        vpid_p->pageid = tfile_p->spill_next_pageid++;
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

      case qmgr_temp_backing::SPILL_OVERFLOW:
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
     * hash-list and connect-by parent-position bytes.  reserve_held still enforces
     * reserved <= cap + worst_case_slack, with worst_case_slack = shard_count * refill_quantum.
     * (The raw-fd read-cache placeholder charge was zeroed out with the raw-fd deletion, 커밋 B #137.) */
    constexpr bool POSITION_BUDGET_ACTIVE = true;
    if (!POSITION_BUDGET_ACTIVE)
      {
	return 0;
      }
    return WORKMEM_POSITION_SAVED_SCAN_BUDGET * projected_tuple_position_bytes
      + WORKMEM_POSITION_HASH_ENTRY_BUDGET * projected_tuple_simple_pos_bytes
      + WORKMEM_CONNECT_BY_PARENT_BUDGET * projected_tuple_position_db_bytes;
  }
}
