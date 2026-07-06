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
#include "qfile_page_spill.hpp"	/* PAGE_SPILL backing */
#include "qfile_spill_file.hpp"	/* qfile::spill_file::set_os_error (shared substrate) */
#include "query_manager.h"
#include "query_workmem.hpp"	/* work_mem accountant (reserve_held / release_held_reservation) */
#include "system_parameter.h"
#include "tde.h"

#include <cstring>
#include <cstdlib>

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace
{
  void put_page_header (PAGE_PTR page_p, QFILE_PAGE_HEADER *header_p);

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

  /* #146 T3 S1 (D2/D5): growth-time high-water charge for MEMBUF pages, batched
   * at MEMBUF_GROWTH_BATCH_PAGES so the accountant sees one atomic charge per
   * batch rather than one per page (mirrors tape_writer::wm_reserve_batch's
   * refill-quantum pattern in qfile_tape.cpp).  tfile_p->wm_reserved_bytes is
   * the high-water charged-so-far amount, not a prepaid budget;
   * wm_reserved_shard is the sticky shard every increment lands on so
   * release_held_reservation can undo the whole thing in one release_held
   * call.  Returns false (nothing charged) if the cap rejects the next batch --
   * the caller degrades to disk-spill backing instead of erroring (D7-2). */
  constexpr int MEMBUF_GROWTH_BATCH_PAGES = 64;

  bool
  charge_membuf_page (QMGR_TEMP_FILE * tfile_p, int next_page_index)
  {
    const std::size_t charged_pages = tfile_p->wm_reserved_bytes / DB_PAGESIZE;
    const std::size_t needed_pages = static_cast<std::size_t> (next_page_index) + 1;

    if (charged_pages >= needed_pages)
      {
        return true;
      }

    std::size_t target_pages = charged_pages + MEMBUF_GROWTH_BATCH_PAGES;
    if (target_pages < needed_pages)
      {
        target_pages = needed_pages;
      }
    if (target_pages > static_cast<std::size_t> (tfile_p->membuf_npages))
      {
        target_pages = static_cast<std::size_t> (tfile_p->membuf_npages);
      }

    const std::size_t delta_bytes = (target_pages - charged_pages) * DB_PAGESIZE;
    if (!temp_page_store::reserve_held_at_shard (delta_bytes, &tfile_p->wm_reserved_shard))
      {
        return false;
      }

    tfile_p->wm_reserved_bytes += delta_bytes;
    return true;
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
  /* PAGE_SPILL consumer shim: an unknown page (e.g. a membuf
   * page routed through the same qmgr call path) is a silent NO_ERROR. */
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

    /* the page-spill handle moves by plain pointer transfer (containment
     * ownership -- no registry reassign), and src resets to pristine MEMBUF. */
    {
      int os_error = 0;
      qfile::page_spill_file *spill_p =
	qfile::page_spill_file::create (static_cast<QUERY_ID> (-11), LOG_FIND_THREAD_TRAN_INDEX (thread_p), 0,
					false, &os_error);
      if (spill_p == NULL)
	{
	  return ER_FAILED;
	}

      src.backing = qmgr_temp_backing::PAGE_SPILL;
      src.page_spill_handle = spill_p;
      src.spill_query_id = static_cast<QUERY_ID> (-11);
      src.spill_owner_tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
      src.spill_next_pageid = 3;

      qmgr_temp_file_move (&dst, &src);

      if (src.page_spill_handle != NULL || src.backing != qmgr_temp_backing::MEMBUF || src.spill_next_pageid != 0
	  || src.spill_query_id != NULL_QUERY_ID || dst.page_spill_handle != spill_p
	  || dst.backing != qmgr_temp_backing::PAGE_SPILL || dst.spill_next_pageid != 3
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
          const bool membuf_has_capacity = (tfile_p->membuf != NULL && tfile_p->membuf_last < tfile_p->membuf_npages - 1);
          if (membuf_has_capacity && charge_membuf_page (tfile_p, tfile_p->membuf_last + 1))
            {
	      QFILE_PAGE_HEADER page_header = QFILE_PAGE_HEADER_INITIALIZER;

              vpid_p->volid = NULL_VOLID;
              vpid_p->pageid = ++(tfile_p->membuf_last);
	      put_page_header (tfile_p->membuf[tfile_p->membuf_last], &page_header);
              return tfile_p->membuf[tfile_p->membuf_last];
            }
          if (membuf_has_capacity)
            {
              /* Capacity remained but the growth charge was rejected: this is
               * cap pressure, not per-op exhaustion (#146 T3 S1 D7-2) --
               * degrade to disk-spill backing early instead of erroring. */
              temp_page_store::record_cap_pressure_spill ();
            }

          /* page-spill backing: the choice is made ONCE at the tfile's first
           * spill; the PAGE_SPILL tag pins it thereafter. */
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
          tfile_p->backing = qmgr_temp_backing::PAGE_SPILL;
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

      case qmgr_temp_backing::PAGE_SPILL:
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

      case qmgr_temp_backing::PAGE_SPILL:
        if (vpid_p->volid == NULL_VOLID && vpid_p->pageid >= 0 && vpid_p->pageid <= tfile_p->membuf_last)
          {
            return fix_membuf_page (thread_p, tfile_p, vpid_p);
          }
        if (tfile_p->page_spill_handle == NULL)
          {
            return unreachable_backing_page (thread_p);
          }
        return tfile_p->page_spill_handle->fix_page (thread_p, vpid_p->pageid);
      }

    return unreachable_backing_page (thread_p);
  }

}
