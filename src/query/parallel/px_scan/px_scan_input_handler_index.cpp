/*
 *
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

/* px_scan_input_handler_index.cpp — facade: delegates to key_range_list / leaf_page_dispatcher / overflow_chain_pool. */

#include "px_scan_input_handler_index.hpp"

#include "error_code.h"
#include "error_manager.h"
#include "heap_file.h"
#include "log_impl.h"
#include "mvcc.h"
#include "object_representation_sr.h"
#include "system_parameter.h"

#include <algorithm>
#include <chrono>

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace parallel_scan
{
  int
  input_handler_index::init_on_main (THREAD_ENTRY *thread_p, INDX_INFO *indx_info, SCAN_ID *scan_id, val_descr *vd,
				     int parallelism)
  {
    int err = m_ranges.init_on_main (thread_p, indx_info, scan_id, vd);
    if (err != NO_ERROR)
      {
	return err;
      }
    m_leaf.init (&m_ranges);
    m_ovf.init (parallelism);
    return bitmap_init_on_main (thread_p, scan_id, parallelism);
  }

  input_handler_index::~input_handler_index ()
  {
    delete[] m_bm_touched;
    delete[] m_bm_whole;
    delete[] m_bm_rows;
  }

  /* An index holding every row of the class under plain column keys. A page marked as a whole is read row by row
   * with the index condition checked on the heap row, which says nothing about a filtered index's own predicate,
   * a function key or a prefix key. */
  static bool
  bitmap_index_is_plain (THREAD_ENTRY *thread_p, const OID *class_oid, const BTID *btid)
  {
    int idx_incache = -1;
    bool plain = false;
    OR_CLASSREP *classrepr = heap_classrepr_get (thread_p, class_oid, NULL, NULL_REPRID, &idx_incache);

    if (classrepr == NULL)
      {
	er_clear ();
	return false;
      }
    for (int i = 0; i < classrepr->n_indexes; i++)
      {
	const OR_INDEX *index = &classrepr->indexes[i];

	if (!BTID_IS_EQUAL (&index->btid, btid))
	  {
	    continue;
	  }
	plain = index->filter_predicate == NULL && index->func_index_info == NULL;
	for (int k = 0; plain && index->attrs_prefix_length != NULL && k < index->n_atts; k++)
	  {
	    plain = index->attrs_prefix_length[k] <= 0;
	  }
	break;
      }
    heap_classrepr_free_and_init (classrepr, &idx_incache);
    return plain;
  }

  int
  input_handler_index::bitmap_init_on_main (THREAD_ENTRY *thread_p, SCAN_ID *scan_id, int parallelism)
  {
    INDX_SCAN_ID *isidp = &scan_id->s.isid;
    FILE_FTAB_COLLECTOR collector = FILE_FTAB_COLLECTOR_INITIALIZER;
    int error_code;

    if (!prm_get_bool_value (PRM_ID_BITMAP_HEAP_SCAN) || isidp->indx_info == nullptr || isidp->indx_info->coverage != 0
	|| isidp->need_count_only || isidp->gby_skip != nullptr || isidp->multi_range_opt.use || isidp->iss.use
	|| isidp->range_pred.pred_expr == nullptr || mvcc_is_mvcc_disabled_class (&isidp->cls_oid)
	|| !bitmap_index_is_plain (thread_p, &isidp->cls_oid, &isidp->indx_info->btid))
      {
	return NO_ERROR;
      }

    /* The sector list below misses every page allocated after it. With the statement's snapshot taken first, the
     * rows on those pages all belong to transactions the snapshot does not see, so a mark outside the list is
     * dropped. */
    if (logtb_get_mvcc_snapshot (thread_p) == NULL)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_MVCC_CANT_GET_SNAPSHOT, 0);
	return ER_MVCC_CANT_GET_SNAPSHOT;
      }
    error_code = file_get_all_data_sectors (thread_p, &isidp->hfid.vfid, &collector);
    if (error_code == NO_ERROR && collector.nsects > 0)
      {
	const size_t nsects = (size_t) collector.nsects;
	const INT64 bound_bits = (INT64) prm_get_bigint_value (PRM_ID_INDEX_SCAN_OID_BITMAP_SIZE) * 8;
	/* a slot takes at least 4 bytes of slot entry and 8 bytes of record */
	const INT64 slot_bound = DB_PAGESIZE / 12;
	const INT64 row_bits = std::min (slot_bound, bound_bits / (INT64) (nsects * DISK_SECTOR_NPAGES));

	m_bm_sectors.assign (collector.partsect_ftab, collector.partsect_ftab + nsects);
	std::sort (m_bm_sectors.begin (), m_bm_sectors.end (),
		   [] (const FILE_PARTIAL_SECTOR &a, const FILE_PARTIAL_SECTOR &b)
	{
	  return a.vsid.volid != b.vsid.volid ? a.vsid.volid < b.vsid.volid : a.vsid.sectid < b.vsid.sectid;
	});
	/* below 64 bits a page the bitmap marks pages only */
	m_bm_row_bits = row_bits >= 64 ? (int) row_bits : 0;
	/* the memory wrapper's operator new returns NULL when out of memory */
	m_bm_touched = new std::atomic<std::uint64_t>[nsects] ();
	m_bm_whole = new std::atomic<std::uint64_t>[nsects] ();
	const size_t row_words = (nsects * DISK_SECTOR_NPAGES * m_bm_row_bits + 63) / 64;
	if (m_bm_row_bits > 0)
	  {
	    m_bm_rows = new std::atomic<std::uint64_t>[row_words] ();
	  }
	if (m_bm_touched == nullptr || m_bm_whole == nullptr || (m_bm_row_bits > 0 && m_bm_rows == nullptr))
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
		    row_words * sizeof (std::uint64_t));
	    error_code = ER_OUT_OF_VIRTUAL_MEMORY;
	  }
	else
	  {
	    m_bm_next_sector.store (0);
	    m_bm_index_pending = parallelism;
	    m_bm_on = true;
	    scan_id->scan_stats.bitmap_heap = 1 + m_bm_row_bits;
	  }
      }
    if (collector.partsect_ftab != NULL)
      {
	db_private_free_and_init (thread_p, collector.partsect_ftab);
      }
    return error_code;
  }

  void
  input_handler_index::bitmap_mark (const OID *oid, int *sector_hint)
  {
    const int sectid = SECTOR_FROM_PAGEID (oid->pageid);
    int s = *sector_hint;

    if (s < 0 || m_bm_sectors[s].vsid.sectid != sectid || m_bm_sectors[s].vsid.volid != oid->volid)
      {
	int low = 0;
	int high = (int) m_bm_sectors.size () - 1;

	s = -1;
	while (low <= high)
	  {
	    const int mid = (low + high) / 2;
	    const VSID &vsid = m_bm_sectors[mid].vsid;

	    if (vsid.volid == oid->volid && vsid.sectid == sectid)
	      {
		s = mid;
		break;
	      }
	    if (vsid.volid < oid->volid || (vsid.volid == oid->volid && vsid.sectid < sectid))
	      {
		low = mid + 1;
	      }
	    else
	      {
		high = mid - 1;
	      }
	  }
	if (s < 0)
	  {
	    /* a sector the heap got after the list was taken */
	    return;
	  }
	*sector_hint = s;
      }

    const int page_offset = oid->pageid - SECTOR_FIRST_PAGEID (sectid);
    const std::uint64_t page_bit = (std::uint64_t) 1 << page_offset;

    if ((m_bm_sectors[s].page_bitmap & page_bit) == 0)
      {
	/* a page the heap got after the list was taken */
	return;
      }
    if (oid->slotid < m_bm_row_bits)
      {
	/* a page's bits follow the previous page's with no gap, so one word may hold bits of two pages */
	const std::uint64_t bit = ((std::uint64_t) s * DISK_SECTOR_NPAGES + page_offset) * m_bm_row_bits + oid->slotid;

	m_bm_rows[bit / 64].fetch_or ((std::uint64_t) 1 << (bit % 64), std::memory_order_relaxed);
      }
    else if ((m_bm_whole[s].load (std::memory_order_relaxed) & page_bit) == 0)
      {
	/* no room for this slot's bit: the page is read as a whole */
	m_bm_whole[s].fetch_or (page_bit, std::memory_order_relaxed);
      }
    if ((m_bm_touched[s].load (std::memory_order_relaxed) & page_bit) == 0)
      {
	m_bm_touched[s].fetch_or (page_bit, std::memory_order_relaxed);
      }
  }

  void
  input_handler_index::bitmap_leave_index_phase ()
  {
    std::lock_guard<std::mutex> lock (m_bm_mutex);

    if (--m_bm_index_pending == 0)
      {
	m_bm_cv.notify_all ();
      }
  }

  bool
  input_handler_index::bitmap_wait_index_phase ()
  {
    std::unique_lock<std::mutex> lock (m_bm_mutex);

    /* a worker that failed before its index phase never leaves it: it raised the interrupt instead */
    while (m_bm_index_pending > 0 && m_interrupt_p->get_code () == interrupt::interrupt_code::NO_INTERRUPT)
      {
	m_bm_cv.wait_for (lock, std::chrono::milliseconds (10));
      }
    return m_interrupt_p->get_code () == interrupt::interrupt_code::NO_INTERRUPT;
  }

  /* No-op: slot_iterator_index drives the leaf-page cursor directly. */
  int
  input_handler_index::initialize (THREAD_ENTRY *thread_p, HFID *hfid, SCAN_ID *scan_id)
  {
    return NO_ERROR;
  }

  int
  input_handler_index::finalize (THREAD_ENTRY *thread_p)
  {
    /* no-op per worker: siblings still scan m_key_val_ranges; freed in cleanup_keys on main thread. */
    return NO_ERROR;
  }

  void
  input_handler_index::cleanup_keys (THREAD_ENTRY *thread_p)
  {
    m_ranges.cleanup_keys (thread_p);
    /* per-helper local_key owned by slot_iterator's m_slot_key; cleared at OVERFLOW_SHARED exit. */
  }
}
