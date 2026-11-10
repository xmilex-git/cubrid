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

/* px_scan_slot_iterator_index.cpp — facade dispatcher: drain-state vs. slot-loop. */

#include "px_scan_slot_iterator_index.hpp"
#include "px_scan_input_handler_index.hpp"

#include "scan_manager.h"

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace parallel_scan
{
  slot_iterator_index::slot_iterator_index ()
    : m_input_handler (nullptr),
      m_slot_walker (),
      m_drain_fsm (),
      m_bm_heap_phase (false),
      m_bm_page_on (false),
      m_bm_page_whole (false),
      m_bm_sector_idx (0),
      m_bm_page_offset (0),
      m_bm_next_slot (0)
  {
    VPID_SET_NULL (&m_bm_vpid);
    OID_SET_NULL (&m_bm_oid);
    /* FSM pokes walker fields via friendship; wire pointer up-front. */
    m_drain_fsm.wire_owner (&m_slot_walker);
  }

  slot_iterator_index::~slot_iterator_index ()
  {
  }

  int
  slot_iterator_index::initialize (THREAD_ENTRY *thread_p, SCAN_ID *scan_id, val_descr *vd)
  {
    m_slot_walker.wire (scan_id, vd, m_input_handler, &m_drain_fsm);
    return NO_ERROR;
  }

  int
  slot_iterator_index::finalize (THREAD_ENTRY *thread_p)
  {
    if (m_bm_page_on)
      {
	/* the scan stopped inside a page of the heap phase */
	m_slot_walker.release_bitmap_page (thread_p);
	m_bm_page_on = false;
      }
    m_drain_fsm.cleanup_on_reset (thread_p);
    m_slot_walker.cleanup_on_reset (thread_p);
    m_slot_walker.unwire ();
    m_input_handler = nullptr;
    return NO_ERROR;
  }

  int
  slot_iterator_index::set_page (THREAD_ENTRY *thread_p, PAGE_PTR page, INT16 slot_hint)
  {
    /* set_page-top semantics: prior leaf-side state torn down first; FSM cleanup runs even if walker had no page. */
    m_drain_fsm.cleanup_on_reset (thread_p);
    return m_slot_walker.set_page (thread_p, page, slot_hint);
  }

  /*
   * Returns S_SUCCESS when a qualified row is available,
   *         S_END when the current page is exhausted,
   *         S_ERROR on error.
   */
  SCAN_CODE
  slot_iterator_index::next_qualified_slot_with_peek (THREAD_ENTRY *thread_p)
  {
    if (m_bm_page_on)
      {
	return next_bitmap_row (thread_p);
      }
    if (m_bm_heap_phase)
      {
	/* a page that was deallocated since the scan started */
	return S_END;
      }

    /* If a drain is in progress, continue it; otherwise walk leaf slots. */
    if (!m_drain_fsm.is_idle ())
      {
	SCAN_CODE sc = m_drain_fsm.drain_next_oid (thread_p);
	if (sc != S_END)
	  {
	    return sc;
	  }
	/* late-joiner (walker has no leaf page) cannot fall through to leaf-slot loop. */
	if (!m_slot_walker.has_page ())
	  {
	    return S_END;
	  }
      }
    return m_slot_walker.scan_next_slot (thread_p);
  }

  int
  slot_iterator_index::set_bitmap_page (THREAD_ENTRY *thread_p, int sector_idx, int page_offset)
  {
    const FILE_PARTIAL_SECTOR &sector = m_input_handler->bitmap_sector (sector_idx);

    m_bm_sector_idx = sector_idx;
    m_bm_page_offset = page_offset;
    m_bm_vpid.volid = sector.vsid.volid;
    m_bm_vpid.pageid = SECTOR_FIRST_PAGEID (sector.vsid.sectid) + page_offset;
    m_bm_page_whole = ((m_input_handler->bitmap_whole (sector_idx) >> page_offset) & 1) != 0;
    m_bm_next_slot = 0;
    OID_SET_NULL (&m_bm_oid);
    m_bm_heap_phase = true;
    m_bm_page_on = true;

    /* a page vacuum deallocated since the scan started is passed here, whichever way it is marked */
    int error_code = m_slot_walker.fix_bitmap_page (thread_p, &m_bm_vpid, &m_bm_page_on);
    if (error_code != NO_ERROR)
      {
	m_bm_page_on = false;
	return error_code;
      }
    return NO_ERROR;
  }

  SCAN_CODE
  slot_iterator_index::next_bitmap_row (THREAD_ENTRY *thread_p)
  {
    SCAN_CODE scan;

    if (m_bm_page_whole)
      {
	/* a page marked as a whole is not read through its row bits as well: a row would come out twice */
	scan = m_slot_walker.next_whole_page_row (thread_p, &m_bm_vpid, &m_bm_oid);
	if (scan != S_SUCCESS)
	  {
	    m_slot_walker.release_bitmap_page (thread_p);
	    m_bm_page_on = false;
	  }
	return scan;
      }

    while (true)
      {
	const int slot = m_input_handler->bitmap_next_row (m_bm_sector_idx, m_bm_page_offset, m_bm_next_slot);

	if (slot < 0)
	  {
	    m_slot_walker.release_bitmap_page (thread_p);
	    m_bm_page_on = false;
	    return S_END;
	  }
	m_bm_next_slot = slot + 1;

	OID oid;

	oid.volid = m_bm_vpid.volid;
	oid.pageid = m_bm_vpid.pageid;
	oid.slotid = (PGSLOTID) slot;

	scan = m_slot_walker.process_bitmap_row (thread_p, &oid);
	if (scan == S_END)
	  {
	    /* not visible or filtered out */
	    continue;
	  }
	if (scan == S_ERROR)
	  {
	    m_slot_walker.release_bitmap_page (thread_p);
	    m_bm_page_on = false;
	  }
	return scan;
      }
  }
}
