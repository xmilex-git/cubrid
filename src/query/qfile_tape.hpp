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
 * qfile_tape.hpp - Phase1 1A scan contract (redesign G005, issue #70).
 *
 * Axis-2 connection structure: a worker's output is a frozen, read-only Tape;
 * an ordered collection of Tapes (a Tapeset) is what a reader scans as one
 * logical tuple stream.  This replaces qfile_connect_list's cross-file
 * page-header next_vpid linkage (SSOT #75 §3.2 B1/B2, ADR 0002/0003/0004).
 *
 * A Tape's logical page space is [ membuf prefix (RAM) ] ++ [ private-file
 * pages (disk) ], addressed by PURE PAGE-OFFSET ARITHMETIC -- no per-page
 * next_vpid follow, no backing-type dispatch, no page directory, no occupancy
 * bitmap.  This is the deliberate departure from the e21917cfd scan that the
 * redesign exists to make: the only thing reused from qfile_scan_next is the
 * in-page tuple walk (tuple-length arithmetic); page advancement becomes
 * page_offset +/- 1 and the tape dimension threads every position branch.
 *
 * Phase 1 is EXPAND/additive: legacy single-backing lists carry no Tapeset
 * (QFILE_LIST_ID_TAPESET == NULL) and keep their exact behavior; the new scan
 * machine activates only when a Tapeset is present.
 */

#ifndef _QFILE_TAPE_HPP_
#define _QFILE_TAPE_HPP_

#include "query_list.h"		/* QFILE_LIST_ID / QFILE_LIST_SCAN_ID / QFILE_TUPLE_POSITION / page macros */
#include "storage_common.h"	/* PAGE_PTR / SCAN_CODE / SCAN_POSITION / SCAN_STATUS */
#include "thread_compat.hpp"	/* THREAD_ENTRY */

#include <vector>

namespace qfile
{
  /*
   * tape - one frozen, read-only logical page space.
   *
   * Logical page N is resolved by pure offset arithmetic (ADR 0003):
   *   N <  prefix_page_count() -> the in-RAM membuf prefix page N
   *   N >= prefix_page_count() -> private-file page (N - prefix_page_count())
   * No next_vpid chain, no directory.  A live list's pages are a dense
   * sequence 0..total_page_count()-1 (no mid-life dealloc).
   */
  class tape
  {
    public:
      virtual ~tape () = default;

      /* Resolve a logical page index to a 16KB CUBRID list-page pointer.
       * Returns nullptr on out-of-range or backing I/O error. */
      virtual PAGE_PTR page_at (THREAD_ENTRY *thread_p, int page_offset) = 0;

      /* Release a page handed out by page_at (no-op for RAM, unfix for file). */
      virtual void release_page (THREAD_ENTRY *thread_p, PAGE_PTR page) = 0;

      virtual int total_page_count () const = 0;
      virtual int prefix_page_count () const = 0;
  };

  /*
   * memory_tape - a Tape whose pages all live in RAM.
   *
   * Models a frozen membuf prefix (the all-RAM, no-spill case) and is the 1A
   * synthetic-test vehicle.  page_at() is a plain array index: the canonical
   * demonstration that addressing is offset arithmetic, independent of any
   * page buffer or file descriptor.
   */
  class memory_tape : public tape
  {
    public:
      memory_tape ();
      /* owns: if true, free() each page buffer on destruction. */
      explicit memory_tape (bool owns);
      ~memory_tape () override;

      /* Append one DB_PAGESIZE list-page buffer (ownership taken iff m_owns). */
      void append_page (char *page);
      void set_owns (bool owns)
      {
	m_owns = owns;
      }

      PAGE_PTR page_at (THREAD_ENTRY *thread_p, int page_offset) override;
      void release_page (THREAD_ENTRY *thread_p, PAGE_PTR page) override;
      int total_page_count () const override
      {
	return (int) m_pages.size ();
      }
      int prefix_page_count () const override
      {
	return (int) m_pages.size ();
      }

    private:
      std::vector<char *> m_pages;
      bool m_owns;

      memory_tape (const memory_tape &) = delete;
      memory_tape &operator= (const memory_tape &) = delete;
  };

  /*
   * tapeset - the ordered collection of Tapes a reader imports and scans as
   * one logical tuple stream (replaces cross-file next_vpid linkage).
   */
  class tapeset
  {
    public:
      tapeset ();
      ~tapeset ();

      /* Append a Tape; deleted on destruction iff m_owns_tapes. */
      void append_tape (tape *tape_p);
      void set_owns_tapes (bool owns)
      {
	m_owns_tapes = owns;
      }

      int tape_count () const
      {
	return (int) m_tapes.size ();
      }
      tape *get_tape (int idx) const
      {
	return (idx >= 0 && idx < (int) m_tapes.size ()) ? m_tapes[idx] : nullptr;
      }

    private:
      std::vector<tape *> m_tapes;
      bool m_owns_tapes;

      tapeset (const tapeset &) = delete;
      tapeset &operator= (const tapeset &) = delete;
  };

  /*
   * tapeset_scan_metrics - measurement hooks for the Tapeset scan path
   * (redesign G003, issue #68; the 1A-reachable slice).  Counts real scan
   * operations so a gate can assert structural invariants of the new scan
   * machine.  page_reads counts pages fetched ONLY via tape::page_at(): the
   * Tapeset scan never calls qmgr_get_old_page/pgbuf_fix, so this is the
   * scan-side of the pgbuf-bypass hard gate (SSOT #75 §6 (6)); the producer-
   * side counter for the per-worker private file backing lands with 1B.
   * Counters are plain longs and only advance on the multi-Tape path, which
   * real single-backing queries never enter -- zero cost for them.
   */
  struct tapeset_scan_metrics
  {
    long page_reads;		/* pages fetched via tape::page_at (all page I/O of the scan) */
    long pgbuf_fixes;		/* pgbuf BCB fixes done by the scan -- MUST stay 0 (bypass gate) */
    long tuple_reads;		/* tuples returned (forward + backward + jump) */
    long tape_advances;		/* cross-Tape transitions */
    long jumps;			/* jump() calls that landed on a tuple */
    long copies;		/* copy-mode retrieves */
    long peeks;			/* peek-mode retrieves */

    tapeset_scan_metrics ()
      : page_reads (0), pgbuf_fixes (0), tuple_reads (0), tape_advances (0), jumps (0), copies (0), peeks (0)
    {
    }
  };
  /*
   * tapeset_scan - the new scan state machine.
   *
   * The tape_idx dimension threads through every position branch
   * (S_BEFORE/S_ON/S_AFTER).  Within a Tape, page advancement is page_offset
   * +/- 1 arithmetic; the in-page tuple walk reuses tuple-length arithmetic.
   * S_END is returned only when the LAST Tape is exhausted.  Empty Tapes and
   * zero-tuple pages are skipped.  Backward and jump use the Tape-relative
   * coordinate (tape_idx, page_offset, byte_offset) -- no prev_vpid walk.
   */
  class tapeset_scan
  {
    public:
      explicit tapeset_scan (tapeset *ts);
      ~tapeset_scan ();

      SCAN_CODE forward (THREAD_ENTRY *thread_p, QFILE_TUPLE_RECORD *tuple_record_p, int peek);
      SCAN_CODE backward (THREAD_ENTRY *thread_p, QFILE_TUPLE_RECORD *tuple_record_p, int peek);
      void save_position (QFILE_TUPLE_POSITION *tuple_position_p) const;
      SCAN_CODE jump (THREAD_ENTRY *thread_p, const QFILE_TUPLE_POSITION *tuple_position_p,
		      QFILE_TUPLE_RECORD *tuple_record_p, int peek);
      void close (THREAD_ENTRY *thread_p);

      /* state-mirror getters for the QFILE_LIST_SCAN_ID bridge */
      SCAN_POSITION position () const
      {
	return m_position;
      }
      SCAN_STATUS status () const
      {
	return m_status;
      }
      QFILE_TUPLE curr_tpl () const
      {
	return m_curr_tpl;
      }
      PAGE_PTR curr_page () const
      {
	return m_page;
      }
      int curr_offset () const
      {
	return m_offset;
      }
      int curr_tplno () const
      {
	return m_tplno;
      }
      int tape_idx () const
      {
	return m_tape_idx;
      }
      int page_offset () const
      {
	return m_page_offset;
      }
      const tapeset_scan_metrics &metrics () const
      {
	return m_metrics;
      }

    private:
      SCAN_CODE retrieve (THREAD_ENTRY *thread_p, QFILE_TUPLE_RECORD *tuple_record_p, int peek);
      void release_page (THREAD_ENTRY *thread_p);
      void set_on (int tape_idx, int page_offset, PAGE_PTR page, int byte_offset, int tplno);

      tapeset *m_tapeset;	/* borrowed */
      SCAN_POSITION m_position;
      SCAN_STATUS m_status;
      int m_tape_idx;		/* current Tape (valid when S_ON) */
      int m_page_offset;	/* current logical page within the Tape */
      PAGE_PTR m_page;		/* currently-held page (released on advance/close) */
      int m_offset;		/* byte offset of current tuple within m_page */
      int m_tplno;		/* tuple number within the page */
      QFILE_TUPLE m_curr_tpl;	/* == m_page + m_offset */
      tapeset_scan_metrics m_metrics;

      tapeset_scan (const tapeset_scan &) = delete;
      tapeset_scan &operator= (const tapeset_scan &) = delete;
  };
}				/* namespace qfile */

/*
 * C++-linkage bridge used by list_file.c (compiled as C++) and exercised
 * directly by the synthetic N-Tape unit test.  Each takes a QFILE_LIST_SCAN_ID
 * whose embedded list_id carries a Tapeset (QFILE_LIST_ID_TAPESET).
 */
int qfile_tapeset_scan_open (QFILE_LIST_SCAN_ID *scan_id_p);
void qfile_tapeset_scan_close (THREAD_ENTRY *thread_p, QFILE_LIST_SCAN_ID *scan_id_p);
SCAN_CODE qfile_tapeset_scan_forward (THREAD_ENTRY *thread_p, QFILE_LIST_SCAN_ID *scan_id_p,
				      QFILE_TUPLE_RECORD *tuple_record_p, int peek);
SCAN_CODE qfile_tapeset_scan_backward (THREAD_ENTRY *thread_p, QFILE_LIST_SCAN_ID *scan_id_p,
				       QFILE_TUPLE_RECORD *tuple_record_p, int peek);
SCAN_CODE qfile_tapeset_scan_jump (THREAD_ENTRY *thread_p, QFILE_LIST_SCAN_ID *scan_id_p,
				   const QFILE_TUPLE_POSITION *tuple_position_p, QFILE_TUPLE_RECORD *tuple_record_p,
				   int peek);
void qfile_tapeset_scan_save_position (QFILE_LIST_SCAN_ID *scan_id_p, QFILE_TUPLE_POSITION *tuple_position_p);

/* Destroy a tapeset owned by a QFILE_LIST_ID (used by qfile_clear_list_id). */
void qfile_tapeset_destroy (void *tapeset_ptr);

#endif /* _QFILE_TAPE_HPP_ */
