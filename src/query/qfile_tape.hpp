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
#include "qfile_buffile.hpp"	/* qfile::buffile (1B private backing) + TDE_ALGORITHM */
#include "qfile_chunk.hpp"	/* qfile::chunk_distributor (R2 distribution) */

#include <vector>
#include <cstdint>
#include <string>

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

      /* Re-entrant read of logical page N into caller scratch (ADR 0005), used
       * by concurrent R2 readers.  RAM prefix pages are returned directly
       * (page_dest unused); a file page is read into `page_dest` (DB_PAGESIZE)
       * and returned.  `tde` carries the per-reader cipher/plain scratch
       * (non-NULL iff the backing is TDE).  nullptr on out-of-range / I/O error. */
      virtual PAGE_PTR read_page_into (THREAD_ENTRY *thread_p, int page_offset, char *page_dest,
				       tde_read_scratch *tde) = 0;

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
      PAGE_PTR read_page_into (THREAD_ENTRY *thread_p, int page_offset, char *page_dest,
			       tde_read_scratch *tde) override;
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
   * buffile_tape - a Tape whose logical page space is
   *   [ membuf prefix (RAM, DB_PAGESIZE pages) ] ++ [ qfile::buffile (disk) ]
   * resolved by pure offset arithmetic (ADR 0003): page N < prefix_count is the
   * RAM prefix page N; otherwise it is buffile page (N - prefix_count).  The
   * spilled (overflowed) Tape produced by membuf Option-A freeze; the tiny
   * (no-spill) case stays a memory_tape with no file.
   *
   * Single-reader: file pages are read into one owned scratch buffer returned
   * by page_at and reused on the next page_at (the scan releases the held page
   * before advancing).  Parallel multi-reader (R2, offset-range work-stealing)
   * gives each reader its own view and is Phase 2.
   */
  class buffile_tape : public tape
  {
    public:
      /* Takes ownership of the prefix RAM pages (each DB_PAGESIZE; freed on
       * destruction iff owns_prefix) and of the buffile (deleted -- which
       * closes+unlinks -- iff owns_buffile). */
      buffile_tape (std::vector<char *> &&prefix_pages, bool owns_prefix, buffile *bf, bool owns_buffile);
      ~buffile_tape () override;

      PAGE_PTR page_at (THREAD_ENTRY *thread_p, int page_offset) override;
      void release_page (THREAD_ENTRY *thread_p, PAGE_PTR page) override;
      PAGE_PTR read_page_into (THREAD_ENTRY *thread_p, int page_offset, char *page_dest,
			       tde_read_scratch *tde) override;
      int total_page_count () const override;
      int prefix_page_count () const override
      {
	return (int) m_prefix.size ();
      }

      const buffile *backing () const
      {
	return m_buffile;
      }

    private:
      std::vector<char *> m_prefix;
      bool m_owns_prefix;
      buffile *m_buffile;
      bool m_owns_buffile;
      char *m_readbuf;		/* DB_PAGESIZE scratch for file pages (single reader R1) */
      tde_read_scratch m_read_scratch;	/* per-tape R1 TDE decrypt scratch (single reader) */

      buffile_tape (const buffile_tape &) = delete;
      buffile_tape &operator= (const buffile_tape &) = delete;
  };

  /*
   * tape_writer - membuf Option-A producer (SSOT #75 §2.2).
   *
   * The first `prefix_budget_pages` pages a worker produces stay in its
   * work_mem buffer (RAM prefix); once that budget is full, overflow pages are
   * appended to a lazily-created per-worker BufFile.  On freeze the run becomes
   * an immutable Tape -- a memory_tape if it never spilled (tiny / no-spill), a
   * buffile_tape otherwise -- with ZERO copy at freeze (the prefix vector and
   * the BufFile handle are transferred to the Tape, matching ADR 0001's
   * zero-copy ownership move).  TDE applies only to the BufFile; the RAM prefix
   * is plaintext.
   *
   * Phase 1 builds and unit-verifies this mechanism; real operators feeding it
   * is Phase 2.
   */
  class tape_writer
  {
    public:
      tape_writer (int prefix_budget_pages, TDE_ALGORITHM tde_algo, const std::string &dir, std::uint64_t seq,
		   unsigned int worker_id);
      ~tape_writer ();

      /* Append one produced DB_PAGESIZE list page (RAM prefix while under
       * budget, else BufFile).  Returns NO_ERROR or an error code. */
      int append_page (THREAD_ENTRY *thread_p, const PAGE_PTR list_page);

      /* Freeze into a read-only Tape; ownership of prefix + BufFile transfers
       * to it.  The writer is spent afterwards.  Returns NULL on error. */
      tape *freeze (THREAD_ENTRY *thread_p);

      bool spilled () const
      {
	return m_buffile != NULL;
      }
      int prefix_pages () const
      {
	return (int) m_prefix.size ();
      }
      int file_pages () const;
      const buffile_metrics *file_metrics () const;

    private:
      int ensure_buffile (THREAD_ENTRY *thread_p);

      int m_prefix_budget;
      TDE_ALGORITHM m_tde_algo;
      std::string m_dir;
      std::uint64_t m_seq;
      unsigned int m_worker_id;
      std::vector<char *> m_prefix;	/* owned until freeze transfers */
      buffile *m_buffile;		/* lazily created; owned until freeze transfers */
      bool m_frozen;

      tape_writer (const tape_writer &) = delete;
      tape_writer &operator= (const tape_writer &) = delete;
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
      bool m_curr_overflow;	/* current tuple is a reassembled overflow run (ADR 0006) */
      int m_overflow_run_end;	/* last logical page of that run (forward O(1) skip) */
      char *m_reasm_raw;	/* reassembly read scratch (file continuation pages) */
      PAGE_PTR m_reasm;
      tde_read_scratch m_reasm_tde;
      tapeset_scan_metrics m_metrics;

      tapeset_scan (const tapeset_scan &) = delete;
      tapeset_scan &operator= (const tapeset_scan &) = delete;
  };

  /*
   * tapeset_reader - one participant's view of a frozen Tapeset for R2 parallel
   * read (ADR 0005/0006).  Claims 64-page Chunks from a shared chunk_distributor
   * and returns the tuples it owns.  All mutable read state is per-reader (own
   * page scratch + own TDE scratch); the only shared mutable state is the
   * distributor's atomic chunk cursor.  An overflow tuple is reassembled by the
   * reader that owns its first page; a reader landing on a continuation page
   * whose start precedes its chunk skips the run in O(1) and bumps the shared
   * cursor past it (so a giant run is read once, not once per skipper).
   */
  class tapeset_reader
  {
    public:
      tapeset_reader (tapeset *ts, chunk_distributor *dist, int reader_id);
      ~tapeset_reader ();

      /* Next tuple this reader owns; S_END when its share is drained. */
      SCAN_CODE next (THREAD_ENTRY *thread_p, QFILE_TUPLE_RECORD *tuple_record_p, int peek);

      const tapeset_scan_metrics &metrics () const
      {
	return m_metrics;
      }

    private:
      SCAN_CODE emit_in_page (THREAD_ENTRY *thread_p, QFILE_TUPLE_RECORD *tuple_record_p, int peek);
      SCAN_CODE reassemble (THREAD_ENTRY *thread_p, tape *tp, int first_page, int run_end, int tuple_len,
			    QFILE_TUPLE_RECORD *tuple_record_p);

      tapeset *m_tapeset;	/* borrowed */
      chunk_distributor *m_dist;	/* borrowed (shared across readers) */
      int m_reader_id;

      chunk_distributor::range m_range;	/* current claimed chunk */
      bool m_have_chunk;
      int m_cur_page;		/* next page to inspect within m_range's Tape */

      /* tuple walk within the current normal page */
      PAGE_PTR m_page;
      int m_count;
      int m_offset;
      int m_tplno;

      char *m_page_raw;		/* DB_PAGESIZE scratch for file pages */
      PAGE_PTR m_page_buf;
      tde_read_scratch m_tde;

      tapeset_scan_metrics m_metrics;

      tapeset_reader (const tapeset_reader &) = delete;
      tapeset_reader &operator= (const tapeset_reader &) = delete;
  };
}				/* namespace qfile */

/* ------------------------------------------------------------------ */
/* Overflow-continuation page-header helpers (ADR 0006).              */
/*                                                                    */
/* The NEW per-worker flat backing lays an overflow tuple out as a    */
/* CONTIGUOUS run of logical pages addressed by offset (no VPID        */
/* chain).  Both the start and continuation pages carry the existing  */
/* QFILE_OVERFLOW_TUPLE_COUNT_FLAG count; they are told apart by the   */
/* first-page offset stored in the field vacated by the old overflow  */
/* VPID: a start page stores its OWN logical offset, a continuation    */
/* stores the start's offset (always < its own) plus the run-end       */
/* offset.  A reader thus learns the whole run from any one page.      */
/* ------------------------------------------------------------------ */

/* Mark `page` (at logical offset `self_page_offset`) the START of an overflow
 * run.  The total tuple length is read from the page's first tuple header. */
void qfile_overflow_set_start (PAGE_PTR page, int self_page_offset);
/* Mark `page` a CONTINUATION of the run starting at `first_page_offset` and
 * ending at `run_end_offset` (both Tape-relative logical page offsets). */
void qfile_overflow_set_continuation (PAGE_PTR page, int first_page_offset, int run_end_offset);
/* True iff `page` is an overflow page (start or continuation). */
bool qfile_overflow_is_overflow_page (const PAGE_PTR page);
/* The run's first (start) logical page offset stored on `page`. */
int qfile_overflow_first_page (const PAGE_PTR page);
/* The run's last logical page offset stored on a continuation `page`. */
int qfile_overflow_run_end (const PAGE_PTR page);
/* Number of contiguous pages a `tuple_length`-byte overflow tuple occupies. */
int qfile_overflow_run_pages (int tuple_length);

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

/* Phase2 2A-1 producer bridge (redesign #78): build a NEW-backed list by
 * appending its completed list-pages to a tape_writer, then freezing into a
 * single-Tape Tapeset.  Pairs with the qfile producer hook in list_file.c.
 * The writer is created without a backing list (pgbuf-bypassed) and consumed
 * (deleted) by freeze_tapeset / destroy. */
void *qfile_producer_create (int prefix_budget_pages, TDE_ALGORITHM tde_algo, unsigned long long seq,
			     unsigned int worker_id);
int qfile_producer_append (THREAD_ENTRY *thread_p, void *writer, const PAGE_PTR full_page);
void *qfile_producer_freeze_tapeset (THREAD_ENTRY *thread_p, void *writer);
void qfile_producer_destroy (void *writer);

/*
 * In-server self-test of the holdable-result lifecycle (Phase1 1C, redesign
 * G007 #72): build a spilled Tapeset behind a QFILE_LIST_ID, reparent its
 * ownership transaction -> session via qfile_copy_list_id(MOVE) with zero copy
 * (census unchanged), read the remaining tuples post-reparent (robust parity),
 * then tear the session list_id down via qfile_clear_list_id and assert
 * orphan-zero -- the private file is unlinked AND the RAM prefix is freed
 * (ADR 0001).  Exercises a real on-disk file (and the TDE-encrypted backing
 * when a cipher is loaded), which the bootless unit test cannot.  Gated by env
 * CUBRID_HELDTAPE_SELFTEST.  Returns 0 on PASS.
 */
int qfile_heldtape_selftest (THREAD_ENTRY *thread_p);

/* In-server self-test of N-reader concurrent read over a frozen (TDE) Tape
 * (ADR 0005, #78 2A-0).  Gated by env CUBRID_TAPEREAD_SELFTEST (debug). */
int qfile_taperead_selftest (THREAD_ENTRY *thread_p);

/* In-server self-test of the 2A-1 NEW-backing producer hook (redesign #78):
 * produce a list via qfile_add_tuple_to_list onto a producer_writer_-attached
 * QFILE_LIST_ID, close (freeze into a Tapeset), then scan via tapeset_scan and
 * assert robust parity.  Gated by env CUBRID_PRODUCER_SELFTEST (debug). */
int qfile_producer_selftest (THREAD_ENTRY *thread_p);

#endif /* _QFILE_TAPE_HPP_ */
