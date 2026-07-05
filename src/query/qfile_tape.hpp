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
 * qfile_tape.hpp - Tape/Tapeset scan model.
 *
 * A worker's output is a frozen, read-only Tape; an ordered collection of
 * Tapes (a Tapeset) is what a reader scans as one logical tuple stream.  A
 * Tape's logical page space is [ membuf prefix (RAM) ] ++ [ private-file pages
 * (disk) ], addressed by PURE PAGE-OFFSET ARITHMETIC -- no per-page next_vpid
 * follow, no backing-type dispatch, no page directory, no occupancy bitmap.
 * Page advancement is page_offset +/- 1 and the tape dimension threads every
 * position branch; the in-page tuple walk is tuple-length arithmetic.
 *
 * Single-backing lists carry no Tapeset (QFILE_LIST_ID_TAPESET == NULL) and
 * keep their exact behavior; the scan machine activates only when a Tapeset is
 * present.
 */

#ifndef _QFILE_TAPE_HPP_
#define _QFILE_TAPE_HPP_

#include "query_list.h"		/* QFILE_LIST_ID / QFILE_LIST_SCAN_ID / QFILE_TUPLE_POSITION / page macros */
#include "storage_common.h"	/* PAGE_PTR / SCAN_CODE / SCAN_POSITION / SCAN_STATUS */
#include "thread_compat.hpp"	/* THREAD_ENTRY */
#include "qfile_buffile.hpp"	/* qfile::buffile private backing + TDE_ALGORITHM */
#include "qfile_chunk.hpp"	/* qfile::chunk_distributor (parallel-read distribution) */

#include <vector>
#include <cstddef>
#include <atomic>
#include <memory>
#include <cstdint>
#include <string>
#include <utility>

namespace qfile
{
  /*
   * tape - one frozen, read-only logical page space.
   *
   * Logical page N is resolved by pure offset arithmetic:
   *   N <  prefix_page_count() -> the in-RAM membuf prefix page N
   *   N >= prefix_page_count() -> private-file page (N - prefix_page_count())
   * No next_vpid chain, no directory.  A live list's pages are a dense
   * sequence 0..total_page_count()-1 (no mid-life dealloc).
   *
   * Frozen = immutable: a Tape carries NO mutable read state.  The one read
   * API is read_page_into with CALLER-owned scratch: every consumer
   * (tapeset_scan, tapeset_reader) supplies its own page buffer + TDE scratch,
   * so any number of interleaved or concurrent readers over the same Tape --
   * including arbitrary re-entrant jump()s -- never clobber each other.
   * Dropping a returned page needs no Tape call: a prefix page is Tape-owned
   * RAM, a file page lives in the caller's scratch, so scan close is
   * independent of Tape/Tapeset liveness.
   */
  class tape
  {
    public:
      virtual ~tape ();		/* releases adopted work_mem charges */

      /* Adopt work_mem accountant charges (byte/shard pairs) covering this
       * Tape's RAM prefix pages; released on destruction.  The reservation
       * follows page ownership (writer -> frozen Tape -> destroy), so a
       * holdable list's resident prefix stays accounted. */
      void adopt_wm_charges (std::vector<std::pair<std::size_t, int>> &&charges);

      /* Re-entrant read of logical page N into caller scratch.  RAM prefix
       * pages are returned directly (page_dest unused); a file page is read
       * into `page_dest` (DB_PAGESIZE) and returned.  `tde` carries the
       * per-caller cipher/plain scratch (non-NULL iff the backing is TDE).
       * nullptr on out-of-range / I/O error. */
      virtual PAGE_PTR read_page_into (THREAD_ENTRY *thread_p, int page_offset, char *page_dest,
				       tde_read_scratch *tde) const = 0;

      virtual int total_page_count () const = 0;
      virtual int prefix_page_count () const = 0;

    private:
      std::vector<std::pair<std::size_t, int>> m_wm_charges;	/* accountant (bytes, shard) per batch */
  };

  /*
   * memory_tape - a Tape whose pages all live in RAM.
   *
   * Models a frozen membuf prefix (the all-RAM, no-spill case).
   * read_page_into() is a plain array index (caller scratch unused):
   * addressing is offset arithmetic, independent of any page buffer or file
   * descriptor.
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

      PAGE_PTR read_page_into (THREAD_ENTRY *thread_p, int page_offset, char *page_dest,
			       tde_read_scratch *tde) const override;
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
   * resolved by pure offset arithmetic: page N < prefix_count is the RAM
   * prefix page N; otherwise it is buffile page (N - prefix_count).  This is
   * the spilled (overflowed) Tape; the tiny (no-spill) case stays a
   * memory_tape with no file.
   *
   * Fully immutable after construction: no read scratch lives here -- every
   * reader passes its own scratch to read_page_into, so interleaved scans over
   * the same frozen Tape are safe.
   */
  class buffile_tape : public tape
  {
    public:
      /* Takes ownership of the prefix RAM pages (each DB_PAGESIZE; freed on
       * destruction iff owns_prefix) and of the buffile (deleted -- which
       * closes+unlinks -- iff owns_buffile). */
      buffile_tape (std::vector<char *> &&prefix_pages, bool owns_prefix, buffile *bf, bool owns_buffile);
      ~buffile_tape () override;

      PAGE_PTR read_page_into (THREAD_ENTRY *thread_p, int page_offset, char *page_dest,
			       tde_read_scratch *tde) const override;
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

      buffile_tape (const buffile_tape &) = delete;
      buffile_tape &operator= (const buffile_tape &) = delete;
  };

  /*
   * tape_writer - membuf producer.
   *
   * The first `prefix_budget_pages` pages a worker produces stay in its
   * work_mem buffer (RAM prefix); once that budget is full, overflow pages are
   * appended to a lazily-created per-worker BufFile.  On freeze the run becomes
   * an immutable Tape -- a memory_tape if it never spilled (tiny / no-spill), a
   * buffile_tape otherwise -- with ZERO copy at freeze (the prefix vector and
   * the BufFile handle are transferred to the Tape).  TDE applies only to the
   * BufFile; the RAM prefix is plaintext.
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
       * to it.  The writer is spent afterwards.  Returns NULL on error --
       * including when a prior append_page failed (see failed ()). */
      tape *freeze (THREAD_ENTRY *thread_p);

      /* Sticky error latch: true once any append_page has failed.  A failed
       * writer must never freeze into a "successful" (silently short) Tape, so
       * freeze () returns NULL while set; also the freeze-time OOM recovery hook. */
      bool failed () const
      {
	return m_failed;
      }

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
      int append_page_impl (THREAD_ENTRY *thread_p, const PAGE_PTR list_page);
      int ensure_buffile (THREAD_ENTRY *thread_p);
      bool wm_reserve_batch ();
      void wm_trim_excess ();
      void wm_release_all ();

      int m_prefix_budget;
      TDE_ALGORITHM m_tde_algo;
      std::string m_dir;
      std::uint64_t m_seq;
      unsigned int m_worker_id;
      std::vector<char *> m_prefix;	/* owned until freeze transfers */
      buffile *m_buffile;		/* lazily created; owned until freeze transfers */
      bool m_frozen;
      bool m_failed;		/* sticky: an append_page failed; freeze must not fake success */
      std::vector<std::pair<std::size_t, int>> m_wm_charges;	/* accountant (bytes, shard) per batch */
      int m_wm_reserved_pages;	/* prefix pages covered by m_wm_charges */

      tape_writer (const tape_writer &) = delete;
      tape_writer &operator= (const tape_writer &) = delete;
  };

  /*
   * tapeset - the ordered collection of Tapes a reader imports and scans as
   * one logical tuple stream.
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

      /* Move all Tapes from src into this Tapeset.  Ownership transfers
       * per-tape (src's slot is NULLed the instant a Tape leaves it), so a
       * mid-transfer allocation failure never leaves a Tape owned by both.
       * src is left empty and must not be used as a Tape source again. */
      void transfer_tapes_from (tapeset *src);

      /* Open-scan liveness cell (debug assert).  A shared_ptr<atomic<int>> so
       * the count cell outlives the Tapeset itself: tapeset_scan copies this
       * shared_ptr at construction and increments/decrements ITS OWN copy,
       * never the Tapeset object -- the cell must be reachable after the
       * Tapeset is destroyed without dereferencing the (possibly freed)
       * Tapeset.  Debug-only consumer: qfile_list_id_open_scan_count()
       * (list_file.c), which casts and loads this cell directly. */
      using scan_count_cell = std::shared_ptr<std::atomic<int>>;
      const scan_count_cell &open_scan_cell () const
      {
	return m_open_scan_cell;
      }

    private:
      std::vector<tape *> m_tapes;
      bool m_owns_tapes;
      scan_count_cell m_open_scan_cell;

      tapeset (const tapeset &) = delete;
      tapeset &operator= (const tapeset &) = delete;
  };

  /*
   * tapeset_scan_metrics - measurement hooks for the Tapeset scan path.
   * Counts real scan operations so a gate can assert structural invariants of
   * the scan machine.  page_reads counts pages fetched ONLY via
   * tape::read_page_into(): the Tapeset scan never calls
   * qmgr_get_old_page/pgbuf_fix, so this is the scan-side of the pgbuf-bypass
   * hard gate.  Counters are plain longs and only advance on the multi-Tape
   * path, which single-backing queries never enter -- zero cost for them.
   */
  struct tapeset_scan_metrics
  {
    long page_reads;		/* pages fetched via tape::read_page_into (all page I/O of the scan) */
    long pgbuf_fixes;		/* pgbuf BCB fixes done by the scan -- MUST stay 0 (bypass gate) */
    long tuple_reads;		/* tuples returned (forward + backward + jump) */
    long tape_advances;		/* cross-Tape transitions */
    long jumps;			/* jump() calls that landed on a tuple */
    long copies;		/* copy-mode retrieves */
    long peeks;			/* peek-mode retrieves */
    long cache_hits;		/* fetch_page served from the per-scan read cache (no pread) -- #144 P1-2 */
    long cache_misses;		/* fetch_page that issued a read_page_into (a pread for file pages) */

    tapeset_scan_metrics ()
      : page_reads (0), pgbuf_fixes (0), tuple_reads (0), tape_advances (0), jumps (0), copies (0), peeks (0)
      , cache_hits (0), cache_misses (0)
    {
    }
  };
  /*
   * tapeset_scan - the scan state machine.
   *
   * The tape_idx dimension threads through every position branch
   * (S_BEFORE/S_ON/S_AFTER).  Within a Tape, page advancement is page_offset
   * +/- 1 arithmetic; the in-page tuple walk reuses tuple-length arithmetic.
   * S_END is returned only when the LAST Tape is exhausted.  Empty Tapes and
   * zero-tuple pages are skipped.  Backward and jump use the Tape-relative
   * coordinate (tape_idx, page_offset, byte_offset) -- no prev_vpid walk.
   *
   * Caller-scratch contract: the scan OWNS its file-page read scratch -- a small
   * per-scan LRU read cache (m_cache[READ_CACHE_SLOTS] + m_read_scratch, slots
   * lazily allocated on first file page; all-RAM Tapesets never allocate).  Every
   * page fetch -- forward, backward, AND jump -- goes through fetch_page() into
   * that scan-owned cache, so N scans interleaved over one frozen Tapeset
   * (tapeset-backed derived-table/CTE self-join) each keep a PRIVATE working set:
   * the cache is per-reader, never shared (avoids the #126/K-12 shared-read-cache
   * contamination hazard).
   *
   * #144 P1-2: the cache collapses repeated pread of a small hot working set of
   * backing pages -- the PHJ probe's random tapeset jumps re-read the same
   * OS-cached build pages millions of times (per-access pread), which is pure
   * syscall+copy CPU.  The frozen backing is append-all-then-freeze immutable, so
   * a cached page can never go stale -- no invalidation is ever needed.  The
   * cache is small (READ_CACHE_SLOTS pages), purely additive (no invariant
   * changed), and reader-local scratch NOT charged to work_mem (like the single
   * m_readbuf it replaces, and like m_reasm).
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
      /* fetch logical page `page_offset` of `tape_p` into the scan-owned
       * scratch (lazily allocated for the first file page); see the
       * caller-scratch contract in the class comment. */
      PAGE_PTR fetch_page (THREAD_ENTRY *thread_p, tape *tape_p, int page_offset);

      tapeset *m_tapeset;	/* borrowed */
      SCAN_POSITION m_position;
      SCAN_STATUS m_status;
      int m_tape_idx;		/* current Tape (valid when S_ON) */
      int m_page_offset;	/* current logical page within the Tape */
      PAGE_PTR m_page;		/* currently-held page (released on advance/close) */
      int m_offset;		/* byte offset of current tuple within m_page */
      int m_tplno;		/* tuple number within the page */
      QFILE_TUPLE m_curr_tpl;	/* == m_page + m_offset */
      bool m_curr_overflow;	/* current tuple is a reassembled overflow run */
      int m_overflow_run_end;	/* last logical page of that run (forward O(1) skip) */
      /* #144 P1-2: small per-scan LRU cache of backing (file) pages.  D4 escape
       * hatch: small (READ_CACHE_SLOTS), additive, scan-owned; slot buffers are
       * lazily malloc'd on first use (all-RAM Tapesets never allocate). */
      static const int READ_CACHE_SLOTS = 8;
      struct read_cache_slot
      {
	char *buf;		/* DB_PAGESIZE, lazily allocated */
	const tape *tape_p;	/* owning Tape of the cached page (identity key) */
	int page_offset;	/* logical page key within tape_p */
	bool valid;
	unsigned long tick;	/* LRU stamp (m_cache_tick at last use) */
      };
      read_cache_slot m_cache[READ_CACHE_SLOTS];
      unsigned long m_cache_tick;	/* monotonic LRU clock */
      tde_read_scratch m_read_scratch;	/* scan-owned TDE decrypt scratch for fetch_page */
      char *m_reasm_raw;	/* reassembly read scratch (file continuation pages) */
      PAGE_PTR m_reasm;
      tde_read_scratch m_reasm_tde;
      char *m_peek_reasm_raw;	/* scan-owned overflow-PEEK result buffer; freed here, never by caller */
      int m_peek_reasm_cap;
      tapeset_scan_metrics m_metrics;
      long m_pgbuf_fix_baseline;	/* pgbuf_get_fix_debug_count() at construction */
      tapeset::scan_count_cell m_open_scan_cell;	/* debug assert; never dereferences the Tapeset (see class comment) */

      tapeset_scan (const tapeset_scan &) = delete;
      tapeset_scan &operator= (const tapeset_scan &) = delete;
  };

  /*
   * tapeset_reader - one participant's view of a frozen Tapeset for parallel
   * read.  Claims 64-page Chunks from a shared chunk_distributor and returns
   * the tuples it owns.  All mutable read state is per-reader (own page scratch
   * + own TDE scratch); the only shared mutable state is the distributor's
   * atomic chunk cursor.  An overflow tuple is reassembled by the reader that
   * owns its first page; a reader landing on a continuation page whose start
   * precedes its chunk skips the run in O(1) and bumps the shared cursor past
   * it (so a giant run is read once, not once per skipper).
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
			    QFILE_TUPLE_RECORD *tuple_record_p, int peek);

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

      char *m_peek_reasm_raw;	/* reader-owned overflow-PEEK result buffer; freed here, never by caller */
      int m_peek_reasm_cap;

      tapeset_scan_metrics m_metrics;
      long m_pgbuf_fix_baseline;	/* pgbuf_get_fix_debug_count() at construction */

      tapeset_reader (const tapeset_reader &) = delete;
      tapeset_reader &operator= (const tapeset_reader &) = delete;
  };

#if !defined (NDEBUG)
  /* OOM fault injection for freeze() (debug-only).  Arm to make the Nth
   * subsequent tape allocation inside freeze() (memory_tape / buffile_tape)
   * behave as the SERVER_MODE noexcept-new does under OOM -- return NULL --
   * so the ownership-recovery path is exercised without exhausting memory.
   * nth <= 0 disarms.  Also armed at boot from env CUBRID_WM_FAULT_ALLOC_AT.
   * Compiled out of release. */
  void tape_fault_arm_alloc_fail (int nth);
#endif /* !NDEBUG */
}				/* namespace qfile */

/* ------------------------------------------------------------------ */
/* Overflow-continuation page-header helpers.                         */
/* ------------------------------------------------------------------ */
/* The tapeset per-worker flat backing lays an overflow tuple out as a
 * CONTIGUOUS run of logical pages addressed by offset (no VPID chain).  Both
 * the start and continuation pages carry the existing
 * QFILE_OVERFLOW_TUPLE_COUNT_FLAG count; they are told apart by the first-page
 * offset stored in the field vacated by the old overflow VPID: a start page
 * stores its OWN logical offset, a continuation stores the start's offset
 * (always < its own) plus the run-end offset.  A reader thus learns the whole
 * run from any one page. */

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

/* Client-fetch-over-Tapeset bridges: serve a tapeset-backed top-level result
 * straight from its frozen Tapeset (no pgbuf materialize).  page_count is the
 * total logical page count across the ordered Tapes; read_global_page reads
 * the `global_index`-th logical page (0-based, across Tape boundaries) into a
 * caller DB_PAGESIZE buffer (TDE handled internally).  `local_offset_out`
 * (optional) receives the page's TAPE-LOCAL logical offset: the overflow-run
 * markers on the page are local coordinates, so the caller translates
 * local<->global as global = global_index - *local + local'.  Returns
 * NO_ERROR/ER_FAILED. */
int qfile_tapeset_page_count (const QFILE_LIST_ID *list_id_p);
int qfile_tapeset_read_global_page (THREAD_ENTRY *thread_p, const QFILE_LIST_ID *list_id_p, int global_index,
				    char *page_dest, int *local_offset_out);

/* Create a tapeset-backed producer for a SORT output list: membuf prefix =
 * work_mem pages, TDE algo from tde_encrypted, process-unique BufFile
 * sequence.  Returns an opaque writer (NULL on failure), consumed by
 * qfile_producer_freeze_tapeset / delete (qfile::tape_writer *). */
void *qfile_producer_create_for_list (THREAD_ENTRY *thread_p, bool tde_encrypted);
/* import src's Tapeset backing into dest (SORT output migration). */
int qfile_tapeset_import (THREAD_ENTRY *thread_p, QFILE_LIST_ID *dest, QFILE_LIST_ID *src);
int qfile_producer_append (THREAD_ENTRY *thread_p, void *writer, const PAGE_PTR full_page);
void *qfile_producer_freeze_tapeset (THREAD_ENTRY *thread_p, void *writer);

#endif /* _QFILE_TAPE_HPP_ */
