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
 * qfile_chunk.hpp - Phase2 MIGRATE R2 parallel-read distribution
 *                   (redesign G008, issue #73; measurement hook for #68).
 *
 * Axis-2 parallel read (R2): when a frozen Tapeset feeds N downstream readers,
 * work is handed out in Chunks -- contiguous 64-page OFFSET ranges over each
 * Tape's whole logical page space ([membuf prefix in RAM] ++ [private file on
 * disk]) -- claimed via one shared atomic counter (work-stealing).  This is the
 * new-backing replacement for the old sector + per-sector-bitmap work-stealing
 * (QFILE_LIST_SECTOR_SCAN_INFO / PR #7173), which is retired at contract
 * (ADR 0003).
 *
 * No per-sector bitmap, no occupancy map: a live list's pages are a dense
 * 0..N-1 sequence (no mid-life dealloc -- qfile_truncate_list resets the whole
 * list) and the backing is a private flat file, so a chunk is pure offset
 * arithmetic.  The membuf prefix is simply the low offsets, range-distributed
 * like any other pages (RAM is immutable and shared-address-space, so any
 * reader reads it directly via tape::page_at).
 *
 * A claimed range is (tape_idx, start_page, page_count).  The reader resolves
 * each page via tape::page_at and reassembles an overflow tuple whose first
 * page it owns; continuation pages are skipped by every reader that does not
 * own the tuple's first page.
 *
 * Per-Tape metadata is two scalars (ADR 0003); the distributor keeps O(n_tapes)
 * cumulative-chunk offsets (n_tapes == parallel degree) and resolves a global
 * chunk index to a range on the fly -- it never materializes a per-page or
 * per-chunk table, so a multi-GB spill (millions of pages) costs nothing extra.
 */

#ifndef _QFILE_CHUNK_HPP_
#define _QFILE_CHUNK_HPP_

#include <atomic>
#include <vector>

namespace qfile
{
  class tapeset;		/* qfile_tape.hpp */

  /*
   * r2_metrics - measurement hook for the offset-range parallel-read
   * distribution (redesign G003/G008, issues #68/#73; SSOT #75 §6 (3)).
   * Records how the Tapeset's logical page space was partitioned among the N
   * readers so a gate can assert the work-stealing stays balanced (chunk-skew
   * coefficient of variation <= 15%) and covers every page exactly once.
   */
  struct r2_metrics
  {
    long total_pages;		/* pages across all Tapes of the Tapeset */
    long total_chunks;		/* chunks the page space was cut into */
    long chunks_claimed;	/* chunks actually handed out */
    long min_reader_pages;	/* fewest pages any reader claimed */
    long max_reader_pages;	/* most pages any reader claimed */
    double cov;			/* coefficient of variation across readers (stddev/mean) */

    r2_metrics ()
      : total_pages (0), total_chunks (0), chunks_claimed (0), min_reader_pages (0), max_reader_pages (0), cov (0.0)
    {
    }
  };

  /*
   * chunk_distributor - 64-page offset-range atomic work-stealing over a
   * frozen Tapeset's logical page space (ADR 0003 R2, tape-model "Parallel
   * read").  Construct once over the Tapeset (or explicit per-Tape page
   * counts) with the reader count; each reader repeatedly claims the next
   * chunk via next_chunk() until the space is exhausted.  Thread-safe through a
   * single atomic fetch_add over the global chunk index; each reader accounts
   * only its own page total (its own slot), so there is no per-page lock and no
   * shared cursor beyond the one atomic.
   */
  class chunk_distributor
  {
    public:
      /* tape-model Chunk == 64 pages (~1 MB). */
      static const int CHUNK_PAGES = 64;

      struct range
      {
	int tape_idx;		/* Tape within the Tapeset */
	int start_page;		/* first logical page offset of the chunk */
	int page_count;		/* pages in this chunk (<= chunk_pages) */
      };

      /* Build over a frozen Tapeset (reads each Tape's total_page_count). */
      chunk_distributor (const tapeset *ts, int n_readers, int chunk_pages = CHUNK_PAGES);
      /* Build over explicit per-Tape page counts (no Tapeset needed). */
      chunk_distributor (const std::vector<int> &tape_page_counts, int n_readers, int chunk_pages = CHUNK_PAGES);
      ~chunk_distributor () = default;

      /* Claim the next chunk for reader `reader_id` (0 .. n_readers-1).
       * Returns true and fills `out` with a (tape, start, count) range, or
       * false when the whole page space is exhausted. */
      bool next_chunk (int reader_id, range &out);

      /* Bump the shared chunk cursor forward past every chunk of `tape_idx`
       * that lies ENTIRELY within [0 .. run_end_page] (ADR 0006).  Called by a
       * reader that consumed (or skipped) an overflow run so its continuation-
       * only chunks are not separately claimed and re-read.  A boundary chunk
       * that also holds post-run tuple starts is left claimable.  Forward-only
       * and lock-free (CAS); safe under concurrent readers. */
      void skip_to_after (int tape_idx, int run_end_page);

      long total_pages () const
      {
	return m_total_pages;
      }
      long total_chunks () const
      {
	return m_total_chunks;
      }
      int n_readers () const
      {
	return m_n_readers;
      }
      /* Pages claimed so far by one reader. */
      long reader_pages (int reader_id) const;
      /* Coefficient of variation of per-reader page counts (stddev / mean);
       * 0 when nothing was claimed.  The chunk-skew balance metric. */
      double coefficient_of_variation () const;
      r2_metrics metrics () const;

    private:
      void build (const std::vector<int> &tape_page_counts);

      int m_chunk_pages;
      int m_n_readers;
      long m_total_pages;
      long m_total_chunks;
      std::vector<int> m_tape_pages;		/* page count per Tape */
      std::vector<long> m_tape_chunk_start;	/* cumulative chunk index per Tape (size n_tapes+1) */
      std::atomic<long> m_next;			/* global next chunk index */
      std::vector<long> m_reader_pages;		/* per-reader claimed pages (reader-private slot) */

      chunk_distributor (const chunk_distributor &) = delete;
      chunk_distributor &operator= (const chunk_distributor &) = delete;
  };
}				/* namespace qfile */

#endif /* _QFILE_CHUNK_HPP_ */
