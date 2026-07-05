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
 * qfile_chunk.cpp - parallel-read distribution.  See qfile_chunk.hpp.
 */

#include "qfile_chunk.hpp"
#include "qfile_tape.hpp"	/* qfile::tapeset / tape::total_page_count */

#include <algorithm>
#include <cmath>

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace qfile
{
  const int chunk_distributor::CHUNK_PAGES;

  chunk_distributor::chunk_distributor (const tapeset *ts, int n_readers, int chunk_pages)
    : m_chunk_pages (chunk_pages > 0 ? chunk_pages : CHUNK_PAGES)
    , m_n_readers (n_readers > 0 ? n_readers : 1)
    , m_total_pages (0)
    , m_total_chunks (0)
    , m_tape_pages ()
    , m_tape_chunk_start ()
    , m_next (0)
    , m_reader_pages ()
  {
    std::vector<int> counts;
    if (ts != NULL)
      {
	const int ntapes = ts->tape_count ();
	counts.reserve (ntapes);
	for (int i = 0; i < ntapes; i++)
	  {
	    const tape *tp = ts->get_tape (i);
	    counts.push_back (tp != NULL ? tp->total_page_count () : 0);
	  }
      }
    build (counts);
  }

  chunk_distributor::chunk_distributor (const std::vector<int> &tape_page_counts, int n_readers, int chunk_pages)
    : m_chunk_pages (chunk_pages > 0 ? chunk_pages : CHUNK_PAGES)
    , m_n_readers (n_readers > 0 ? n_readers : 1)
    , m_total_pages (0)
    , m_total_chunks (0)
    , m_tape_pages ()
    , m_tape_chunk_start ()
    , m_next (0)
    , m_reader_pages ()
  {
    build (tape_page_counts);
  }

  void
  chunk_distributor::build (const std::vector<int> &tape_page_counts)
  {
    const int ntapes = (int) tape_page_counts.size ();
    m_tape_pages.resize (ntapes);
    m_tape_chunk_start.resize (ntapes + 1);

    long cum_chunks = 0;
    m_tape_chunk_start[0] = 0;
    for (int i = 0; i < ntapes; i++)
      {
	const int pages = tape_page_counts[i] > 0 ? tape_page_counts[i] : 0;
	m_tape_pages[i] = pages;
	m_total_pages += pages;
	/* ceil(pages / chunk_pages); an empty Tape contributes 0 chunks. */
	const long chunks = (pages + m_chunk_pages - 1) / m_chunk_pages;
	cum_chunks += chunks;
	m_tape_chunk_start[i + 1] = cum_chunks;
      }
    m_total_chunks = cum_chunks;
    m_reader_pages.assign (m_n_readers, 0);
  }

  bool
  chunk_distributor::next_chunk (int reader_id, range &out)
  {
    if (reader_id < 0 || reader_id >= m_n_readers)
      {
	return false;
      }

    const long g = m_next.fetch_add (1, std::memory_order_relaxed);
    if (g >= m_total_chunks)
      {
	return false;
      }

    /* Locate the Tape owning global chunk g: the largest ti with
     * m_tape_chunk_start[ti] <= g.  upper_bound returns the first entry > g, so
     * ti = that index - 1; this correctly skips empty Tapes (their start ==
     * the next Tape's start). */
    const std::vector<long>::const_iterator it =
	    std::upper_bound (m_tape_chunk_start.begin (), m_tape_chunk_start.end (), g);
    const int ti = (int) (it - m_tape_chunk_start.begin ()) - 1;

    const long local_chunk = g - m_tape_chunk_start[ti];
    const int start_page = (int) (local_chunk * m_chunk_pages);
    int count = m_tape_pages[ti] - start_page;
    if (count > m_chunk_pages)
      {
	count = m_chunk_pages;
      }

    out.tape_idx = ti;
    out.start_page = start_page;
    out.page_count = count;
    m_reader_pages[reader_id] += count;
    return true;
  }

  void
  chunk_distributor::skip_to_after (int tape_idx, int run_end_page)
  {
    if (tape_idx < 0 || tape_idx >= (int) m_tape_pages.size () || run_end_page < 0)
      {
	return;
      }
    /* Chunks of this Tape fully covered by [0 .. run_end_page]: the largest
     * local chunk c with (c+1)*chunk_pages-1 <= run_end_page, count = (c+1) =
     * (run_end_page+1)/chunk_pages.  A boundary chunk that also holds post-run
     * pages is NOT counted, so it stays claimable. */
    long full_local = (long) (run_end_page + 1) / (long) m_chunk_pages;
    const long tape_chunks = m_tape_chunk_start[tape_idx + 1] - m_tape_chunk_start[tape_idx];
    if (full_local > tape_chunks)
      {
	full_local = tape_chunks;
      }
    const long target = m_tape_chunk_start[tape_idx] + full_local;

    /* forward-only bump: never rewind the cursor (other readers may be ahead). */
    long cur = m_next.load (std::memory_order_relaxed);
    while (cur < target)
      {
	if (m_next.compare_exchange_weak (cur, target, std::memory_order_relaxed))
	  {
	    break;
	  }
      }
  }

  long
  chunk_distributor::reader_pages (int reader_id) const
  {
    if (reader_id < 0 || reader_id >= m_n_readers)
      {
	return 0;
      }
    return m_reader_pages[reader_id];
  }

  double
  chunk_distributor::coefficient_of_variation () const
  {
    if (m_n_readers <= 0)
      {
	return 0.0;
      }
    long sum = 0;
    for (long pages : m_reader_pages)
      {
	sum += pages;
      }
    const double mean = (double) sum / (double) m_n_readers;
    if (mean <= 0.0)
      {
	return 0.0;
      }
    double accum = 0.0;
    for (long pages : m_reader_pages)
      {
	const double d = (double) pages - mean;
	accum += d * d;
      }
    const double stddev = std::sqrt (accum / (double) m_n_readers);
    return stddev / mean;
  }

  r2_metrics
  chunk_distributor::metrics () const
  {
    r2_metrics m;
    m.total_pages = m_total_pages;
    m.total_chunks = m_total_chunks;
    const long handed = m_next.load (std::memory_order_relaxed);
    m.chunks_claimed = handed < m_total_chunks ? handed : m_total_chunks;

    long lo = -1;
    long hi = -1;
    for (long pages : m_reader_pages)
      {
	if (lo < 0 || pages < lo)
	  {
	    lo = pages;
	  }
	if (pages > hi)
	  {
	    hi = pages;
	  }
      }
    m.min_reader_pages = lo < 0 ? 0 : lo;
    m.max_reader_pages = hi < 0 ? 0 : hi;
    m.cov = coefficient_of_variation ();
    return m;
  }
}				/* namespace qfile */
