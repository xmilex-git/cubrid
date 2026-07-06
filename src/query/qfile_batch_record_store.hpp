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
 * qfile_batch_record_store.hpp - raw batch record store (issue #147 T1 D2).
 *
 * A thin append/sequential-read record framing layer over qfile::buffile.
 * This is the "batch file" substrate the Grace hash-join rebuild spills its
 * inner/outer batches to: a forward-only stream of records
 *
 *     { UINT32 hash, QFILE_TUPLE tuple }
 *
 * where the tuple bytes are copied byte-for-byte from the build/probe list
 * (no re-encoding -- QFILE_GET_TUPLE_LENGTH (tuple) is trusted as the exact
 * byte length to copy and to reconstruct on read). Records are packed
 * densely into the underlying buffile's DB_PAGESIZE pages; a record is free
 * to straddle a page boundary (the framing below, not the page layout,
 * carries record identity), so no space is wasted the way a self-contained
 * per-page tuple format would.
 *
 * Lifecycle (single instance, single thread of use, no reopen): construct ->
 * append() any number of times (write phase) -> read() any number of times
 * until eof (forward-only read phase; the first read() call implicitly
 * freezes the write phase) -> destroy (closes + unlinks the backing file,
 * same as buffile). Interleaving append() after the first read() is a usage
 * error (returns ER_FAILED without touching the store).
 *
 * TDE passes through unchanged: `tde_algo` is forwarded to the underlying
 * buffile::create() exactly like any other buffile consumer.
 *
 * Memory: the write-side staging page, the read-side staging page, and the
 * record-reconstruction buffer (grows to the largest record seen so far) are
 * charged to the work_mem accountant (layer-2 real-usage charge, best-effort
 * -- see temp_page_store::reserve_held / reserve_held_soft), the same style
 * as qfile::tape_writer's prefix charges and HLS_SPILL's stage buffer.
 */

#ifndef _QFILE_BATCH_RECORD_STORE_HPP_
#define _QFILE_BATCH_RECORD_STORE_HPP_

#include "qfile_buffile.hpp"
#include "query_list.h"		/* QFILE_TUPLE */
#include "thread_compat.hpp"	/* THREAD_ENTRY */
#include "tde.h"		/* TDE_ALGORITHM */

#include <cstdint>
#include <utility>
#include <vector>

namespace qfile
{
  class batch_record_store
  {
    public:
      /* Create a private batch file under `dir` (same directory-resolution
       * contract as buffile::create -- caller resolves `dir` via
       * buffile::default_scratch_dir(), no fallback here). Returns NULL on
       * failure and sets *os_error_out to errno (0 on success). */
      static batch_record_store *create (THREAD_ENTRY *thread_p, const char *dir, std::uint64_t seq,
					  unsigned int worker_id, TDE_ALGORITHM tde_algo, int *os_error_out);

      ~batch_record_store ();

      /* Append one record (write phase only -- ER_FAILED if a read() already
       * happened). `tuple`'s byte length is taken verbatim from
       * QFILE_GET_TUPLE_LENGTH (tuple); those bytes are copied unchanged. */
      int append (THREAD_ENTRY *thread_p, UINT32 hash, QFILE_TUPLE tuple);

      /* Forward-only sequential read of the next record. On the first call,
       * implicitly closes the write phase (flushes any partially-filled
       * page). *tuple_out points into an internal buffer valid until the
       * next read() call or destruction -- copy out if the caller needs it
       * to outlive that. *eof_out is set true (and *hash_out / *tuple_out
       * left untouched) once the stream is exhausted; NO_ERROR is returned in
       * both the "record read" and the "eof" case, an error code otherwise. */
      int read (THREAD_ENTRY *thread_p, UINT32 *hash_out, QFILE_TUPLE *tuple_out, bool *eof_out);

      /* Explicit early close: flushes/frees resources and releases the
       * accountant charges. Idempotent; also run by the destructor as a
       * safety net. The backing file itself is unlinked by the destructor
       * (matches buffile's close+unlink-on-destroy contract). */
      void close (THREAD_ENTRY *thread_p);

      long record_count () const
      {
	return m_records_written;
      }
      const buffile_metrics &metrics () const
      {
	return m_bf->metrics ();
      }
      const std::string &path () const
      {
	return m_bf->path ();
      }

      batch_record_store (const batch_record_store &) = delete;
      batch_record_store &operator= (const batch_record_store &) = delete;

    private:
      batch_record_store ();

      int write_bytes (THREAD_ENTRY *thread_p, const char *data, std::size_t len);
      int read_bytes (THREAD_ENTRY *thread_p, char *dst, std::size_t len);
      int finalize_write (THREAD_ENTRY *thread_p);
      int ensure_recon_capacity (std::size_t needed);
      void charge_bytes (std::size_t bytes);
      void release_all_charges ();

      buffile *m_bf;

      /* write side */
      char *m_wr_page;		/* DB_PAGESIZE staging buffer for the page being filled */
      int m_wr_offset;		/* bytes valid in m_wr_page so far */
      bool m_write_closed;	/* finalize_write() has run: past the write phase */
      std::size_t m_bytes_written;	/* total logical stream bytes appended */
      long m_records_written;

      /* read side */
      char *m_rd_page;		/* DB_PAGESIZE buffer holding the currently loaded on-disk page */
      int m_rd_page_no;		/* page index currently loaded into m_rd_page, -1 = none yet */
      int m_rd_in_page_off;	/* read cursor within m_rd_page */
      std::size_t m_rd_byte_cursor;	/* total logical stream bytes consumed so far */
      tde_read_scratch m_rd_scratch;

      char *m_recon_buf;	/* record reconstruction buffer (hash + tuple), grows as needed */
      std::size_t m_recon_cap;

      bool m_closed;
      std::vector<std::pair<std::size_t, int>> m_wm_charges;	/* accountant (bytes, shard) charges */
  };
}				/* namespace qfile */

/*
 * In-server self-test of the batch record store: round-trip of varying
 * record sizes (small / exactly page-boundary-straddling / larger-than-a-page),
 * a large sequential batch (unlink-on-close check), a TDE round-trip when a
 * cipher is loaded, the DB-volume-directory path rule (issue #147 D-SP1), and
 * work_mem accountant charge/release (no leak). Gated by env
 * CUBRID_WM_BATCHSTORE_SELFTEST in qmgr_initialize (debug-only). Returns 0 on
 * PASS.
 */
int qfile_batch_record_store_selftest (THREAD_ENTRY *thread_p);

#endif /* _QFILE_BATCH_RECORD_STORE_HPP_ */
