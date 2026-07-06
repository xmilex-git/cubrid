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
 * qfile_batch_record_store.cpp - raw batch record store.
 * See qfile_batch_record_store.hpp for the design rationale.
 */

#include "qfile_batch_record_store.hpp"

#include "error_manager.h"
#include "object_representation.h"	/* OR_GET_INT, used by QFILE_GET_TUPLE_LENGTH */
#include "query_workmem.hpp"	/* work_mem accountant: reserve_held / reserve_held_soft / release_held */
#include "storage_common.h"	/* DB_PAGESIZE */
#include "system_parameter.h"	/* prm_get_integer_value / PRM_ID_TDE_DEFAULT_ALGORITHM */
#include "tde.h"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/stat.h>		/* stat -- unlink-on-close check */

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace qfile
{
  batch_record_store::batch_record_store ()
    : m_bf (NULL)
    , m_wr_page (NULL)
    , m_wr_offset (0)
    , m_write_closed (false)
    , m_bytes_written (0)
    , m_records_written (0)
    , m_rd_page (NULL)
    , m_rd_page_no (-1)
    , m_rd_in_page_off (0)
    , m_rd_byte_cursor (0)
    , m_rd_scratch ()
    , m_recon_buf (NULL)
    , m_recon_cap (0)
    , m_closed (false)
    , m_wm_charges ()
  {
  }

  batch_record_store::~batch_record_store ()
  {
    close (NULL);
  }

  void
  batch_record_store::charge_bytes (std::size_t bytes)
  {
    if (bytes == 0)
      {
	return;
      }
    int shard = -1;
    if (!temp_page_store::reserve_held (bytes, &shard))
      {
	/* Layer-2 accounting is best-effort: the batch file must stay usable
	 * under pool saturation (D4 -- op-limit enforcement lives at the
	 * build-memory layer, not here), so an overshoot is soft-charged
	 * instead of refused. */
	temp_page_store::record_degrade ();
	temp_page_store::reserve_held_soft (bytes, &shard);
      }
    m_wm_charges.emplace_back (bytes, shard);
  }

  void
  batch_record_store::release_all_charges ()
  {
    for (const std::pair<std::size_t, int> &charge : m_wm_charges)
      {
	temp_page_store::release_held (charge.first, charge.second);
      }
    m_wm_charges.clear ();
  }

  batch_record_store *
  batch_record_store::create (THREAD_ENTRY *thread_p, const char *dir, std::uint64_t seq, unsigned int worker_id,
			       TDE_ALGORITHM tde_algo, int *os_error_out)
  {
    buffile *bf = buffile::create (thread_p, dir, seq, worker_id, tde_algo, os_error_out);
    if (bf == NULL)
      {
	return NULL;
      }

    batch_record_store *store = new batch_record_store ();
    store->m_bf = bf;

    store->m_wr_page = static_cast<char *> (malloc (DB_PAGESIZE));
    store->m_rd_page = static_cast<char *> (malloc (DB_PAGESIZE));
    store->m_recon_cap = DB_PAGESIZE;
    store->m_recon_buf = static_cast<char *> (malloc (store->m_recon_cap));
    if (store->m_wr_page == NULL || store->m_rd_page == NULL || store->m_recon_buf == NULL)
      {
	delete store;
	if (os_error_out != NULL)
	  {
	    *os_error_out = ENOMEM;
	  }
	return NULL;
      }
    store->charge_bytes ((std::size_t) DB_PAGESIZE * 2 + store->m_recon_cap);

    return store;
  }

  void
  batch_record_store::close (THREAD_ENTRY *thread_p)
  {
    if (m_closed)
      {
	return;
      }
    /* Best-effort: an error here just means the trailing partial page (if
     * any) is lost, which only matters if the caller never read it anyway. */
    (void) finalize_write (thread_p);

    release_all_charges ();
    free (m_wr_page);
    m_wr_page = NULL;
    free (m_rd_page);
    m_rd_page = NULL;
    free (m_recon_buf);
    m_recon_buf = NULL;
    m_recon_cap = 0;

    if (m_bf != NULL)
      {
	delete m_bf;	/* closes + unlinks the backing file */
	m_bf = NULL;
      }
    m_closed = true;
  }

  int
  batch_record_store::ensure_recon_capacity (std::size_t needed)
  {
    if (needed <= m_recon_cap)
      {
	return NO_ERROR;
      }
    std::size_t new_cap = m_recon_cap;
    while (new_cap < needed)
      {
	new_cap *= 2;
      }
    char *grown = static_cast<char *> (realloc (m_recon_buf, new_cap));
    if (grown == NULL)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, new_cap);
	return ER_OUT_OF_VIRTUAL_MEMORY;
      }
    charge_bytes (new_cap - m_recon_cap);
    m_recon_buf = grown;
    m_recon_cap = new_cap;
    return NO_ERROR;
  }

  int
  batch_record_store::write_bytes (THREAD_ENTRY *thread_p, const char *data, std::size_t len)
  {
    const char *src = data;
    while (len > 0)
      {
	if (m_wr_offset == DB_PAGESIZE)
	  {
	    int rc = m_bf->append_page (thread_p, (PAGE_PTR) m_wr_page);
	    if (rc != NO_ERROR)
	      {
		return rc;
	      }
	    m_wr_offset = 0;
	  }
	const std::size_t chunk = std::min (len, (std::size_t) (DB_PAGESIZE - m_wr_offset));
	std::memcpy (m_wr_page + m_wr_offset, src, chunk);
	m_wr_offset += (int) chunk;
	src += chunk;
	len -= chunk;
	m_bytes_written += chunk;
      }
    return NO_ERROR;
  }

  int
  batch_record_store::append (THREAD_ENTRY *thread_p, UINT32 hash, QFILE_TUPLE tuple)
  {
    if (m_write_closed || tuple == NULL)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	return ER_FAILED;
      }

    const int tuple_len = QFILE_GET_TUPLE_LENGTH (tuple);
    if (tuple_len <= 0)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	return ER_FAILED;
      }

    int rc = write_bytes (thread_p, (const char *) &hash, sizeof (hash));
    if (rc != NO_ERROR)
      {
	return rc;
      }
    rc = write_bytes (thread_p, tuple, (std::size_t) tuple_len);
    if (rc != NO_ERROR)
      {
	return rc;
      }

    m_records_written++;
    return NO_ERROR;
  }

  int
  batch_record_store::finalize_write (THREAD_ENTRY *thread_p)
  {
    if (m_write_closed)
      {
	return NO_ERROR;
      }
    if (m_bf != NULL)
      {
	if (m_wr_offset > 0)
	  {
	    int rc = m_bf->append_page (thread_p, (PAGE_PTR) m_wr_page);
	    if (rc != NO_ERROR)
	      {
		return rc;
	      }
	    m_wr_offset = 0;
	  }
	int rc = m_bf->flush (thread_p);
	if (rc != NO_ERROR)
	  {
	    return rc;
	  }
      }
    m_write_closed = true;
    m_rd_byte_cursor = 0;
    m_rd_page_no = -1;
    m_rd_in_page_off = 0;
    return NO_ERROR;
  }

  int
  batch_record_store::read_bytes (THREAD_ENTRY *thread_p, char *dst, std::size_t len)
  {
    char *out = dst;
    while (len > 0)
      {
	if (m_rd_page_no < 0 || m_rd_in_page_off == DB_PAGESIZE)
	  {
	    const int next_page_no = (m_rd_page_no < 0) ? 0 : m_rd_page_no + 1;
	    int rc = m_bf->read_page (thread_p, next_page_no, (PAGE_PTR) m_rd_page, &m_rd_scratch);
	    if (rc != NO_ERROR)
	      {
		return rc;
	      }
	    m_rd_page_no = next_page_no;
	    m_rd_in_page_off = 0;
	  }
	const std::size_t chunk = std::min (len, (std::size_t) (DB_PAGESIZE - m_rd_in_page_off));
	std::memcpy (out, m_rd_page + m_rd_in_page_off, chunk);
	m_rd_in_page_off += (int) chunk;
	out += chunk;
	len -= chunk;
	m_rd_byte_cursor += chunk;
      }
    return NO_ERROR;
  }

  int
  batch_record_store::read (THREAD_ENTRY *thread_p, UINT32 *hash_out, QFILE_TUPLE *tuple_out, bool *eof_out)
  {
    if (!m_write_closed)
      {
	int rc = finalize_write (thread_p);
	if (rc != NO_ERROR)
	  {
	    return rc;
	  }
      }

    if (m_rd_byte_cursor >= m_bytes_written)
      {
	*eof_out = true;
	return NO_ERROR;
      }

    UINT32 hash;
    int rc = read_bytes (thread_p, (char *) &hash, sizeof (hash));
    if (rc != NO_ERROR)
      {
	return rc;
      }

    /* Peek the tuple's self-describing length from just its first 4 bytes
     * (QFILE_GET_TUPLE_LENGTH only reads offset 0), then read the rest of
     * the verbatim tuple bytes into the reconstruction buffer. */
    rc = ensure_recon_capacity (sizeof (int));
    if (rc != NO_ERROR)
      {
	return rc;
      }
    rc = read_bytes (thread_p, m_recon_buf, sizeof (int));
    if (rc != NO_ERROR)
      {
	return rc;
      }
    const int tuple_len = QFILE_GET_TUPLE_LENGTH (m_recon_buf);
    if (tuple_len < (int) sizeof (int))
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
	return ER_FAILED;
      }
    rc = ensure_recon_capacity ((std::size_t) tuple_len);
    if (rc != NO_ERROR)
      {
	return rc;
      }
    rc = read_bytes (thread_p, m_recon_buf + sizeof (int), (std::size_t) tuple_len - sizeof (int));
    if (rc != NO_ERROR)
      {
	return rc;
      }

    *hash_out = hash;
    *tuple_out = m_recon_buf;
    *eof_out = false;
    return NO_ERROR;
  }
}				/* namespace qfile */

/* ------------------------------------------------------------------ */
/* In-server self-test.  Gated by env CUBRID_WM_BATCHSTORE_SELFTEST.    */
/* ------------------------------------------------------------------ */

namespace
{
  /* Build a throwaway QFILE_TUPLE of exactly `len` bytes: length header at
   * offset 0 (QFILE_GET_TUPLE_LENGTH's contract), the rest a pattern keyed on
   * (rec_no, byte offset) so a round-trip mismatch is detectable. */
  char *
  batchstore_make_tuple (int len, int rec_no)
  {
    char *buf = static_cast<char *> (malloc ((std::size_t) len));
    if (buf == NULL)
      {
	return NULL;
      }
    QFILE_PUT_TUPLE_LENGTH (buf, len);
    for (int b = (int) sizeof (int); b < len; b++)
      {
	buf[b] = (char) ((rec_no * 31 + b) & 0xff);
      }
    return buf;
  }

  bool
  batchstore_tuple_matches (const char *buf, int len, int rec_no)
  {
    if (QFILE_GET_TUPLE_LENGTH (buf) != len)
      {
	return false;
      }
    for (int b = (int) sizeof (int); b < len; b++)
      {
	if (buf[b] != (char) ((rec_no * 31 + b) & 0xff))
	  {
	    return false;
	  }
      }
    return true;
  }

  /* Round-trip a fixed list of record sizes chosen to cover: smaller than a
   * page, exactly straddling a page boundary, and larger than a page. */
  int
  batchstore_selftest_roundtrip (THREAD_ENTRY *thread_p, const std::string &dir, TDE_ALGORITHM algo)
  {
    const int sizes[] = { 16, 32, DB_PAGESIZE - 100, 500, DB_PAGESIZE, DB_PAGESIZE * 2 + 37, 64, 8 };
    const int nsizes = (int) (sizeof (sizes) / sizeof (sizes[0]));

    int os_error = 0;
    qfile::batch_record_store *store =
	    qfile::batch_record_store::create (thread_p, dir.c_str (), (std::uint64_t) -11, 0, algo, &os_error);
    if (store == NULL)
      {
	return ER_FAILED;
      }

    /* path rule (#147 D-SP1/S1 gate 3): the store's backing file must live
     * under the resolved DB-volume-rooted scratch dir. */
    if (store->path ().compare (0, dir.size (), dir) != 0)
      {
	delete store;
	return ER_FAILED;
      }

    int rc = NO_ERROR;
    for (int i = 0; i < nsizes && rc == NO_ERROR; i++)
      {
	char *tuple = batchstore_make_tuple (sizes[i], i);
	if (tuple == NULL)
	  {
	    rc = ER_FAILED;
	    break;
	  }
	rc = store->append (thread_p, (UINT32) (i * 7 + 1), tuple);
	free (tuple);
      }

    for (int i = 0; i < nsizes && rc == NO_ERROR; i++)
      {
	UINT32 hash = 0;
	QFILE_TUPLE tuple = NULL;
	bool eof = false;
	rc = store->read (thread_p, &hash, &tuple, &eof);
	if (rc != NO_ERROR)
	  {
	    break;
	  }
	if (eof || hash != (UINT32) (i * 7 + 1) || !batchstore_tuple_matches (tuple, sizes[i], i))
	  {
	    rc = ER_FAILED;
	    break;
	  }
      }

    if (rc == NO_ERROR)
      {
	UINT32 hash = 0;
	QFILE_TUPLE tuple = NULL;
	bool eof = false;
	rc = store->read (thread_p, &hash, &tuple, &eof);
	if (rc == NO_ERROR && !eof)
	  {
	    rc = ER_FAILED;	/* stream must be exhausted */
	  }
      }

    delete store;
    return rc;
  }

  /* Large sequential batch: many small records, then verify the backing file
   * is gone (close+unlink) once the store is destroyed. */
  int
  batchstore_selftest_bulk_and_unlink (THREAD_ENTRY *thread_p, const std::string &dir)
  {
    const int NRECS = 40000;
    const int RECLEN = 24;	/* > sizeof(int), small and uniform */

    int os_error = 0;
    qfile::batch_record_store *store =
	    qfile::batch_record_store::create (thread_p, dir.c_str (), (std::uint64_t) -12, 0, TDE_ALGORITHM_NONE,
						&os_error);
    if (store == NULL)
      {
	return ER_FAILED;
      }
    const std::string path = store->path ();

    int rc = NO_ERROR;
    for (int i = 0; i < NRECS && rc == NO_ERROR; i++)
      {
	char *tuple = batchstore_make_tuple (RECLEN, i);
	if (tuple == NULL)
	  {
	    rc = ER_FAILED;
	    break;
	  }
	rc = store->append (thread_p, (UINT32) i, tuple);
	free (tuple);
      }

    for (int i = 0; i < NRECS && rc == NO_ERROR; i++)
      {
	UINT32 hash = 0;
	QFILE_TUPLE tuple = NULL;
	bool eof = false;
	rc = store->read (thread_p, &hash, &tuple, &eof);
	if (rc != NO_ERROR)
	  {
	    break;
	  }
	if (eof || hash != (UINT32) i || !batchstore_tuple_matches (tuple, RECLEN, i))
	  {
	    rc = ER_FAILED;
	    break;
	  }
      }

    if (rc == NO_ERROR && store->record_count () != NRECS)
      {
	rc = ER_FAILED;
      }

    delete store;

    if (rc == NO_ERROR)
      {
	struct stat st;
	if (stat (path.c_str (), &st) == 0 || errno != ENOENT)
	  {
	    rc = ER_FAILED;	/* backing file must be gone after close+unlink */
	  }
      }
    return rc;
  }

  /* work_mem accountant charge/release: reserved bytes must rise while the
   * store is alive and return to baseline (no leak) once it is destroyed. */
  int
  batchstore_selftest_accountant (THREAD_ENTRY *thread_p, const std::string &dir)
  {
    const std::size_t baseline = temp_page_store::reserved_bytes ();

    int os_error = 0;
    qfile::batch_record_store *store =
	    qfile::batch_record_store::create (thread_p, dir.c_str (), (std::uint64_t) -13, 0, TDE_ALGORITHM_NONE,
						&os_error);
    if (store == NULL)
      {
	return ER_FAILED;
      }

    int rc = NO_ERROR;
    /* a record bigger than the initial recon capacity forces at least one
     * accountant charge for the grown reconstruction buffer. */
    char *tuple = batchstore_make_tuple (DB_PAGESIZE * 3, 0);
    if (tuple == NULL)
      {
	delete store;
	return ER_FAILED;
      }
    rc = store->append (thread_p, 1, tuple);
    free (tuple);
    if (rc == NO_ERROR)
      {
	UINT32 hash = 0;
	QFILE_TUPLE out = NULL;
	bool eof = false;
	rc = store->read (thread_p, &hash, &out, &eof);
      }

    const std::size_t during = temp_page_store::reserved_bytes ();
    delete store;
    const std::size_t after = temp_page_store::reserved_bytes ();

    if (rc != NO_ERROR)
      {
	return rc;
      }
    if (during <= baseline || after != baseline)
      {
	return ER_FAILED;
      }
    return NO_ERROR;
  }
}				/* anonymous namespace */

int
qfile_batch_record_store_selftest (THREAD_ENTRY *thread_p)
{
  std::string dir;
  if (!qfile::buffile::default_scratch_dir (dir))
    {
      return ER_FAILED;
    }

  int rc = batchstore_selftest_roundtrip (thread_p, dir, TDE_ALGORITHM_NONE);

  if (rc == NO_ERROR)
    {
      rc = batchstore_selftest_bulk_and_unlink (thread_p, dir);
    }

  if (rc == NO_ERROR)
    {
      rc = batchstore_selftest_accountant (thread_p, dir);
    }

  if (rc == NO_ERROR && tde_is_loaded ())
    {
      const TDE_ALGORITHM def = (TDE_ALGORITHM) prm_get_integer_value (PRM_ID_TDE_DEFAULT_ALGORITHM);
      if (def != TDE_ALGORITHM_NONE)
	{
	  rc = batchstore_selftest_roundtrip (thread_p, dir, def);
	}
    }

  er_log_debug (ARG_FILE_LINE, "BATCHSTORE_SELFTEST result=%d (0=PASS)\n", rc);
  fprintf (stderr, "BATCHSTORE_SELFTEST result=%d (0=PASS)\n", rc);
  return rc;
}
