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
 * query_workmem.cpp - work-memory (work_mem) reservation accounting.
 * See query_workmem.hpp.
 */

#include "query_workmem.hpp"

#include "perf_monitor.h"
#include "qfile_page_spill.hpp"	/* page_spill_file (release_held_reservation delete) */
#include "query_manager.h"		/* QMGR_TEMP_FILE */
#include "storage_common.h"		/* DB_PAGESIZE */
#include "system_parameter.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <limits>
#include <mutex>
#include <thread>

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace
{
  constexpr std::size_t WORKMEM_SHARD_COUNT = 64;
  constexpr std::size_t WORKMEM_REFILL_QUANTUM_BYTES = 256ULL * 1024ULL;
  constexpr std::size_t WORKMEM_MIN_CAP_BYTES = 64ULL * 1024ULL * 1024ULL;
  constexpr std::size_t WORKMEM_MAX_CAP_BYTES = 4ULL * 1024ULL * 1024ULL * 1024ULL;
  constexpr std::size_t WORKMEM_MIN_TEMP_FILE_BYTES = 4ULL * 1024ULL * 1024ULL;
  constexpr std::size_t WORKMEM_MAX_TEMP_FILE_BYTES = 256ULL * 1024ULL * 1024ULL;
  constexpr std::size_t WORKMEM_POSITION_SAVED_SCAN_BUDGET = 64;
  constexpr std::size_t WORKMEM_POSITION_HASH_ENTRY_BUDGET = 4096;
  constexpr std::size_t WORKMEM_CONNECT_BY_PARENT_BUDGET = 1024;

  struct alignas (64) workmem_shard
  {
    std::atomic<int64_t> reserved { 0 };
  };

  struct workmem_accountant
  {
    std::once_flag init_once;
    std::atomic<int64_t> cap { 0 };
    workmem_shard shards[WORKMEM_SHARD_COUNT];
  };

  workmem_accountant g_accountant;
  int64_t
  clamp_to_accounting_bytes (std::size_t bytes) noexcept
  {
    if (bytes > static_cast<std::size_t> (std::numeric_limits<int64_t>::max ()))
      {
        return std::numeric_limits<int64_t>::max ();
      }

    return static_cast<int64_t> (bytes);
  }

  std::size_t
  checked_add_bytes (std::size_t lhs, std::size_t rhs) noexcept
  {
    if (lhs > std::numeric_limits<std::size_t>::max () - rhs)
      {
        return std::numeric_limits<std::size_t>::max ();
      }

    return lhs + rhs;
  }

  std::size_t
  checked_pages_to_bytes (std::size_t pages) noexcept
  {
    if (pages > (std::numeric_limits<std::size_t>::max () / DB_PAGESIZE))
      {
        return std::numeric_limits<std::size_t>::max ();
      }

    return pages * DB_PAGESIZE;
  }

  std::size_t
  checked_bytes_to_pages (std::size_t bytes) noexcept
  {
    return (bytes + DB_PAGESIZE - 1) / DB_PAGESIZE;
  }

  void
  init_accountant () noexcept
  {
    /* #144 P3 D1 (escape hatch ②): the cap RESPECTS PRM_ID_WORK_MEM but stays
     * bounded to a system-proportional ceiling so it cannot blow up with
     * concurrency.  The cap is a GLOBAL budget shared across concurrent consumers
     * (headroom = cap - total_reserved), so bounding it at data_buffer/2 caps the
     * aggregate reservation regardless of how large work_mem is:
     *   cap = clamp( min( max(work_mem, data_buffer/8), data_buffer/2 ), 64MiB, 4GiB )
     * data_buffer/8 keeps the prior floor for small work_mem; data_buffer/2 is the
     * concurrency-safety ceiling (escape hatch ① = raise to work_mem with only a
     * 4GiB ceiling was rejected: it allows N×work_mem aggregate blowup).  A build
     * that still exceeds the cap degrades to spill exactly as before (graceful
     * fallback).  This promotes MID-SIZE in-mem-eligible builds (footprint in
     * (data_buffer/8, data_buffer/2]) that the old fixed data_buffer/8 cap spilled;
     * builds ≳ data_buffer/2 (e.g. a 5.12M-row self-join over a 512MiB pool) remain
     * spilled by design -- that residual is attributed to the #65 w5 structural
     * track (accounted budget vs develop's whole-pool free cache), not a regression.
     * Surfaced only via perf peek. */
    const int page_buffer_pages = std::max (prm_get_integer_value (PRM_ID_PAGE_BUFFER_SIZE), 0);
    const std::size_t data_buffer_bytes = checked_pages_to_bytes (static_cast<std::size_t> (page_buffer_pages));
    const std::size_t db_eighth = (data_buffer_bytes > 0) ? data_buffer_bytes / 8 : WORKMEM_MIN_CAP_BYTES;
    const std::size_t db_half = (data_buffer_bytes > 0) ? data_buffer_bytes / 2 : WORKMEM_MIN_CAP_BYTES;
    const std::size_t work_mem_bytes = static_cast<std::size_t> (prm_get_bigint_value (PRM_ID_WORK_MEM));
    const std::size_t desired = std::min (std::max (work_mem_bytes, db_eighth), db_half);
    const std::size_t capped = std::min (std::max (desired, WORKMEM_MIN_CAP_BYTES), WORKMEM_MAX_CAP_BYTES);

    g_accountant.cap.store (clamp_to_accounting_bytes (capped), std::memory_order_release);
  }

  void
  ensure_init () noexcept
  {
    std::call_once (g_accountant.init_once, init_accountant);
  }

  std::size_t
  exact_reserved_bytes () noexcept
  {
    ensure_init ();

    std::size_t sum = 0;
    for (std::size_t i = 0; i < WORKMEM_SHARD_COUNT; i++)
      {
        const int64_t shard_reserved = g_accountant.shards[i].reserved.load (std::memory_order_acquire);
        if (shard_reserved > 0)
          {
            sum = checked_add_bytes (sum, static_cast<std::size_t> (shard_reserved));
          }
      }

    return sum;
  }

  int
  choose_shard () noexcept
  {
    const std::size_t hash = std::hash<std::thread::id> {} (std::this_thread::get_id ());
    return static_cast<int> (hash & (WORKMEM_SHARD_COUNT - 1));
  }
}

namespace temp_page_store
{
  /* Internal accounting helpers consumed only within this TU, so they are
   * declared static here instead of in query_workmem.hpp -- the public
   * accountant surface stays limited to cap_bytes / reserved_bytes. */
  static std::size_t headroom_bytes () noexcept;
  static std::size_t shard_count () noexcept;
  static std::size_t refill_quantum_bytes () noexcept;
  static std::size_t worst_case_slack_bytes () noexcept;
  static std::size_t overshoot_limit_bytes () noexcept;
  static std::size_t position_budget_bytes () noexcept;

  budget_result
  reserve_membuf_budget (int requested_pages, std::size_t *reserved_bytes_out, int *reserved_shard_out) noexcept
  {
    const std::size_t cap = cap_bytes ();
    const std::size_t headroom = headroom_bytes ();
    const std::size_t requested_page_count = static_cast<std::size_t> (std::max (requested_pages, 0));
    const std::size_t per_file_target_bytes = std::min (std::max (cap / 8, WORKMEM_MIN_TEMP_FILE_BYTES),
							WORKMEM_MAX_TEMP_FILE_BYTES);
    const std::size_t target_pages = checked_bytes_to_pages (per_file_target_bytes);
    const std::size_t requested = std::max (requested_page_count, target_pages);
    std::size_t pages = requested;

    if (cap > 0 && reservation_bytes_for_pages (requested) > headroom)
      {
        const std::size_t requested_bytes = reservation_bytes_for_pages (requested);

        if (headroom == 0 || requested_bytes == 0)
          {
            pages = requested > 0 ? 1 : 0;
          }
        else
          {
            pages = std::max (static_cast<std::size_t> (1), requested * headroom / requested_bytes);
          }
      }

    const std::size_t bytes = reservation_bytes_for_pages (pages);
    int shard = -1;
    reserve_held_soft (bytes, &shard);
    if (shard >= 0)
      {
        const bool over_cap = pages != requested;
        if (over_cap)
          {
            record_degrade ();
          }
        *reserved_bytes_out = bytes;
        *reserved_shard_out = shard;
        return budget_result { static_cast<int> (std::min<std::size_t> (pages,
									 static_cast<std::size_t> (std::numeric_limits<int>::max ()))),
			       over_cap, false };
      }

    *reserved_bytes_out = 0;
    *reserved_shard_out = -1;
    return budget_result { 0, true, true };
  }

  void
  release_held_reservation (QMGR_TEMP_FILE * tfile_p) noexcept
  {
    if (tfile_p == NULL)
      {
        return;
      }

    if (tfile_p->page_spill_handle != NULL)
      {
        /* containment ownership: delete = close + unlink + census */
        delete tfile_p->page_spill_handle;
        tfile_p->page_spill_handle = NULL;
      }

    if (tfile_p->wm_reserved_bytes == 0)
      {
        return;
      }

    release_held (tfile_p->wm_reserved_bytes, tfile_p->wm_reserved_shard);
    tfile_p->wm_reserved_bytes = 0;
    tfile_p->wm_reserved_shard = -1;
  }

  bool
  reserve_held (std::size_t bytes, int *shard_out) noexcept
  {
    ensure_init ();

    const int shard = choose_shard ();
    const int64_t accounting_bytes = clamp_to_accounting_bytes (bytes);
    g_accountant.shards[shard].reserved.fetch_add (accounting_bytes, std::memory_order_acq_rel);

    const std::size_t observed = exact_reserved_bytes ();
    if (observed <= overshoot_limit_bytes ())
      {
        *shard_out = shard;
        return true;
      }

    const std::size_t reconciled = exact_reserved_bytes ();
    if (reconciled <= cap_bytes ())
      {
        *shard_out = shard;
        return true;
      }

    g_accountant.shards[shard].reserved.fetch_sub (accounting_bytes, std::memory_order_acq_rel);
    *shard_out = -1;
    return false;
  }

  void
  reserve_held_soft (std::size_t bytes, int *shard_out) noexcept
  {
    ensure_init ();

    const int shard = choose_shard ();
    g_accountant.shards[shard].reserved.fetch_add (clamp_to_accounting_bytes (bytes), std::memory_order_acq_rel);
    *shard_out = shard;
  }

  void
  release_held (std::size_t bytes, int shard_index) noexcept
  {
    if (bytes == 0 || shard_index < 0 || shard_index >= static_cast<int> (WORKMEM_SHARD_COUNT))
      {
        return;
      }

    g_accountant.shards[shard_index].reserved.fetch_sub (clamp_to_accounting_bytes (bytes), std::memory_order_acq_rel);
  }

  std::size_t
  reservation_bytes_for_pages (std::size_t pages) noexcept
  {
    return checked_add_bytes (checked_pages_to_bytes (pages), position_budget_bytes ());
  }

  void
  record_degrade () noexcept
  {
    perfmon_inc_stat_to_global (PSTAT_WORKMEM_NUM_DEGRADES);
  }

  std::size_t
  cap_bytes () noexcept
  {
    ensure_init ();
    const int64_t cap = g_accountant.cap.load (std::memory_order_acquire);
    return cap > 0 ? static_cast<std::size_t> (cap) : 0;
  }

  std::size_t
  reserved_bytes () noexcept
  {
    return exact_reserved_bytes ();
  }

  std::size_t
  headroom_bytes () noexcept
  {
    const std::size_t cap = cap_bytes ();
    const std::size_t reserved = reserved_bytes ();

    return (reserved < cap) ? cap - reserved : 0;
  }

  std::size_t
  shard_count () noexcept
  {
    return WORKMEM_SHARD_COUNT;
  }

  std::size_t
  refill_quantum_bytes () noexcept
  {
    return WORKMEM_REFILL_QUANTUM_BYTES;
  }

  std::size_t
  worst_case_slack_bytes () noexcept
  {
    return shard_count () * refill_quantum_bytes ();
  }

  std::size_t
  overshoot_limit_bytes () noexcept
  {
    return checked_add_bytes (cap_bytes (), worst_case_slack_bytes ());
  }

  std::size_t
  position_budget_bytes () noexcept
  {
    /* Fixed-size segment-aware tuple positions.  Charge the held reservation for widened saved-scan,
     * hash-list and connect-by parent-position bytes.  reserve_held still enforces
     * reserved <= cap + worst_case_slack, with worst_case_slack = shard_count * refill_quantum. */
    return WORKMEM_POSITION_SAVED_SCAN_BUDGET * projected_tuple_position_bytes
      + WORKMEM_POSITION_HASH_ENTRY_BUDGET * projected_tuple_simple_pos_bytes
      + WORKMEM_CONNECT_BY_PARENT_BUDGET * projected_tuple_position_db_bytes;
  }
}
