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
 * temp_page_store.hpp - temporary query page backing and work-memory accounting
 */

#ifndef _TEMP_PAGE_STORE_HPP_
#define _TEMP_PAGE_STORE_HPP_

#include "config.h"

#include "storage_common.h"
#include "system.h"
#include "thread_compat.hpp"

#include <cstddef>

struct qmgr_temp_file;
typedef struct qmgr_temp_file QMGR_TEMP_FILE;

enum class qmgr_temp_backing : int
{
  MEMBUF = 0,
  PGBUF_PINNED,
  PRIVATE_SPILL_FALLBACK,
  SHARED_SPILL,
  PAGE_SPILL		/* (c′) per-tfile page-spill cache, #132; the sole OLD-tier
				 * membuf-overflow backing since 커밋 B deleted raw-fd (#74 §5, #137) */
};

namespace temp_page_store
{
  constexpr std::size_t projected_tuple_position_bytes = 48;
  constexpr std::size_t projected_tuple_position_db_bytes = 40;
  constexpr std::size_t projected_tuple_simple_pos_bytes = 32;

  struct budget_result
  {
    int pages_granted;
    bool over_cap;
    bool hard_oom;
  };

  /* (c′) PAGE_SPILL consumer shims (#132) -- keep the qmgr dispatch
   * branches symmetric with the pre-커밋 B rawfd_* pair contract. */
  int page_spill_flush_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, PAGE_PTR page_p, int free_page) noexcept;
  int spill_release_fixed_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, PAGE_PTR page_p) noexcept;

  int qmgr_temp_file_move_selftest (THREAD_ENTRY * thread_p) noexcept;

  PAGE_PTR alloc_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, VPID * vpid_p);
  PAGE_PTR fix_old_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, VPID * vpid_p);

  budget_result reserve_membuf_budget (int requested_pages, std::size_t *reserved_bytes_out,
                                        int *reserved_shard_out) noexcept;
  void release_held_reservation (QMGR_TEMP_FILE * tfile_p) noexcept;

  bool reserve_held (std::size_t bytes, int *shard_out) noexcept;
  void reserve_held_soft (std::size_t bytes, int *shard_out) noexcept;
  void release_held (std::size_t bytes, int shard_index) noexcept;

  std::size_t reservation_bytes_for_pages (std::size_t pages) noexcept;
  std::size_t reservation_bytes_for_degree (UINT32 degree, std::size_t pages_per_worker = 0) noexcept;
  void record_degrade () noexcept;

  std::size_t cap_bytes () noexcept;
  std::size_t reserved_bytes () noexcept;
  std::size_t headroom_bytes () noexcept;
  std::size_t shard_count () noexcept;
  std::size_t refill_quantum_bytes () noexcept;
  std::size_t worst_case_slack_bytes () noexcept;
  std::size_t overshoot_limit_bytes () noexcept;
  std::size_t position_budget_bytes () noexcept;
}
#endif /* _TEMP_PAGE_STORE_HPP_ */
