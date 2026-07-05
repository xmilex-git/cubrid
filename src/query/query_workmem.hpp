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
 * query_workmem.hpp - work-memory (work_mem) reservation accounting.
 *
 * The sharded work_mem accountant, separate from the qmgr temp-file backing
 * dispatcher in temp_page_store.  Leaf header (forward declarations only) so
 * consumers that only account (perf, hash_scan, qfile_tape) do not pull the
 * dispatcher.  The declarations stay in namespace temp_page_store to preserve
 * every call site unchanged.
 */

#ifndef _QUERY_WORKMEM_HPP_
#define _QUERY_WORKMEM_HPP_

#include "config.h"
#include "system.h"

#include <cstddef>

struct qmgr_temp_file;
typedef struct qmgr_temp_file QMGR_TEMP_FILE;

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

  budget_result reserve_membuf_budget (int requested_pages, std::size_t *reserved_bytes_out,
                                        int *reserved_shard_out) noexcept;
  void release_held_reservation (QMGR_TEMP_FILE * tfile_p) noexcept;

  bool reserve_held (std::size_t bytes, int *shard_out) noexcept;
  void reserve_held_soft (std::size_t bytes, int *shard_out) noexcept;
  void release_held (std::size_t bytes, int shard_index) noexcept;

  std::size_t reservation_bytes_for_pages (std::size_t pages) noexcept;
  void record_degrade () noexcept;

  std::size_t cap_bytes () noexcept;
  std::size_t reserved_bytes () noexcept;
}
#endif /* _QUERY_WORKMEM_HPP_ */
