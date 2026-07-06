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

  void release_held_reservation (QMGR_TEMP_FILE * tfile_p) noexcept;

  bool reserve_held (std::size_t bytes, int *shard_out) noexcept;
  void reserve_held_soft (std::size_t bytes, int *shard_out) noexcept;
  void release_held (std::size_t bytes, int shard_index) noexcept;

  /* #146 T3 S1 (D2/D5): growth-time charge for a charge holder that
   * accumulates its reservation incrementally (currently: MEMBUF high-water
   * page charging in temp_page_store::alloc_page).  *shard_inout == -1 means
   * "pick a shard now"; on success it is set to the shard the bytes landed on
   * and every subsequent call for the same holder must pass that same value
   * back in, so the whole reservation lands on one shard and release_held can
   * undo it exactly.  On failure (cap reached) nothing is charged and
   * *shard_inout is left unchanged -- the caller degrades (e.g. to disk-spill
   * backing) without losing any reservation already held. */
  bool reserve_held_at_shard (std::size_t bytes, int *shard_inout) noexcept;

  std::size_t reservation_bytes_for_pages (std::size_t pages) noexcept;
  void record_degrade () noexcept;
  /* #146 T3 S1 (D7-2): cap reached -> charge rejected -> the caller spills
   * early instead of erroring.  Distinct from record_degrade() (existing
   * per-op soft-degrade counter, left as-is). */
  void record_cap_pressure_spill () noexcept;

  std::size_t cap_bytes () noexcept;
  std::size_t reserved_bytes () noexcept;
  /* #146 T3 S1 (§6): historical high-water of reserved_bytes(), observed at
   * every successful charge. */
  std::size_t reserved_peak_bytes () noexcept;

  /* #146 T3 S0 (contract only): per-operation layer-1 hard limit, PG two-tier
   * model (D7/D8).  row_store = sort/tuplestore/list-membuf state, limit =
   * session work_mem.  hash = hash build/agg-hash/memoize-like state, limit =
   * session work_mem * session hash_mem_multiplier.  This slice adds the
   * accessor only; existing call sites keep reading PRM_ID_WORK_MEM directly
   * until a later slice switches them over. */
  enum class op_workmem_kind
  {
    row_store,
    hash
  };

  std::size_t op_limit_bytes (op_workmem_kind kind) noexcept;
}
#endif /* _QUERY_WORKMEM_HPP_ */
