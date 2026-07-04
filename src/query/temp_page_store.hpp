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

#include <atomic>
#include <functional>
#include <cstddef>
#include <cstdint>
#include <string>

struct qmgr_temp_file;
typedef struct qmgr_temp_file QMGR_TEMP_FILE;

enum class qmgr_temp_backing : int
{
  MEMBUF = 0,
  PGBUF_PINNED,
  PRIVATE_SPILL_FALLBACK,
  SHARED_SPILL,
  RAW_FD_OVERFLOW,
  PAGE_SPILL_OVERFLOW		/* (c′) per-tfile page-spill cache, #132; the default OLD-tier overflow
				 * since 커밋 A (#135).  RAW_FD_OVERFLOW remains as the
				 * CUBRID_WM_SPILL_NEW=0 opt-out until 커밋 B (#74 §5) */
};

namespace temp_page_store
{
  constexpr std::size_t projected_tuple_position_bytes = 48;
  constexpr std::size_t projected_tuple_position_db_bytes = 40;
  constexpr std::size_t projected_tuple_simple_pos_bytes = 32;
  constexpr std::size_t future_read_cache_placeholder_bytes = 32ULL * IO_MAX_PAGE_SIZE;

  enum class raw_fd_access_hint : int
  {
    SEQUENTIAL_ONCE = 0,
    RANDOM_REACCESS
  };

  struct budget_result
  {
    int pages_granted;
    bool over_cap;
    bool hard_oom;
  };

  struct raw_fd_key
  {
    std::uint64_t boot_incarnation;
    std::uint64_t file_seq;
  };
  struct rawfd_sec_key
  {
    std::uint64_t file_seq;
    PAGEID page_index;

    bool operator== (const rawfd_sec_key &other) const noexcept
    {
      return file_seq == other.file_seq && page_index == other.page_index;
    }
  };


  struct raw_fd_page_coordinate
  {
    std::uint64_t raw_fd_segment_id;
    PAGEID page_index;
    std::size_t tuple_offset;
  };

  class raw_fd_file
  {
    public:
      raw_fd_file () noexcept;
      ~raw_fd_file () noexcept;

      raw_fd_file (raw_fd_file &&other) noexcept = delete;
      raw_fd_file &operator= (raw_fd_file &&other) noexcept = delete;

      raw_fd_file (const raw_fd_file &) = delete;
      raw_fd_file &operator= (const raw_fd_file &) = delete;

      int fd () const noexcept;
      const std::string &path () const noexcept;
      QUERY_ID query_id () const noexcept;
      int owner_tran_index () const noexcept;
      unsigned int worker_id () const noexcept;
      raw_fd_key key () const noexcept;
      std::uint64_t segment_id () const noexcept;
      raw_fd_access_hint access_hint () const noexcept;
      bool is_open () const noexcept;
      bool destroyed () const noexcept;
      void mark_destroyed () noexcept;
      void close_and_unlink () noexcept;
      void attach_temp_file (QMGR_TEMP_FILE * tfile_p) noexcept;
      int detach_for_unlink (std::string &path_out) noexcept;

    private:
      friend raw_fd_file *create_raw_fd_file (THREAD_ENTRY * thread_p, QUERY_ID query_id, int owner_tran_index,
                                              unsigned int worker_id, int *os_error_out) noexcept;
      friend void destroy_raw_fd_file (raw_fd_file *file_p) noexcept;
      friend void reassign_raw_fd_owner (raw_fd_file *file_p, QMGR_TEMP_FILE * new_owner) noexcept;
      friend int rawfd_write_page (THREAD_ENTRY * thread_p, raw_fd_file &file, PAGEID page_index, PAGE_PTR page_p) noexcept;
      friend PAGE_PTR rawfd_pos_read (THREAD_ENTRY * thread_p, raw_fd_file &file,
                                      const raw_fd_page_coordinate &coordinate) noexcept;
      friend void rawfd_invalidate_cached_page (raw_fd_file &file, PAGEID page_index) noexcept;
      friend int rawfd_rewrite_page (THREAD_ENTRY * thread_p, raw_fd_file &file, PAGEID page_index,
                                     PAGE_PTR page_p) noexcept;

#ifdef RAWFD_UNIT_TEST
      friend void rawfd_test_init_file (raw_fd_file &file, std::uint64_t file_seq) noexcept;
#endif
#ifdef RAWFD_UNIT_TEST
      friend int rawfd_test_t1_hash_collision () noexcept;
      friend int rawfd_test_t2_aba_validation () noexcept;
      friend int rawfd_test_t3_retry_bound () noexcept;
      friend int rawfd_test_t4_destroyed_gate () noexcept;
      friend int rawfd_test_t5_purge_order () noexcept;
#endif
      int m_fd;
      std::string m_path;
      QUERY_ID m_query_id;
      int m_owner_tran_index;
      unsigned int m_worker_id;
      raw_fd_key m_key;
      std::uint64_t m_segment_id;
      bool m_tde_encrypted;
      QMGR_TEMP_FILE *m_tfile_owner;
      std::atomic<bool> m_destroyed { false };
  };

  bool raw_fd_master_enabled () noexcept;
  void initialize_raw_fd_boot_sweep () noexcept;
  bool raw_fd_writes_enabled () noexcept;

  raw_fd_file *create_raw_fd_file (THREAD_ENTRY * thread_p, QUERY_ID query_id, int owner_tran_index,
                                   unsigned int worker_id, int *os_error_out) noexcept;
  void destroy_raw_fd_file (raw_fd_file *file_p) noexcept;
  void reassign_raw_fd_owner (raw_fd_file *file_p, QMGR_TEMP_FILE * new_owner) noexcept;
  void cleanup_query_raw_fd_files (int owner_tran_index, QUERY_ID query_id) noexcept;
  void reap_raw_fd_files () noexcept;

  int rawfd_write_page (THREAD_ENTRY * thread_p, raw_fd_file &file, PAGEID page_index, PAGE_PTR page_p) noexcept;
  PAGE_PTR rawfd_pos_read (THREAD_ENTRY * thread_p, raw_fd_file &file,
                           const raw_fd_page_coordinate &coordinate) noexcept;
  void rawfd_invalidate_cached_page (raw_fd_file &file, PAGEID page_index) noexcept;
  int rawfd_rewrite_page (THREAD_ENTRY * thread_p, raw_fd_file &file, PAGEID page_index, PAGE_PTR page_p) noexcept;
  int rawfd_flush_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, PAGE_PTR page_p, int free_page) noexcept;
  int rawfd_release_fixed_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, PAGE_PTR page_p) noexcept;

  /* (c′) PAGE_SPILL_OVERFLOW consumer shims (#132) -- same signatures/contract
   * as the rawfd_* pair so the qmgr dispatch branches stay symmetric. */
  int spill_flush_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, PAGE_PTR page_p, int free_page) noexcept;
  int spill_release_fixed_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, PAGE_PTR page_p) noexcept;

  int rawfd_single_worker_tde_positioned_read_parity (THREAD_ENTRY * thread_p) noexcept;
  int rawfd_mutation_nonce_selftest (THREAD_ENTRY * thread_p) noexcept;
  int qmgr_temp_file_move_selftest (THREAD_ENTRY * thread_p) noexcept;
#ifdef RAWFD_UNIT_TEST
  int rawfd_test_t1_hash_collision () noexcept;
  int rawfd_test_t2_aba_validation () noexcept;
  int rawfd_test_t3_retry_bound () noexcept;
  int rawfd_test_t4_destroyed_gate () noexcept;
  int rawfd_test_t5_purge_order () noexcept;
#endif

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

namespace std
{
  template <>
  struct hash<temp_page_store::rawfd_sec_key>
  {
    std::size_t operator() (const temp_page_store::rawfd_sec_key &key) const noexcept
    {
      const std::uint64_t page_component = static_cast<std::uint64_t> (static_cast<std::int64_t> (key.page_index));
      const std::uint64_t h = key.file_seq ^ (page_component * 0x9E3779B97F4A7C15ULL);
      return static_cast<std::size_t> (h ^ (h >> 32));
    }
  };
}
#endif /* _TEMP_PAGE_STORE_HPP_ */
