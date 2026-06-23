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
  RAW_FD_OVERFLOW
};

namespace temp_page_store
{
  constexpr std::size_t projected_tuple_position_bytes = 48;
  constexpr std::size_t projected_tuple_position_db_bytes = 32;
  constexpr std::size_t projected_tuple_simple_pos_bytes = 24;
  constexpr std::size_t future_read_cache_placeholder_bytes = 512ULL * 1024ULL;

  struct raw_fd_key
  {
    std::uint64_t boot_incarnation;
    std::uint64_t file_seq;
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
      bool is_open () const noexcept;
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

      int m_fd;
      std::string m_path;
      QUERY_ID m_query_id;
      int m_owner_tran_index;
      unsigned int m_worker_id;
      raw_fd_key m_key;
      std::uint64_t m_segment_id;
      QMGR_TEMP_FILE *m_tfile_owner;
  };

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
  int rawfd_flush_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, PAGE_PTR page_p, int free_page) noexcept;
  void rawfd_release_fixed_page (QMGR_TEMP_FILE * tfile_p, PAGE_PTR page_p) noexcept;

  int rawfd_single_worker_tde_positioned_read_parity (THREAD_ENTRY * thread_p) noexcept;
  int qmgr_temp_file_move_selftest (THREAD_ENTRY * thread_p) noexcept;

  PAGE_PTR alloc_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, VPID * vpid_p);
  PAGE_PTR fix_old_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, VPID * vpid_p);

  bool reserve_membuf_budget (int requested_pages, int *granted_pages_out, std::size_t *reserved_bytes_out,
                              int *reserved_shard_out) noexcept;
  void release_held_reservation (QMGR_TEMP_FILE * tfile_p) noexcept;

  bool reserve_held (std::size_t bytes, int *shard_out) noexcept;
  void release_held (std::size_t bytes, int shard_index) noexcept;

  std::size_t reservation_bytes_for_pages (std::size_t pages) noexcept;
  std::size_t reservation_bytes_for_degree (UINT32 degree, std::size_t pages_per_worker = 0) noexcept;
  UINT32 clamp_degree_for_workmem (UINT32 requested_degree, std::size_t pages_per_worker = 0) noexcept;
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
