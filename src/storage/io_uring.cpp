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

#if SERVER_MODE
#include "io_uring.hpp"

namespace iouring
{

  void manager::initialize()
  {
    const int IOURING_FLAGS = 0;
    m_curr_sqes = 0;
    io_uring_queue_init (IO_URING_DEFAULT_QUEUE_SIZE, &m_ring, IOURING_FLAGS);
  }

  void manager::finalize()
  {
    io_uring_queue_exit (&m_ring);
  }

  /* read request - not system call until submit() calls */
  return_code manager::add_read_req (UINT64 key, void *buf, int fd, UINT32 size, off_t offset)
  {
    if (m_curr_sqes >= IO_URING_DEFAULT_QUEUE_SIZE)
      {
	return return_code::QUEUE_FULL;
      }

    struct io_uring_sqe *sqe = io_uring_get_sqe (&m_ring);
    if (sqe == nullptr)
      {
	return return_code::ERROR;
      }

    io_uring_prep_read (sqe, fd, buf, size, offset);
    sqe->user_data = key;
    m_curr_sqes++;
    return return_code::SUCCESS;
  }

  void manager::submit()
  {
    io_uring_submit (&m_ring);
  }

  /* wait read request until one returned */
  return_code manager::wait_read_req (UINT64 *key_ptr)
  {
    struct io_uring_cqe *cqe;
    int error_code;
    if (m_curr_sqes <= 0)
      {
	return return_code::QUEUE_EMPTY;
      }

    error_code = io_uring_wait_cqe (&m_ring, &cqe);
    if (error_code < 0)
      {
	return return_code::ERROR;
      }

    if (cqe->res < 0)
      {
	return return_code::ERROR;
      }

    *key_ptr = cqe->user_data;
    io_uring_cqe_seen (&m_ring, cqe);
    m_curr_sqes--;

    return return_code::SUCCESS;
  }
}
#endif
