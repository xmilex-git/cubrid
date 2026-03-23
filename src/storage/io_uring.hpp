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

#ifndef _IO_URING_HPP_
#define _IO_URING_HPP_

#include <liburing.h>
#include "storage_common.h"
#include "system.h"
#include "dbtype_def.h"

namespace iouring
{
  enum class return_code:UINT8
  {
    SUCCESS = 0,
    ERROR = 1,
    QUEUE_FULL = 2,
    QUEUE_EMPTY = 3
  };

  const size_t IO_URING_DEFAULT_QUEUE_SIZE = 1 << 11;

  class manager
  {
    public:
      /* initialize & finalize */
      void initialize();
      void finalize();

      /* read request - not system call until submit() calls */
      return_code add_read_req (UINT64 key, void *buf, int fd, UINT32 size, off_t offset);
      void submit();

      /* wait read request until one returned */
      return_code wait_read_req (UINT64 *key_ptr);

      size_t m_curr_sqes;

    private:
      struct io_uring m_ring;
  };
}

#endif /* _IO_URING_HPP_ */

#endif /* SERVER_MODE */
