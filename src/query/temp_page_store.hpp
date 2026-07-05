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
  PRIVATE_SPILL_FALLBACK,
  PAGE_SPILL		/* (c′) per-tfile page-spill cache, #132; the sole OLD-tier
				 * membuf-overflow backing since 커밋 B deleted raw-fd (#74 §5, #137) */
};

namespace temp_page_store
{
  /* (c′) PAGE_SPILL consumer shim (#132) -- keep the qmgr dispatch
   * branch symmetric with the pre-커밋 B rawfd_* pair contract. */
  int spill_release_fixed_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, PAGE_PTR page_p) noexcept;

  int qmgr_temp_file_move_selftest (THREAD_ENTRY * thread_p) noexcept;

  PAGE_PTR alloc_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, VPID * vpid_p);
  PAGE_PTR fix_old_page (THREAD_ENTRY * thread_p, QMGR_TEMP_FILE * tfile_p, VPID * vpid_p);
}
#endif /* _TEMP_PAGE_STORE_HPP_ */
