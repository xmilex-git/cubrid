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
 * parallel_heap_scan.hpp - Parallel Heap Scan.
 */

#ifndef _PARALLEL_HEAP_SCAN_H_
#define _PARALLEL_HEAP_SCAN_H_
#include "dbtype.h"
#include "scan_manager.h"
#include "thread_manager.hpp"

#define HP_PARALLELISM 2
#define HP_RESULT_QUEUE_SIZE 21


class parallel_heap_scan_context;
class parallel_heap_scan_task;
class parallel_heap_scan_master
{
  public:

    cubthread::entry_workpool *m_workpool;
    parallel_heap_scan_context *m_context;
    std::atomic<bool> m_is_reset_once;
    std::atomic<bool> m_is_start_once;
    SCAN_ID *m_scan_id;
    int parallelism;

    parallel_heap_scan_master() = delete;
    parallel_heap_scan_master (int tran_index, SCAN_ID *scan_id, size_t pool_size, size_t task_max_count,
			       std::size_t core_count);
    ~parallel_heap_scan_master();
    SCAN_CODE get_result (THREAD_ENTRY *thread_p, SCAN_ID *scan_id);
    void start (THREAD_ENTRY *thread_p);
    void reset (SCAN_ID *scan_id);
    void start_tasks (SCAN_ID *scan_id);
    void end();

    // ... 필요한 메서드들 추가
};

extern SCAN_CODE
scan_next_parallel_heap_scan (THREAD_ENTRY *thread_p, SCAN_ID *scan_id);
extern int
scan_reset_scan_block_parallel_heap_scan (THREAD_ENTRY *thread_p, SCAN_ID *scan_id);
extern void
scan_end_parallel_heap_scan (THREAD_ENTRY *thread_p, SCAN_ID *scan_id);
extern void
scan_close_parallel_heap_scan (THREAD_ENTRY *thread_p, SCAN_ID *scan_id);
extern int
scan_open_parallel_heap_scan (THREAD_ENTRY *thread_p, SCAN_ID *scan_id,
			      /* fields of SCAN_ID */
			      bool mvcc_select_lock_needed, SCAN_OPERATION_TYPE scan_op_type, int fixed,
			      int grouped, QPROC_SINGLE_FETCH single_fetch, DB_VALUE *join_dbval,
			      val_list_node *val_list, VAL_DESCR *vd,
			      /* fields of HEAP_SCAN_ID */
			      OID *cls_oid, HFID *hfid, regu_variable_list_node *regu_list_pred,
			      PRED_EXPR *pr, regu_variable_list_node *regu_list_rest, int num_attrs_pred,
			      ATTR_ID *attrids_pred, HEAP_CACHE_ATTRINFO *cache_pred, int num_attrs_rest,
			      ATTR_ID *attrids_rest, HEAP_CACHE_ATTRINFO *cache_rest, SCAN_TYPE scan_type,
			      DB_VALUE **cache_recordinfo, regu_variable_list_node *regu_list_recordinfo,
			      bool is_partition_table);
extern int
scan_start_parallel_heap_scan (THREAD_ENTRY *thread_p, SCAN_ID *scan_id);

extern int
scan_check_parallel_heap_scan_possible (THREAD_ENTRY *thread_p, void *spec, bool mvcc_select_lock_needed);
#endif /* _PARALLEL_HEAP_SCAN_H_ */
