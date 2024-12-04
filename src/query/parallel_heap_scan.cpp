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
 * subquery_cache.c - Correlated Scalar Subquery Result Cache.
 */


#ident "$Id$"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "parallel_heap_scan.hpp"

#include "thread_manager.hpp"
#include "thread_entry_task.hpp"
#include "memory_alloc.h"
#include "memory_private_allocator.hpp"
#include "xasl.h"

#include "regu_var.hpp"


// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

static int regu_var_list_len (REGU_VARIABLE_LIST list);
static int regu_var_list_capture_to_dbvalue_array (REGU_VARIABLE_LIST list, DB_VALUE_ARRAY *dbvalue_array);
static int dbvalue_array_copy_to_regu_var_list (DB_VALUE_ARRAY *dbvalue_array, REGU_VARIABLE_LIST *list);
static REGU_VARIABLE_LIST regu_var_list_clone (REGU_VARIABLE_LIST *list);

class ParallelHeapScanResultQueueEntry
{
  public:
    DB_VALUE_ARRAY pred_val_array;
    DB_VALUE_ARRAY rest_val_array;
    SCAN_CODE scan_code;
    OID curr_oid;
    bool valid;
  
  int capture_pred_regu_var_list (REGU_VARIABLE_LIST *list);
  int capture_rest_regu_var_list (REGU_VARIABLE_LIST *list);
  int copy_to_pred_regu_var_list (REGU_VARIABLE_LIST *list);
  int copy_to_rest_regu_var_list (REGU_VARIABLE_LIST *list);

};

class ParallelHeapScanResultQueue
{
  public:
    ParallelHeapScanResultQueueEntry entries[HP_RESULT_QUEUE_SIZE];
    std::mutex mutex;
    std::condition_variable cond;
    int start;
    int end;
    int waiting;

    static int init ();
    static void destroy ();
    static int clear ();
    static bool isFull ();
    static bool isEmpty ();
    static int enqueue (ParallelHeapScanResultQueueEntry *entry);
    static int dequeue (ParallelHeapScanResultQueueEntry *entry);
};






