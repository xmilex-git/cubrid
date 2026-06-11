/*
 *
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
 * px_scan_input_handler_stream.cpp
 */

#include "px_scan_input_handler_stream.hpp"

#include "error_code.h"
#include "error_manager.h"

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace parallel_scan
{
  int
  input_handler_stream::init_on_main (THREAD_ENTRY *thread_p, parallel_query::stream_source *source_p,
				      int parallelism)
  {
    (void) thread_p;
    (void) parallelism;

    if (source_p == nullptr)
      {
	assert_release (false);
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
	return ER_QPROC_INVALID_XASLNODE;
      }

    /* single-drain open; a second consumer or a re-open is refused inside (fan-out=1, R4) */
    int error_code = source_p->open ();
    if (error_code != NO_ERROR)
      {
	m_source_p = nullptr;
	return error_code;
      }

    m_source_p = source_p;
    return NO_ERROR;
  }

  SCAN_CODE
  input_handler_stream::get_next_batch (THREAD_ENTRY *thread_p, parallel_query::row_batch &out_batch)
  {
    if (m_source_p == nullptr)
      {
	assert_release (false);
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
	return S_ERROR;
      }

    /* MPMC pop is worker-safe; no thread-local claim state is needed (unlike the list
     * handler's sector cursor).  Ownership of out_batch.buf transfers to this worker on
     * S_SUCCESS. */
    return m_source_p->next_batch (thread_p, out_batch);
  }

  int
  input_handler_stream::initialize (THREAD_ENTRY *thread_p, HFID *hfid, SCAN_ID *scan_id)
  {
    (void) thread_p;
    (void) hfid;
    (void) scan_id;
    return NO_ERROR;
  }

  int
  input_handler_stream::finalize (THREAD_ENTRY *thread_p)
  {
    (void) thread_p;
    return NO_ERROR;
  }

  void
  input_handler_stream::cleanup_on_main (THREAD_ENTRY *thread_p)
  {
    if (m_source_p != nullptr)
      {
	/* R11: before EOS this aborts the channel (prompt producer stop, no spurious
	 * error); after EOS it is a quiet transition.  Residue is freed later by the
	 * pipeline teardown runner via stream_source::drain_residue () once all
	 * producers are joined -- never here, where producers may still be live. */
	m_source_p->close (thread_p);
	m_source_p = nullptr;
      }
  }
}
