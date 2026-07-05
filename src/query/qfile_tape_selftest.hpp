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
 * qfile_tape_selftest.hpp - in-server self-test entry points for the tape /
 * tapeset backing (moved out of qfile_tape.cpp).  Debug-invoked from
 * query_manager.c, gated by CUBRID_WM_*_SELFTEST env vars.
 */

#ifndef _QFILE_TAPE_SELFTEST_HPP_
#define _QFILE_TAPE_SELFTEST_HPP_

#include "thread_compat.hpp"	/* THREAD_ENTRY */

/*
 * In-server self-test of the holdable-result lifecycle (Phase1 1C, redesign
 * G007 #72): build a spilled Tapeset behind a QFILE_LIST_ID, reparent its
 * ownership transaction -> session via qfile_copy_list_id(MOVE) with zero copy
 * (census unchanged), read the remaining tuples post-reparent (robust parity),
 * then tear the session list_id down via qfile_clear_list_id and assert
 * orphan-zero -- the private file is unlinked AND the RAM prefix is freed
 * (ADR 0001).  Exercises a real on-disk file (and the TDE-encrypted backing
 * when a cipher is loaded), which the bootless unit test cannot.  Gated by env
 * CUBRID_WM_HELDTAPE_SELFTEST.  Returns 0 on PASS.
 */
int qfile_heldtape_selftest (THREAD_ENTRY *thread_p);

/* In-server self-test of N-reader concurrent read over a frozen (TDE) Tape
 * (ADR 0005, #78 2A-0).  Gated by env CUBRID_WM_TAPEREAD_SELFTEST (debug). */
int qfile_taperead_selftest (THREAD_ENTRY *thread_p);

/* In-server self-test of the 2A-1 NEW-backing producer hook (redesign #78):
 * produce a list via qfile_add_tuple_to_list onto a producer_writer_-attached
 * QFILE_LIST_ID, close (freeze into a Tapeset), then scan via tapeset_scan and
 * assert robust parity.  Gated by env CUBRID_WM_PRODUCER_SELFTEST (debug). */
int qfile_producer_selftest (THREAD_ENTRY *thread_p);

#if !defined (NDEBUG)
/* In-server self-test of the close/freeze ENOSPC failure-propagation contract
 * (#86): drive the buffile flush fault injector so an append/freeze flush
 * fails, then assert freeze () returns NULL and a failed close makes scan-open
 * raise ER_QPROC_OUT_OF_TEMP_SPACE (never a silent 0-row / truncated result),
 * with the census back to baseline.  Gated by env CUBRID_WM_CLOSE_FAULT_SELFTEST
 * (debug).  Returns 0 on PASS. */
int qfile_close_fault_selftest (THREAD_ENTRY *thread_p);

/* In-server self-test of freeze() OOM ownership recovery (#95): drive the tape
 * alloc fault injector so each freeze path's noexcept-new returns NULL, then
 * assert freeze()==NULL (no crash, no fake success), ER_OUT_OF_VIRTUAL_MEMORY,
 * ownership retained by the writer, and census back to baseline after teardown
 * (fd/file/prefix reclaimed).  Gated by env CUBRID_WM_FREEZE_OOM_SELFTEST
 * (debug).  Returns 0 on PASS. */
int qfile_freeze_oom_selftest (THREAD_ENTRY *thread_p);

/* In-server self-test of the BufFile fd-exhaustion error mapping (#125): drive
 * the buffile create fault injector so open () reports EMFILE then ENFILE, and
 * assert each spill append raises ER_QPROC_OUT_OF_TEMP_SPACE (parity with the
 * legacy raw-fd is_fd_or_space_error mapping), not a generic ER_FAILED, with the
 * census back to baseline.  Gated by env CUBRID_WM_EMFILE_FAULT_SELFTEST
 * (debug).  Returns 0 on PASS. */
int qfile_emfile_fault_selftest (THREAD_ENTRY *thread_p);
#endif /* !NDEBUG */

#endif /* _QFILE_TAPE_SELFTEST_HPP_ */
