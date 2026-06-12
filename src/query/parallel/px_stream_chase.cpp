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
 * px_stream_chase.cpp - probe-input chase for the gated streaming hash-join (D1)
 *
 * See px_stream_chase.hpp for the contract.  Load-bearing rules implemented here:
 *  - read-behind frontier: readers never fix the page at the writer's published
 *    last_vpid until the list is closed (the writer's fixed last page is the ONLY
 *    mutable page; everything strictly before it is immutable -- the
 *    parallel_scan::list_id_header rule);
 *  - the writer never blocks on readers: frontier publication is plain atomic stores +
 *    a lock-free notify; readers' waits use a bounded wait_for as the lost-wakeup
 *    backstop;
 *  - R11 prompt stop: the writer checks the stop flag at every inserted tuple and
 *    unwinds via the normal error path, but the stop is NOT an error -- the task
 *    wrapper clears the thread error, resets the benign query-entry error, publishes
 *    closed+stopped and parks no message;
 *  - the end-of-job xasl clear (which re-allocates the list id's type list) waits for
 *    the driver's release_meta () so it can never race the hash-join's reads of the
 *    open list's metadata; every join path releases meta first, so it cannot deadlock.
 */

#if !defined (SERVER_MODE) && !defined (SA_MODE)
#error Belongs to server module
#endif /* !defined (SERVER_MODE) && !defined (SA_MODE) */

#include "px_stream_chase.hpp"

#include <cassert>
#include <chrono>

#include "error_manager.h"
#include "list_file.h"		/* qfile_copy_list_id, qfile_clear_list_id */
#include "memory_alloc.h"
#include "object_representation.h"	/* QFILE_GET_NEXT_VPID */
#include "px_stream_metrics.hpp"
#include "px_worker_manager.hpp"
#include "query_executor.h"	/* qexec_execute_mainblock, qexec_deep_copy_xasl_state, ... */
#include "query_manager.h"	/* qmgr_get_old_page, qmgr_reset_query_error_interrupted */
#include "system_parameter.h"	/* er_log_debug gate */
#include "thread_entry.hpp"
#include "thread_entry_task.hpp"
#include "xasl.h"

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace parallel_query
{
  /*
   * chase_task - the detached writer job: runs the probe-input buildlist's mainblock on
   * the dedicated reserved worker (the px subquery job pattern, stop-aware).
   */
  class chase_task : public cubthread::entry_task
  {
    public:
      explicit chase_task (hjoin_chase *chase)
	: m_chase (chase)
      {
      }

      void execute (cubthread::entry &thread_ref) final;
      void retire () final;

    private:
      hjoin_chase *m_chase;
  };

  hjoin_chase::hjoin_chase (THREAD_ENTRY *parent_thread_p, xasl_node *aptr, xasl_state *xstate_p,
			    worker_manager *worker)
    : m_parent_thread_p (parent_thread_p),
      m_aptr (aptr),
      m_xasl_state_p (xstate_p),
      m_worker (worker),
      m_metrics (NULL),
      m_mtx (),
      m_cv (),
      m_valid (false),
      m_closed (false),
      m_error (false),
      m_stopped (false),
      m_first_vpid (0),
      m_last_vpid (0),
      m_pages_published (0),
      m_tfile (NULL),
      m_stop_requested (false),
      m_meta_released (false),
      m_task_done (false),
      m_joined (false),
      m_cursor_started (false),
      m_cursor_ended (false),
      m_err_messages (),
      m_src_slot_cnt (0)
  {
    VPID_SET_NULL (&m_cursor_next);

    for (int i = 0; i < CHASE_MAX_SRC_SLOTS; i++)
      {
	m_src_slots[i].list.store (NULL, std::memory_order_relaxed);
	m_src_slots[i].last_vpid.store (0, std::memory_order_relaxed);
	m_src_slots[i].closed.store (false, std::memory_order_relaxed);
      }
  }

  void
  hjoin_chase::notify_all_waiters ()
  {
    /* deliberately lock-free: the WRITER must never block behind a reader holding
     * m_mtx across a page fix (R5: writer never blocked by readers).  Reader waits use
     * a bounded wait_for, so a racing notify costs at most one timeout interval. */
    m_cv.notify_all ();
  }

  /*
   * ---- writer side (chase job thread) ----
   */

  void
  hjoin_chase::writer_on_open (QFILE_LIST_ID *list_id)
  {
    assert (list_id != NULL);

    if (list_id->is_domain_resolved && !m_valid.load (std::memory_order_relaxed))
      {
	/* type list final at open: the hash-join may read the open list's metadata */
	m_valid.store (true, std::memory_order_release);
	notify_all_waiters ();
      }
  }

  void
  hjoin_chase::publish_frontier (QFILE_LIST_ID *list_id, bool closing)
  {
    vpid64 last, first;

    last.u64 = 0;
    last.vpid = list_id->last_vpid;
    first.u64 = 0;
    first.vpid = list_id->first_vpid;

    if (m_tfile.load (std::memory_order_relaxed) == NULL && list_id->tfile_vfid != NULL)
      {
	m_tfile.store (list_id->tfile_vfid, std::memory_order_release);
      }

    if (first.vpid.pageid != NULL_PAGEID)
      {
	m_first_vpid.store (first.u64, std::memory_order_release);
      }

    /* readable pages: everything strictly before the (still mutable) last page; after
     * close the whole chain is final */
    int pages = closing ? list_id->page_cnt : ((list_id->page_cnt > 0) ? list_id->page_cnt - 1 : 0);
    m_pages_published.store (pages, std::memory_order_release);

    if (last.vpid.pageid != NULL_PAGEID)
      {
	m_last_vpid.store (last.u64, std::memory_order_release);
      }

    if (!m_valid.load (std::memory_order_relaxed) && list_id->is_domain_resolved)
      {
	m_valid.store (true, std::memory_order_release);
      }

    if (m_metrics != NULL)
      {
	m_metrics->chase_pages.store ((std::uint64_t) pages, std::memory_order_relaxed);
      }

    notify_all_waiters ();
  }

  int
  hjoin_chase::writer_on_tuple (THREAD_ENTRY *thread_p, QFILE_LIST_ID *list_id)
  {
    (void) thread_p;

    if (m_stop_requested.load (std::memory_order_acquire))
      {
	/* R11 prompt stop: unwind through the normal error path; the task wrapper
	 * recognizes the requested stop and publishes closed+stopped with NO error */
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_INTERRUPTED, 0);
	return ER_INTERRUPTED;
      }

    vpid64 last;

    last.u64 = 0;
    last.vpid = list_id->last_vpid;

    /* single writer: a relaxed self-read of the last published value is exact */
    if (last.u64 != m_last_vpid.load (std::memory_order_relaxed))
      {
	publish_frontier (list_id, false);
      }

    return NO_ERROR;
  }

  /*
   * ---- reader side (probe workers) ----
   */

  PAGE_PTR
  hjoin_chase::reader_next_page (cubthread::entry &thread_ref)
  {
    std::unique_lock<std::mutex> lock (m_mtx);

    while (true)
      {
	if (m_cursor_ended)
	  {
	    return NULL;
	  }

	if (m_error.load (std::memory_order_acquire))
	  {
	    /* writer failed: the input is incomplete -- this is an ERROR, never a
	     * silent short read (the parked writer error is surfaced at teardown) */
	    m_cursor_ended = true;
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_INTERRUPTED, 0);
	    return NULL;
	  }

	bool closed = m_closed.load (std::memory_order_acquire);
	vpid64 last;
	last.u64 = m_last_vpid.load (std::memory_order_acquire);

	if (!m_cursor_started)
	  {
	    vpid64 first;
	    first.u64 = m_first_vpid.load (std::memory_order_acquire);

	    if (first.u64 != 0 && first.vpid.pageid != NULL_PAGEID)
	      {
		m_cursor_next = first.vpid;
		m_cursor_started = true;
		continue;
	      }
	    if (closed)
	      {
		/* writer finished without ever producing a page: empty input */
		m_cursor_ended = true;
		return NULL;
	      }
	  }
	else if (m_cursor_next.pageid == NULL_PAGEID)
	  {
	    /* the real chain end (only reachable once closed) */
	    m_cursor_ended = true;
	    return NULL;
	  }
	else if (closed || last.u64 == 0 || !VPID_EQ (&m_cursor_next, &last.vpid))
	  {
	    /* strictly behind the frontier (or the frontier is final): hand the page
	     * to exactly this reader and advance the shared cursor */
	    VPID fix_vpid = m_cursor_next;
	    qmgr_temp_file *tfile = m_tfile.load (std::memory_order_acquire);

	    assert (tfile != NULL);

	    PAGE_PTR page = qmgr_get_old_page (&thread_ref, &fix_vpid, tfile);
	    if (page == NULL)
	      {
		assert (er_errid () != NO_ERROR);
		m_cursor_ended = true;
		return NULL;
	      }

	    QFILE_GET_NEXT_VPID (&m_cursor_next, page);
	    return page;
	  }

	/* frontier reached: wait for writer progress (stop/abort = clean wind-down;
	 * the consumer is gone or the input is no longer needed -- R11) */
	if (m_stop_requested.load (std::memory_order_acquire))
	  {
	    m_cursor_ended = true;
	    return NULL;
	  }

	std::uint64_t t0 = (m_metrics != NULL) ? stream_metrics_now_us () : 0;

	m_cv.wait_for (lock, std::chrono::milliseconds (5));

	if (m_metrics != NULL)
	  {
	    m_metrics->chase_wait_us.fetch_add (stream_metrics_now_us () - t0, std::memory_order_relaxed);
	    m_metrics->chase_wait_cnt.fetch_add (1, std::memory_order_relaxed);
	  }
      }
  }

  /*
   * ---- MERGEABLE-gather source slots ----
   */

  void
  hjoin_chase::src_publish (QFILE_LIST_ID *src_list, bool closing)
  {
    int cnt = m_src_slot_cnt.load (std::memory_order_acquire);
    int idx;

    for (idx = 0; idx < cnt; idx++)
      {
	if (m_src_slots[idx].list.load (std::memory_order_acquire) == src_list)
	  {
	    break;
	  }
      }

    if (idx == cnt)
      {
	/* register a new slot (claim is append-only; one writer per list, so the same
	 * list can never be registered twice concurrently) */
	idx = m_src_slot_cnt.fetch_add (1, std::memory_order_acq_rel);
	if (idx >= CHASE_MAX_SRC_SLOTS)
	  {
	    /* cannot happen: slots >> max scan parallelism; degrade to no publication
	     * (the gather then consumes this list only after ALL workers finish) */
	    assert (false);
	    m_src_slot_cnt.store (CHASE_MAX_SRC_SLOTS, std::memory_order_release);
	    return;
	  }
      }

    vpid64 last;

    last.u64 = 0;
    last.vpid = src_list->last_vpid;
    m_src_slots[idx].last_vpid.store (last.u64, std::memory_order_release);
    if (closing)
      {
	m_src_slots[idx].closed.store (true, std::memory_order_release);
      }
    /* the list pointer is published LAST: a slot is consumable only once its frontier
     * is visible */
    m_src_slots[idx].list.store (src_list, std::memory_order_release);

    notify_all_waiters ();
  }

  QFILE_LIST_ID *
  hjoin_chase::src_get (int idx, VPID *frontier_out, bool *closed_out)
  {
    QFILE_LIST_ID *src_list = m_src_slots[idx].list.load (std::memory_order_acquire);

    if (src_list == NULL)
      {
	return NULL;
      }

    vpid64 last;

    last.u64 = m_src_slots[idx].last_vpid.load (std::memory_order_acquire);
    *frontier_out = last.vpid;
    *closed_out = m_src_slots[idx].closed.load (std::memory_order_acquire);

    return src_list;
  }

  void
  hjoin_chase::copy_wait ()
  {
    std::unique_lock<std::mutex> lock (m_mtx);

    m_cv.wait_for (lock, std::chrono::milliseconds (5));
  }

  /*
   * ---- driver-side waits / joins ----
   */

  int
  hjoin_chase::wait_input_usable (THREAD_ENTRY *thread_p)
  {
    {
      std::unique_lock<std::mutex> lock (m_mtx);

      while (!m_valid.load (std::memory_order_acquire) && !m_closed.load (std::memory_order_acquire)
	     && !m_error.load (std::memory_order_acquire))
	{
	  m_cv.wait_for (lock, std::chrono::milliseconds (10));
	}
    }

    if (m_error.load (std::memory_order_acquire))
      {
	join (thread_p);
	return surface_writer_error ();
      }

    return NO_ERROR;
  }

  void
  hjoin_chase::release_meta ()
  {
    if (!m_meta_released.exchange (true, std::memory_order_acq_rel))
      {
	notify_all_waiters ();
      }
  }

  void
  hjoin_chase::request_stop ()
  {
    if (!m_stop_requested.exchange (true, std::memory_order_acq_rel))
      {
	if (m_metrics != NULL)
	  {
	    m_metrics->chase_stop_request_us.store (stream_metrics_now_us (), std::memory_order_relaxed);
	  }
      }
    notify_all_waiters ();
  }

  void
  hjoin_chase::join (THREAD_ENTRY *thread_p)
  {
    (void) thread_p;

    if (m_joined.load (std::memory_order_acquire))
      {
	return;
      }

    /* meta must be released BEFORE waiting for the task (the task waits for it before
     * its end-of-job clear) -- this ordering makes the join deadlock-free */
    release_meta ();

    {
      std::unique_lock<std::mutex> lock (m_mtx);

      while (!m_task_done.load (std::memory_order_acquire))
	{
	  m_cv.wait_for (lock, std::chrono::milliseconds (10));
	}
    }

    /* drain the dedicated worker (the task's retire) without releasing the reservation */
    m_worker->wait_workers ();

    m_joined.store (true, std::memory_order_release);
  }

  int
  hjoin_chase::join_for_data (THREAD_ENTRY *thread_p)
  {
    {
      std::unique_lock<std::mutex> lock (m_mtx);

      while (!m_closed.load (std::memory_order_acquire) && !m_error.load (std::memory_order_acquire))
	{
	  m_cv.wait_for (lock, std::chrono::milliseconds (10));
	}
    }

    join (thread_p);

    if (m_error.load (std::memory_order_acquire))
      {
	return surface_writer_error ();
      }

    return NO_ERROR;
  }

  int
  hjoin_chase::surface_writer_error ()
  {
    {
      std::lock_guard<std::mutex> lock (m_err_messages.m_mutex);

      if (!m_err_messages.m_error_messages.empty ()
	  && (er_errid () == NO_ERROR || er_errid () == ER_INTERRUPTED))
	{
	  cuberr::context::get_thread_local_context ().get_current_error_level ().swap (
		  *m_err_messages.m_error_messages[0]);
	}
    }

    if (er_errid () == NO_ERROR)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_INTERRUPTED, 0);
      }

    return er_errid ();
  }

  /*
   * ---- pipeline chase-binding hooks ----
   */

  void
  hjoin_chase::pipeline_request_stop (void *handle)
  {
    if (handle != NULL)
      {
	((hjoin_chase *) handle)->request_stop ();
      }
  }

  void
  hjoin_chase::pipeline_join (THREAD_ENTRY *thread_p, void *handle)
  {
    if (handle != NULL)
      {
	((hjoin_chase *) handle)->join (thread_p);
      }
  }

  /*
   * ---- arm / reap (driver) ----
   */

  hjoin_chase *
  hjoin_chase::try_arm (THREAD_ENTRY *thread_p, xasl_node *hashjoin_xasl, xasl_node *probe_aptr,
			xasl_state *xstate_p)
  {
    assert (hashjoin_xasl != NULL && hashjoin_xasl->proc.hashjoin.chase_state == NULL);
    assert (probe_aptr != NULL && probe_aptr->chase_progress == NULL);
    assert (xstate_p != NULL);

    /* the writer's DEDICATED worker: best-effort, taken before (and outside) the
     * pipeline's atomic D_p + D_c grant, held for the writer's whole lifetime (R5) */
    worker_manager *worker = worker_manager::try_reserve_workers (1);
    if (worker == NULL)
      {
	/* shortfall: no chase -- the aptr is dispatched as today (pre-emit fallback) */
	er_clear ();
	return NULL;
      }

    /* stable per-chase xasl_state: deep-copied HERE, single-threaded on the driver, so
     * the detached task never reads the driver's live xasl_state concurrently; the
     * task re-copies from THIS stable copy on its own thread (heap symmetry) */
    xasl_state *xstate_copy = qexec_deep_copy_xasl_state (thread_p, xstate_p);
    if (xstate_copy == NULL)
      {
	er_clear ();
	worker->release_workers ();
	return NULL;
      }

    hjoin_chase *chase = (hjoin_chase *) db_private_alloc (thread_p, sizeof (hjoin_chase));
    if (chase == NULL)
      {
	er_clear ();
	qexec_free_xasl_state (thread_p, xstate_copy);
	worker->release_workers ();
	return NULL;
      }
    chase = placement_new (chase, thread_p, probe_aptr, xstate_copy, worker);

    /* publish BEFORE the task can run: the writer-side hooks key off these */
    probe_aptr->chase_progress = (void *) chase;
    hashjoin_xasl->proc.hashjoin.chase_state = (void *) chase;

    chase_task *task = new chase_task (chase);
    worker->push_task (task);

    er_log_debug (ARG_FILE_LINE, "HJSTREAM chase-armed: hashjoin %p probe_input %p (dedicated writer worker)\n",
		  (void *) hashjoin_xasl, (void *) probe_aptr);

    return chase;
  }

  void
  hjoin_chase::reap (THREAD_ENTRY *thread_p, xasl_node *hashjoin_xasl)
  {
    hjoin_chase *chase = (hjoin_chase *) hashjoin_xasl->proc.hashjoin.chase_state;

    if (chase == NULL)
      {
	return;
      }

    /* idempotent prompt stop + join: on every path that reaches reap the input data is
     * either fully consumed (writer long closed; both calls are no-ops) or no longer
     * needed (error / empty-build short-circuit / teardown) */
    chase->request_stop ();
    chase->join (thread_p);

    /* improve an already-failing query's generic interrupt with the writer's real
     * parked error; NEVER raise anything on a succeeding query (A8) */
    if (chase->has_writer_error () && er_errid () == ER_INTERRUPTED)
      {
	std::lock_guard<std::mutex> lock (chase->m_err_messages.m_mutex);

	if (!chase->m_err_messages.m_error_messages.empty ())
	  {
	    cuberr::context::get_thread_local_context ().get_current_error_level ().swap (
		    *chase->m_err_messages.m_error_messages[0]);
	  }
      }

    hashjoin_xasl->proc.hashjoin.chase_state = NULL;
    if (chase->m_aptr != NULL)
      {
	chase->m_aptr->chase_progress = NULL;
      }


    /* release the dedicated worker reservation exactly once (frees the manager) */
    chase->m_worker->release_workers ();
    chase->m_worker = NULL;

    qexec_free_xasl_state (thread_p, chase->m_xasl_state_p);
    chase->m_xasl_state_p = NULL;

    chase->~hjoin_chase ();
    db_private_free (thread_p, chase);
  }

  /*
   * ---- the detached writer job ----
   */

  void
  chase_task::execute (cubthread::entry &thread_ref)
  {
    hjoin_chase *chase = m_chase;
    THREAD_ENTRY *cur_thread_p = &thread_ref;
    THREAD_ENTRY *parent_thread_p = chase->m_parent_thread_p;
    xasl_node *aptr = chase->m_aptr;
    xasl_state *xstate_copy = NULL;
    int err = NO_ERROR;

    /* thread context: the px subquery job pattern (execute_job_internal) */
    css_conn_entry *saved_conn = cur_thread_p->conn_entry;
    int saved_tran_index = cur_thread_p->tran_index;
    bool saved_on_trace = cur_thread_p->on_trace;
    THREAD_ENTRY *saved_orig = cur_thread_p->m_px_orig_thread_entry;

    cur_thread_p->conn_entry = parent_thread_p->conn_entry;
    cur_thread_p->tran_index = parent_thread_p->tran_index;
    cur_thread_p->on_trace = false;	/* the chase is never armed on trace runs */
    cur_thread_p->m_px_orig_thread_entry = parent_thread_p;

    /* per-job deep copy on THIS thread (alloc/free heap symmetry, the px job pattern) */
    xstate_copy = qexec_deep_copy_xasl_state (cur_thread_p, chase->m_xasl_state_p);
    if (xstate_copy == NULL)
      {
	chase->m_err_messages.move_top_error_message_to_this ();
	chase->m_error.store (true, std::memory_order_release);
      }
    else if (chase->m_stop_requested.load (std::memory_order_acquire))
      {
	/* stopped before it ever ran: nothing produced, nothing to clean */
	chase->m_stopped.store (true, std::memory_order_release);
      }
    else
      {
	err = qexec_execute_mainblock (cur_thread_p, aptr, xstate_copy, NULL);

	if (err == NO_ERROR)
	  {
	    /* publish the FINAL frontier (whole chain readable through NULL_PAGEID) */
	    if (aptr->list_id != NULL)
	      {
		chase->publish_frontier (aptr->list_id, true);
	      }
	  }
	else if (chase->m_stop_requested.load (std::memory_order_acquire))
	  {
	    /* R11: a requested stop is NOT an error -- nothing reads the incomplete
	     * list afterwards; clear the benign unwind error from this thread AND from
	     * the query entry (the writer's exit_on_error parked ER_INTERRUPTED there),
	     * so the query returns its correct (prefix) result with no spurious error */
	    qmgr_reset_query_error_interrupted (cur_thread_p, xstate_copy->query_id);
	    er_clear ();
	    aptr->status = XASL_SUCCESS;
	    chase->m_stopped.store (true, std::memory_order_release);
	  }
	else
	  {
	    /* real writer error: park it (surfaced reader/driver-side); never touch the
	     * driver's live error context, never interrupt the transaction */
	    chase->m_err_messages.move_top_error_message_to_this ();
	    chase->m_error.store (true, std::memory_order_release);
	  }
      }

    if (chase->m_metrics != NULL)
      {
	chase->m_metrics->chase_writer_end_us.store (stream_metrics_now_us (), std::memory_order_relaxed);
	chase->m_metrics->chase_stopped.store (chase->m_stopped.load (std::memory_order_relaxed) ? 1 : 0,
					       std::memory_order_relaxed);
      }

    /* closed on EVERY exit path; wakes the hash-join wait and all readers */
    chase->m_closed.store (true, std::memory_order_release);
    chase->notify_all_waiters ();

    /* metadata handshake: the driver may still be reading the open list's type list /
     * domains; the clear below re-allocates them.  Every join path releases meta
     * first, so this wait is bounded and deadlock-free. */
    {
      std::unique_lock<std::mutex> lock (chase->m_mtx);

      while (!chase->m_meta_released.load (std::memory_order_acquire))
	{
	  chase->m_cv.wait_for (lock, std::chrono::milliseconds (10));
	}
    }

    /* worker-side end-of-job cleanup, list preserved (the px subquery job pattern);
     * readers only ever touch the published header copies and the immutable pages,
     * never the live list-id struct, so this cannot race them */
    if (xstate_copy != NULL)
      {
	QFILE_LIST_ID list_id_copy;
	bool has_list = false;

	if (aptr->list_id != NULL && aptr->list_id->type_list.type_cnt > 0)
	  {
	    qfile_copy_list_id (&list_id_copy, aptr->list_id, true, QFILE_MOVE_DEPENDENT);
	    has_list = true;
	    qfile_clear_list_id (aptr->list_id);
	  }

	(void) qexec_clear_xasl_for_parallel_aptr (cur_thread_p, aptr, true);

	if (has_list)
	  {
	    qfile_copy_list_id (aptr->list_id, &list_id_copy, true, QFILE_MOVE_DEPENDENT);
	    qfile_clear_list_id (&list_id_copy);
	  }

	qexec_free_xasl_state (cur_thread_p, xstate_copy);
      }

    /* restore worker thread context */
    cur_thread_p->conn_entry = saved_conn;
    cur_thread_p->tran_index = saved_tran_index;
    cur_thread_p->on_trace = saved_on_trace;
    cur_thread_p->m_px_orig_thread_entry = saved_orig;

    /* the task's LAST signal: after this the driver's join may proceed */
    chase->m_task_done.store (true, std::memory_order_release);
    chase->notify_all_waiters ();
  }

  void
  chase_task::retire ()
  {
    worker_manager *worker = m_chase->m_worker;

    delete this;

    /* very last touch: the driver's join waits on this via wait_workers () */
    worker->pop_task ();
  }
}

/*
 * C-style wrappers (engine .c files)
 */

bool
qexec_hjoin_chase_try_arm (THREAD_ENTRY *thread_p, xasl_node *hashjoin_xasl, xasl_node *probe_aptr,
			   xasl_state *xstate_p)
{
  return parallel_query::hjoin_chase::try_arm (thread_p, hashjoin_xasl, probe_aptr, xstate_p) != NULL;
}

void
qexec_hjoin_chase_reap (THREAD_ENTRY *thread_p, xasl_node *hashjoin_xasl)
{
  parallel_query::hjoin_chase::reap (thread_p, hashjoin_xasl);
}

void
qexec_hjoin_chase_writer_open (void *chase_arg, QFILE_LIST_ID *list_id)
{
  ((parallel_query::hjoin_chase *) chase_arg)->writer_on_open (list_id);
}

int
qexec_hjoin_chase_writer_on_tuple (THREAD_ENTRY *thread_p, void *chase_arg, QFILE_LIST_ID *list_id)
{
  return ((parallel_query::hjoin_chase *) chase_arg)->writer_on_tuple (thread_p, list_id);
}

bool
qexec_hjoin_chase_is_closed (void *chase_arg)
{
  return ((parallel_query::hjoin_chase *) chase_arg)->is_closed ();
}

int
qexec_hjoin_chase_pages_published (void *chase_arg)
{
  return ((parallel_query::hjoin_chase *) chase_arg)->pages_published ();
}

int
qexec_hjoin_chase_wait_input_usable (THREAD_ENTRY *thread_p, void *chase_arg)
{
  return ((parallel_query::hjoin_chase *) chase_arg)->wait_input_usable (thread_p);
}

void
qexec_hjoin_chase_release_meta (void *chase_arg)
{
  ((parallel_query::hjoin_chase *) chase_arg)->release_meta ();
}

int
qexec_hjoin_chase_join_for_data (THREAD_ENTRY *thread_p, void *chase_arg)
{
  return ((parallel_query::hjoin_chase *) chase_arg)->join_for_data (thread_p);
}

void
qexec_hjoin_chase_request_stop (void *chase_arg)
{
  ((parallel_query::hjoin_chase *) chase_arg)->request_stop ();
}

void
qexec_hjoin_chase_join (THREAD_ENTRY *thread_p, void *chase_arg)
{
  ((parallel_query::hjoin_chase *) chase_arg)->join (thread_p);
}

void
qexec_hjoin_chase_src_publish (void *chase_arg, QFILE_LIST_ID *src_list)
{
  ((parallel_query::hjoin_chase *) chase_arg)->src_publish (src_list, false);
}

void
qexec_hjoin_chase_src_close (void *chase_arg, QFILE_LIST_ID *src_list)
{
  ((parallel_query::hjoin_chase *) chase_arg)->src_publish (src_list, true);
}

int
qexec_hjoin_chase_src_count (void *chase_arg)
{
  return ((parallel_query::hjoin_chase *) chase_arg)->src_count ();
}

QFILE_LIST_ID *
qexec_hjoin_chase_src_get (void *chase_arg, int idx, VPID *frontier_out, bool *closed_out)
{
  return ((parallel_query::hjoin_chase *) chase_arg)->src_get (idx, frontier_out, closed_out);
}

void
qexec_hjoin_chase_src_consume (void *chase_arg, int idx)
{
  ((parallel_query::hjoin_chase *) chase_arg)->src_consume (idx);
}

bool
qexec_hjoin_chase_stop_requested (void *chase_arg)
{
  return ((parallel_query::hjoin_chase *) chase_arg)->stop_requested ();
}

void
qexec_hjoin_chase_copy_wait (void *chase_arg)
{
  ((parallel_query::hjoin_chase *) chase_arg)->copy_wait ();
}
