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
 * px_stream_chase.hpp - probe-input chase for the gated streaming hash-join (D1)
 *
 * Lets the streamed hash-join START (build + arm + probe) as soon as its BUILD input is
 * ready, while the probe-INPUT buildlist (the heavy side) is still materializing: the
 * probe workers read the input list file page-by-page STRICTLY BEHIND its single writer,
 * reusing the proven read-behind-frontier rule of parallel_scan::list_id_header
 * (px_scan_result_handler.cpp): readers never touch the page at the writer's published
 * last_vpid until the list is closed (the writer's fixed last_pgptr is the only mutable
 * page; everything strictly before it is immutable).
 *
 * NOT a second streamed edge (SSOT section 6): no transport, no emit, no consumer of a
 * streamed source -- the probe input is still a fully materialized list file (R7's bound
 * unchanged); the chase only relaxes WHEN readers may begin.
 *
 * Deadlock freedom (R5), layered -- the chase writer sits OUTSIDE the atomic D_p + D_c
 * pipeline grant:
 *  1. pool acquisition: the writer's dedicated worker is a non-blocking
 *     try_reserve_workers (1) taken BEFORE the pipeline grant exists and held for the
 *     writer's whole lifetime, so the writer can never sit queued behind pipeline tasks;
 *     its internal parallel heap-scan workers are best-effort grants that degrade the
 *     scan in-job -- the writer never blocks waiting for pool workers.  No acquisition
 *     blocks while holding another grant.
 *  2. execution: probe workers wait on writer PROGRESS (data); teardown waits on writer
 *     COMPLETION, prompt because the close-consumer step requests the stop first; the
 *     writer waits on NOTHING owned by the pipeline.  All added wait edges point one way
 *     (pipeline -> writer), so no cycle is possible.
 *
 * Early close / abort (R11): request_stop () makes the writer observe the stop at its
 * per-tuple publication checkpoint and unwind promptly; a requested stop is NOT an error
 * (closed + stopped published, no parked message, no spurious query error).
 *
 * Lifetime: armed by the hash-join mainblock's aptr dispatch (one HJOIN_CHASE_STATE per
 * HASHJOIN_PROC execution, hung on proc.hashjoin.chase_state); joined either by the
 * pipeline's ordered teardown (streamed path: close consumer -> join producers -> JOIN
 * CHASE -> drain -> release -> free bundle) or by the hash-join itself before any
 * materialized read of the input (refusal ladder); freed exactly once by
 * qexec_hjoin_chase_reap on the mainblock/driver epilogue.
 *
 * Error model: C error model only -- no exceptions; er_set + return codes.
 */

#ifndef _PX_STREAM_CHASE_HPP_
#define _PX_STREAM_CHASE_HPP_

#if !defined (SERVER_MODE) && !defined (SA_MODE)
#error Belongs to server module
#endif /* !defined (SERVER_MODE) && !defined (SA_MODE) */

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>

#include "px_interrupt.hpp"	/* parallel_query::err_messages_with_lock */
#include "query_list.h"		/* QFILE_LIST_ID */
#include "storage_common.h"	/* VPID, PAGE_PTR */
#include "thread_compat.hpp"	/* THREAD_ENTRY */

/*
 * Forward Declarations
 */

struct qmgr_temp_file;
struct xasl_node;
struct xasl_state;

namespace cubthread
{
  class entry;
}

namespace parallel_query
{
  struct stream_metrics;
  class worker_manager;

  /*
   * hjoin_chase - one probe-input chase: progress header (single writer, N readers,
   * wait-capable), the shared reader cursor, and the detached writer-job bookkeeping
   * (dedicated 1-worker reservation, parked errors, join/meta handshakes).
   */
  class hjoin_chase
  {
    public:
      union vpid64
      {
	std::uint64_t u64;
	VPID vpid;
      };

      /* DRIVER: reserve the dedicated writer worker and launch the probe-input
       * buildlist as a detached chase job.  Returns NULL on any refusal (no error
       * raised; the caller dispatches the aptr as today).  On success the state is
       * published on hashjoin_xasl->proc.hashjoin.chase_state and
       * probe_aptr->chase_progress. */
      static hjoin_chase *try_arm (THREAD_ENTRY *thread_p, xasl_node *hashjoin_xasl, xasl_node *probe_aptr,
				   xasl_state *xstate_p);

      /* DRIVER: idempotent stop + join + free; releases the dedicated worker and NULLs
       * both back-pointers.  Must run on the arming (driver) thread. */
      static void reap (THREAD_ENTRY *thread_p, xasl_node *hashjoin_xasl);

      /* ---- driver-side state queries / waits ---- */
      bool is_closed () const
      {
	return m_closed.load (std::memory_order_acquire);
      }
      bool has_writer_error () const
      {
	return m_error.load (std::memory_order_acquire);
      }
      bool is_joined () const
      {
	return m_joined.load (std::memory_order_acquire);
      }
      int pages_published () const
      {
	return m_pages_published.load (std::memory_order_acquire);
      }
      xasl_node *get_aptr () const
      {
	return m_aptr;
      }

      /* wait until the open input is usable (type list published) or the writer ended:
       * (valid || closed || error).  On writer error: joins the chase, surfaces the
       * parked error, returns it.  NO_ERROR otherwise. */
      int wait_input_usable (THREAD_ENTRY *thread_p);

      /* the driver finished reading the OPEN list's metadata (type list / domains);
       * only after this may the writer job run its end-of-job xasl clear (which
       * re-allocates the list id's type list).  Idempotent. */
      void release_meta ();

      /* natural-completion join (the materialized path needs the COMPLETE list):
       * waits for the writer to finish producing, joins it, surfaces a writer error.
       * Returns NO_ERROR when the closed list is fully usable. */
      int join_for_data (THREAD_ENTRY *thread_p);

      /* R11 prompt stop: the writer observes it at its per-tuple checkpoint and
       * unwinds within one check interval.  ANY-THREAD, idempotent. */
      void request_stop ();

      /* join the writer job (release_meta first -- never deadlocks) and drain the
       * dedicated worker; does NOT release the reservation (reap does).  Idempotent. */
      void join (THREAD_ENTRY *thread_p);

      /* surface the writer's parked error into the current thread (first message wins)
       * when the thread has nothing better; ensures er_errid () != NO_ERROR. */
      int surface_writer_error ();

      /* ---- pipeline chase-binding hooks (px_stream_pipeline) ---- */
      static void pipeline_request_stop (void *handle);
      static void pipeline_join (THREAD_ENTRY *thread_p, void *handle);

      /* A7 metrics sink (non-owning; bound at pipeline arm) */
      void set_metrics (stream_metrics *metrics)
      {
	m_metrics = metrics;
      }

      /* ---- writer side (chase job thread) ---- */

      /* list opened: publish "type list usable" once domains are final */
      void writer_on_open (QFILE_LIST_ID *list_id);

      /* per-inserted-tuple checkpoint: publishes the read frontier on page-boundary
       * change and honors a requested stop (returns ER_INTERRUPTED, er_set). */
      int writer_on_tuple (THREAD_ENTRY *thread_p, QFILE_LIST_ID *list_id);

      /* ---- reader side (probe workers) ---- */

      /* hand out the next readable page (strictly behind the frontier; after close,
       * through the real NULL_PAGEID end).  Exactly-once per page.  NULL = end of input
       * OR error (er_errid () set) OR stop/abort (clean end; no error). */
      PAGE_PTR reader_next_page (cubthread::entry &thread_ref);

      qmgr_temp_file *reader_tfile () const
      {
	return m_tfile.load (std::memory_order_acquire);
      }

      /* ---- MERGEABLE-gather source slots (per-worker frontier publication) ---- */

      static const int CHASE_MAX_SRC_SLOTS = 64;

      struct src_slot
      {
	std::atomic<QFILE_LIST_ID *> list;	/* registered worker list; NULL = free/consumed */
	std::atomic<std::uint64_t> last_vpid;	/* worker frontier (vpid64); readers stay STRICTLY before */
	std::atomic<bool> closed;		/* worker finalized (list closed; walk to the real end) */
      };

      /* worker side: register-or-update this worker list's frontier / mark it closed */
      void src_publish (QFILE_LIST_ID *src_list, bool closing);

      /* gather side (single consumer): slot iteration + consumption */
      int src_count () const
      {
	return m_src_slot_cnt.load (std::memory_order_acquire);
      }
      QFILE_LIST_ID *src_get (int idx, VPID *frontier_out, bool *closed_out);
      void src_consume (int idx)
      {
	m_src_slots[idx].list.store (NULL, std::memory_order_release);
      }
      bool stop_requested () const
      {
	return m_stop_requested.load (std::memory_order_acquire);
      }
      void copy_wait ();	/* bounded wait for source progress (gather thread) */

      /* public for placement_new inside try_arm (db_private_alloc pattern) */
      hjoin_chase (THREAD_ENTRY *parent_thread_p, xasl_node *aptr, xasl_state *xstate_p, worker_manager *worker);

    private:
      ~hjoin_chase () = default;

      hjoin_chase (const hjoin_chase &) = delete;
      hjoin_chase &operator= (const hjoin_chase &) = delete;

      void publish_frontier (QFILE_LIST_ID *list_id, bool closing);
      void notify_all_waiters ();

      friend class chase_task;

      /* immutable after arm */
      THREAD_ENTRY *m_parent_thread_p;	/* the arming (driver) thread */
      xasl_node *m_aptr;		/* the chased probe-input buildlist */
      xasl_state *m_xasl_state_p;	/* chase-owned deep copy made at arm on the driver (stable
					 * source; the task re-copies on its own thread for heap
					 * symmetry); freed at reap on the driver */
      worker_manager *m_worker;		/* dedicated 1-worker reservation (owned; released at reap) */
      stream_metrics *m_metrics;	/* non-owning A7 sink; may stay NULL */

      /* one mutex/cv pair: reader waits, cursor, join/meta handshakes */
      std::mutex m_mtx;
      std::condition_variable m_cv;

      /* progress header (single writer publishes; N readers consume) */
      std::atomic<bool> m_valid;	/* list opened; type list final/published */
      std::atomic<bool> m_closed;	/* writer finished -- every exit path */
      std::atomic<bool> m_error;	/* writer exited with a real error */
      std::atomic<bool> m_stopped;	/* writer exited on a requested stop (R11) -- not an error */
      std::atomic<std::uint64_t> m_first_vpid;	/* vpid64; 0 = not published */
      std::atomic<std::uint64_t> m_last_vpid;	/* readers consume STRICTLY BEFORE this until closed */
      std::atomic<int> m_pages_published;
      std::atomic<qmgr_temp_file *> m_tfile;	/* immutable once published */

      /* control flags */
      std::atomic<bool> m_stop_requested;
      std::atomic<bool> m_meta_released;
      std::atomic<bool> m_task_done;
      std::atomic<bool> m_joined;

      /* shared reader cursor (guarded by m_mtx); each page handed to exactly one reader */
      VPID m_cursor_next;
      bool m_cursor_started;
      bool m_cursor_ended;

      /* parked writer error (surfaced driver-side; never swapped into a live context) */
      err_messages_with_lock m_err_messages;

      /* MERGEABLE-gather sources: the chased buildlist's parallel scan writes N
       * per-worker final-format lists; each worker publishes its own read frontier
       * here (single writer per slot) and the chase-task gather thread copies the
       * immutable prefixes page-by-page into the final (dest) list as they appear --
       * instead of today's wait-for-all + wholesale page-relink merge, which has no
       * incremental frontier to chase.  Slots are append-only; a consumed source is
       * destroyed by the gather and its slot NULLed. */
      src_slot m_src_slots[CHASE_MAX_SRC_SLOTS];
      std::atomic<int> m_src_slot_cnt;
  };
}

/*
 * C-style wrappers for the .c engine files (compiled as C++): all take the opaque
 * chase handle (xasl->chase_progress / proc.hashjoin.chase_state).
 */

extern bool qexec_hjoin_chase_try_arm (THREAD_ENTRY *thread_p, xasl_node *hashjoin_xasl, xasl_node *probe_aptr,
				       xasl_state *xstate_p);
extern void qexec_hjoin_chase_reap (THREAD_ENTRY *thread_p, xasl_node *hashjoin_xasl);
extern void qexec_hjoin_chase_writer_open (void *chase_arg, QFILE_LIST_ID *list_id);
extern int qexec_hjoin_chase_writer_on_tuple (THREAD_ENTRY *thread_p, void *chase_arg, QFILE_LIST_ID *list_id);
extern bool qexec_hjoin_chase_is_closed (void *chase_arg);
extern int qexec_hjoin_chase_pages_published (void *chase_arg);
extern int qexec_hjoin_chase_wait_input_usable (THREAD_ENTRY *thread_p, void *chase_arg);
extern void qexec_hjoin_chase_release_meta (void *chase_arg);
extern int qexec_hjoin_chase_join_for_data (THREAD_ENTRY *thread_p, void *chase_arg);
extern void qexec_hjoin_chase_request_stop (void *chase_arg);
extern void qexec_hjoin_chase_join (THREAD_ENTRY *thread_p, void *chase_arg);

/* MERGEABLE-gather source publication + the incremental copy gather (px_scan side) */
extern void qexec_hjoin_chase_src_publish (void *chase_arg, QFILE_LIST_ID *src_list);
extern void qexec_hjoin_chase_src_close (void *chase_arg, QFILE_LIST_ID *src_list);
extern int qexec_hjoin_chase_src_count (void *chase_arg);
extern QFILE_LIST_ID *qexec_hjoin_chase_src_get (void *chase_arg, int idx, VPID *frontier_out, bool *closed_out);
extern void qexec_hjoin_chase_src_consume (void *chase_arg, int idx);
extern bool qexec_hjoin_chase_stop_requested (void *chase_arg);
extern void qexec_hjoin_chase_copy_wait (void *chase_arg);

#endif /* _PX_STREAM_CHASE_HPP_ */
