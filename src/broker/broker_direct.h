/*
 *  Copyright 2016 CUBRID Corporation
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

/*
 * broker_direct.h - broker side of the server connection handoff (stage B1)
 *
 * Active when the broker's DIRECT_HANDOFF conf is ON: instead of dispatching
 * to a CAS, the broker peeks the driver's db_info packet (after emitting the
 * 4-byte NO_ERROR ack the CAS used to send), routes by dbname to the target
 * server's adoption socket, and hands the client fd off over a persistent
 * control channel (#117).  C-linkage facade over broker_direct.cpp so
 * broker.c stays C.
 */

#ifndef _BROKER_DIRECT_H_
#define _BROKER_DIRECT_H_

#include <pthread.h>

#include "broker_max_heap.h"
#include "broker_shm.h"
#include "porting.h"

/* requests on the broker's session control socket: one uint32 in, one int32 out (sessions reached, -1 when a
 * server did not answer) */
#define BRD_CONTROL_CONFIG 1	/* the conf in shared memory changed: give it to the sessions */
#define BRD_CONTROL_RESET 2	/* cubrid broker reset */
#define BRD_CONTROL_SOCKET_FORMAT "%sbr_session_%x"	/* the socket directory, the broker's appl shm key */

#ifdef __cplusplus
extern "C"
{
#endif

/* start the peek engine and channel manager.  job enqueue plumbing is the
 * receiver/dispatch pair's own (shm heap + mutex/cond), passed in so the
 * queue semantics stay untouched (#117 D3). */
  int brd_init (const char *broker_name, int max_slots, T_SHM_APPL_SERVER * shm_appl, int shm_key,
		const char *ssl_db, T_MAX_HEAP_NODE * job_queue, int job_queue_size,
		pthread_mutex_t * job_queue_mutex, pthread_cond_t * job_queue_cond);
  void brd_final (void);

/* receiver thread: takes ownership of clt_sock_fd right after header
 * validation/ACL; sends the NO_ERROR ack, parks the fd until db_info is
 * complete, absorbs health checks, then enqueues *job into the job queue */
  void brd_park_client (SOCKET clt_sock_fd, const T_MAX_HEAP_NODE * job);

/* dispatch thread: takes ownership of the dequeued job's fd; waits for a
 * slot, hands the fd off to the dbname's server, replies to the driver on
 * failure.  Never returns the fd. */
  void brd_dispatch_job (T_MAX_HEAP_NODE * job);

/* receiver thread, "QC" path: token in the reply's pid slot; anti-spoof by
 * client ip/port as today, and the session checks the session id the request carries (has_session_id) as
 * broker.c does for a CAS.  Returns 0 on forwarded cancel, -1 when the token
 * is unknown or spoofed (caller replies CAS_ER_QUERY_CANCEL). */
  int brd_cancel (unsigned int token, const unsigned char *clt_ip, unsigned short clt_port, bool has_session_id,
		  unsigned int session_id);

/* receiver thread, "ST" path: fn_status of the adopted session of this token and session id, or
 * FN_STATUS_NONE when unknown */
  int brd_status (unsigned int token, unsigned int session_id);

/* receiver thread, non-blocking "ST"/"QC" variants (workspace#259 axis 3):
 * brd_status/brd_cancel may dial the server (10 s) or wait for a control
 * reply (5 s); on the receiver thread that stalls new-connection acceptance
 * for every database behind the broker.  These hand clt_sock_fd to a
 * short-lived helper thread that computes the reply, writes it and closes
 * the socket.  The caller must not touch clt_sock_fd afterwards.  new_frame
 * selects the "X1" reply encoding (driver_info bytes 2-3 of the header) over
 * the legacy 4-byte CAS_CONV_ERROR_TO_OLD code. */
  void brd_status_reply_async (SOCKET clt_sock_fd, unsigned int token, unsigned int session_id);
  void brd_cancel_reply_async (SOCKET clt_sock_fd, unsigned int token, const unsigned char *clt_ip,
			       unsigned short clt_port, bool has_session_id, unsigned int session_id, bool new_frame,
			       const char *cas_req_header);

#ifdef __cplusplus
}
#endif

#endif				/* _BROKER_DIRECT_H_ */
