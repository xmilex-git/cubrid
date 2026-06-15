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
 * vresid2_log.h - CBRD-26927 V-RESID part 2 DEBUG-ONLY instrumentation logger.
 *
 * Header-only, append-only log sink used to prove temp-file retire / live-chain
 * VFID disjointness under the CBRD-26927 repro load. Verification only; emits
 * NOTHING and links to NOTHING in a release build (entire body behind
 * #if !defined(NDEBUG)). Output goes to the file named by the VRESID2_LOG env
 * var; if that var is unset/empty the logger is a no-op.
 *
 * Each call formats one full line into a stack buffer and emits it with a single
 * O_APPEND write(2). For lines <= PIPE_BUF this append is atomic, so concurrent
 * worker threads (and the two translation units that include this header) never
 * interleave a line -- no shared FILE* / no shared mutex needed.
 */

#ifndef _VRESID2_LOG_H_
#define _VRESID2_LOG_H_

#if !defined(NDEBUG)

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/syscall.h>

static inline long long
vresid2_now_ns (void)
{
  struct timespec ts;
  clock_gettime (CLOCK_MONOTONIC, &ts);
  return (long long) ts.tv_sec * 1000000000LL + (long long) ts.tv_nsec;
}

static inline long
vresid2_tid (void)
{
  return (long) syscall (SYS_gettid);
}

static inline void
vresid2_logf (const char *fmt, ...)
{
  char buf[1024];
  va_list ap;
  int n;
  const char *path;
  int fd;

  va_start (ap, fmt);
  n = vsnprintf (buf, sizeof (buf), fmt, ap);
  va_end (ap);
  if (n <= 0)
    {
      return;
    }
  if (n > (int) sizeof (buf))
    {
      n = (int) sizeof (buf);	/* truncated; still one atomic write */
    }

  path = getenv ("VRESID2_LOG");
  if (path == NULL || path[0] == '\0')
    {
      return;
    }

  fd = open (path, O_WRONLY | O_APPEND | O_CREAT, 0644);
  if (fd >= 0)
    {
      ssize_t w = write (fd, buf, (size_t) n);
      (void) w;
      close (fd);
    }
}

#endif /* !NDEBUG */
#endif /* _VRESID2_LOG_H_ */
