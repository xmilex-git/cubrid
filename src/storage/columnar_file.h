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
 * columnar_file.h - columnar storage file (FILE_COLUMNAR) management
 *
 * A columnar class stores its data in a single FILE_COLUMNAR file per class.
 * The class record reuses the HFID slot to point at this file: vfid is the
 * columnar file and hpgid is the first metapage (sticky first page).
 * The metapage chain holds the fixed-width stripe directory; stripe data
 * pages are appended by the write path.
 */

#ifndef _COLUMNAR_FILE_H_
#define _COLUMNAR_FILE_H_

#if !defined (SERVER_MODE) && !defined (SA_MODE)
#error Belongs to server module
#endif /* !defined (SERVER_MODE) && !defined (SA_MODE) */

#include "storage_common.h"
#include "oid.h"
#include "recovery.h"
#include "thread_compat.hpp"

#define COLUMNAR_METAPAGE_MAGIC 0x434f4c31	/* "COL1" */
#define COLUMNAR_METAPAGE_VERSION 1

/* Header of a columnar metapage. Stripe directory entries (fixed width,
 * appended by the write path) follow the header within the page. */
typedef struct columnar_metapage_header COLUMNAR_METAPAGE_HEADER;
struct columnar_metapage_header
{
  INT32 magic;			/* COLUMNAR_METAPAGE_MAGIC */
  INT32 version;		/* COLUMNAR_METAPAGE_VERSION */
  INT32 entry_count;		/* number of stripe directory entries in this page */
  INT32 reserved;		/* alignment / future use */
  VPID next_metapage;		/* next metapage in chain; NULL when last */
  OID class_oid;		/* owning class */
};

extern int columnar_file_create (THREAD_ENTRY * thread_p, HFID * hfid, const OID * class_oid);
extern int columnar_file_destroy (THREAD_ENTRY * thread_p, const HFID * hfid);
extern int columnar_rv_metapage_init_redo (THREAD_ENTRY * thread_p, LOG_RCV * rcv);

#endif /* _COLUMNAR_FILE_H_ */
