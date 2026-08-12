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
#define COLUMNAR_METAPAGE_VERSION 2

/* Default stripe/chunk parameters (Citus defaults) */
#define COLUMNAR_DEFAULT_STRIPE_ROW_COUNT  150000
#define COLUMNAR_DEFAULT_CHUNK_ROW_COUNT   10000

/* ========================================================================== */
/* Compression types                                                          */
/* ========================================================================== */
typedef enum
{
  COLUMNAR_COMPRESS_NONE = 0,
  COLUMNAR_COMPRESS_LZ4 = 1,
  COLUMNAR_COMPRESS_ZSTD = 2
} COLUMNAR_COMPRESSION_TYPE;

/* ========================================================================== */
/* Metapage header                                                            */
/* ========================================================================== */

/*
 * Header of a columnar metapage.  Stripe directory entries follow the
 * header within the page.
 *
 * Layout (descending alignment — no padding holes):
 *   INT64 × 2, OID (8B), VPID (6B padded to 8B ctx), INT32 × 2
 */
typedef struct columnar_metapage_header COLUMNAR_METAPAGE_HEADER;
struct columnar_metapage_header
{
  /* --- 8-byte aligned --- */
  INT64 next_stripe_id;		/* monotonic stripe id counter */
  INT64 next_row_number;	/* monotonic row number counter */
  OID class_oid;		/* owning class (8 bytes) */
  VPID next_metapage;		/* next metapage in chain; NULL when last */
  /* --- 4-byte aligned --- */
  INT32 magic;			/* COLUMNAR_METAPAGE_MAGIC */
  INT32 version;		/* COLUMNAR_METAPAGE_VERSION */
  INT32 entry_count;		/* stripe directory entries in this page */
  INT32 reserved;		/* alignment / future use */
};

/* ========================================================================== */
/* Stripe directory entry (on-disk, metapage chain)                           */
/* ========================================================================== */

/*
 * Fixed-width entry in the metapage directory.  Reader scans these
 * sequentially, so fields are ordered by descending alignment to
 * eliminate padding holes (56 bytes, 8-byte aligned).
 *
 *   ┌─ INT64 ×3 ─────────────────────────────────────┐  0..23
 *   │ stripe_id  first_row_number  insert_mvccid      │
 *   ├─ INT32 ×3 ─────────────────────────────────────┤ 24..35
 *   │ row_count  chunk_group_count  page_count         │
 *   ├─ VPID ×2 + INT16 ×2 ───────────────────────────┤ 36..51
 *   │ start_vpid  footer_vpid  footer_offset  reserved │
 *   ├─ INT32 ─────────────────────────────────────────┤ 52..55
 *   │ padding (struct alignment to 8)                  │
 *   └─────────────────────────────────────────────────┘
 */
typedef struct columnar_stripe_dir_entry COLUMNAR_STRIPE_DIR_ENTRY;
struct columnar_stripe_dir_entry
{
  /* 8-byte fields first */
  INT64 stripe_id;		/* unique stripe identifier */
  INT64 first_row_number;	/* first row number in this stripe */
  INT64 insert_mvccid;		/* writer's MVCCID — reader visibility (#12) */
  /* 4-byte fields */
  INT32 row_count;		/* number of rows in this stripe */
  INT32 chunk_group_count;	/* number of chunk groups */
  INT32 page_count;		/* total pages (data + footer) */
  /* VPID fields (6 bytes each, naturally packed) */
  VPID start_vpid;		/* first data page of the stripe */
  VPID footer_vpid;		/* page containing stripe footer start */
  /* 2-byte fields */
  INT16 footer_offset;		/* offset within footer page */
  INT16 reserved1;
  /* struct-alignment pad to 8 */
  INT32 reserved2;
};
/* static_assert(sizeof == 64) checked in columnar_writer.c */

/* Number of directory entries that fit in a metapage after the header */
#define COLUMNAR_META_ENTRIES_PER_PAGE \
  ((DB_PAGESIZE - (int) sizeof (COLUMNAR_METAPAGE_HEADER)) / (int) sizeof (COLUMNAR_STRIPE_DIR_ENTRY))

/* ========================================================================== */
/* Stripe footer magic/version                                                */
/* ========================================================================== */
#define COLUMNAR_FOOTER_MAGIC   0x53465452	/* "SFTR" */
#define COLUMNAR_FOOTER_VERSION 1

/* ========================================================================== */
/* Public API                                                                 */
/* ========================================================================== */
extern int columnar_file_create (THREAD_ENTRY * thread_p, HFID * hfid, const OID * class_oid);
extern int columnar_file_destroy (THREAD_ENTRY * thread_p, const HFID * hfid);
extern int columnar_rv_metapage_init_redo (THREAD_ENTRY * thread_p, LOG_RCV * rcv);

#endif /* _COLUMNAR_FILE_H_ */
