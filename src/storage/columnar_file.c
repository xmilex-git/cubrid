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
 * columnar_file.c - columnar storage file (FILE_COLUMNAR) management
 */

#ident "$Id$"

#include "columnar_file.h"

#include "file_manager.h"
#include "log_append.hpp"
#include "log_manager.h"
#include "page_buffer.h"
#include "recovery.h"

#include <string.h>

/*
 * columnar_file_create () - create the columnar data file of a class and
 *                           format its first metapage
 *   return: error code
 *   hfid(in/out): all fields are set on success; the caller should have set
 *                 vfid.volid as a hint. vfid becomes the columnar file and
 *                 hpgid the first metapage.
 *   class_oid(in): OID of the class owning the file
 */
int
columnar_file_create (THREAD_ENTRY * thread_p, HFID * hfid, const OID * class_oid)
{
  COLUMNAR_METAPAGE_HEADER metapage_header;
  FILE_DESCRIPTORS des;
  VPID vpid;
  LOG_DATA_ADDR addr;
  PAGE_TYPE ptype = PAGE_COLUMNAR;
  int error_code = NO_ERROR;

  assert (class_oid != NULL && !OID_ISNULL (class_oid));

  addr.pgptr = NULL;
  log_sysop_start (thread_p);

  memset (hfid, 0, sizeof (HFID));
  HFID_SET_NULL (hfid);

  memset (&des, 0, sizeof (des));
  des.heap.class_oid = *class_oid;

  error_code = file_create_with_npages (thread_p, FILE_COLUMNAR, 1, &des, &hfid->vfid);
  if (error_code != NO_ERROR)
    {
      ASSERT_ERROR ();
      goto error;
    }

  error_code = file_alloc_sticky_first_page (thread_p, &hfid->vfid, file_init_page_type, &ptype, &vpid, &addr.pgptr);
  if (error_code != NO_ERROR)
    {
      ASSERT_ERROR ();
      goto error;
    }
  if (addr.pgptr == NULL || vpid.volid != hfid->vfid.volid)
    {
      assert_release (false);
      error_code = ER_FAILED;
      goto error;
    }

  hfid->hpgid = vpid.pageid;

  /* update file descriptor to include the hfid */
  des.heap.hfid = *hfid;
  error_code = file_descriptor_update (thread_p, &hfid->vfid, &des);
  if (error_code != NO_ERROR)
    {
      ASSERT_ERROR ();
      goto error;
    }

  /* format the first metapage: empty stripe directory */
  memset (&metapage_header, 0, sizeof (metapage_header));
  metapage_header.magic = COLUMNAR_METAPAGE_MAGIC;
  metapage_header.version = COLUMNAR_METAPAGE_VERSION;
  metapage_header.entry_count = 0;
  VPID_SET_NULL (&metapage_header.next_metapage);
  metapage_header.class_oid = *class_oid;

  memcpy (addr.pgptr, &metapage_header, sizeof (metapage_header));

  /* file and pages are deallocated on undo (abort); redo-only content log */
  addr.vfid = &hfid->vfid;
  addr.offset = 0;
  log_append_redo_data (thread_p, RVCOL_METAPAGE_INIT, &addr, sizeof (metapage_header), &metapage_header);
  pgbuf_set_dirty (thread_p, addr.pgptr, DONT_FREE);

  pgbuf_unfix_and_init (thread_p, addr.pgptr);

  log_sysop_attach_to_outer (thread_p);

  return NO_ERROR;

error:
  assert (error_code != NO_ERROR);

  if (addr.pgptr != NULL)
    {
      pgbuf_unfix_and_init (thread_p, addr.pgptr);
    }

  hfid->vfid.fileid = NULL_FILEID;
  hfid->hpgid = NULL_PAGEID;

  log_sysop_abort (thread_p);
  return error_code;
}

/*
 * columnar_file_destroy () - destroy the columnar data file of a class
 *   return: error code
 *   hfid(in): columnar file identifier (HFID slot of the class)
 *
 * Note: columnar files never carry MVCC log records, so no vacuum
 *       dropped-file registration is needed.
 */
int
columnar_file_destroy (THREAD_ENTRY * thread_p, const HFID * hfid)
{
  assert (hfid != NULL && !HFID_IS_NULL (hfid));

  file_postpone_destroy (thread_p, &hfid->vfid);

  return NO_ERROR;
}

/*
 * columnar_rv_metapage_init_redo () - redo the initialization of a columnar
 *                                     metapage
 *   return: error code
 *   rcv(in): recovery data: metapage content at rcv->offset
 */
int
columnar_rv_metapage_init_redo (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  assert (rcv->pgptr != NULL);
  assert (rcv->offset >= 0 && rcv->offset + rcv->length <= DB_PAGESIZE);

  pgbuf_set_page_ptype (thread_p, rcv->pgptr, PAGE_COLUMNAR);
  memcpy (rcv->pgptr + rcv->offset, rcv->data, rcv->length);
  pgbuf_set_dirty (thread_p, rcv->pgptr, DONT_FREE);

  return NO_ERROR;
}
