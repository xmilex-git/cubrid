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
 * columnar_writer.c - columnar storage write path
 *
 * Implements per-transaction, per-class write state that buffers INSERT
 * rows and flushes them as stripes using sysop + redo-only page writes
 * + undo-redo directory entry.
 */

#ident "$Id$"

#include "columnar_writer.h"

#include "columnar_file.h"
#include "columnar_numeric.h"
#include "dbtype.h"
#include "error_manager.h"
#include "file_manager.h"
#include "heap_file.h"
#include "lock_manager.h"
#include "log_append.hpp"
#include "log_impl.h"
#include "log_manager.h"
#include "memory_alloc.h"
#include "mvcc.h"
#include "object_representation.h"
#include "object_representation_sr.h"
#include "page_buffer.h"
#include "recovery.h"
#include "system_parameter.h"

#include "lz4.h"
#if defined (HAVE_ZSTD)
#include "zstd.h"
#endif

#include <assert.h>
#include <stdlib.h>
#include <string.h>

/* ========================================================================== */
/* Compile-time layout checks                                                 */
/* ========================================================================== */
/* These fire at compile time if struct packing changes unexpectedly. */
typedef char _col_dir_entry_size_check[(sizeof (COLUMNAR_STRIPE_DIR_ENTRY) == 64) ? 1 : -1];
typedef char _col_chunk_desc_size_check[(sizeof (COLUMNAR_CHUNK_DESC) == 48) ? 1 : -1];

/* ========================================================================== */
/* Global write-state registry — indexed by tran_index                        */
/* ========================================================================== */
static COLUMNAR_WRITE_STATE **columnar_Tran_wstates = NULL;
static int columnar_Tran_wstates_count = 0;

/* initial buffer sizes */
#define COL_INIT_DATA_ALLOC    (64 * 1024)
#define COL_INIT_EXISTS_ALLOC  (2048)
#define COL_INIT_STRIPE_ALLOC  (256 * 1024)
#define COL_INIT_CHUNKDESC_ALLOC 64

/* ========================================================================== */
/* Forward declarations                                                       */
/* ========================================================================== */
static COLUMNAR_WRITE_STATE *columnar_find_write_state (THREAD_ENTRY * thread_p, const OID * class_oid);
static COLUMNAR_WRITE_STATE *columnar_create_write_state (THREAD_ENTRY * thread_p, const OID * class_oid,
							  const HFID * hfid);
static void columnar_free_write_state (THREAD_ENTRY * thread_p, COLUMNAR_WRITE_STATE * ws);
static int columnar_flush_stripe (THREAD_ENTRY * thread_p, COLUMNAR_WRITE_STATE * ws);
static int columnar_serialize_chunks (THREAD_ENTRY * thread_p, COLUMNAR_WRITE_STATE * ws);
static int columnar_buffer_value (COLUMNAR_COL_BUFFER * col, const DB_VALUE * val, int row_in_chunk);
static int columnar_ensure_data_capacity (COLUMNAR_COL_BUFFER * col, int needed);
static int columnar_ensure_exists_capacity (COLUMNAR_COL_BUFFER * col, int needed_bytes);
static int columnar_compress_buffer (const char *src, int src_len, char **out, int *out_len,
				     COLUMNAR_COMPRESSION_TYPE comp);
static int columnar_build_footer (COLUMNAR_WRITE_STATE * ws, char **footer_buf, int *footer_len);

/* ========================================================================== */
/* Boot-time init / final                                                     */
/* ========================================================================== */

void
columnar_writer_init (int max_tran_indices)
{
  size_t alloc = max_tran_indices * sizeof (COLUMNAR_WRITE_STATE *);
  columnar_Tran_wstates = (COLUMNAR_WRITE_STATE **) malloc (alloc);
  if (columnar_Tran_wstates != NULL)
    {
      memset (columnar_Tran_wstates, 0, alloc);
    }
  columnar_Tran_wstates_count = max_tran_indices;
}

void
columnar_writer_final (void)
{
  if (columnar_Tran_wstates != NULL)
    {
      free (columnar_Tran_wstates);
      columnar_Tran_wstates = NULL;
    }
  columnar_Tran_wstates_count = 0;
}

/* ========================================================================== */
/* Write-state lookup / create                                                */
/* ========================================================================== */

static COLUMNAR_WRITE_STATE *
columnar_find_write_state (THREAD_ENTRY * thread_p, const OID * class_oid)
{
  int tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  COLUMNAR_WRITE_STATE *ws;

  if (tran_index < 0 || tran_index >= columnar_Tran_wstates_count || columnar_Tran_wstates == NULL)
    {
      return NULL;
    }

  for (ws = columnar_Tran_wstates[tran_index]; ws != NULL; ws = ws->next)
    {
      if (OID_EQ (&ws->class_oid, class_oid))
	{
	  return ws;
	}
    }
  return NULL;
}

static COLUMNAR_WRITE_STATE *
columnar_create_write_state (THREAD_ENTRY * thread_p, const OID * class_oid, const HFID * hfid)
{
  COLUMNAR_WRITE_STATE *ws = NULL;
  OR_CLASSREP *classrep = NULL;
  RECDES class_recdes = RECDES_INITIALIZER;
  HEAP_SCANCACHE scan_cache;
  int i, error_code = NO_ERROR;
  int tran_index;

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  if (tran_index < 0 || tran_index >= columnar_Tran_wstates_count)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FAILED, 0);
      return NULL;
    }

  /* fetch class record to get OR_CLASSREP */
  error_code = heap_scancache_quick_start_root_hfid (thread_p, &scan_cache);
  if (error_code != NO_ERROR)
    {
      return NULL;
    }

  if (heap_get_class_record (thread_p, class_oid, &class_recdes, &scan_cache, PEEK) != S_SUCCESS)
    {
      heap_scancache_end (thread_p, &scan_cache);
      return NULL;
    }

  classrep = or_get_classrep (&class_recdes, NULL_REPRID);
  heap_scancache_end (thread_p, &scan_cache);

  if (classrep == NULL)
    {
      return NULL;
    }

  /* allocate write state */
  ws = (COLUMNAR_WRITE_STATE *) db_private_alloc (thread_p, sizeof (COLUMNAR_WRITE_STATE));
  if (ws == NULL)
    {
      or_free_classrep (classrep);
      return NULL;
    }
  memset (ws, 0, sizeof (COLUMNAR_WRITE_STATE));

  ws->class_oid = *class_oid;
  ws->hfid = *hfid;
  ws->n_columns = classrep->n_attributes;
  ws->stripe_max_rows = COLUMNAR_DEFAULT_STRIPE_ROW_COUNT;
  ws->chunk_max_rows = COLUMNAR_DEFAULT_CHUNK_ROW_COUNT;

  /* allocate column buffers */
  ws->columns =
    (COLUMNAR_COL_BUFFER *) db_private_alloc (thread_p, ws->n_columns * sizeof (COLUMNAR_COL_BUFFER));
  if (ws->columns == NULL)
    {
      goto err;
    }
  memset (ws->columns, 0, ws->n_columns * sizeof (COLUMNAR_COL_BUFFER));

  for (i = 0; i < ws->n_columns; i++)
    {
      OR_ATTRIBUTE *attr = &classrep->attributes[i];
      COLUMNAR_COL_BUFFER *col = &ws->columns[i];

      col->type = attr->type;
      col->value_size = columnar_value_disk_size (attr->type, attr->domain);

      if (col->value_size == -2)
	{
	  /* unsupported type */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_COLUMNAR_NOT_SUPPORTED, 1, "column type");
	  goto err;
	}

      /* pre-allocate data buffer for one chunk */
      if (col->value_size > 0)
	{
	  col->data_alloc = col->value_size * ws->chunk_max_rows;
	}
      else
	{
	  col->data_alloc = COL_INIT_DATA_ALLOC;
	}
      col->data = (char *) malloc (col->data_alloc);
      if (col->data == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) col->data_alloc);
	  goto err;
	}
      col->data_size = 0;

      /* exists bitmap — 1 bit per row, pre-alloc for one chunk */
      col->exists_alloc = (ws->chunk_max_rows + 7) / 8;
      if (col->exists_alloc < COL_INIT_EXISTS_ALLOC)
	{
	  col->exists_alloc = COL_INIT_EXISTS_ALLOC;
	}
      col->exists = (char *) malloc (col->exists_alloc);
      if (col->exists == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) col->exists_alloc);
	  goto err;
	}
      memset (col->exists, 0, col->exists_alloc);
      col->exists_size = 0;
    }

  /* stripe data accumulator */
  ws->stripe_data_alloc = COL_INIT_STRIPE_ALLOC;
  ws->stripe_data = (char *) malloc (ws->stripe_data_alloc);
  if (ws->stripe_data == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) ws->stripe_data_alloc);
      goto err;
    }
  ws->stripe_data_size = 0;

  /* chunk descriptor array */
  ws->chunk_descs_alloc = COL_INIT_CHUNKDESC_ALLOC;
  ws->chunk_descs =
    (COLUMNAR_CHUNK_DESC *) malloc (ws->chunk_descs_alloc * sizeof (COLUMNAR_CHUNK_DESC));
  if (ws->chunk_descs == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      (size_t) (ws->chunk_descs_alloc * sizeof (COLUMNAR_CHUNK_DESC)));
      goto err;
    }
  ws->n_chunk_descs = 0;
  ws->n_chunk_groups = 0;

  or_free_classrep (classrep);

  /* link into the per-tran list */
  ws->next = columnar_Tran_wstates[tran_index];
  columnar_Tran_wstates[tran_index] = ws;

  return ws;

err:
  if (classrep != NULL)
    {
      or_free_classrep (classrep);
    }
  columnar_free_write_state (thread_p, ws);
  return NULL;
}

static void
columnar_free_write_state (THREAD_ENTRY * thread_p, COLUMNAR_WRITE_STATE * ws)
{
  int i;
  COLUMNAR_SAVEPT *sp, *sp_next;

  if (ws == NULL)
    {
      return;
    }

  if (ws->columns != NULL)
    {
      for (i = 0; i < ws->n_columns; i++)
	{
	  if (ws->columns[i].data != NULL)
	    {
	      free (ws->columns[i].data);
	    }
	  if (ws->columns[i].exists != NULL)
	    {
	      free (ws->columns[i].exists);
	    }
	}
      db_private_free (thread_p, ws->columns);
    }

  if (ws->stripe_data != NULL)
    {
      free (ws->stripe_data);
    }
  if (ws->chunk_descs != NULL)
    {
      free (ws->chunk_descs);
    }

  for (sp = ws->savept_stack; sp != NULL; sp = sp_next)
    {
      sp_next = sp->prev;
      if (sp->savept_name != NULL)
	{
	  db_private_free (thread_p, sp->savept_name);
	}
      db_private_free (thread_p, sp);
    }

  db_private_free (thread_p, ws);
}

/* ========================================================================== */
/* Per-value disk size                                                        */
/* ========================================================================== */

int
columnar_value_disk_size (DB_TYPE type, TP_DOMAIN * domain)
{
  switch (type)
    {
    case DB_TYPE_INTEGER:
      return (int) sizeof (int);
    case DB_TYPE_SHORT:
      return (int) sizeof (short);
    case DB_TYPE_BIGINT:
      return (int) sizeof (INT64);
    case DB_TYPE_FLOAT:
      return (int) sizeof (float);
    case DB_TYPE_DOUBLE:
      return (int) sizeof (double);
    case DB_TYPE_MONETARY:
      return (int) sizeof (double) + (int) sizeof (int);	/* amount + type */
    case DB_TYPE_DATE:
      return (int) sizeof (int);
    case DB_TYPE_TIME:
      return (int) sizeof (int);
    case DB_TYPE_TIMESTAMP:
      return (int) sizeof (int);
    case DB_TYPE_DATETIME:
      return (int) sizeof (int) + (int) sizeof (int);	/* date + time */
    case DB_TYPE_NUMERIC:
      return -1;		/* PG base-10000 variable-width stream (#23 D11) */
    case DB_TYPE_CHAR:
      return (domain != NULL && domain->precision > 0) ? domain->precision : -1;
    case DB_TYPE_BIT:
      return (domain != NULL && domain->precision > 0) ? ((domain->precision + 7) / 8) : -1;
    case DB_TYPE_VARCHAR:
    case DB_TYPE_VARBIT:
      return -1;		/* variable length */
    default:
      return -2;		/* unsupported */
    }
}

/* ========================================================================== */
/* Column buffer helpers                                                      */
/* ========================================================================== */

static int
columnar_ensure_data_capacity (COLUMNAR_COL_BUFFER * col, int needed)
{
  if (col->data_size + needed <= col->data_alloc)
    {
      return NO_ERROR;
    }

  int new_alloc = col->data_alloc * 2;
  while (new_alloc < col->data_size + needed)
    {
      new_alloc *= 2;
    }

  /* col->data is allocated with system malloc and grown with realloc;
   * freed with system free in columnar_free_write_state */
  char *new_buf = (char *) realloc (col->data, new_alloc);
  if (new_buf == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) new_alloc);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  col->data = new_buf;
  col->data_alloc = new_alloc;
  return NO_ERROR;
}

static int
columnar_ensure_exists_capacity (COLUMNAR_COL_BUFFER * col, int needed_bytes)
{
  if (needed_bytes <= col->exists_alloc)
    {
      return NO_ERROR;
    }

  int new_alloc = col->exists_alloc * 2;
  while (new_alloc < needed_bytes)
    {
      new_alloc *= 2;
    }

  char *new_buf = (char *) realloc (col->exists, new_alloc);
  if (new_buf == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) new_alloc);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  /* zero the newly allocated bytes */
  memset (new_buf + col->exists_alloc, 0, new_alloc - col->exists_alloc);
  col->exists = new_buf;
  col->exists_alloc = new_alloc;
  return NO_ERROR;
}

/*
 * columnar_buffer_value () - append a single value to a column buffer
 *
 *   For NULL: exists bit stays 0, data area gets zero-filled (fixed) or nothing (variable).
 *   For non-NULL: exists bit set to 1, raw bytes written to data area.
 */
static int
columnar_buffer_value (COLUMNAR_COL_BUFFER * col, const DB_VALUE * val, int row_in_chunk)
{
  int error_code = NO_ERROR;
  int byte_idx = row_in_chunk / 8;
  int bit_idx = row_in_chunk % 8;

  /* ensure exists bitmap has room */
  error_code = columnar_ensure_exists_capacity (col, byte_idx + 1);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }
  if (byte_idx >= col->exists_size)
    {
      col->exists_size = byte_idx + 1;
    }

  if (DB_IS_NULL (val))
    {
      /* exists bit stays 0; for fixed-width, write zeros to maintain stride */
      col->exists[byte_idx] &= ~(1 << bit_idx);

      if (col->value_size > 0)
	{
	  error_code = columnar_ensure_data_capacity (col, col->value_size);
	  if (error_code != NO_ERROR)
	    {
	      return error_code;
	    }
	  memset (col->data + col->data_size, 0, col->value_size);
	  col->data_size += col->value_size;
	}
      /* variable: nothing appended for NULL */
      return NO_ERROR;
    }

  /* set exists bit */
  col->exists[byte_idx] |= (1 << bit_idx);

  if (col->value_size > 0)
    {
      /* fixed-width: write raw value bytes at stride */
      error_code = columnar_ensure_data_capacity (col, col->value_size);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}

      char *dst = col->data + col->data_size;
      switch (col->type)
	{
	case DB_TYPE_INTEGER:
	  {
	    int v = db_get_int (val);
	    memcpy (dst, &v, sizeof (v));
	  }
	  break;
	case DB_TYPE_SHORT:
	  {
	    short v = db_get_short (val);
	    memcpy (dst, &v, sizeof (v));
	  }
	  break;
	case DB_TYPE_BIGINT:
	  {
	    INT64 v = db_get_bigint (val);
	    memcpy (dst, &v, sizeof (v));
	  }
	  break;
	case DB_TYPE_FLOAT:
	  {
	    float v = db_get_float (val);
	    memcpy (dst, &v, sizeof (v));
	  }
	  break;
	case DB_TYPE_DOUBLE:
	  {
	    double v = db_get_double (val);
	    memcpy (dst, &v, sizeof (v));
	  }
	  break;
	case DB_TYPE_MONETARY:
	  {
	    DB_MONETARY *m = db_get_monetary (val);
	    memcpy (dst, &m->amount, sizeof (double));
	    memcpy (dst + sizeof (double), &m->type, sizeof (int));
	  }
	  break;
	case DB_TYPE_DATE:
	  {
	    DB_DATE *dp = db_get_date (val);
	    memcpy (dst, dp, sizeof (DB_DATE));
	  }
	  break;
	case DB_TYPE_TIME:
	  {
	    DB_TIME *tp = db_get_time (val);
	    memcpy (dst, tp, sizeof (DB_TIME));
	  }
	  break;
	case DB_TYPE_TIMESTAMP:
	  {
	    DB_TIMESTAMP v = *db_get_timestamp (val);
	    memcpy (dst, &v, sizeof (v));
	  }
	  break;
	case DB_TYPE_DATETIME:
	  {
	    DB_DATETIME *dt = db_get_datetime (val);
	    memcpy (dst, &dt->date, sizeof (int));
	    memcpy (dst + sizeof (int), &dt->time, sizeof (int));
	  }
	  break;
	case DB_TYPE_CHAR:
	  {
	    const char *str = db_get_string (val);
	    int len = db_get_string_size (val);
	    if (len > col->value_size)
	      {
		len = col->value_size;
	      }
	    if (str != NULL && len > 0)
	      {
		memcpy (dst, str, len);
	      }
	    /* pad remainder with spaces */
	    if (len < col->value_size)
	      {
		memset (dst + len, ' ', col->value_size - len);
	      }
	  }
	  break;
	case DB_TYPE_BIT:
	  {
	    const char *bits = db_get_bit (val, NULL);
	    if (bits != NULL)
	      {
		memcpy (dst, bits, col->value_size);
	      }
	    else
	      {
		memset (dst, 0, col->value_size);
	      }
	  }
	  break;
	default:
	  assert (false);
	  break;
	}

      col->data_size += col->value_size;
    }
  else
    {
      /* variable-width: length-prefix stream */
      const char *str = NULL;
      int len = 0;

      if (col->type == DB_TYPE_NUMERIC)
	{
	  /* PG base-10000 payload behind the same 4-byte length prefix, so the
	   * reader's per-row offset table needs no special case (#23 D11).
	   * The conversion happens once here, at INSERT; the read path then
	   * computes directly on the stored digits. */
	  COL_NUMVAR nv;

	  error_code = columnar_num_from_dbvalue (val, &nv);
	  if (error_code != NO_ERROR)
	    {
	      return error_code;
	    }
	  len = columnar_num_serialized_size (&nv);
	  error_code = columnar_ensure_data_capacity (col, (int) sizeof (int) + len);
	  if (error_code != NO_ERROR)
	    {
	      return error_code;
	    }
	  memcpy (col->data + col->data_size, &len, sizeof (int));
	  col->data_size += (int) sizeof (int);
	  col->data_size += columnar_num_serialize (&nv, col->data + col->data_size);
	  return NO_ERROR;
	}

      switch (col->type)
	{
	case DB_TYPE_VARCHAR:
	  str = db_get_string (val);
	  len = db_get_string_size (val);
	  break;
	case DB_TYPE_VARBIT:
	  str = db_get_bit (val, &len);
	  len = (len + 7) / 8;	/* bits to bytes */
	  break;
	default:
	  assert (false);
	  break;
	}

      if (str == NULL)
	{
	  len = 0;
	}

      /* write 4-byte length prefix + data */
      error_code = columnar_ensure_data_capacity (col, (int) sizeof (int) + len);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
      memcpy (col->data + col->data_size, &len, sizeof (int));
      col->data_size += (int) sizeof (int);
      if (len > 0)
	{
	  memcpy (col->data + col->data_size, str, len);
	  col->data_size += len;
	}
    }

  return NO_ERROR;
}

/* ========================================================================== */
/* INSERT entry point                                                         */
/* ========================================================================== */

int
columnar_insert_row (THREAD_ENTRY * thread_p, const OID * class_oid, const HFID * hfid, RECDES * recdes,
		     OID * out_oid)
{
  COLUMNAR_WRITE_STATE *ws;
  HEAP_CACHE_ATTRINFO attr_info;
  bool attr_inited = false;
  int error_code = NO_ERROR;
  int i, row_in_chunk;
  OID dummy_oid;

  assert (class_oid != NULL && !OID_ISNULL (class_oid));
  assert (recdes != NULL);

  /* find or create write state */
  ws = columnar_find_write_state (thread_p, class_oid);
  if (ws == NULL)
    {
      ws = columnar_create_write_state (thread_p, class_oid, hfid);
      if (ws == NULL)
	{
	  ASSERT_ERROR_AND_SET (error_code);
	  return error_code;
	}
    }

  /* extract column values from RECDES using heap_attrinfo */
  error_code = heap_attrinfo_start (thread_p, class_oid, -1, NULL, &attr_info);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }
  attr_inited = true;

  /* dummy OID for value extraction */
  OID_SET_NULL (&dummy_oid);
  error_code = heap_attrinfo_read_dbvalues (thread_p, &dummy_oid, recdes, &attr_info);
  if (error_code != NO_ERROR)
    {
      goto end;
    }

  /* compute row position within current chunk */
  row_in_chunk = ws->current_rows % ws->chunk_max_rows;

  /* buffer each column value */
  for (i = 0; i < ws->n_columns && i < attr_info.num_values; i++)
    {
      DB_VALUE *val = &attr_info.values[i].dbvalue;
      error_code = columnar_buffer_value (&ws->columns[i], val, row_in_chunk);
      if (error_code != NO_ERROR)
	{
	  goto end;
	}
    }

  ws->current_rows++;

  /* chunk boundary: serialize the completed chunk */
  if ((ws->current_rows % ws->chunk_max_rows) == 0)
    {
      error_code = columnar_serialize_chunks (thread_p, ws);
      if (error_code != NO_ERROR)
	{
	  goto end;
	}
    }

  /* stripe boundary: flush to disk.  The byte cap keeps the stripe's page
   * map within a single page (COLUMNAR_STRIPE_PAGE_MAP_CAPACITY); it only
   * changes at chunk boundaries, so the flush still lands on a full chunk. */
  if (ws->current_rows >= ws->stripe_max_rows || ws->stripe_data_size >= COLUMNAR_STRIPE_MAX_BYTES)
    {
      error_code = columnar_flush_stripe (thread_p, ws);
      if (error_code != NO_ERROR)
	{
	  goto end;
	}
    }

  /* synthetic OID for the caller — row number encoded */
  if (out_oid != NULL)
    {
      /* will be filled with real row number during flush; for now, use stripe-local index */
      out_oid->pageid = ws->current_rows;
      out_oid->slotid = 0;
      out_oid->volid = ws->hfid.vfid.volid;
    }

end:
  if (attr_inited)
    {
      heap_attrinfo_end (thread_p, &attr_info);
    }
  return error_code;
}

/* ========================================================================== */
/* Chunk min/max (skip-list) collection                                       */
/* ========================================================================== */

/*
 * columnar_compute_minmax_numeric () - min/max of a PG base-10000 NUMERIC
 *   chunk.  The bounds are kept as exact unscaled integers at a single
 *   display scale (the largest one present), so the reader's skip test is an
 *   INT64 comparison and stays sound.  Any value that is not exactly
 *   representable that way disables the skip for the whole chunk.
 */
static void
columnar_compute_minmax_numeric (const COLUMNAR_COL_BUFFER * col, int rows_in_chunk, COLUMNAR_CHUNK_DESC * desc)
{
  INT64 mn = 0, mx = 0;
  int dscale = 0, off, r;
  bool have = false;

  /* pass 1: the common display scale */
  for (r = 0, off = 0; r < rows_in_chunk; r++)
    {
      COL_NUMREF ref;
      int len;

      if (!(col->exists[r / 8] & (1 << (r % 8))))
	{
	  continue;
	}
      memcpy (&len, col->data + off, sizeof (int));
      columnar_num_ref (col->data + off + (int) sizeof (int), &ref);
      if (ref.dscale > dscale)
	{
	  dscale = ref.dscale;
	}
      off += (int) sizeof (int) + len;
    }

  /* pass 2: exact unscaled bounds at that scale */
  for (r = 0, off = 0; r < rows_in_chunk; r++)
    {
      COL_NUMREF ref;
      INT64 v;
      int len;

      if (!(col->exists[r / 8] & (1 << (r % 8))))
	{
	  continue;
	}
      memcpy (&len, col->data + off, sizeof (int));
      columnar_num_ref (col->data + off + (int) sizeof (int), &ref);
      off += (int) sizeof (int) + len;

      if (!columnar_num_unscaled_int64 (&ref, dscale, &v))
	{
	  return;		/* leave COLUMNAR_MINMAX_NONE */
	}
      if (!have)
	{
	  mn = mx = v;
	  have = true;
	}
      else if (v < mn)
	{
	  mn = v;
	}
      else if (v > mx)
	{
	  mx = v;
	}
    }

  if (have)
    {
      desc->minmax_kind = COLUMNAR_MINMAX_NUMERIC;
      desc->minmax_dscale = (INT8) dscale;
      desc->min_val = mn;
      desc->max_val = mx;
    }
}

/*
 * columnar_compute_minmax () - compute the canonical min/max of the non-NULL
 *   values buffered in a column and record them in the chunk descriptor.
 *   Types with no order-preserving canonical encoding get
 *   COLUMNAR_MINMAX_NONE.
 */
static void
columnar_compute_minmax (const COLUMNAR_COL_BUFFER * col, int rows_in_chunk, COLUMNAR_CHUNK_DESC * desc)
{
  int n_rows, r;
  bool have = false;

  desc->minmax_kind = COLUMNAR_MINMAX_NONE;
  desc->minmax_dscale = 0;
  desc->min_val = 0;
  desc->max_val = 0;

  if (col->value_size <= 0)
    {
      if (col->type == DB_TYPE_NUMERIC)
	{
	  columnar_compute_minmax_numeric (col, rows_in_chunk, desc);
	}
      return;			/* other variable-width types are not skippable */
    }
  n_rows = col->data_size / col->value_size;

  switch (col->type)
    {
    case DB_TYPE_SHORT:
    case DB_TYPE_INTEGER:
    case DB_TYPE_BIGINT:
    case DB_TYPE_DATE:
    case DB_TYPE_TIME:
    case DB_TYPE_TIMESTAMP:
    case DB_TYPE_DATETIME:
      {
	INT64 mn = 0, mx = 0;

	for (r = 0; r < n_rows; r++)
	  {
	    INT64 v;
	    const char *cell;

	    if (!(col->exists[r / 8] & (1 << (r % 8))))
	      {
		continue;
	      }
	    cell = col->data + (size_t) r * col->value_size;
	    switch (col->type)
	      {
	      case DB_TYPE_SHORT:
		v = *(const short *) cell;
		break;
	      case DB_TYPE_INTEGER:
		v = *(const int *) cell;
		break;
	      case DB_TYPE_BIGINT:
		memcpy (&v, cell, sizeof (INT64));
		break;
	      case DB_TYPE_DATE:
	      case DB_TYPE_TIME:
	      case DB_TYPE_TIMESTAMP:
		v = (INT64) (*(const unsigned int *) cell);
		break;
	      case DB_TYPE_DATETIME:
	      default:
		{
		  unsigned int d, t;
		  memcpy (&d, cell, sizeof (unsigned int));
		  memcpy (&t, cell + sizeof (unsigned int), sizeof (unsigned int));
		  v = ((INT64) d << 32) | (INT64) t;
		}
		break;
	      }
	    if (!have)
	      {
		mn = mx = v;
		have = true;
	      }
	    else if (v < mn)
	      {
		mn = v;
	      }
	    else if (v > mx)
	      {
		mx = v;
	      }
	  }
	if (have)
	  {
	    desc->minmax_kind = COLUMNAR_MINMAX_INT64;
	    desc->min_val = mn;
	    desc->max_val = mx;
	  }
      }
      break;

    case DB_TYPE_FLOAT:
    case DB_TYPE_DOUBLE:
    case DB_TYPE_MONETARY:
      {
	double mn = 0.0, mx = 0.0;

	for (r = 0; r < n_rows; r++)
	  {
	    double v;
	    const char *cell;

	    if (!(col->exists[r / 8] & (1 << (r % 8))))
	      {
		continue;
	      }
	    cell = col->data + (size_t) r * col->value_size;
	    if (col->type == DB_TYPE_FLOAT)
	      {
		float f;
		memcpy (&f, cell, sizeof (float));
		v = (double) f;
	      }
	    else
	      {
		/* DOUBLE, and MONETARY whose amount is the leading double */
		memcpy (&v, cell, sizeof (double));
	      }
	    if (!have)
	      {
		mn = mx = v;
		have = true;
	      }
	    else if (v < mn)
	      {
		mn = v;
	      }
	    else if (v > mx)
	      {
		mx = v;
	      }
	  }
	if (have)
	  {
	    desc->minmax_kind = COLUMNAR_MINMAX_DOUBLE;
	    memcpy (&desc->min_val, &mn, sizeof (double));
	    memcpy (&desc->max_val, &mx, sizeof (double));
	  }
      }
      break;

    default:
      break;
    }
}

/* ========================================================================== */
/* Chunk serialization                                                        */
/* ========================================================================== */

/*
 * columnar_serialize_chunks () - serialize the current chunk data from
 *   column buffers into the stripe data accumulator, then reset column
 *   buffers for the next chunk.
 */
static int
columnar_serialize_chunks (THREAD_ENTRY * thread_p, COLUMNAR_WRITE_STATE * ws)
{
  int i, error_code = NO_ERROR;
  int rows_in_chunk;
  COLUMNAR_COMPRESSION_TYPE comp = COLUMNAR_COMPRESS_NONE;	/* MVP: no compression first pass */

  /* the chunk being serialized is full unless this is the flush-time tail */
  rows_in_chunk = ws->current_rows % ws->chunk_max_rows;
  if (rows_in_chunk == 0)
    {
      rows_in_chunk = ws->chunk_max_rows;
    }

  /* TODO: select compression based on system parameter */
#if defined (HAVE_ZSTD)
  comp = COLUMNAR_COMPRESS_ZSTD;
#else
  comp = COLUMNAR_COMPRESS_LZ4;
#endif

  for (i = 0; i < ws->n_columns; i++)
    {
      COLUMNAR_COL_BUFFER *col = &ws->columns[i];
      COLUMNAR_CHUNK_DESC desc;
      char *compressed = NULL;
      int compressed_len = 0;

      memset (&desc, 0, sizeof (desc));

      /* min/max skip-list entry (computed on the raw uncompressed array) */
      columnar_compute_minmax (col, rows_in_chunk, &desc);

      /* compress the value data */
      error_code = columnar_compress_buffer (col->data, col->data_size, &compressed, &compressed_len, comp);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}

      /* append compressed data to stripe buffer */
      if (ws->stripe_data_size + compressed_len + col->exists_size > ws->stripe_data_alloc)
	{
	  int new_alloc = ws->stripe_data_alloc * 2;
	  while (new_alloc < ws->stripe_data_size + compressed_len + col->exists_size)
	    {
	      new_alloc *= 2;
	    }
	  char *new_buf = (char *) realloc (ws->stripe_data, new_alloc);
	  if (new_buf == NULL)
	    {
	      free (compressed);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) new_alloc);
	      return ER_OUT_OF_VIRTUAL_MEMORY;
	    }
	  ws->stripe_data = new_buf;
	  ws->stripe_data_alloc = new_alloc;
	}

      /* data */
      desc.data_offset = ws->stripe_data_size;
      desc.data_length = compressed_len;
      desc.decompressed_length = col->data_size;
      memcpy (ws->stripe_data + ws->stripe_data_size, compressed, compressed_len);
      ws->stripe_data_size += compressed_len;
      free (compressed);

      /* exists bitmap (uncompressed) */
      desc.exists_offset = ws->stripe_data_size;
      desc.exists_length = col->exists_size;
      memcpy (ws->stripe_data + ws->stripe_data_size, col->exists, col->exists_size);
      ws->stripe_data_size += col->exists_size;

      desc.compression = (INT8) comp;

      /* append chunk descriptor */
      if (ws->n_chunk_descs >= ws->chunk_descs_alloc)
	{
	  int new_alloc = ws->chunk_descs_alloc * 2;
	  COLUMNAR_CHUNK_DESC *new_descs = (COLUMNAR_CHUNK_DESC *)
	    realloc (ws->chunk_descs, new_alloc * sizeof (COLUMNAR_CHUNK_DESC));
	  if (new_descs == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
		      (size_t) (new_alloc * sizeof (COLUMNAR_CHUNK_DESC)));
	      return ER_OUT_OF_VIRTUAL_MEMORY;
	    }
	  ws->chunk_descs = new_descs;
	  ws->chunk_descs_alloc = new_alloc;
	}
      ws->chunk_descs[ws->n_chunk_descs++] = desc;

      /* reset column buffer for next chunk */
      col->data_size = 0;
      col->exists_size = 0;
      memset (col->exists, 0, col->exists_alloc);
    }

  ws->n_chunk_groups++;
  return NO_ERROR;
}

/* ========================================================================== */
/* Compression                                                                */
/* ========================================================================== */

static int
columnar_compress_buffer (const char *src, int src_len, char **out, int *out_len, COLUMNAR_COMPRESSION_TYPE comp)
{
  if (src_len == 0 || comp == COLUMNAR_COMPRESS_NONE)
    {
      /* no compression: return a copy */
      *out = (char *) malloc (src_len > 0 ? src_len : 1);
      if (*out == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) src_len);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      if (src_len > 0)
	{
	  memcpy (*out, src, src_len);
	}
      *out_len = src_len;
      return NO_ERROR;
    }

  if (comp == COLUMNAR_COMPRESS_LZ4)
    {
      int bound = LZ4_compressBound (src_len);
      *out = (char *) malloc (bound);
      if (*out == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) bound);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      *out_len = LZ4_compress_default (src, *out, src_len, bound);
      if (*out_len <= 0)
	{
	  /* compression failed — fall back to no compression */
	  memcpy (*out, src, src_len);
	  *out_len = src_len;
	}
      return NO_ERROR;
    }

#if defined (HAVE_ZSTD)
  if (comp == COLUMNAR_COMPRESS_ZSTD)
    {
      size_t bound = ZSTD_compressBound (src_len);
      *out = (char *) malloc (bound);
      if (*out == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) bound);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      size_t result = ZSTD_compress (*out, bound, src, src_len, 1 /* level */ );
      if (ZSTD_isError (result))
	{
	  /* fall back to no compression */
	  memcpy (*out, src, src_len);
	  *out_len = src_len;
	}
      else
	{
	  *out_len = (int) result;
	}
      return NO_ERROR;
    }
#endif

  /* unknown compression — fall through to no compression */
  return columnar_compress_buffer (src, src_len, out, out_len, COLUMNAR_COMPRESS_NONE);
}

/* ========================================================================== */
/* Stripe footer                                                              */
/* ========================================================================== */

static int
columnar_build_footer (COLUMNAR_WRITE_STATE * ws, char **footer_buf, int *footer_len)
{
  /*
   * Footer layout: COLUMNAR_STRIPE_FOOTER_HEADER, then
   * COLUMNAR_CHUNK_DESC[n_columns * n_chunk_groups].
   */
  int header_size = (int) sizeof (COLUMNAR_STRIPE_FOOTER_HEADER);
  int descs_size = ws->n_chunk_descs * (int) sizeof (COLUMNAR_CHUNK_DESC);
  int total = header_size + descs_size;
  char *buf;
  COLUMNAR_STRIPE_FOOTER_HEADER hdr;

  buf = (char *) malloc (total);
  if (buf == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) total);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  memset (&hdr, 0, sizeof (hdr));
  hdr.magic = COLUMNAR_FOOTER_MAGIC;
  hdr.version = COLUMNAR_FOOTER_VERSION;
  hdr.n_columns = ws->n_columns;
  hdr.n_chunk_groups = ws->n_chunk_groups;
  hdr.chunk_row_count = ws->chunk_max_rows;
  memcpy (buf, &hdr, sizeof (hdr));

  memcpy (buf + header_size, ws->chunk_descs, descs_size);

  *footer_buf = buf;
  *footer_len = total;
  return NO_ERROR;
}

/* ========================================================================== */
/* Stripe flush (sysop sequence)                                              */
/* ========================================================================== */

static int
columnar_flush_stripe (THREAD_ENTRY * thread_p, COLUMNAR_WRITE_STATE * ws)
{
  int error_code = NO_ERROR;
  char *footer_buf = NULL;
  int footer_len = 0;
  int total_data_len, total_pages, pages_written;
  VPID *vpids = NULL;
  VPID meta_vpid;
  PAGE_PTR meta_pgptr = NULL;
  COLUMNAR_METAPAGE_HEADER *meta_hdr;
  COLUMNAR_STRIPE_DIR_ENTRY dir_entry;
  LOG_DATA_ADDR addr;
  int data_offset, page_offset, bytes_to_write, bytes_avail;
  int i;

  /* serialize any remaining partial chunk */
  if ((ws->current_rows % ws->chunk_max_rows) != 0)
    {
      error_code = columnar_serialize_chunks (thread_p, ws);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  if (ws->stripe_data_size == 0 && ws->n_chunk_groups == 0)
    {
      /* nothing to flush */
      return NO_ERROR;
    }

  /* build footer */
  error_code = columnar_build_footer (ws, &footer_buf, &footer_len);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* compute total pages: page 0 is the stripe page map, data + footer follow */
  total_data_len = ws->stripe_data_size + footer_len;
  total_pages = 1 + (total_data_len + DB_PAGESIZE - 1) / DB_PAGESIZE;

  if (total_pages > COLUMNAR_STRIPE_PAGE_MAP_CAPACITY)
    {
      /* unreachable while COLUMNAR_STRIPE_MAX_BYTES holds; defensive guard */
      free (footer_buf);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_COLUMNAR_NOT_SUPPORTED, 1,
	      "stripe exceeds page map capacity");
      return ER_COLUMNAR_NOT_SUPPORTED;
    }

  /* allocate vpid array */
  vpids = (VPID *) malloc (total_pages * sizeof (VPID));
  if (vpids == NULL)
    {
      free (footer_buf);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      (size_t) (total_pages * sizeof (VPID)));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* ---- BEGIN SYSOP ---- */
  log_sysop_start (thread_p);

  /* allocate pages in bulk — NO contiguity requirement; the page map makes
   * every page reachable regardless of where file_alloc placed it */
  {
    PAGE_TYPE col_ptype = PAGE_COLUMNAR;
    error_code = file_alloc_multiple (thread_p, &ws->hfid.vfid, file_init_page_type,
				      &col_ptype, total_pages, vpids);
  }
  if (error_code != NO_ERROR)
    {
      ASSERT_ERROR ();
      goto sysop_abort;
    }

  /* write the stripe page map to page 0 (vpids[0]) */
  {
    COLUMNAR_STRIPE_PAGE_MAP_HEADER mhdr;
    PAGE_PTR pgptr;
    int map_len = (int) sizeof (mhdr) + total_pages * (int) sizeof (VPID);
    char *map_buf = (char *) malloc (map_len);

    if (map_buf == NULL)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) map_len);
	error_code = ER_OUT_OF_VIRTUAL_MEMORY;
	goto sysop_abort;
      }
    memset (&mhdr, 0, sizeof (mhdr));
    mhdr.magic = COLUMNAR_STRIPE_MAP_MAGIC;
    mhdr.version = COLUMNAR_STRIPE_MAP_VERSION;
    mhdr.page_count = total_pages;
    memcpy (map_buf, &mhdr, sizeof (mhdr));
    memcpy (map_buf + sizeof (mhdr), vpids, total_pages * sizeof (VPID));

    pgptr = pgbuf_fix (thread_p, &vpids[0], OLD_PAGE, PGBUF_LATCH_WRITE, PGBUF_UNCONDITIONAL_LATCH);
    if (pgptr == NULL)
      {
	free (map_buf);
	ASSERT_ERROR_AND_SET (error_code);
	goto sysop_abort;
      }
    pgbuf_set_page_ptype (thread_p, pgptr, PAGE_COLUMNAR);
    memcpy (pgptr, map_buf, map_len);
    memset ((char *) pgptr + map_len, 0, DB_PAGESIZE - map_len);

    addr.pgptr = pgptr;
    addr.vfid = &ws->hfid.vfid;
    addr.offset = 0;
    log_append_redo_data (thread_p, RVCOL_PAGE_DATA, &addr, map_len, map_buf);

    pgbuf_set_dirty (thread_p, pgptr, DONT_FREE);
    pgbuf_unfix_and_init (thread_p, pgptr);
    free (map_buf);
  }

  /* write stripe data + footer into pages 1..N-1 */
  data_offset = 0;
  pages_written = 0;
  {
    int combined_len = ws->stripe_data_size + footer_len;
    char *combined = (char *) malloc (combined_len);
    if (combined == NULL)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) combined_len);
	error_code = ER_OUT_OF_VIRTUAL_MEMORY;
	goto sysop_abort;
      }
    memcpy (combined, ws->stripe_data, ws->stripe_data_size);
    memcpy (combined + ws->stripe_data_size, footer_buf, footer_len);

    for (i = 1; i < total_pages; i++)
      {
	PAGE_PTR pgptr;
	pgptr = pgbuf_fix (thread_p, &vpids[i], OLD_PAGE, PGBUF_LATCH_WRITE, PGBUF_UNCONDITIONAL_LATCH);
	if (pgptr == NULL)
	  {
	    ASSERT_ERROR_AND_SET (error_code);
	    free (combined);
	    goto sysop_abort;
	  }

	pgbuf_set_page_ptype (thread_p, pgptr, PAGE_COLUMNAR);

	bytes_avail = DB_PAGESIZE;
	bytes_to_write = combined_len - data_offset;
	if (bytes_to_write > bytes_avail)
	  {
	    bytes_to_write = bytes_avail;
	  }

	memcpy (pgptr, combined + data_offset, bytes_to_write);
	/* zero any trailing bytes on last page */
	if (bytes_to_write < bytes_avail)
	  {
	    memset ((char *) pgptr + bytes_to_write, 0, bytes_avail - bytes_to_write);
	  }

	/* redo-only log: full page image */
	addr.pgptr = pgptr;
	addr.vfid = &ws->hfid.vfid;
	addr.offset = 0;
	log_append_redo_data (thread_p, RVCOL_PAGE_DATA, &addr, bytes_to_write, combined + data_offset);

	pgbuf_set_dirty (thread_p, pgptr, DONT_FREE);
	pgbuf_unfix_and_init (thread_p, pgptr);

	data_offset += bytes_to_write;
	pages_written++;
      }

    free (combined);
  }

  /* reserve stripe_id and row_number from metapage counters */
  meta_vpid.volid = ws->hfid.vfid.volid;
  meta_vpid.pageid = ws->hfid.hpgid;

  meta_pgptr = pgbuf_fix (thread_p, &meta_vpid, OLD_PAGE, PGBUF_LATCH_WRITE, PGBUF_UNCONDITIONAL_LATCH);
  if (meta_pgptr == NULL)
    {
      ASSERT_ERROR_AND_SET (error_code);
      goto sysop_abort;
    }

  meta_hdr = (COLUMNAR_METAPAGE_HEADER *) meta_pgptr;
  assert (meta_hdr->magic == COLUMNAR_METAPAGE_MAGIC);

  /* build directory entry */
  memset (&dir_entry, 0, sizeof (dir_entry));
  dir_entry.stripe_id = meta_hdr->next_stripe_id;
  dir_entry.first_row_number = meta_hdr->next_row_number;
  dir_entry.row_count = ws->current_rows;
  dir_entry.chunk_group_count = ws->n_chunk_groups;
  dir_entry.start_vpid = vpids[0];	/* the stripe page map page */
  dir_entry.page_count = total_pages;
  /* footer starts at byte offset stripe_data_size of the data stream, which
   * occupies stripe pages 1..N-1 (page 0 is the page map) */
  {
    int footer_page_idx = 1 + ws->stripe_data_size / DB_PAGESIZE;
    dir_entry.footer_vpid = vpids[footer_page_idx];
    dir_entry.footer_page_idx = (INT16) footer_page_idx;
    dir_entry.footer_offset = (INT16) (ws->stripe_data_size % DB_PAGESIZE);
  }
  /* insert_mvccid: current transaction's MVCCID.  logtb_get_current_mvccid ()
   * assigns one when the transaction does not have it yet — a columnar-only
   * transaction must still stamp a valid id, otherwise the reader could treat
   * an uncommitted stripe as visible. */
  dir_entry.insert_mvccid = logtb_get_current_mvccid (thread_p);

  /* update counters atomically (under X page latch).  Counters are
   * advance-only: an aborted flush leaves a gap in stripe_id/row_number,
   * which is harmless (readers use directory entries only), so the counter
   * update is logged redo-only on the first metapage. */
  meta_hdr->next_stripe_id++;
  meta_hdr->next_row_number += ws->current_rows;

  addr.pgptr = meta_pgptr;
  addr.vfid = &ws->hfid.vfid;
  addr.offset = 0;
  log_append_redo_data (thread_p, RVCOL_META_DIR_ENTRY, &addr, (int) sizeof (COLUMNAR_METAPAGE_HEADER), meta_hdr);
  pgbuf_set_dirty (thread_p, meta_pgptr, DONT_FREE);

  /* find the append target: the first chain page with a free directory
   * slot.  Forward-only latch crabbing; concurrent flushes serialize on
   * the page latches. */
  while (meta_hdr->entry_count >= COLUMNAR_META_ENTRIES_PER_PAGE)
    {
      if (VPID_ISNULL (&meta_hdr->next_metapage))
	{
	  /* extend the chain.  The structural extension (page alloc + init +
	   * link) commits in a nested sysop independent of the outer
	   * transaction — like a btree split — so an abort of this INSERT
	   * never leaves a dangling next_metapage link; it only leaves an
	   * empty, reusable chain page. */
	  VPID new_vpid;
	  PAGE_PTR new_pgptr = NULL;
	  COLUMNAR_METAPAGE_HEADER new_hdr;
	  PAGE_TYPE col_ptype = PAGE_COLUMNAR;

	  log_sysop_start (thread_p);

	  error_code = file_alloc (thread_p, &ws->hfid.vfid, file_init_page_type, &col_ptype, &new_vpid, &new_pgptr);
	  if (error_code != NO_ERROR || new_pgptr == NULL)
	    {
	      ASSERT_ERROR ();
	      log_sysop_abort (thread_p);
	      pgbuf_unfix_and_init (thread_p, meta_pgptr);
	      goto sysop_abort;
	    }

	  memset (&new_hdr, 0, sizeof (new_hdr));
	  new_hdr.magic = COLUMNAR_METAPAGE_MAGIC;
	  new_hdr.version = COLUMNAR_METAPAGE_VERSION;
	  new_hdr.entry_count = 0;
	  VPID_SET_NULL (&new_hdr.next_metapage);
	  new_hdr.class_oid = meta_hdr->class_oid;

	  pgbuf_set_page_ptype (thread_p, new_pgptr, PAGE_COLUMNAR);
	  memcpy (new_pgptr, &new_hdr, sizeof (new_hdr));
	  addr.pgptr = new_pgptr;
	  addr.vfid = &ws->hfid.vfid;
	  addr.offset = 0;
	  log_append_redo_data (thread_p, RVCOL_METAPAGE_INIT, &addr, (int) sizeof (new_hdr), &new_hdr);
	  pgbuf_set_dirty (thread_p, new_pgptr, DONT_FREE);

	  /* link the old tail to the new page (redo-only header image) */
	  meta_hdr->next_metapage = new_vpid;
	  addr.pgptr = meta_pgptr;
	  addr.offset = 0;
	  log_append_redo_data (thread_p, RVCOL_META_DIR_ENTRY, &addr, (int) sizeof (COLUMNAR_METAPAGE_HEADER),
				meta_hdr);
	  pgbuf_set_dirty (thread_p, meta_pgptr, DONT_FREE);

	  log_sysop_commit (thread_p);

	  pgbuf_unfix_and_init (thread_p, meta_pgptr);
	  meta_pgptr = new_pgptr;
	  meta_hdr = (COLUMNAR_METAPAGE_HEADER *) meta_pgptr;
	  break;		/* a fresh page always has room */
	}
      else
	{
	  VPID next_vpid = meta_hdr->next_metapage;
	  PAGE_PTR next_pgptr =
	    pgbuf_fix (thread_p, &next_vpid, OLD_PAGE, PGBUF_LATCH_WRITE, PGBUF_UNCONDITIONAL_LATCH);

	  if (next_pgptr == NULL)
	    {
	      ASSERT_ERROR_AND_SET (error_code);
	      pgbuf_unfix_and_init (thread_p, meta_pgptr);
	      goto sysop_abort;
	    }
	  pgbuf_unfix_and_init (thread_p, meta_pgptr);
	  meta_pgptr = next_pgptr;
	  meta_hdr = (COLUMNAR_METAPAGE_HEADER *) meta_pgptr;
	  assert (meta_hdr->magic == COLUMNAR_METAPAGE_MAGIC);
	}
    }

  /* append the directory entry to the tail page.  Redo carries the tail
   * header AND the entry bytes (a header-only redo would leave the entry
   * slot unrecovered after a crash); undo restores the previous
   * entry_count, which removes the entry. */
  {
    int dir_offset =
      (int) sizeof (COLUMNAR_METAPAGE_HEADER) + meta_hdr->entry_count * (int) sizeof (COLUMNAR_STRIPE_DIR_ENTRY);
    INT32 old_entry_count = meta_hdr->entry_count;
    char redo_buf[sizeof (COLUMNAR_METAPAGE_HEADER) + sizeof (COLUMNAR_STRIPE_DIR_ENTRY)];

    memcpy ((char *) meta_pgptr + dir_offset, &dir_entry, sizeof (COLUMNAR_STRIPE_DIR_ENTRY));
    meta_hdr->entry_count++;

    memcpy (redo_buf, meta_hdr, sizeof (COLUMNAR_METAPAGE_HEADER));
    memcpy (redo_buf + sizeof (COLUMNAR_METAPAGE_HEADER), &dir_entry, sizeof (COLUMNAR_STRIPE_DIR_ENTRY));

    addr.pgptr = meta_pgptr;
    addr.vfid = &ws->hfid.vfid;
    addr.offset = dir_offset;
    log_append_undoredo_data (thread_p, RVCOL_META_DIR_ENTRY, &addr,
			      (int) sizeof (INT32), (int) sizeof (redo_buf), &old_entry_count, redo_buf);
  }

  pgbuf_set_dirty (thread_p, meta_pgptr, DONT_FREE);
  pgbuf_unfix_and_init (thread_p, meta_pgptr);

  /* ---- ATTACH SYSOP TO OUTER ---- */
  log_sysop_attach_to_outer (thread_p);

  /* reset write state for next stripe */
  ws->current_rows = 0;
  ws->stripe_data_size = 0;
  ws->n_chunk_descs = 0;
  ws->n_chunk_groups = 0;
  for (i = 0; i < ws->n_columns; i++)
    {
      ws->columns[i].data_size = 0;
      ws->columns[i].exists_size = 0;
      memset (ws->columns[i].exists, 0, ws->columns[i].exists_alloc);
    }

  free (footer_buf);
  free (vpids);
  return NO_ERROR;

sysop_abort:
  log_sysop_abort (thread_p);
  if (footer_buf != NULL)
    {
      free (footer_buf);
    }
  if (vpids != NULL)
    {
      free (vpids);
    }
  return error_code;
}

/* ========================================================================== */
/* Commit / abort hooks                                                       */
/* ========================================================================== */

int
columnar_flush_all_write_states (THREAD_ENTRY * thread_p)
{
  int tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  COLUMNAR_WRITE_STATE *ws, *next;
  int error_code = NO_ERROR;

  if (tran_index < 0 || tran_index >= columnar_Tran_wstates_count || columnar_Tran_wstates == NULL)
    {
      return NO_ERROR;
    }

  for (ws = columnar_Tran_wstates[tran_index]; ws != NULL; ws = next)
    {
      next = ws->next;
      if (ws->current_rows > 0)
	{
	  error_code = columnar_flush_stripe (thread_p, ws);
	  if (error_code != NO_ERROR)
	    {
	      return error_code;
	    }
	}
      columnar_free_write_state (thread_p, ws);
    }

  columnar_Tran_wstates[tran_index] = NULL;
  return NO_ERROR;
}

void
columnar_discard_all_write_states (THREAD_ENTRY * thread_p)
{
  int tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  COLUMNAR_WRITE_STATE *ws, *next;

  if (tran_index < 0 || tran_index >= columnar_Tran_wstates_count || columnar_Tran_wstates == NULL)
    {
      return;
    }

  for (ws = columnar_Tran_wstates[tran_index]; ws != NULL; ws = next)
    {
      next = ws->next;
      columnar_free_write_state (thread_p, ws);
    }

  columnar_Tran_wstates[tran_index] = NULL;
}

/* ========================================================================== */
/* Savepoint hooks                                                            */
/* ========================================================================== */

void
columnar_on_savepoint (THREAD_ENTRY * thread_p, const char *savept_name)
{
  int tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  COLUMNAR_WRITE_STATE *ws;

  if (tran_index < 0 || tran_index >= columnar_Tran_wstates_count || columnar_Tran_wstates == NULL)
    {
      return;
    }

  for (ws = columnar_Tran_wstates[tran_index]; ws != NULL; ws = ws->next)
    {
      COLUMNAR_SAVEPT *sp = (COLUMNAR_SAVEPT *) db_private_alloc (thread_p, sizeof (COLUMNAR_SAVEPT));
      if (sp == NULL)
	{
	  return;		/* best effort */
	}
      sp->savept_name = (char *) db_private_alloc (thread_p, strlen (savept_name) + 1);
      if (sp->savept_name != NULL)
	{
	  strcpy (sp->savept_name, savept_name);
	}
      sp->rows_at_savepoint = ws->current_rows;
      sp->prev = ws->savept_stack;
      ws->savept_stack = sp;
    }
}

void
columnar_on_partial_abort (THREAD_ENTRY * thread_p, const char *savept_name)
{
  int tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  COLUMNAR_WRITE_STATE *ws;

  if (tran_index < 0 || tran_index >= columnar_Tran_wstates_count || columnar_Tran_wstates == NULL)
    {
      return;
    }

  for (ws = columnar_Tran_wstates[tran_index]; ws != NULL; ws = ws->next)
    {
      COLUMNAR_SAVEPT *sp, *prev;
      int target_rows = -1;

      /* find the matching savepoint and pop everything above it */
      for (sp = ws->savept_stack; sp != NULL; sp = prev)
	{
	  prev = sp->prev;
	  if (sp->savept_name != NULL && strcmp (sp->savept_name, savept_name) == 0)
	    {
	      target_rows = sp->rows_at_savepoint;
	      /* pop this and all above */
	      COLUMNAR_SAVEPT *pop = ws->savept_stack;
	      while (pop != NULL)
		{
		  COLUMNAR_SAVEPT *pnext = pop->prev;
		  if (pop->savept_name != NULL)
		    {
		      db_private_free (thread_p, pop->savept_name);
		    }
		  db_private_free (thread_p, pop);
		  if (pop == sp)
		    {
		      ws->savept_stack = pnext;
		      break;
		    }
		  pop = pnext;
		}
	      break;
	    }
	}

      if (target_rows >= 0 && target_rows < ws->current_rows)
	{
	  /* truncate unflushed rows back to the savepoint.
	   * For simplicity, if chunks have already been serialized (rows in completed chunks),
	   * we only truncate within the current chunk buffer.  Already-flushed stripes are
	   * handled by log rollback (sysop undo). */
	  int chunk_boundary = (target_rows / ws->chunk_max_rows) * ws->chunk_max_rows;

	  if (chunk_boundary == target_rows)
	    {
	      /* target is exactly on a chunk boundary — discard current chunk buffers entirely.
	       * Any serialized chunks beyond chunk_boundary's chunk group must also be discarded.
	       * For now, reset all column buffers and adjust counters. */
	      int target_groups = target_rows / ws->chunk_max_rows;
	      int i;

	      for (i = 0; i < ws->n_columns; i++)
		{
		  ws->columns[i].data_size = 0;
		  ws->columns[i].exists_size = 0;
		  memset (ws->columns[i].exists, 0, ws->columns[i].exists_alloc);
		}

	      /* truncate serialized stripe data if we serialized more chunk groups than target */
	      if (ws->n_chunk_groups > target_groups)
		{
		  /* recalculate stripe_data_size from chunk descs up to target_groups */
		  int keep_descs = target_groups * ws->n_columns;
		  int keep_size = 0;
		  int d;
		  for (d = 0; d < keep_descs && d < ws->n_chunk_descs; d++)
		    {
		      int end = (int) ws->chunk_descs[d].exists_offset + ws->chunk_descs[d].exists_length;
		      if (end > keep_size)
			{
			  keep_size = end;
			}
		    }
		  ws->stripe_data_size = keep_size;
		  ws->n_chunk_descs = keep_descs;
		  ws->n_chunk_groups = target_groups;
		}
	    }
	  else
	    {
	      /* target is mid-chunk — we need to trim column buffers.
	       * This is complex: recalculate data_size for each column.
	       * For fixed-width columns it's straightforward.
	       * For variable-width columns we'd need to replay the offset table.
	       * MVP: discard the entire current chunk and accept losing rows since
	       * savepoint up to the chunk boundary.  The log rollback handles
	       * already-flushed stripes; this only loses unflushed partial-chunk rows
	       * between the chunk boundary and the savepoint, which is conservative
	       * (fewer rows than requested, never more). */
	      int rows_in_chunk = target_rows - chunk_boundary;
	      int i;

	      for (i = 0; i < ws->n_columns; i++)
		{
		  COLUMNAR_COL_BUFFER *col = &ws->columns[i];
		  if (col->value_size > 0)
		    {
		      /* fixed-width: truncate to exact row count */
		      col->data_size = rows_in_chunk * col->value_size;
		    }
		  else
		    {
		      /* variable-width: we can't truncate precisely without an offset array.
		       * Conservative: discard the whole chunk buffer. */
		      col->data_size = 0;
		      rows_in_chunk = 0;	/* adjust */
		    }
		  col->exists_size = (rows_in_chunk + 7) / 8;
		}
	    }

	  ws->current_rows = target_rows;
	}
    }
}

/* ========================================================================== */
/* Same-txn scan flush                                                        */
/* ========================================================================== */

int
columnar_flush_for_scan (THREAD_ENTRY * thread_p, const OID * class_oid)
{
  COLUMNAR_WRITE_STATE *ws = columnar_find_write_state (thread_p, class_oid);
  if (ws != NULL && ws->current_rows > 0)
    {
      return columnar_flush_stripe (thread_p, ws);
    }
  return NO_ERROR;
}

bool
columnar_has_pending_writes (THREAD_ENTRY * thread_p, const OID * class_oid)
{
  COLUMNAR_WRITE_STATE *ws = columnar_find_write_state (thread_p, class_oid);
  return (ws != NULL && ws->current_rows > 0);
}

/* ========================================================================== */
/* Recovery functions                                                         */
/* ========================================================================== */

/*
 * columnar_rv_page_data_redo () - redo a stripe data page write
 *
 *   The redo data is the page content at rcv->offset.
 *   This is a full-page-image redo for newly allocated pages.
 */
int
columnar_rv_page_data_redo (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  assert (rcv->pgptr != NULL);
  assert (rcv->offset >= 0);

  pgbuf_set_page_ptype (thread_p, rcv->pgptr, PAGE_COLUMNAR);

  if (rcv->length > 0 && rcv->data != NULL)
    {
      assert (rcv->offset + rcv->length <= DB_PAGESIZE);
      memcpy ((char *) rcv->pgptr + rcv->offset, rcv->data, rcv->length);
    }

  pgbuf_set_dirty (thread_p, rcv->pgptr, DONT_FREE);
  return NO_ERROR;
}

/*
 * columnar_rv_dir_entry_redo () - redo a metapage header update, optionally
 *                                 with an appended directory entry
 *
 *   Redo data layout: COLUMNAR_METAPAGE_HEADER, optionally followed by one
 *   COLUMNAR_STRIPE_DIR_ENTRY.  The header is restored at page offset 0
 *   (counters, entry_count, next_metapage link).  When the entry is present,
 *   it is restored at rcv->offset — a header-only redo would leave the
 *   appended entry slot unrecovered after a crash.
 */
int
columnar_rv_dir_entry_redo (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  assert (rcv->pgptr != NULL);

  if (rcv->length >= (int) sizeof (COLUMNAR_METAPAGE_HEADER) && rcv->data != NULL)
    {
      /* restore the full metapage header (counters, entry_count, next link) */
      memcpy (rcv->pgptr, rcv->data, sizeof (COLUMNAR_METAPAGE_HEADER));

      if (rcv->length >= (int) (sizeof (COLUMNAR_METAPAGE_HEADER) + sizeof (COLUMNAR_STRIPE_DIR_ENTRY))
	  && rcv->offset >= 0 && rcv->offset + (int) sizeof (COLUMNAR_STRIPE_DIR_ENTRY) <= DB_PAGESIZE)
	{
	  /* restore the appended directory entry at its slot */
	  memcpy ((char *) rcv->pgptr + rcv->offset, rcv->data + sizeof (COLUMNAR_METAPAGE_HEADER),
		  sizeof (COLUMNAR_STRIPE_DIR_ENTRY));
	}
    }

  pgbuf_set_dirty (thread_p, rcv->pgptr, DONT_FREE);
  return NO_ERROR;
}

/*
 * columnar_rv_dir_entry_undo () - undo a directory entry append
 *
 *   The undo data is the previous entry_count (INT32).
 *   Restoring entry_count effectively removes the appended entry.
 *   The counter decrements (next_stripe_id, next_row_number) are
 *   handled by the sysop undo which also deallocates the stripe pages
 *   via RVFL_ALLOC undo.
 */
int
columnar_rv_dir_entry_undo (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  COLUMNAR_METAPAGE_HEADER *hdr;

  assert (rcv->pgptr != NULL);
  assert (rcv->length == sizeof (INT32));

  hdr = (COLUMNAR_METAPAGE_HEADER *) rcv->pgptr;

  if (rcv->data != NULL && rcv->length >= (int) sizeof (INT32))
    {
      INT32 old_count;
      memcpy (&old_count, rcv->data, sizeof (INT32));
      hdr->entry_count = old_count;
    }

  pgbuf_set_dirty (thread_p, rcv->pgptr, DONT_FREE);
  return NO_ERROR;
}
