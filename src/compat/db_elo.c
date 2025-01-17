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

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "elo.h"
#include "error_manager.h"
#include "storage_common.h"
#include "object_primitive.h"
#include "db.h"
#include "elo.h"
#include "db_elo.h"
#include "dbtype.h"
// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

/*
 * db_elo.c - DB_API for ELO layer
 */

/*
 * db_create_fbo () - create a new fbo value
 * return: NO_ERROR if successful, error code otherwise
 * value(in): DB_VALUE
 * type(in): one of DB_TYPE_BLOB, DB_TYPE_CLOB
 */
int
db_create_fbo (DB_VALUE * value, DB_TYPE type)
{
  DB_ELO elo;
  int ret;

  CHECK_1ARG_ERROR (value);

  ret = elo_create (&elo);
  if (ret == NO_ERROR)
    {
      ret = db_make_elo (value, type, &elo);
      if (ret == NO_ERROR)
	{
	  value->need_clear = true;
	}
    }

  return ret;
}

/*
 * db_elo_copy_structure () -
 * return:
 * src(in):
 * dest(out):
 */
int
db_elo_copy_structure (const DB_ELO * src, DB_ELO * dest)
{
  CHECK_2ARGS_ERROR (src, dest);

  return elo_copy_structure (src, dest);
}

/*
 * db_elo_free_structure () -
 * return:
 * elo(in):
 */
void
db_elo_free_structure (DB_ELO * elo)
{
  if (elo != NULL)
    {
      elo_free_structure (elo);
    }
}

/*
 * db_elo_copy () -
 * return:
 * src(in):
 * dest(out):
 */
int
db_elo_copy (DB_ELO * src, DB_ELO * dest)
{
  CHECK_2ARGS_ERROR (src, dest);

  return elo_copy (src, dest);
}

/*
 * db_elo_delete () -
 * return:
 * elo(in):
 */
int
db_elo_delete (DB_ELO * elo)
{
  CHECK_1ARG_ERROR (elo);

  return elo_delete (elo, false);
}

/*
 * db_elo_size () -
 * return:
 * elo(in):
 */
DB_BIGINT
db_elo_size (DB_ELO * elo)
{
  CHECK_1ARG_ERROR (elo);

  return elo_size (elo);
}

/*
 * db_elo_read () -
 *
 * return: error code or NO_ERROR
 * elo(in):
 * pos(in):
 * buf(out):
 * count(in):
 */
int
db_elo_read (const DB_ELO * elo, off_t pos, void *buf, size_t count, DB_BIGINT * read_bytes)
{
  INT64 ret;

  if (elo == NULL || pos < 0 || buf == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  ret = elo_read (elo, pos, buf, count);
  if (ret < 0)
    {
      return (int) ret;
    }
  if (read_bytes != NULL)
    {
      *read_bytes = ret;
    }
  return NO_ERROR;
}

/*
 * db_elo_write () -
 *
 * return:
 * elo(in):
 * pos(in):
 * buf(in):
 * count(in):
 */
int
db_elo_write (DB_ELO * elo, off_t pos, const void *buf, size_t count, DB_BIGINT * written_bytes)
{
  INT64 ret;

  if (elo == NULL || pos < 0 || buf == NULL || count == 0)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  ret = elo_write (elo, pos, buf, count);
  if (ret < 0)
    {
      return (int) ret;
    }
  if (written_bytes != NULL)
    {
      *written_bytes = ret;
    }
  return NO_ERROR;
}
