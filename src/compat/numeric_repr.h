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
 * numeric_repr.h - how the digits of a NUMERIC value are laid out in its DB_VALUE buffer (DB_NUMERIC::d.buf) and on
 *                  disk. Outside numeric_opfunc.c, code that reads or writes those bytes goes through
 *                  numeric_repr_*; a build has one representation.
 */

#ifndef _NUMERIC_REPR_H_
#define _NUMERIC_REPR_H_

#ident "$Id$"

#include "byte_order.h"
#include "dbtype_def.h"

#include <stdint.h>
#include <string.h>

/*
 * NUMERIC_HEADER_SIZE
 * The numeric header is 3 bytes in size.  Shared by the generic handler (mr_data_readval_numeric () in
 * object_primitive.c) and the heap's fixed-column decode kernel (heap_attr_readval_numeric () in
 * heap_file.c), which parse the same bytes and must agree on where the digits start.
 * - header[0]: Total byte size of the numeric data (header + data).
 *              Bit 7 (0x80): Data sign bit (1 = negative, 0 = positive).
 *              Bits 0-6: Total byte size value (4 ~ 20 bytes).
 *                        Minimum: precision 1 -> data 1 byte + header 3 = 4 bytes (aligned to 4).
 *                        Maximum: precision 40 -> data 17 bytes + header 3 = 20 bytes (aligned to 20).
 * - header[1]: Precision and scale sign bit.
 *              Bit 7 (0x80): Scale sign bit (1 = negative scale, 0 = positive scale).
 *              Bits 0-6: Precision value (1 ~ 40).
 * - header[2]: Scale value.
 *              Bits 0-7: Scale absolute value (0 ~ 214).
 *                        Positive scale: 0 ~ 252.
 *                        Negative scale: -214 to -1 (stored as absolute value, sign in header[1] bit 7).
 */
#define NUMERIC_HEADER_SIZE (3)

/* binary NUMERIC representation: the magnitude is one big-endian binary integer of DB_NUMERIC_BUF_SIZE bytes; the
 * disk keeps its low bytes, the header plus the digits padded to a multiple of 4. Plain C: dbtype_function.c is
 * compiled as C. */

/* disk size (header included) of a float NUMERIC by its precision */
static inline int
numeric_repr_disk_size_by_precision (int precision)
{
  static const int size_by_precision[DB_MAX_NUMERIC_PRECISION + 1] = {
    0, 4, 4, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 12, 12, 12, 12, 12, 12, 12,
    12, 12, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 20, 20, 20, 20, 20, 20, 20, 20,
    20
  };

  return size_by_precision[precision];
}

/* disk size (header included) of a fixed NUMERIC by its value: a fixed precision is at most 38, so the magnitude
 * fits in the low 16 bytes */
static inline int
numeric_repr_disk_size_by_value (const unsigned char *buf)
{
  static const int size_by_bytes[17] = {
    0, 4, 8, 8, 8, 8, 12, 12, 12, 12, 16, 16, 16, 16, 20, 20, 20
  };
  uint64_t word_buf[2];

  /* the bytes stay big-endian: on a little-endian host the leading zero bytes are the trailing zero bits */
  memcpy (word_buf, buf + 1, sizeof (word_buf));
  if (word_buf[0] != 0)
    {
#if OR_BYTE_ORDER == OR_LITTLE_ENDIAN
      return size_by_bytes[16 - (__builtin_ctzll (word_buf[0]) >> 3)];
#else
      return size_by_bytes[16 - (__builtin_clzll (word_buf[0]) >> 3)];
#endif
    }
  if (word_buf[1] != 0)
    {
#if OR_BYTE_ORDER == OR_LITTLE_ENDIAN
      return size_by_bytes[8 - (__builtin_ctzll (word_buf[1]) >> 3)];
#else
      return size_by_bytes[8 - (__builtin_clzll (word_buf[1]) >> 3)];
#endif
    }
  return size_by_bytes[1];	/* zero */
}

/* the buffer from the digits of a disk value of disk_size bytes; 0 when no disk value has that size */
static inline int
numeric_repr_read_disk (unsigned char *buf, const unsigned char *digits, int disk_size)
{
  const int digits_size = disk_size - NUMERIC_HEADER_SIZE;

  if (disk_size < 4 || disk_size > 20 || (disk_size & 3) != 0)
    {
      return 0;
    }
  memset (buf, 0, DB_NUMERIC_BUF_SIZE - digits_size);
  memcpy (buf + DB_NUMERIC_BUF_SIZE - digits_size, digits, digits_size);
  return 1;
}

/* the digits of the disk value of disk_size bytes */
static inline void
numeric_repr_write_disk (unsigned char *digits, const unsigned char *buf, int disk_size)
{
  const int digits_size = disk_size - NUMERIC_HEADER_SIZE;

  memcpy (digits, buf + DB_NUMERIC_BUF_SIZE - digits_size, digits_size);
}

/* 32-bit fold of the whole buffer */
static inline unsigned int
numeric_repr_fold32 (const unsigned char *buf)
{
  unsigned int words[4];
  unsigned int last = 0;

  memcpy (words, buf, sizeof (words));
  memcpy (&last, buf + 16, 1);
  return words[0] ^ words[1] ^ words[2] ^ words[3] ^ last;
}

#endif /* _NUMERIC_REPR_H_ */
