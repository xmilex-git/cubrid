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
 * fixture.cpp — vhb::build_fixture() (value_handle_bench.hpp contract). Owned by 30-P2-Harness.
 *
 * GROUND TRUTH: every row is a REAL CUBRID disk image. Each generated DB_VALUE is serialized
 * through the exact pr_type/OR_BUF machinery the server itself uses (object_primitive.c's
 * writeval family), following the same call shape as query_opfunc.c:382-385 /
 * query_aggregate.cpp:843-844 (pr_data_writeval_disk_size() sizes the buffer, or_init() wraps
 * it, pr_type::data_writeval() writes into it) — never a hand-rolled encoding. Compressed
 * VARCHAR goes through the identical mr_writeval_char_type_common()/LZ4 path a real heap/list
 * write would take (pr_Enable_string_compression defaults true, object_primitive.c:913, and is
 * never toggled by this harness), so the LOW-entropy CV fixture's >=2:1 compression ratio
 * assertion below is measuring the same compression a real ≥255B VARCHAR column gets.
 */

#include "value_handle_bench.hpp"

#include "dbtype.h"
#include "error_code.h"
#include "numeric_opfunc.h"
#include "object_primitive.h"
#include "object_representation.h"

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <random>
#include <stdexcept>
#include <string>

namespace vhb
{
namespace
{
  /* ---- matrix row counts (P2.3 matrix, ralplan/grilled fixture-shapes contract) ---- */
  constexpr std::size_t FL_ROW_COUNT = 1'000'000;             /* FL_FILTER / FL_SORT / PEEK_VS_COPY */
  constexpr std::size_t MED_ROW_COUNT = 500'000;              /* CV_*, UV_PEEK, NUM_*, ABBREV_SUBCELL */

  constexpr int CV_LEN = 300;                                 /* VARCHAR(300), compressible cells   */
  constexpr int UV_LEN = 100;                                 /* VARCHAR(100), uncompressed cells   */
  constexpr int NUM_PRECISION = 15;                            /* NUMERIC(15,2)                      */
  constexpr int NUM_SCALE = 2;

  /* [LOW hygiene, architect finding] CV fixtures (any entropy) MUST actually compress >=2:1;
   * assert a floor, don't just hope. Originally only asserted for LOW entropy (HIGH's 8B
   * pseudo-random prefix was assumed to risk falling short) — re-checked and extended to HIGH
   * too, see assert_cv_compression_ratio() below: HIGH's random prefix is only 8 of CV_LEN(300)
   * bytes, leaving an overwhelmingly redundant 'x'-filled tail (292B) that LZ4 crushes just as
   * effectively as LOW's single differentiator byte does, so no filler-redundancy adjustment
   * was needed to clear the same floor. */
  constexpr double CV_MIN_COMPRESSION_RATIO = 2.0;

  std::size_t
  matrix_row_count (cell_id c)
  {
    switch (c)
      {
      case cell_id::FL_FILTER:
      case cell_id::FL_SORT:
      case cell_id::PEEK_VS_COPY:
	return FL_ROW_COUNT;
      default:
	return MED_ROW_COUNT;
      }
  }

  /* ---- disk-image serialization: the one place this file touches pr_type/OR_BUF ---- */
  void
  serialize_value (DB_VALUE *value, std::vector<char> &out, int &len)
  {
    int size = pr_data_writeval_disk_size (value);
    out.assign (static_cast<std::size_t> (size), 0);
    OR_BUF buf;
    or_init (&buf, out.data (), size);
    const PR_TYPE *pr_type = pr_type_from_id (DB_VALUE_DOMAIN_TYPE (value));
    if (pr_type == NULL || pr_type->data_writeval (&buf, value) != NO_ERROR)
      {
	throw std::runtime_error ("vhb::build_fixture: pr_type::data_writeval failed");
      }
    len = size;
  }

  void
  append_row (serialized_column &col, DB_VALUE *value)
  {
    std::vector<char> img;
    int len = 0;
    serialize_value (value, img, len);
    col.vals.push_back (std::move (img));
    col.lengths.push_back (len);
  }

  /* ---- per-type column builders — all deterministic in (row_count, seed) ---- */

  serialized_column
  make_int_column (std::size_t rows, std::uint32_t seed)
  {
    serialized_column col;
    col.dbtype = static_cast<int> (DB_TYPE_INTEGER);
    col.precision = 0;
    col.vals.reserve (rows);
    col.lengths.reserve (rows);
    std::mt19937 rng (seed);
    std::uniform_int_distribution<std::int32_t> dist (INT32_MIN / 2, INT32_MAX / 2);
    for (std::size_t i = 0; i < rows; i++)
      {
	DB_VALUE v;
	db_make_int (&v, dist (rng));
	append_row (col, &v);
      }
    return col;
  }

  serialized_column
  make_bigint_column (std::size_t rows, std::uint32_t seed)
  {
    serialized_column col;
    col.dbtype = static_cast<int> (DB_TYPE_BIGINT);
    col.precision = 0;
    col.vals.reserve (rows);
    col.lengths.reserve (rows);
    std::mt19937_64 rng (seed);
    std::uniform_int_distribution<std::int64_t> dist (INT64_MIN / 2, INT64_MAX / 2);
    for (std::size_t i = 0; i < rows; i++)
      {
	DB_VALUE v;
	db_make_bigint (&v, dist (rng));
	append_row (col, &v);
      }
    return col;
  }

  serialized_column
  make_double_column (std::size_t rows, std::uint32_t seed)
  {
    serialized_column col;
    col.dbtype = static_cast<int> (DB_TYPE_DOUBLE);
    col.precision = 0;
    col.vals.reserve (rows);
    col.lengths.reserve (rows);
    std::mt19937_64 rng (seed);
    std::uniform_real_distribution<double> dist (-1.0e12, 1.0e12);
    for (std::size_t i = 0; i < rows; i++)
      {
	DB_VALUE v;
	db_make_double (&v, dist (rng));
	append_row (col, &v);
      }
    return col;
  }

  serialized_column
  make_date_column (std::size_t rows, std::uint32_t seed)
  {
    serialized_column col;
    col.dbtype = static_cast<int> (DB_TYPE_DATE);
    col.precision = 0;
    col.vals.reserve (rows);
    col.lengths.reserve (rows);
    std::mt19937 rng (seed);
    std::uniform_int_distribution<int> year_dist (1970, 2037);
    std::uniform_int_distribution<int> month_dist (1, 12);
    std::uniform_int_distribution<int> day_dist (1, 28); /* avoid month-length edge cases */
    for (std::size_t i = 0; i < rows; i++)
      {
	DB_VALUE v;
	db_make_date (&v, month_dist (rng), day_dist (rng), year_dist (rng));
	append_row (col, &v);
      }
    return col;
  }

  serialized_column
  make_timestamp_column (std::size_t rows, std::uint32_t seed)
  {
    serialized_column col;
    col.dbtype = static_cast<int> (DB_TYPE_TIMESTAMP);
    col.precision = 0;
    col.vals.reserve (rows);
    col.lengths.reserve (rows);
    std::mt19937 rng (seed);
    std::uniform_int_distribution<std::uint32_t> dist (0u, 2'000'000'000u);
    for (std::size_t i = 0; i < rows; i++)
      {
	DB_VALUE v;
	db_make_timestamp (&v, static_cast<DB_C_TIMESTAMP> (dist (rng)));
	append_row (col, &v);
      }
    return col;
  }

  /* content generator for compressible VARCHAR columns (CV_SORT/CV_MERGE/ABBREV_SUBCELL) —
   * pure function of (content_key, seed, entropy) so duplicate-pair rows (CV_MERGE) that share
   * a content_key get byte-identical content regardless of call order.
   *   LOW  = CHR(65 + content_key % 26) + LPAD filler         (parity/filter fixtures)
   *   HIGH = 8 pseudo-random leading chars + LPAD filler       (D-G5 SORT-cell default)
   * The filler tail is a repeated byte in both modes — that redundancy is what drives the
   * >=2:1 LZ4 compression ratio; only the differentiator (leading byte(s)) varies per row.
   *
   * [HIGH-2, architect finding] LOW's single-byte differentiator only has 26 distinct values
   * (`content_key % 26`) — fine for its original parity/filter use (row COUNT matters, not
   * content cardinality), but degenerate for any caller that needs more than 26 distinct
   * *content* values to survive intact (e.g. CV_MERGE's 50%-duplicate-pair distinct-value
   * cardinality, which main.cpp's entropy_for_cell() now routes to HIGH specifically to avoid
   * this). Do not reuse LOW for a high-content-cardinality caller without re-checking this. */
  void
  fill_compressible_content (char *buf, int len, entropy e, std::uint64_t content_key, std::uint32_t seed)
  {
    static const char printable[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    constexpr std::size_t printable_n = sizeof (printable) - 1;

    if (e == entropy::LOW)
      {
	buf[0] = static_cast<char> ('A' + (content_key % 26));
	for (int i = 1; i < len; i++)
	  {
	    buf[i] = 'x';
	  }
      }
    else
      {
	std::mt19937 local_rng (seed ^ static_cast<std::uint32_t> (content_key * 2654435761ULL));
	int prefix = len < 8 ? len : 8;
	for (int i = 0; i < prefix; i++)
	  {
	    buf[i] = printable[local_rng () % printable_n];
	  }
	for (int i = prefix; i < len; i++)
	  {
	    buf[i] = 'x';
	  }
      }
  }

  serialized_column
  make_compressible_varchar_column (std::size_t rows, int len, entropy e, bool duplicate_pairs,
				     std::uint32_t seed)
  {
    serialized_column col;
    col.dbtype = static_cast<int> (DB_TYPE_VARCHAR);
    col.precision = len;
    col.vals.reserve (rows);
    col.lengths.reserve (rows);
    std::vector<char> content (static_cast<std::size_t> (len));
    for (std::size_t i = 0; i < rows; i++)
      {
	std::uint64_t content_key = duplicate_pairs ? (static_cast<std::uint64_t> (i) / 2)
						     : static_cast<std::uint64_t> (i);
	fill_compressible_content (content.data (), len, e, content_key, seed);
	DB_VALUE v;
	db_make_varchar (&v, len, content.data (), len, INTL_CODESET_ISO88591, LANG_COLL_ISO_BINARY);
	append_row (col, &v);
      }
    return col;
  }

  /* uncompressed VARCHAR(100) — always below OR_MINIMUM_STRING_LENGTH_FOR_COMPRESSION (255), so
   * content need not avoid redundancy; fully pseudo-random for a realistic peek/filter shape. */
  serialized_column
  make_uncompressed_varchar_column (std::size_t rows, std::uint32_t seed)
  {
    serialized_column col;
    col.dbtype = static_cast<int> (DB_TYPE_VARCHAR);
    col.precision = UV_LEN;
    col.vals.reserve (rows);
    col.lengths.reserve (rows);
    static const char printable[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    constexpr std::size_t printable_n = sizeof (printable) - 1;
    std::mt19937 rng (seed);
    std::vector<char> content (UV_LEN);
    for (std::size_t i = 0; i < rows; i++)
      {
	for (int j = 0; j < UV_LEN; j++)
	  {
	    content[j] = printable[rng () % printable_n];
	  }
	DB_VALUE v;
	db_make_varchar (&v, UV_LEN, content.data (), UV_LEN, INTL_CODESET_ISO88591, LANG_COLL_ISO_BINARY);
	append_row (col, &v);
      }
    return col;
  }

  serialized_column
  make_numeric_column (std::size_t rows, std::uint32_t seed)
  {
    serialized_column col;
    col.dbtype = static_cast<int> (DB_TYPE_NUMERIC);
    col.precision = NUM_PRECISION;
    col.vals.reserve (rows);
    col.lengths.reserve (rows);
    std::mt19937_64 rng (seed);
    /* 13 integer digits + 2 fractional digits <= NUMERIC(15,2)'s declared precision. */
    std::uniform_int_distribution<std::int64_t> int_part_dist (-9'999'999'999'999LL, 9'999'999'999'999LL);
    std::uniform_int_distribution<int> cents_dist (0, 99);
    for (std::size_t i = 0; i < rows; i++)
      {
	std::int64_t int_part = int_part_dist (rng);
	int cents = cents_dist (rng);
	char str[40];
	std::snprintf (str, sizeof (str), "%lld.%02d", static_cast<long long> (int_part), cents);
	DB_VALUE v;
	if (numeric_coerce_string_to_num (str, static_cast<int> (std::strlen (str)), INTL_CODESET_ISO88591, &v)
	    != NO_ERROR)
	  {
	    throw std::runtime_error ("vhb::build_fixture: numeric_coerce_string_to_num failed");
	  }
	append_row (col, &v);
      }
    return col;
  }

  /* [LOW hygiene, architect finding] the compressed-varchar fixture must actually compress
   * >=2:1 — assert it rather than hope, for EVERY entropy mode this harness produces (LOW and
   * HIGH), not just LOW. HIGH-entropy sort fixtures still legitimately compress somewhat less
   * than LOW (D-G5's random prefix consumes 8 of CV_LEN(300) bytes instead of 1), but the
   * redundant 'x'-filled tail is still >=97% of the row, so the same 2:1 floor holds
   * comfortably for both — verified empirically; no filler-redundancy adjustment was needed. */
  void
  assert_cv_compression_ratio (const serialized_column &col, entropy e, int raw_len)
  {
    if (col.lengths.empty ())
      {
	return;
      }
    double total = 0.0;
    for (int len : col.lengths)
      {
	total += static_cast<double> (len);
      }
    double avg = total / static_cast<double> (col.lengths.size ());
    double ratio = static_cast<double> (raw_len) / avg;
    if (ratio < CV_MIN_COMPRESSION_RATIO)
      {
	throw std::runtime_error (
	    std::string ("vhb::build_fixture: CV fixture failed >=2:1 compression ratio assertion (entropy=")
	    + (e == entropy::LOW ? "LOW" : "HIGH") + ", raw=" + std::to_string (raw_len)
	    + "B, avg serialized=" + std::to_string (avg) + "B, ratio=" + std::to_string (ratio) + ")");
      }
  }
} // namespace

fixture
build_fixture (cell_id c, entropy e, std::size_t row_override)
{
  fixture f;
  f.sort_entropy = e;
  f.row_count = row_override != 0 ? row_override : matrix_row_count (c);
  const std::uint32_t seed = f.seed;
  const std::size_t rows = f.row_count;

  switch (c)
    {
    case cell_id::FL_FILTER:
    case cell_id::FL_SORT:
      f.cols.push_back (make_int_column (rows, seed + 1));
      f.cols.push_back (make_bigint_column (rows, seed + 2));
      f.cols.push_back (make_double_column (rows, seed + 3));
      f.cols.push_back (make_date_column (rows, seed + 4));
      f.cols.push_back (make_timestamp_column (rows, seed + 5));
      break;

    case cell_id::CV_SORT:
    case cell_id::ABBREV_SUBCELL:
      f.cols.push_back (make_compressible_varchar_column (rows, CV_LEN, e, /* duplicate_pairs */ false,
							    seed + 10));
      assert_cv_compression_ratio (f.cols.back (), e, CV_LEN);
      break;

    case cell_id::CV_MERGE:
      f.cols.push_back (make_compressible_varchar_column (rows, CV_LEN, e, /* duplicate_pairs */ true,
							    seed + 11));
      assert_cv_compression_ratio (f.cols.back (), e, CV_LEN);
      break;

    case cell_id::UV_PEEK:
      f.cols.push_back (make_uncompressed_varchar_column (rows, seed + 20));
      break;

    case cell_id::NUM_SORT:
    case cell_id::NUM_AGG_INPUT:
      f.cols.push_back (make_numeric_column (rows, seed + 30));
      break;

    case cell_id::PEEK_VS_COPY:
      f.cols.push_back (make_int_column (rows, seed + 40));
      f.cols.push_back (make_uncompressed_varchar_column (rows, seed + 41));
      break;

    case cell_id::CELL_COUNT:
    default:
      throw std::runtime_error (std::string ("vhb::build_fixture: unhandled cell ") + cell_name (c));
    }

  return f;
}
} // namespace vhb
