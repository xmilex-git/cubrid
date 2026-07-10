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
 * variant_cmpdisk.cpp — "B-cmpdisk": today's-mechanism baseline variant (value_handle_bench.hpp
 * contract). Owned by 30-P2-Harness.
 *
 * B operates directly on the fixture's serialized disk images via pr_type dispatch — no
 * ValueSlot, no per-value handle, no DB_VALUE materialization except where the *baseline itself*
 * already materializes one (NUM_AGG_INPUT's / PEEK_VS_COPY's readval). This is the literal
 * mechanism the P1 design docs measure against:
 *   - filter/UV_PEEK: range predicate via pr_type::data_cmpdisk against a bound value taken
 *     directly from the fixture's own row (row_count/3) — object_primitive.h f_data_cmpdisk
 *     generic dispatch (p1.3-proposals.md (ii) §(a)).
 *   - FL_SORT/CV_SORT/NUM_SORT/ABBREV_SUBCELL/CV_MERGE: disk-direct sort/merge comparator via
 *     data_cmpdisk, the same dispatch list_file.c:4394/4443/4612-4643 wires as
 *     get_data_cmpdisk_function() — for CV_SORT this is mr_cmpdisk_char_type_common(),
 *     object_primitive.c:11897-11960, which decompresses BOTH operands on EVERY comparison —
 *     that per-compare malloc+decompress IS the baseline p1.1-valueslot-design.md §s.2.1's
 *     detoast-once mechanism is measured against, so it is deliberately left untouched here.
 *   - NUM_AGG_INPUT: data_readval into a DB_VALUE per row + numeric_db_value_add accumulation
 *     (today's aggregate-input path; mr_data_readval_numeric fires once per row here, vs. the
 *     double-readval-per-comparison NUM_SORT baseline pays during an O(N log N) sort).
 *   - PEEK_VS_COPY: heap_attrvalue_read's literal chain — heap_attrvalue_point_{fixed,variable}
 *     (peek: locate raw bytes) ALWAYS followed by heap_attrvalue_transform_to_dbvalue (copy:
 *     data_readval into a materialized DB_VALUE), modeled here as one data_readval() call per
 *     attribute access — real byte-copy work performed and counted, never a bare counter++ — for
 *     BOTH columns and BOTH the k=2 within-row references A-slot/A-handle model for this cell
 *     (p0.3-peek-copy-profile.md §2; see run_peek_vs_copy() below).
 *   - ABBREV_SUBCELL: per the P2 acceptance criteria, B's contribution is the full-compare
 *     baseline side only — mechanically identical to CV_SORT (the abbreviated 8B-prefix-proxy
 *     "upside" is a campaign-scope/D-G5 measurement target this baseline variant never claims).
 *
 * Digest convention — RECONCILED with variant_valueslot.cpp / variant_pervalue.cpp (the
 * reference pair, which commit to matching each other "bit for bit"; this file's predicates and
 * digest formula are re-derived from their code directly, superseding an earlier IRC broadcast
 * that assumed a different seed/predicate scheme before those two files were read):
 *   - seed = f.seed (fixture.seed), not a fixed FNV offset basis.
 *   - one fnv1a() call per cell over a single concatenated byte buffer built from the OUTPUT
 *     sequence — not a chained call per output row.
 *   - FL_FILTER/FL_SORT: buffer = native-endian 8B per deformed FL cell (append_u64_native
 *     equivalent), 5 columns per row, in fixture column order.
 *   - CV_SORT/CV_MERGE/ABBREV_SUBCELL/NUM_SORT: buffer = each surviving/ordered row's ORIGINAL
 *     fixture-serialized bytes (fixture.cols[0].vals[row]), in output order.
 *   - UV_PEEK/PEEK_VS_COPY: buffer = the row's PEEKED CONTENT bytes (header-stripped, via
 *     or_get_varchar_compression_lengths) for the VARCHAR column, native-u64 for the INT column.
 *   - NUM_AGG_INPUT: buffer = the raw 8 bytes of the accumulated int64 unscaled-value proxy
 *     (exact/lossless for NUMERIC(15,2), p<=18 per p1.1-valueslot-design.md §s.3), read directly
 *     out of THIS LOOP's own timed accumulator DB_VALUE (`acc`'s DB_NUMERIC magnitude buffer) —
 *     never an untimed, independent re-derivation from the fixture's raw bytes. Mathematically
 *     equal to the naive per-row proxy sum (numeric_db_value_add is exact decimal arithmetic, no
 *     precision loss within int64 range), so the digest still matches variant_valueslot.cpp /
 *     variant_pervalue.cpp bit for bit.
 *   - bound row for every filter/peek predicate = the fixture's OWN row at index row_count/3
 *     (or row 0 if row_count<3) — never a synthetic constant.
 * The *predicate outcome* and *sort order* this file computes via pr_type::data_cmpdisk are
 * mathematically equivalent to variant_valueslot.cpp's native/decoded comparisons (both compute
 * the same total order over the same domain), so digests match despite the different mechanism
 * under test — exactly the point of the P2.4 cross-variant parity check.
 */

#include "value_handle_bench.hpp"

#include "dbtype.h"
#include "error_code.h"
#include "numeric_opfunc.h"
#include "object_domain.h"
#include "object_primitive.h"
#include "object_representation.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <vector>

namespace vhb
{
namespace
{
  constexpr int NUM_SCALE = 2; /* NUMERIC(15,2) — matches fixture.cpp's NUM_SCALE */

  enum class fl_family : std::uint8_t { I32, I64, F64 };

  fl_family
  fl_family_of (int dbtype)
  {
    switch (static_cast<DB_TYPE> (dbtype))
      {
      case DB_TYPE_BIGINT:
	return fl_family::I64;
      case DB_TYPE_DOUBLE:
	return fl_family::F64;
      default:
	return fl_family::I32; /* INTEGER/DATE/TIMESTAMP: 4B big-endian disk image */
      }
  }

  /* deform a raw FL_INLINE disk image into a native-endian 8B cell — used ONLY to build the
   * digest buffer in the exact byte shape variant_valueslot.cpp's append_u64_native() produces;
   * the actual filter/sort DECISIONS below go through pr_type::data_cmpdisk, not this function. */
  std::uint64_t
  deform_fl_inline (int dbtype, const char *raw)
  {
    std::uint64_t cell = 0;
    switch (fl_family_of (dbtype))
      {
      case fl_family::I64:
	{
	  DB_BIGINT v;
	  OR_GET_BIGINT (raw, &v);
	  std::memcpy (&cell, &v, sizeof (v));
	  break;
	}
      case fl_family::F64:
	{
	  double v;
	  OR_GET_DOUBLE (raw, &v);
	  std::memcpy (&cell, &v, sizeof (v));
	  break;
	}
      case fl_family::I32:
      default:
	{
	  int v = OR_GET_INT (raw);
	  cell = static_cast<std::uint64_t> (static_cast<std::int64_t> (v));
	  break;
	}
      }
    return cell;
  }

  void
  append_u64_native (std::vector<char> &out, std::uint64_t v)
  {
    out.insert (out.end (), reinterpret_cast<const char *> (&v), reinterpret_cast<const char *> (&v) + sizeof (v));
  }

  void
  append_bytes (std::vector<char> &out, const char *p, std::size_t n)
  {
    out.insert (out.end (), p, p + n);
  }

  struct varchar_view
  {
    const char *data = nullptr;
    int len = 0;
  };

  /* header-only peek: never decompresses. For UV_PEEK/PEEK_VS_COPY's always-uncompressed VARCHAR
   * column this already IS the plain content (matches variant_valueslot.cpp's peek_varchar()). */
  varchar_view
  peek_varchar (const char *raw, int raw_len)
  {
    OR_BUF buf;
    or_init (&buf, const_cast<char *> (raw), raw_len);
    int compressed_size = 0, decompressed_size = 0;
    if (or_get_varchar_compression_lengths (&buf, &compressed_size, &decompressed_size) != NO_ERROR)
      {
	return {};
      }
    varchar_view v;
    v.data = buf.ptr;
    v.len = compressed_size > 0 ? compressed_size : decompressed_size;
    return v;
  }

  /* int64 unscaled-value proxy from an in-memory NUMERIC DB_VALUE's own DB_NUMERIC magnitude
   * buffer (db_get_numeric(): DB_NUMERIC_BUF_SIZE(17) bytes, big-endian, NO on-disk header — the
   * sign lives in DB_VALUE_NUMERIC_IS_VALUE_NEGATIVE(), not in this buffer). Exact for
   * precision<=18 (p1.1-valueslot-design.md §s.3), the same low-8-bytes construction
   * variant_valueslot.cpp's decode_numeric_once() uses on the on-disk byte shape, so
   * NUM_AGG_INPUT's digest matches bit for bit despite the different source buffer shape. */
  std::int64_t
  numeric_int64_proxy_of (const DB_VALUE &v)
  {
    const unsigned char *mag = reinterpret_cast<const unsigned char *> (db_get_numeric (&v));
    std::uint64_t umag = 0;
    for (int i = DB_NUMERIC_BUF_SIZE - 8; i < DB_NUMERIC_BUF_SIZE; i++)
      {
	umag = (umag << 8) | mag[i];
      }
    return DB_VALUE_NUMERIC_IS_VALUE_NEGATIVE (&v) ? -static_cast<std::int64_t> (umag)
						    : static_cast<std::int64_t> (umag);
  }

  TP_DOMAIN *
  resolve_domain (int dbtype, int precision)
  {
    DB_TYPE t = static_cast<DB_TYPE> (dbtype);
    switch (t)
      {
      case DB_TYPE_VARCHAR:
	return tp_domain_construct (DB_TYPE_VARCHAR, NULL, precision, 0, NULL);
      case DB_TYPE_NUMERIC:
	return tp_domain_construct (DB_TYPE_NUMERIC, NULL, precision, NUM_SCALE, NULL);
      default:
	return tp_domain_resolve_default (t);
      }
  }

  inline char *
  mutable_ptr (const std::vector<char> &v)
  {
    return const_cast<char *> (v.data ());
  }

  /* canonical bound row every filter/peek predicate uses — the fixture's OWN data, never a
   * synthetic constant (matches variant_valueslot.cpp's bound_row_). */
  std::size_t
  bound_row_of (std::size_t row_count)
  {
    return row_count >= 3 ? row_count / 3 : 0;
  }

  /* disk-direct stable sort of one column via data_cmpdisk (the mechanism under test); returns
   * the sorted row-index permutation. */
  std::vector<std::size_t>
  sort_column (const serialized_column &col, TP_DOMAIN *dom)
  {
    std::vector<std::size_t> order (col.vals.size ());
    for (std::size_t i = 0; i < order.size (); i++)
      {
	order[i] = i;
      }
    std::stable_sort (order.begin (), order.end (), [&] (std::size_t a, std::size_t b)
      {
	return dom->type->data_cmpdisk (col.vals[a].data (), col.vals[b].data (), dom, 0, 1, nullptr) < 0;
      });
    return order;
  }

  class variant_b_cmpdisk : public variant
  {
  public:
    const char *
    name () const override
    {
      return "B-cmpdisk";
    }

    bool
    prepare (cell_id /* c */, const fixture &f) override
    {
      /* [L1 fix, mirrors variant_flatbuffers.cpp's prepare_numeric_passthrough()] prepare() lands
       * here once per cell across one process's run (col_domains_ persists on the variant
       * instance across cells) — free the PREVIOUS cell's tp_domain_construct()-owned domains
       * (VARCHAR/NUMERIC) before rebuilding, or every VARCHAR/NUMERIC cell after the first leaks
       * one uncached TP_DOMAIN per column. Domains from tp_domain_resolve_default() are shared
       * cache entries and must never be freed here. */
      for (std::size_t i = 0; i < col_domains_.size (); i++)
	{
	  if (i < col_domains_owned_.size () && col_domains_owned_[i] && col_domains_[i] != nullptr)
	    {
	      tp_domain_free (col_domains_[i]);
	    }
	}

      col_domains_.assign (f.cols.size (), nullptr);
      col_domains_owned_.assign (f.cols.size (), false);
      for (std::size_t i = 0; i < f.cols.size (); i++)
	{
	  col_domains_[i] = resolve_domain (f.cols[i].dbtype, f.cols[i].precision);
	  DB_TYPE t = static_cast<DB_TYPE> (f.cols[i].dbtype);
	  col_domains_owned_[i] = (t == DB_TYPE_VARCHAR || t == DB_TYPE_NUMERIC);
	}
      return true; /* B-cmpdisk implements every cell in the matrix */
    }

    cell_result run_cell (cell_id c, const fixture &f) override;

  private:
    std::vector<TP_DOMAIN *> col_domains_;
    std::vector<bool> col_domains_owned_; /* [L1 fix] parallel to col_domains_: true iff tp_domain_construct()-owned */

    cell_result run_fl_filter (const fixture &f);
    cell_result run_fl_sort (const fixture &f);
    cell_result run_cv_sort_like (const fixture &f); /* CV_SORT + ABBREV_SUBCELL */
    cell_result run_cv_merge (const fixture &f);
    cell_result run_uv_peek (const fixture &f);
    cell_result run_num_sort (const fixture &f);
    cell_result run_num_agg_input (const fixture &f);
    cell_result run_peek_vs_copy (const fixture &f);
  };

  /* ---- FL_FILTER asymmetry [LOW, architect finding] ---- B short-circuits the per-row column
   * loop on the first failing column (`break`, below) — matches a real disk-direct predicate
   * evaluator's ability to stop dispatching data_cmpdisk once the composite AND is already false.
   * variant_valueslot.cpp / variant_pervalue.cpp's A-side reference deforms all 5 FL columns
   * unconditionally every row (no `break`), because ValueSlot's/the rejected handle's per-row
   * deform is a single up-front step feeding every subsequent access, not a column-by-column
   * short-circuiting predicate. This is a deliberate, conservative asymmetry: A pays for building
   * the full row unconditionally; B/C only ever pay for the columns actually needed to decide
   * survival. It gives B/C a small structural head start on rows that fail on an early column and
   * is left as-is (no behavior change) — documented here and at variant_flatbuffers.cpp's
   * run_fl_filter() so it isn't mistaken for an oversight. */
  cell_result
  variant_b_cmpdisk::run_fl_filter (const fixture &f)
  {
    std::size_t bound_row = bound_row_of (f.row_count);
    std::vector<char> digest_bytes;
    auto t0 = std::chrono::steady_clock::now ();

    for (std::size_t r = 0; r < f.row_count; r++)
      {
	bool survives = true;
	for (std::size_t ci = 0; ci < f.cols.size (); ci++)
	  {
	    const serialized_column &col = f.cols[ci];
	    TP_DOMAIN *dom = col_domains_[ci];
	    if (dom->type->data_cmpdisk (col.vals[r].data (), col.vals[bound_row].data (), dom, 0, 1, nullptr)
		> DB_EQ)
	      {
		survives = false;
		break;
	      }
	  }
	if (survives)
	  {
	    for (std::size_t ci = 0; ci < f.cols.size (); ci++)
	      {
		append_u64_native (digest_bytes, deform_fl_inline (f.cols[ci].dbtype, f.cols[ci].vals[r].data ()));
	      }
	  }
      }

    auto t1 = std::chrono::steady_clock::now ();
    cell_result res;
    res.elapsed_us =
      static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
    return res;
  }

  cell_result
  variant_b_cmpdisk::run_fl_sort (const fixture &f)
  {
    std::vector<std::size_t> order (f.row_count);
    for (std::size_t i = 0; i < order.size (); i++)
      {
	order[i] = i;
      }

    auto t0 = std::chrono::steady_clock::now ();

    std::stable_sort (order.begin (), order.end (), [this, &f] (std::size_t a, std::size_t b)
      {
	for (std::size_t ci = 0; ci < f.cols.size (); ci++)
	  {
	    TP_DOMAIN *dom = col_domains_[ci];
	    int c = dom->type->data_cmpdisk (f.cols[ci].vals[a].data (), f.cols[ci].vals[b].data (), dom, 0, 1,
					      nullptr);
	    if (c != 0)
	      {
		return c < 0;
	      }
	  }
	return false;
      });

    auto t1 = std::chrono::steady_clock::now ();

    std::vector<char> digest_bytes;
    digest_bytes.reserve (f.row_count * f.cols.size () * 8);
    for (std::size_t r : order)
      {
	for (std::size_t ci = 0; ci < f.cols.size (); ci++)
	  {
	    append_u64_native (digest_bytes, deform_fl_inline (f.cols[ci].dbtype, f.cols[ci].vals[r].data ()));
	  }
      }

    cell_result res;
    res.elapsed_us =
      static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
    return res;
  }

  cell_result
  variant_b_cmpdisk::run_cv_sort_like (const fixture &f)
  {
    const serialized_column &col = f.cols[0];
    TP_DOMAIN *dom = col_domains_[0];

    auto t0 = std::chrono::steady_clock::now ();
    std::vector<std::size_t> order = sort_column (col, dom);
    auto t1 = std::chrono::steady_clock::now ();

    std::vector<char> digest_bytes;
    for (std::size_t r : order)
      {
	append_bytes (digest_bytes, col.vals[r].data (), col.vals[r].size ());
      }

    cell_result res;
    res.elapsed_us =
      static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
    return res;
  }

  cell_result
  variant_b_cmpdisk::run_cv_merge (const fixture &f)
  {
    const serialized_column &col = f.cols[0];
    TP_DOMAIN *dom = col_domains_[0];

    auto t0 = std::chrono::steady_clock::now ();

    std::vector<std::size_t> order = sort_column (col, dom);

    std::vector<char> digest_bytes;
    std::uint64_t distinct = 0;
    for (std::size_t i = 0; i < order.size (); i++)
      {
	bool is_dup = i > 0
	  && dom->type->data_cmpdisk (col.vals[order[i]].data (), col.vals[order[i - 1]].data (), dom, 0, 1, nullptr)
	  == DB_EQ;
	if (!is_dup)
	  {
	    append_bytes (digest_bytes, col.vals[order[i]].data (), col.vals[order[i]].size ());
	    distinct++;
	  }
      }

    auto t1 = std::chrono::steady_clock::now ();

    cell_result res;
    res.elapsed_us =
      static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
    res.aux_counter_a = distinct;
    return res;
  }

  cell_result
  variant_b_cmpdisk::run_uv_peek (const fixture &f)
  {
    const serialized_column &col = f.cols[0];
    TP_DOMAIN *dom = col_domains_[0];
    std::size_t bound_row = bound_row_of (f.row_count);
    const char *bound_bytes = col.vals[bound_row].data ();

    std::vector<char> digest_bytes;
    std::uint64_t survivors = 0;
    auto t0 = std::chrono::steady_clock::now ();

    for (std::size_t r = 0; r < col.vals.size (); r++)
      {
	if (dom->type->data_cmpdisk (col.vals[r].data (), bound_bytes, dom, 0, 1, nullptr) <= DB_EQ)
	  {
	    varchar_view v = peek_varchar (col.vals[r].data (), col.lengths[r]);
	    append_bytes (digest_bytes, v.data, static_cast<std::size_t> (v.len));
	    survivors++;
	  }
      }

    auto t1 = std::chrono::steady_clock::now ();
    cell_result res;
    res.elapsed_us =
      static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
    res.aux_counter_a = survivors;
    return res;
  }

  cell_result
  variant_b_cmpdisk::run_num_sort (const fixture &f)
  {
    const serialized_column &col = f.cols[0];
    TP_DOMAIN *dom = col_domains_[0];

    auto t0 = std::chrono::steady_clock::now ();
    std::vector<std::size_t> order = sort_column (col, dom);
    auto t1 = std::chrono::steady_clock::now ();

    std::vector<char> digest_bytes;
    for (std::size_t r : order)
      {
	append_bytes (digest_bytes, col.vals[r].data (), col.vals[r].size ());
      }

    cell_result res;
    res.elapsed_us =
      static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
    return res;
  }

  cell_result
  variant_b_cmpdisk::run_num_agg_input (const fixture &f)
  {
    const serialized_column &col = f.cols[0];
    TP_DOMAIN *dom = col_domains_[0];

    DB_VALUE acc;
    if (numeric_coerce_string_to_num ("0.00", 4, INTL_CODESET_ISO88591, &acc) != NO_ERROR)
      {
	throw std::runtime_error ("variant_b_cmpdisk: NUM_AGG_INPUT accumulator init failed");
      }

    auto t0 = std::chrono::steady_clock::now ();
    for (std::size_t r = 0; r < col.vals.size (); r++)
      {
	DB_VALUE rowval;
	OR_BUF buf;
	or_init (&buf, mutable_ptr (col.vals[r]), col.lengths[r]);
	/* readval into a DB_VALUE per row + accumulate — today's aggregate-input path
	 * (mr_data_readval_numeric fires once per row here, not once per compare). */
	if (dom->type->data_readval (&buf, &rowval, dom, col.lengths[r], true, nullptr, 0) != NO_ERROR)
	  {
	    throw std::runtime_error ("variant_b_cmpdisk: NUM_AGG_INPUT readval failed");
	  }
	DB_VALUE new_acc;
	if (numeric_db_value_add (&acc, &rowval, &new_acc) != NO_ERROR)
	  {
	    throw std::runtime_error ("variant_b_cmpdisk: NUM_AGG_INPUT accumulate failed");
	  }
	acc = new_acc;
      }
    auto t1 = std::chrono::steady_clock::now ();

    /* [MED-1 fix] digest derives from THIS LOOP's own timed accumulator (`acc`, a real DB_VALUE
     * built via data_readval + numeric_db_value_add) — never an untimed, independent re-derivation
     * from the fixture's raw bytes. See numeric_int64_proxy_of()'s header comment for why this
     * still matches variant_valueslot.cpp / variant_pervalue.cpp bit for bit. */
    std::int64_t proxy_sum = numeric_int64_proxy_of (acc);

    cell_result res;
    res.elapsed_us =
      static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (&proxy_sum, sizeof (proxy_sum), f.seed);
    res.aux_counter_a = col.vals.size ();
    return res;
  }

  /* ---- PEEK_VS_COPY [HIGH-1 fix] ---- Models heap_attrvalue_read's literal, unconditional
   * peek->copy chain (p0.3-peek-copy-profile.md §2): heap_attrvalue_point_{fixed,variable} (peek:
   * locate the raw bytes) is ALWAYS followed by heap_attrvalue_transform_to_dbvalue (copy: readval
   * the located bytes into a freshly materialized DB_VALUE — real work, performed via
   * data_readval(), never a bare counter++), for EVERY attribute access, independent of row
   * survival. Per the k=2 within-row reference both A-slot and A-handle model for this cell (one
   * filter-evaluation access, one simulated second/projection access), B — the legacy baseline
   * that has no slot to skip the re-crossing — pays the full chain again for BOTH columns on the
   * second reference too: 2 cols x 2 refs x (peek+copy) = 4 peeks + 4 copies per row, every row,
   * matching A-handle's counter shape exactly (contrast: A-slot's ValueSlot deforms once and the
   * second reference is a free array read, 2 peeks + 1 copy per row — the whole point of the
   * cell). The filter predicate itself still goes through B's disk-direct data_cmpdisk mechanism
   * (mirrors run_fl_filter/run_uv_peek), evaluated off the first reference's materialized values,
   * exactly as a caller only ever sees heap_attrvalue_read's result after the chain above runs. */
  cell_result
  variant_b_cmpdisk::run_peek_vs_copy (const fixture &f)
  {
    const serialized_column &int_col = f.cols[0];
    const serialized_column &vc_col = f.cols[1];
    TP_DOMAIN *int_dom = col_domains_[0];
    TP_DOMAIN *vc_dom = col_domains_[1];
    std::size_t bound_row = bound_row_of (f.row_count);
    const char *bound_int_bytes = int_col.vals[bound_row].data ();
    /* [L2 fix, pro-A bias] the bound row's varchar header peek is per-CELL work (like A's
     * bound_varchar_ built once in prepare()), not per-row work — hoisted above t0 instead of
     * being recomputed unconditionally on every row inside the timed loop below, which inflated
     * B's measured cost relative to A and exaggerated (B-A)/B in A's favor. */
    varchar_view bound_vc = peek_varchar (vc_col.vals[bound_row].data (), vc_col.lengths[bound_row]);

    std::vector<char> digest_bytes;
    std::uint64_t peeks = 0;
    std::uint64_t copies = 0;
    auto t0 = std::chrono::steady_clock::now ();

    for (std::size_t r = 0; r < int_col.vals.size (); r++)
      {
	DB_VALUE int_val[2];
	DB_VALUE vc_val[2];
	bool int_ok = false, vc_ok = false;

	for (int k = 0; k < 2; k++)
	  {
	    OR_BUF int_buf;
	    or_init (&int_buf, mutable_ptr (int_col.vals[r]), int_col.lengths[r]);
	    peeks++; /* heap_attrvalue_point_fixed: locate the raw fixed-length bytes */
	    if (int_dom->type->data_readval (&int_buf, &int_val[k], int_dom, int_col.lengths[r], true, nullptr, 0)
		!= NO_ERROR)
	      {
		throw std::runtime_error ("variant_b_cmpdisk: PEEK_VS_COPY int readval failed");
	      }
	    copies++; /* heap_attrvalue_transform_to_dbvalue: copy, ALWAYS called */

	    OR_BUF vc_buf;
	    or_init (&vc_buf, mutable_ptr (vc_col.vals[r]), vc_col.lengths[r]);
	    peeks++; /* heap_attrvalue_point_variable: locate the raw varchar bytes */
	    if (vc_dom->type->data_readval (&vc_buf, &vc_val[k], vc_dom, vc_col.lengths[r], true, nullptr, 0)
		!= NO_ERROR)
	      {
		throw std::runtime_error ("variant_b_cmpdisk: PEEK_VS_COPY varchar readval failed");
	      }
	    copies++;

	    if (k == 0)
	      {
		int_ok = int_dom->type->data_cmpdisk (int_col.vals[r].data (), bound_int_bytes, int_dom, 0, 1,
						       nullptr) <= DB_EQ;
		const char *vc_data = db_get_string (&vc_val[0]);
		int vc_len = db_get_string_size (&vc_val[0]);
		int n = std::min (vc_len, bound_vc.len);
		int c = n ? std::memcmp (vc_data, bound_vc.data, static_cast<std::size_t> (n)) : 0;
		if (c == 0)
		  {
		    c = (vc_len < bound_vc.len) ? -1 : (vc_len > bound_vc.len) ? 1 : 0;
		  }
		vc_ok = c <= 0;
	      }
	  }

	if (int_ok && vc_ok)
	  {
	    append_u64_native (digest_bytes, deform_fl_inline (int_col.dbtype, int_col.vals[r].data ()));
	    append_bytes (digest_bytes, db_get_string (&vc_val[1]),
			  static_cast<std::size_t> (db_get_string_size (&vc_val[1])));
	  }

	pr_clear_value (&int_val[0]);
	pr_clear_value (&int_val[1]);
	pr_clear_value (&vc_val[0]);
	pr_clear_value (&vc_val[1]);
      }

    auto t1 = std::chrono::steady_clock::now ();
    cell_result res;
    res.elapsed_us =
      static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
    res.aux_counter_a = peeks;
    res.aux_counter_b = copies;
    return res;
  }

  cell_result
  variant_b_cmpdisk::run_cell (cell_id c, const fixture &f)
  {
    switch (c)
      {
      case cell_id::FL_FILTER:
	return run_fl_filter (f);
      case cell_id::FL_SORT:
	return run_fl_sort (f);
      case cell_id::CV_SORT:
      case cell_id::ABBREV_SUBCELL:
	return run_cv_sort_like (f);
      case cell_id::CV_MERGE:
	return run_cv_merge (f);
      case cell_id::UV_PEEK:
	return run_uv_peek (f);
      case cell_id::NUM_SORT:
	return run_num_sort (f);
      case cell_id::NUM_AGG_INPUT:
	return run_num_agg_input (f);
      case cell_id::PEEK_VS_COPY:
	return run_peek_vs_copy (f);
      case cell_id::CELL_COUNT:
      default:
	throw std::runtime_error (std::string ("variant_b_cmpdisk: unhandled cell ") + cell_name (c));
      }
  }
} // namespace

variant *
make_variant_b_cmpdisk ()
{
  return new variant_b_cmpdisk ();
}
} // namespace vhb
