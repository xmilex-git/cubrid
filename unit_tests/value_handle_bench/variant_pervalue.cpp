/*
 * variant_pervalue.cpp — "A-handle": D-G1 re-verification contrast.
 *
 * grilled_plan.md D-G1 rejects ralplan.md P1.1(a)'s original per-value `ValueHandle`
 * ({kind, type, len, <=8B inline-or-ptr, scan_generation_snapshot}, ~24B/value, self-describing,
 * docs/value-handle/p1/p1.1-valueslot-design.md §a.0) in favor of the per-scan ValueSlot
 * (variant_valueslot.cpp). This variant re-implements the SAME 9 cells through the REJECTED
 * per-value handle shape so the bench can measure what was actually given up:
 *
 *   - every value access constructs a fresh ~24B handle (kind/type/len/generation re-dispatched
 *     per value, not hoisted to a column-position vector the way ValueSlot's att_types/att_class
 *     are, §a.0/§a.1) — paid on EVERY row access, never amortized across a scan.
 *   - pointer-backed payloads (compressed varchar, NUMERIC) are heap-allocated per access (no
 *     shared per-scan scratch, no sort-owned arena) — real malloc/free churn per value, the direct
 *     cost of "no slot reuse."
 *   - VARCHAR access always peeks THEN copies into owned handle storage — a self-describing handle
 *     has no att_class-driven zero-copy path, so it cannot reproduce ValueSlot's VC_RAW win; this
 *     matches P0.3's documented legacy 1:1 peek/copy ratio (p1.1-valueslot-design.md §a.4) rather
 *     than ValueSlot's peek-without-copy divergence.
 *   - PEEK_VS_COPY's within-row repeat reference reconstructs the handle again from scratch (no
 *     array to re-read), the concrete "duplicates hoisted information N times per row" cost §a.0
 *     names as the rejection rationale.
 *
 * Output values/row-order/digest MUST match variant_valueslot.cpp and variant_cmpdisk.cpp bit for
 * bit — only the per-access construction COST differs, never the result.
 *
 * BENCH-ONLY. Owns exactly this file (+ variant_valueslot.cpp) per P2 file-ownership split.
 */

#include "value_handle_bench.hpp"

#include "error_code.h"
#include "dbtype_def.h"
#include "object_representation.h"
#include "object_primitive.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

namespace vhb
{
namespace
{
  /* ---- the REJECTED per-value handle shape, ralplan.md P1.1(a) / design doc §a.0 ---- */
  enum class handle_kind : std::uint8_t { INLINE_I32, INLINE_I64, INLINE_F64, NUMERIC_PTR, VARCHAR_PTR, NONE };

  struct value_handle
  {
    handle_kind kind = handle_kind::NONE;
    std::uint16_t type = 0;			/* DB_TYPE, re-tagged per value (no hoisted att_types) */
    std::uint16_t len = 0;			/* byte length for pointer-backed payloads */
    std::uint32_t scan_generation_snapshot = 0;	/* rejected design's per-value generation field */
    union
    {
      std::uint64_t inline_val;
      const char *ptr;
    } payload{};
  };

  /* ---- NUMERIC decode buffer: heap-allocated per value access, never a shared/reused slot ---- */
  constexpr int k_numeric_header_size = 3;		/* NUMERIC_HEADER_SIZE, object_primitive.c:131 */
  constexpr unsigned char k_numeric_sign_bit = 0x80;	/* NUMERIC_VALUE_SIGN_BIT_MASK */

  struct numeric_decoded
  {
    unsigned char magnitude[DB_NUMERIC_BUF_SIZE] = { 0 };
    bool negative = false;
    bool proxy_valid = false;
    std::int64_t int64_proxy = 0;
  };

  void
  decode_numeric_once (const unsigned char *raw, numeric_decoded &out)
  {
    int disk_size = raw[0] & 0x7F;
    out.negative = (raw[0] & k_numeric_sign_bit) != 0;
    int precision = raw[1] & 0x7F;
    int mag_len = disk_size - k_numeric_header_size;
    std::memset (out.magnitude, 0, sizeof (out.magnitude));
    if (mag_len > 0)
      {
	std::memcpy (out.magnitude + (DB_NUMERIC_BUF_SIZE - mag_len), raw + k_numeric_header_size, mag_len);
      }
    if (precision <= 18)
      {
	std::uint64_t mag = 0;
	for (int i = DB_NUMERIC_BUF_SIZE - 8; i < DB_NUMERIC_BUF_SIZE; ++i)
	  {
	    mag = (mag << 8) | out.magnitude[i];
	  }
	out.int64_proxy = out.negative ? -static_cast<std::int64_t> (mag) : static_cast<std::int64_t> (mag);
	out.proxy_valid = true;
      }
    else
      {
	out.proxy_valid = false;
      }
  }

  int
  numeric_compare_decoded (const numeric_decoded &a, const numeric_decoded &b)
  {
    if (a.proxy_valid && b.proxy_valid)
      {
	return (a.int64_proxy < b.int64_proxy) ? -1 : (a.int64_proxy > b.int64_proxy) ? 1 : 0;
      }
    if (a.negative != b.negative)
      {
	return a.negative ? -1 : 1;
      }
    int c = std::memcmp (a.magnitude, b.magnitude, DB_NUMERIC_BUF_SIZE);
    return a.negative ? -c : c;
  }

  int
  byte_compare (const char *a, int alen, const char *b, int blen)
  {
    int n = std::min (alen, blen);
    int c = n ? std::memcmp (a, b, static_cast<std::size_t> (n)) : 0;
    if (c != 0)
      {
	return c;
      }
    return (alen < blen) ? -1 : (alen > blen) ? 1 : 0;
  }

  void
  append_bytes (std::vector<char> &out, const char *p, std::size_t n)
  {
    out.insert (out.end (), p, p + n);
  }

  void
  append_u64_native (std::vector<char> &out, std::uint64_t v)
  {
    out.insert (out.end (), reinterpret_cast<const char *> (&v), reinterpret_cast<const char *> (&v) + sizeof (v));
  }

  bool
  is_fl_inline_type (int dbtype)
  {
    switch (static_cast<DB_TYPE> (dbtype))
      {
      case DB_TYPE_INTEGER:
      case DB_TYPE_BIGINT:
      case DB_TYPE_DOUBLE:
      case DB_TYPE_DATE:
      case DB_TYPE_TIMESTAMP:
	return true;
      default:
	return false;
      }
  }

  /* ---- per-value handle construction: re-dispatches type EVERY call, no hoisted class vector ----
   * `gen` stands in for the rejected design's scan_generation_snapshot (§a.0 point 3) — carried
   * faithfully in the struct, not used for anything a bench needs to assert. */
  value_handle
  make_handle_fl (int dbtype, const char *raw, std::uint32_t gen)
  {
    value_handle h;
    h.type = static_cast<std::uint16_t> (dbtype);
    h.scan_generation_snapshot = gen;
    /* per-value re-dispatch (D-G1 a.0: "re-dispatches type lookup per value instead of once per
     * column-position") — this switch runs on every single access, unlike ValueSlot's att_class[]
     * lookup which is computed once at scan open. */
    switch (static_cast<DB_TYPE> (dbtype))
      {
      case DB_TYPE_BIGINT:
	{
	  h.kind = handle_kind::INLINE_I64;
	  DB_BIGINT v;
	  OR_GET_BIGINT (raw, &v);
	  std::memcpy (&h.payload.inline_val, &v, sizeof (v));
	  break;
	}
      case DB_TYPE_DOUBLE:
	{
	  h.kind = handle_kind::INLINE_F64;
	  double v;
	  OR_GET_DOUBLE (raw, &v);
	  std::memcpy (&h.payload.inline_val, &v, sizeof (v));
	  break;
	}
      default:
	{
	  h.kind = handle_kind::INLINE_I32;
	  int v = OR_GET_INT (raw);
	  h.payload.inline_val = static_cast<std::uint64_t> (static_cast<std::int64_t> (v));
	  break;
	}
      }
    return h;
  }

  int
  fl_handle_compare (const value_handle &a, const value_handle &b)
  {
    switch (a.kind)
      {
      case handle_kind::INLINE_I64:
	{
	  std::int64_t va, vb;
	  std::memcpy (&va, &a.payload.inline_val, sizeof (va));
	  std::memcpy (&vb, &b.payload.inline_val, sizeof (vb));
	  return (va < vb) ? -1 : (va > vb) ? 1 : 0;
	}
      case handle_kind::INLINE_F64:
	{
	  double va, vb;
	  std::memcpy (&va, &a.payload.inline_val, sizeof (va));
	  std::memcpy (&vb, &b.payload.inline_val, sizeof (vb));
	  return (va < vb) ? -1 : (va > vb) ? 1 : 0;
	}
      default:
	{
	  std::int64_t va = static_cast<std::int64_t> (a.payload.inline_val);
	  std::int64_t vb = static_cast<std::int64_t> (b.payload.inline_val);
	  return (va < vb) ? -1 : (va > vb) ? 1 : 0;
	}
      }
  }

  /* NUMERIC per-value handle: heap-allocates its own decode buffer every access (no sort-owned
   * arena, no per-scan numeric_scratch reuse — the direct "no slot reuse" cost). Ownership returned
   * to the caller, who must keep it alive exactly as long as the handle is in use. */
  std::unique_ptr<numeric_decoded>
  make_handle_numeric (const char *raw, value_handle &h, std::uint32_t gen)
  {
    auto decoded = std::make_unique<numeric_decoded> ();
    decode_numeric_once (reinterpret_cast<const unsigned char *> (raw), *decoded);
    h.kind = handle_kind::NUMERIC_PTR;
    h.type = static_cast<std::uint16_t> (DB_TYPE_NUMERIC);
    h.scan_generation_snapshot = gen;
    h.payload.ptr = reinterpret_cast<const char *> (decoded.get ());
    return decoded;
  }

  /* VARCHAR per-value handle: ALWAYS peeks then copies into freshly heap-allocated owned storage,
   * compressed or not — a self-describing handle has no att_class-driven zero-copy peek path
   * (§a.0's rejection point: it "re-invents a lightweight DB_VALUE"), so every access pays the full
   * peek+copy chain P0.3 documents for the legacy path. `peeks`/`copies` are bumped by exactly one
   * each, faithfully modeling the 1:1 ratio ValueSlot's VC_RAW breaks and this design cannot. */
  std::unique_ptr<std::vector<char>>
  make_handle_varchar (const char *raw, int raw_len, value_handle &h, std::uint32_t gen,
			std::uint64_t &peeks, std::uint64_t &copies)
  {
    OR_BUF buf;
    or_init (&buf, const_cast<char *> (raw), raw_len);
    int compressed_size = 0, decompressed_size = 0;
    or_get_varchar_compression_lengths (&buf, &compressed_size, &decompressed_size);
    ++peeks;

    auto owned = std::make_unique<std::vector<char>> (static_cast<std::size_t> (decompressed_size) + 1);
    if (compressed_size > 0)
      {
	pr_get_compressed_data_from_buffer (&buf, owned->data (), compressed_size, decompressed_size);
      }
    else
      {
	std::memcpy (owned->data (), buf.ptr, static_cast<std::size_t> (decompressed_size));
      }
    ++copies;
    owned->resize (static_cast<std::size_t> (decompressed_size));

    h.kind = handle_kind::VARCHAR_PTR;
    h.type = static_cast<std::uint16_t> (DB_TYPE_VARCHAR);
    h.len = static_cast<std::uint16_t> (decompressed_size);
    h.scan_generation_snapshot = gen;
    h.payload.ptr = owned->data ();
    return owned;
  }

  /* ==================================================================================== */

  class variant_a_handle final : public variant
  {
  public:
    const char *
    name () const override
    {
      return "A-handle";
    }

    bool prepare (cell_id c, const fixture &f) override;
    cell_result run_cell (cell_id c, const fixture &f) override;

  private:
    int natts_ = 0;
    std::vector<int> dbtype_;
    int bound_row_ = 0;
    std::vector<std::uint64_t> bound_fl_;
    std::vector<char> bound_varchar_;
    std::uint32_t generation_ = 0;	/* rejected design's per-scan-snapshot counter, bumped once per prepare() */

    cell_result run_fl_filter (const fixture &f);
    cell_result run_fl_sort (const fixture &f);
    cell_result run_cv_sort (const fixture &f);
    cell_result run_cv_merge (const fixture &f);
    cell_result run_uv_peek (const fixture &f);
    cell_result run_num_sort (const fixture &f);
    cell_result run_num_agg_input (const fixture &f);
    cell_result run_peek_vs_copy (const fixture &f);
    cell_result run_abbrev_subcell (const fixture &f);
  };

  bool
  variant_a_handle::prepare (cell_id c, const fixture &f)
  {
    natts_ = static_cast<int> (f.cols.size ());
    dbtype_.resize (natts_);
    for (int i = 0; i < natts_; ++i)
      {
	dbtype_[i] = f.cols[i].dbtype;
      }

    bound_row_ = (f.row_count >= 3) ? static_cast<int> (f.row_count / 3) : 0;
    bound_fl_.assign (natts_, 0);
    bound_varchar_.clear ();
    ++generation_;

    switch (c)
      {
      case cell_id::FL_FILTER:
      case cell_id::PEEK_VS_COPY:
	for (int i = 0; i < natts_; ++i)
	  {
	    const serialized_column &col = f.cols[i];
	    if (is_fl_inline_type (dbtype_[i]))
	      {
		value_handle h = make_handle_fl (dbtype_[i], col.vals[bound_row_].data (), generation_);
		bound_fl_[i] = h.payload.inline_val;
	      }
	    else
	      {
		value_handle h;
		std::uint64_t dummy_a = 0, dummy_b = 0;
		auto owned = make_handle_varchar (col.vals[bound_row_].data (), col.lengths[bound_row_], h,
						   generation_, dummy_a, dummy_b);
		bound_varchar_.assign (owned->begin (), owned->end ());
	      }
	  }
	break;
      case cell_id::UV_PEEK:
	{
	  value_handle h;
	  std::uint64_t dummy_a = 0, dummy_b = 0;
	  auto owned = make_handle_varchar (f.cols[0].vals[bound_row_].data (), f.cols[0].lengths[bound_row_], h,
					     generation_, dummy_a, dummy_b);
	  bound_varchar_.assign (owned->begin (), owned->end ());
	  break;
	}
      default:
	break;
      }
    return true;
  }

  cell_result
  variant_a_handle::run_cell (cell_id c, const fixture &f)
  {
    switch (c)
      {
      case cell_id::FL_FILTER:
	return run_fl_filter (f);
      case cell_id::FL_SORT:
	return run_fl_sort (f);
      case cell_id::CV_SORT:
	return run_cv_sort (f);
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
      case cell_id::ABBREV_SUBCELL:
	return run_abbrev_subcell (f);
      default:
	return cell_result{};
      }
  }

  /* ---- FL_FILTER: fresh handle per column per row — no hoisted att_types, re-dispatched every call ---- */
  cell_result
  variant_a_handle::run_fl_filter (const fixture &f)
  {
    std::vector<char> digest_bytes;
    auto t0 = std::chrono::steady_clock::now ();

    for (std::size_t r = 0; r < f.row_count; ++r)
      {
	std::uint64_t cells[5];
	bool survives = true;
	for (int i = 0; i < natts_; ++i)
	  {
	    value_handle h = make_handle_fl (dbtype_[i], f.cols[i].vals[r].data (), generation_);
	    cells[i] = h.payload.inline_val;
	    if (fl_handle_compare (h, value_handle{ h.kind, h.type, 0, generation_, { bound_fl_[i] } }) > 0)
	      {
		survives = false;
	      }
	  }
	if (survives)
	  {
	    for (int i = 0; i < natts_; ++i)
	      {
		append_u64_native (digest_bytes, cells[i]);
	      }
	  }
      }

    auto t1 = std::chrono::steady_clock::now ();
    cell_result res;
    res.elapsed_us = static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
    return res;
  }

  /* ---- FL_SORT: per-row/per-column handle construction feeds the comparator directly (the
   * rejected design has no Datum1Cache concept — a handle's inline_val is used the same way a
   * slot's datum1 would be, but every handle was freshly built, tag fields included) ---- */
  cell_result
  variant_a_handle::run_fl_sort (const fixture &f)
  {
    struct row_keys
    {
      std::uint64_t vals[5];
      std::size_t row;
    };
    std::vector<row_keys> entries (f.row_count);

    auto t0 = std::chrono::steady_clock::now ();

    for (std::size_t r = 0; r < f.row_count; ++r)
      {
	for (int i = 0; i < natts_; ++i)
	  {
	    value_handle h = make_handle_fl (dbtype_[i], f.cols[i].vals[r].data (), generation_);
	    entries[r].vals[i] = h.payload.inline_val;
	  }
	entries[r].row = r;
      }

    std::stable_sort (entries.begin (), entries.end (), [this] (const row_keys &a, const row_keys &b)
    {
      for (int i = 0; i < natts_; ++i)
	{
	  value_handle ha{ handle_kind::NONE, 0, 0, 0, { a.vals[i] } };
	  value_handle hb{ handle_kind::NONE, 0, 0, 0, { b.vals[i] } };
	  ha.kind = is_fl_inline_type (dbtype_[i])
		    ? (dbtype_[i] == DB_TYPE_BIGINT ? handle_kind::INLINE_I64
		       : dbtype_[i] == DB_TYPE_DOUBLE ? handle_kind::INLINE_F64 : handle_kind::INLINE_I32)
		    : handle_kind::INLINE_I32;
	  hb.kind = ha.kind;
	  int c = fl_handle_compare (ha, hb);
	  if (c != 0)
	    {
	      return c < 0;
	    }
	}
      return false;
    });

    auto t1 = std::chrono::steady_clock::now ();

    std::vector<char> digest_bytes;
    digest_bytes.reserve (f.row_count * natts_ * 8);
    for (const row_keys &e : entries)
      {
	for (int i = 0; i < natts_; ++i)
	  {
	    append_u64_native (digest_bytes, e.vals[i]);
	  }
      }

    cell_result res;
    res.elapsed_us = static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
    return res;
  }

  /* ---- CV_SORT: per-value handle heap-allocates its own owned copy — one malloc per row, no arena ---- */
  cell_result
  variant_a_handle::run_cv_sort (const fixture &f)
  {
    const serialized_column &col = f.cols[0];
    struct entry
    {
      std::unique_ptr<std::vector<char>> owned;
      std::size_t row;
    };
    std::vector<entry> entries (f.row_count);

    auto t0 = std::chrono::steady_clock::now ();

    std::uint64_t peeks = 0, copies = 0;
    for (std::size_t r = 0; r < f.row_count; ++r)
      {
	value_handle h;
	entries[r].owned = make_handle_varchar (col.vals[r].data (), col.lengths[r], h, generation_, peeks, copies);
	entries[r].row = r;
      }

    std::stable_sort (entries.begin (), entries.end (), [] (const entry &a, const entry &b)
    {
      return byte_compare (a.owned->data (), static_cast<int> (a.owned->size ()),
			    b.owned->data (), static_cast<int> (b.owned->size ())) < 0;
    });

    auto t1 = std::chrono::steady_clock::now ();

    std::vector<char> digest_bytes;
    for (const entry &e : entries)
      {
	append_bytes (digest_bytes, col.vals[e.row].data (), static_cast<std::size_t> (col.lengths[e.row]));
      }

    cell_result res;
    res.elapsed_us = static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
    return res;
  }

  /* ---- CV_MERGE: same per-value heap-allocated handles as CV_SORT, then adjacent dedup ---- */
  cell_result
  variant_a_handle::run_cv_merge (const fixture &f)
  {
    const serialized_column &col = f.cols[0];
    struct entry
    {
      std::unique_ptr<std::vector<char>> owned;
      std::size_t row;
    };
    std::vector<entry> entries (f.row_count);

    auto t0 = std::chrono::steady_clock::now ();

    std::uint64_t peeks = 0, copies = 0;
    for (std::size_t r = 0; r < f.row_count; ++r)
      {
	value_handle h;
	entries[r].owned = make_handle_varchar (col.vals[r].data (), col.lengths[r], h, generation_, peeks, copies);
	entries[r].row = r;
      }

    std::stable_sort (entries.begin (), entries.end (), [] (const entry &a, const entry &b)
    {
      return byte_compare (a.owned->data (), static_cast<int> (a.owned->size ()),
			    b.owned->data (), static_cast<int> (b.owned->size ())) < 0;
    });

    std::vector<char> digest_bytes;
    const entry *prev = nullptr;
    for (const entry &e : entries)
      {
	if (prev != nullptr
	    && byte_compare (prev->owned->data (), static_cast<int> (prev->owned->size ()),
			      e.owned->data (), static_cast<int> (e.owned->size ())) == 0)
	  {
	    continue;
	  }
	append_bytes (digest_bytes, col.vals[e.row].data (), static_cast<std::size_t> (col.lengths[e.row]));
	prev = &e;
      }

    auto t1 = std::chrono::steady_clock::now ();

    cell_result res;
    res.elapsed_us = static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
    return res;
  }

  /* ---- UV_PEEK: even a peek/filter access builds+copies a full handle — no VC_RAW zero-copy path ---- */
  cell_result
  variant_a_handle::run_uv_peek (const fixture &f)
  {
    const serialized_column &col = f.cols[0];
    std::vector<char> digest_bytes;
    std::uint64_t peeks = 0, copies = 0;

    auto t0 = std::chrono::steady_clock::now ();

    for (std::size_t r = 0; r < f.row_count; ++r)
      {
	value_handle h;
	auto owned = make_handle_varchar (col.vals[r].data (), col.lengths[r], h, generation_, peeks, copies);
	if (byte_compare (owned->data (), static_cast<int> (owned->size ()),
			   bound_varchar_.data (), static_cast<int> (bound_varchar_.size ())) <= 0)
	  {
	    append_bytes (digest_bytes, owned->data (), owned->size ());
	  }
      }

    auto t1 = std::chrono::steady_clock::now ();
    cell_result res;
    res.elapsed_us = static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
    return res;
  }

  /* ---- NUM_SORT: per-value NUMERIC decode buffer, heap-allocated fresh per row, no arena ---- */
  cell_result
  variant_a_handle::run_num_sort (const fixture &f)
  {
    const serialized_column &col = f.cols[0];
    struct entry
    {
      std::unique_ptr<numeric_decoded> decoded;
      std::size_t row;
    };
    std::vector<entry> entries (f.row_count);

    auto t0 = std::chrono::steady_clock::now ();

    for (std::size_t r = 0; r < f.row_count; ++r)
      {
	value_handle h;
	entries[r].decoded = make_handle_numeric (col.vals[r].data (), h, generation_);
	entries[r].row = r;
      }

    std::stable_sort (entries.begin (), entries.end (), [] (const entry &a, const entry &b)
    {
      return numeric_compare_decoded (*a.decoded, *b.decoded) < 0;
    });

    auto t1 = std::chrono::steady_clock::now ();

    std::vector<char> digest_bytes;
    for (const entry &e : entries)
      {
	append_bytes (digest_bytes, col.vals[e.row].data (), static_cast<std::size_t> (col.lengths[e.row]));
      }

    cell_result res;
    res.elapsed_us = static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
    return res;
  }

  /* ---- NUM_AGG_INPUT: per-row heap-allocated decode + accumulate — one-shot digest ---- */
  cell_result
  variant_a_handle::run_num_agg_input (const fixture &f)
  {
    const serialized_column &col = f.cols[0];
    std::int64_t acc = 0;

    auto t0 = std::chrono::steady_clock::now ();

    for (std::size_t r = 0; r < f.row_count; ++r)
      {
	value_handle h;
	auto decoded = make_handle_numeric (col.vals[r].data (), h, generation_);
	acc += decoded->proxy_valid ? decoded->int64_proxy : 0;
      }

    auto t1 = std::chrono::steady_clock::now ();
    cell_result res;
    res.elapsed_us = static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (&acc, sizeof (acc), f.seed);
    return res;
  }

  /* ---- PEEK_VS_COPY: the crux contrast. Every within-row reference reconstructs its handle from
   * scratch — no array to re-read, so a k=2 reference workload pays peek(+copy) TWICE per column
   * per row, unlike ValueSlot's once-per-row deform. This is D-G1 a.0's rejection rationale made
   * concrete and counted. ---- */
  cell_result
  variant_a_handle::run_peek_vs_copy (const fixture &f)
  {
    int int_col = -1, vc_col = -1;
    for (int i = 0; i < natts_; ++i)
      {
	if (is_fl_inline_type (dbtype_[i]) && int_col < 0)
	  {
	    int_col = i;
	  }
	else if (!is_fl_inline_type (dbtype_[i]) && vc_col < 0)
	  {
	    vc_col = i;
	  }
      }

    std::vector<char> digest_bytes;
    std::uint64_t peeks = 0, copies = 0;

    auto t0 = std::chrono::steady_clock::now ();

    for (std::size_t r = 0; r < f.row_count; ++r)
      {
	/* reference #1: filter evaluation. Separate handles for the INT and VARCHAR columns —
	 * make_handle_varchar() overwrites its `value_handle&` out-param, so reusing one handle for
	 * both columns would clobber the INT cell via the union alias (real bug caught by --parity). */
	value_handle h1_int = make_handle_fl (dbtype_[int_col], f.cols[int_col].vals[r].data (), generation_);
	peeks += 1;
	copies += 1;		/* FL handle construction always materializes a copy — no slot to skip it */

	value_handle h1_vc;
	std::unique_ptr<std::vector<char>> vc1 = make_handle_varchar (
	    f.cols[vc_col].vals[r].data (), f.cols[vc_col].lengths[r], h1_vc, generation_, peeks, copies);

	bool survives = fl_handle_compare (h1_int, value_handle{ h1_int.kind, h1_int.type, 0, generation_, { bound_fl_[int_col] } }) <= 0
			&& byte_compare (vc1->data (), static_cast<int> (vc1->size ()),
					  bound_varchar_.data (), static_cast<int> (bound_varchar_.size ())) <= 0;

	/* reference #2: simulated projection re-access — no slot, so this REBUILDS both handles from
	 * scratch, paying peek+copy again for each column (the honest "no slot reuse" cost). */
	value_handle h2_int = make_handle_fl (dbtype_[int_col], f.cols[int_col].vals[r].data (), generation_);
	peeks += 1;
	copies += 1;
	value_handle h2_vc;
	std::unique_ptr<std::vector<char>> vc2 = make_handle_varchar (
	    f.cols[vc_col].vals[r].data (), f.cols[vc_col].lengths[r], h2_vc, generation_, peeks, copies);

	if (survives)
	  {
	    append_u64_native (digest_bytes, h2_int.payload.inline_val);
	    append_bytes (digest_bytes, vc2->data (), vc2->size ());
	  }
      }

    auto t1 = std::chrono::steady_clock::now ();
    cell_result res;
    res.elapsed_us = static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
    res.aux_counter_a = peeks;
    res.aux_counter_b = copies;
    return res;
  }

  /* ---- ABBREV_SUBCELL: same 8B-prefix content-proxy technique as A-slot, applied on top of the
   * per-value handle's owned (heap-allocated, not arena-carved) storage. Reference order must still
   * equal the full byte-compare sort; digest formula matches CV_SORT's. ---- */
  cell_result
  variant_a_handle::run_abbrev_subcell (const fixture &f)
  {
    const serialized_column &col = f.cols[0];
    struct entry
    {
      std::unique_ptr<std::vector<char>> owned;
      std::uint64_t prefix;
      std::size_t row;
    };
    std::vector<entry> entries (f.row_count);

    auto t0 = std::chrono::steady_clock::now ();

    std::uint64_t peeks = 0, copies = 0;
    for (std::size_t r = 0; r < f.row_count; ++r)
      {
	value_handle h;
	auto owned = make_handle_varchar (col.vals[r].data (), col.lengths[r], h, generation_, peeks, copies);
	std::uint64_t prefix = 0;
	int n = std::min (static_cast<int> (owned->size ()), 8);
	for (int i = 0; i < n; ++i)
	  {
	    prefix = (prefix << 8) | static_cast<unsigned char> ((*owned)[static_cast<std::size_t> (i)]);
	  }
	prefix <<= (8 - n) * 8;
	entries[r] = { std::move (owned), prefix, r };
      }

    std::uint64_t proxy_decisive = 0, full_compare_fallback = 0;

    std::stable_sort (entries.begin (), entries.end (),
		       [&proxy_decisive, &full_compare_fallback] (const entry &a, const entry &b)
    {
      if (a.prefix != b.prefix)
	{
	  ++proxy_decisive;
	  return a.prefix < b.prefix;
	}
      ++full_compare_fallback;
      return byte_compare (a.owned->data (), static_cast<int> (a.owned->size ()),
			    b.owned->data (), static_cast<int> (b.owned->size ())) < 0;
    });

    auto t1 = std::chrono::steady_clock::now ();

    std::vector<char> digest_bytes;
    for (const entry &e : entries)
      {
	append_bytes (digest_bytes, col.vals[e.row].data (), static_cast<std::size_t> (col.lengths[e.row]));
      }

    cell_result res;
    res.elapsed_us = static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
    res.aux_counter_a = proxy_decisive;
    res.aux_counter_b = full_compare_fallback;
    return res;
  }

}  // namespace

variant *
make_variant_a_handle ()
{
  static variant_a_handle instance;
  return &instance;
}

}  // namespace vhb
