/*
 * variant_valueslot.cpp — "A-slot": bench-local embodiment of the stage-1 ValueSlot design.
 *
 * Grounded against docs/value-handle/p1/p1.1-valueslot-design.md:
 *   - deform-once, incremental, per-row (§a.2/§a.3)
 *   - NUMERIC(15,2) decode-once + exact int64 datum1 proxy for p<=18 (§s.3)
 *   - compressed varchar: detoast-once at SORT-ENTRY build into sort-owned per-entry storage,
 *     NEVER the per-scan deform-time scratch (§s.2/§s.2.1); non-sort accesses stay zero-copy
 *     peeks (§a.3 VC_RAW row) or lazy/per-scan-scratch (§a.3 VC_COMPRESSED row).
 *   - no 8B *content* proxy anywhere except ABBREV_SUBCELL, which exists precisely to measure
 *     that (deferred, campaign-only per D-G5) upside (§s.4).
 *
 * BENCH-ONLY. Owns exactly this file (+ variant_pervalue.cpp) per P2 file-ownership split.
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
#include <stdexcept>
#include <vector>

namespace vhb
{
namespace
{
  /* ---- att_class (design §a.1/§a.3) — fixed once per column, never touched again ---- */
  enum class att_class : std::uint8_t
  {
    FL_INLINE, NUMERIC_DECODED, VC_RAW, VC_COMPRESSED, FALLBACK
  };

  /* ---- FL family: how the 5 fixture FL types interpret their 8B values[] cell ---- */
  enum class fl_family : std::uint8_t { I32, I64, F64 };

  att_class
  classify (int dbtype)
  {
    switch (static_cast<DB_TYPE> (dbtype))
      {
      case DB_TYPE_INTEGER:
      case DB_TYPE_BIGINT:
      case DB_TYPE_DOUBLE:
      case DB_TYPE_DATE:
      case DB_TYPE_TIMESTAMP:
	return att_class::FL_INLINE;
      case DB_TYPE_NUMERIC:
	return att_class::NUMERIC_DECODED;
      case DB_TYPE_VARCHAR:
      case DB_TYPE_CHAR:
	/* VC_RAW vs VC_COMPRESSED is a per-VALUE header bit (object_primitive.c:13800-13850), not a
	 * per-column property — resolved per-row at deform time, see peek_varchar()/detoast_into_entry(). */
	return att_class::VC_RAW;
      default:
	return att_class::FALLBACK;
      }
  }

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
	return fl_family::I32;	/* INTEGER/DATE/TIMESTAMP: 4B big-endian disk image */
      }
  }

  /* ---- FL_INLINE deform (§a.3 row 1): exactly one OR_GET_* bswap, native-endian 8B cell ---- */
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

  /* three-way compare of two already-deformed FL_INLINE cells, dispatched by column family */
  int
  fl_compare (int dbtype, std::uint64_t a, std::uint64_t b)
  {
    switch (fl_family_of (dbtype))
      {
      case fl_family::I64:
	{
	  std::int64_t va, vb;
	  std::memcpy (&va, &a, sizeof (va));
	  std::memcpy (&vb, &b, sizeof (vb));
	  return (va < vb) ? -1 : (va > vb) ? 1 : 0;
	}
      case fl_family::F64:
	{
	  double va, vb;
	  std::memcpy (&va, &a, sizeof (va));
	  std::memcpy (&vb, &b, sizeof (vb));
	  return (va < vb) ? -1 : (va > vb) ? 1 : 0;
	}
      case fl_family::I32:
      default:
	{
	  std::int64_t va = static_cast<std::int64_t> (a);
	  std::int64_t vb = static_cast<std::int64_t> (b);
	  return (va < vb) ? -1 : (va > vb) ? 1 : 0;
	}
      }
  }

  /* ---- NUMERIC decode-once (§a.3 NUMERIC_DECODED row / §s.3) ----
   * Mirrors mr_data_readval_numeric's wire parse (object_primitive.c:8760-8836) at the byte level
   * without the DB_VALUE/TP_DOMAIN machinery a bench doesn't need: 3-byte header + leading-zero-
   * truncated big-endian magnitude tail, reconstructed into a DB_NUMERIC_BUF_SIZE-shaped buffer the
   * slot/sort-entry owns (decoded once, never re-parsed per access/comparison). */
  constexpr int k_numeric_header_size = 3;			/* NUMERIC_HEADER_SIZE, object_primitive.c:131 */
  constexpr unsigned char k_numeric_sign_bit = 0x80;		/* NUMERIC_VALUE_SIGN_BIT_MASK */

  struct numeric_decoded
  {
    unsigned char magnitude[DB_NUMERIC_BUF_SIZE] = { 0 };	/* slot/entry-owned decode-once storage */
    int precision = 0;
    int scale = 0;
    bool negative = false;
    bool proxy_valid = false;					/* §s.3 safe range, p <= 18 */
    std::int64_t int64_proxy = 0;
  };

  void
  decode_numeric_once (const unsigned char *raw, numeric_decoded &out)
  {
    int disk_size = raw[0] & 0x7F;
    out.negative = (raw[0] & k_numeric_sign_bit) != 0;
    out.precision = raw[1] & 0x7F;
    bool neg_scale = (raw[1] & k_numeric_sign_bit) != 0;
    out.scale = neg_scale ? -static_cast<int> (raw[2]) : static_cast<int> (raw[2]);

    int mag_len = disk_size - k_numeric_header_size;
    std::memset (out.magnitude, 0, sizeof (out.magnitude));
    if (mag_len > 0)
      {
	std::memcpy (out.magnitude + (DB_NUMERIC_BUF_SIZE - mag_len), raw + k_numeric_header_size, mag_len);
      }

    if (out.precision <= 18)
      {
	/* §s.3 exact proxy: low 8 bytes (buf[9..16]) big-endian -> native int64, sign-applied. Pure
	 * register op on already-decoded storage — no re-parse. */
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
	out.proxy_valid = false;	/* p in [19,40]: no lossless int64 proxy, §s.3 unsafe range */
      }
  }

  /* p19-40 fallback comparator: full magnitude compare against decode-once storage (no double-readval
   * — the design's win over legacy mr_data_cmpdisk_numeric is preserved even off the datum1 fast path) */
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

  /* ---- varchar deform / detoast (§a.3 VC_RAW / VC_COMPRESSED rows, §s.2.1) ---- */

  /* Non-sort peek (§a.3 row 3/4): resolve the header, never decompress unless forced. VC_RAW returns
   * a zero-copy pointer straight into the disk image; VC_COMPRESSED returns a pointer to the
   * still-compressed payload — lazy, matches the deform-time model without needing a scratch buffer
   * for a pure peek/filter access. */
  struct varchar_view
  {
    const char *data = nullptr;
    int len = 0;
    bool was_compressed = false;
  };

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
    v.was_compressed = (compressed_size > 0);
    v.data = buf.ptr;
    v.len = v.was_compressed ? compressed_size : decompressed_size;
    return v;
  }

  /* Sort-entry build (§s.2.1 LEADER DESIGN DECISION): detoast/copy exactly once into a slice of a
   * bench-side arena the sort owns — never the per-scan scratch. Uncompressed VARCHAR is durably
   * copied too (§s.2 VC_RAW sort-entry row: "qfile_make_sort_key already durably memcpys the raw key
   * bytes into the entry"), so post-build the sort never touches the original disk image again.
   * `dest` must have at least decompressed_size+1 bytes of headroom (pr_get_compressed_data_from_buffer
   * writes a trailing NUL); callers reserve that stride. Returns the logical content length.
   * [LOW hygiene, architect finding] both or_get_varchar_compression_lengths() and
   * pr_get_compressed_data_from_buffer() return an error code that this function used to ignore;
   * both are now checked and abort the cell (throw) on failure instead of silently proceeding with
   * a garbage/zero length or an under-filled `dest`. */
  int
  detoast_into_entry (const char *raw, int raw_len, char *dest)
  {
    OR_BUF buf;
    or_init (&buf, const_cast<char *> (raw), raw_len);
    int compressed_size = 0, decompressed_size = 0;
    if (or_get_varchar_compression_lengths (&buf, &compressed_size, &decompressed_size) != NO_ERROR)
      {
	throw std::runtime_error ("variant_a_slot: detoast_into_entry: or_get_varchar_compression_lengths failed");
      }
    if (compressed_size > 0)
      {
	if (pr_get_compressed_data_from_buffer (&buf, dest, compressed_size, decompressed_size) != NO_ERROR)
	  {
	    throw std::runtime_error ("variant_a_slot: detoast_into_entry: pr_get_compressed_data_from_buffer failed");
	  }
      }
    else
      {
	std::memcpy (dest, buf.ptr, static_cast<std::size_t> (decompressed_size));
      }
    return decompressed_size;
  }

  /* Non-sort bound/reference decode: same detoast-once contract as detoast_into_entry(), but sizes
   * and owns its own destination vector (used once per prepare(), never in the timed per-row path).
   * [LOW hygiene, architect finding] rc-checked the same way detoast_into_entry() now is. */
  void
  decode_varchar_plain (const char *raw, int raw_len, std::vector<char> &out)
  {
    OR_BUF buf;
    or_init (&buf, const_cast<char *> (raw), raw_len);
    int compressed_size = 0, decompressed_size = 0;
    if (or_get_varchar_compression_lengths (&buf, &compressed_size, &decompressed_size) != NO_ERROR)
      {
	throw std::runtime_error ("variant_a_slot: decode_varchar_plain: or_get_varchar_compression_lengths failed");
      }
    out.resize (static_cast<std::size_t> (decompressed_size) + 1);
    if (compressed_size > 0)
      {
	if (pr_get_compressed_data_from_buffer (&buf, out.data (), compressed_size, decompressed_size) != NO_ERROR)
	  {
	    throw std::runtime_error ("variant_a_slot: decode_varchar_plain: pr_get_compressed_data_from_buffer failed");
	  }
      }
    else
      {
	std::memcpy (out.data (), buf.ptr, static_cast<std::size_t> (decompressed_size));
      }
    out.resize (static_cast<std::size_t> (decompressed_size));
  }

  /* header-only pass (no decompression) to size the sort-owned arena once, up front.
   * [LOW hygiene, architect finding] rc-checked; a header-parse failure here would otherwise
   * silently under-size the shared arena_ and corrupt every subsequent detoast_into_entry() call
   * in the same prepare(), so this one aborts loudest of the three. */
  std::size_t
  total_decompressed_len (const serialized_column &col)
  {
    std::size_t total = 0;
    for (std::size_t r = 0; r < col.vals.size (); ++r)
      {
	OR_BUF buf;
	or_init (&buf, const_cast<char *> (col.vals[r].data ()), col.lengths[r]);
	int csize = 0, dsize = 0;
	if (or_get_varchar_compression_lengths (&buf, &csize, &dsize) != NO_ERROR)
	  {
	    throw std::runtime_error ("variant_a_slot: total_decompressed_len: or_get_varchar_compression_lengths failed");
	  }
	total += static_cast<std::size_t> (dsize);
      }
    return total;
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

  /* ==================================================================================== */

  class variant_a_slot final : public variant
  {
  public:
    const char *
    name () const override
    {
      return "A-slot";
    }

    bool prepare (cell_id c, const fixture &f) override;
    cell_result run_cell (cell_id c, const fixture &f) override;

  private:
    int natts_ = 0;
    std::vector<int> dbtype_;
    std::vector<int> precision_;
    std::vector<att_class> class_;

    /* filter-cell bounds, derived once at prepare() (untimed) from a canonical fixture-only row
     * index (row_count/3) — every variant derives the same bound independently, zero shared state */
    int bound_row_ = 0;
    std::vector<std::uint64_t> bound_fl_;
    std::vector<char> bound_varchar_;

    /* sort-cell scratch: sized once in prepare() (header-only pass, no decompression), filled once
     * per run_cell() call (the timed, per-row detoast-once work, §s.2.1) */
    std::vector<char> arena_;

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
  variant_a_slot::prepare (cell_id c, const fixture &f)
  {
    natts_ = static_cast<int> (f.cols.size ());
    dbtype_.resize (natts_);
    precision_.resize (natts_);
    class_.resize (natts_);
    for (int i = 0; i < natts_; ++i)
      {
	dbtype_[i] = f.cols[i].dbtype;
	precision_[i] = f.cols[i].precision;
	class_[i] = classify (dbtype_[i]);
      }

    bound_row_ = (f.row_count >= 3) ? static_cast<int> (f.row_count / 3) : 0;
    bound_fl_.assign (natts_, 0);
    bound_varchar_.clear ();
    arena_.clear ();

    switch (c)
      {
      case cell_id::FL_FILTER:
      case cell_id::PEEK_VS_COPY:
	for (int i = 0; i < natts_; ++i)
	  {
	    const serialized_column &col = f.cols[i];
	    if (class_[i] == att_class::FL_INLINE)
	      {
		bound_fl_[i] = deform_fl_inline (dbtype_[i], col.vals[bound_row_].data ());
	      }
	    else if (class_[i] == att_class::VC_RAW)
	      {
		decode_varchar_plain (col.vals[bound_row_].data (), col.lengths[bound_row_], bound_varchar_);
	      }
	  }
	break;
      case cell_id::UV_PEEK:
	decode_varchar_plain (f.cols[0].vals[bound_row_].data (), f.cols[0].lengths[bound_row_], bound_varchar_);
	break;
      case cell_id::CV_SORT:
      case cell_id::CV_MERGE:
      case cell_id::ABBREV_SUBCELL:
	{
	  std::size_t total = total_decompressed_len (f.cols[0]);
	  arena_.resize (total + f.cols[0].vals.size ());	/* +1 pad/row, §s.2.1 stride */
	  break;
	}
      default:
	break;
      }
    return true;
  }

  cell_result
  variant_a_slot::run_cell (cell_id c, const fixture &f)
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

  /* ---- FL_FILTER: deform-once per column per row (native compare vs bound), all 5 cols ----
   * [LOW asymmetry note, architect finding] this loop deliberately does NOT `break` out of the
   * inner per-column loop once `survives` goes false — every one of the 5 columns is deformed
   * unconditionally, every row (matches variant_pervalue.cpp's A-handle sibling). Contrast
   * variant_cmpdisk.cpp's / variant_flatbuffers.cpp's B/C run_fl_filter(), which DO short-circuit
   * on the first failing column. This is a deliberate, conservative, no-behavior-change asymmetry
   * (A's per-row deform is a single up-front step feeding every later access, not a
   * column-by-column short-circuiting predicate the way B/C's disk-direct/table dispatch is) —
   * documented at both sides so it isn't mistaken for an oversight. */
  cell_result
  variant_a_slot::run_fl_filter (const fixture &f)
  {
    std::vector<char> digest_bytes;
    auto t0 = std::chrono::steady_clock::now ();

    for (std::size_t r = 0; r < f.row_count; ++r)
      {
	std::uint64_t cells[5];
	bool survives = true;
	for (int i = 0; i < natts_; ++i)
	  {
	    cells[i] = deform_fl_inline (dbtype_[i], f.cols[i].vals[r].data ());
	    if (fl_compare (dbtype_[i], cells[i], bound_fl_[i]) > 0)
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

  /* ---- FL_SORT: Datum1Cache, all 5 cols inline (free), multi-key stable sort, no tuple touch ---- */
  cell_result
  variant_a_slot::run_fl_sort (const fixture &f)
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
	    entries[r].vals[i] = deform_fl_inline (dbtype_[i], f.cols[i].vals[r].data ());
	  }
	entries[r].row = r;
      }

    std::stable_sort (entries.begin (), entries.end (), [this] (const row_keys &a, const row_keys &b)
    {
      for (int i = 0; i < natts_; ++i)
	{
	  int c = fl_compare (dbtype_[i], a.vals[i], b.vals[i]);
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

  /* ---- CV_SORT: detoast-once at sort-entry build into sort-owned arena (§s.2.1), byte-compare ---- */
  cell_result
  variant_a_slot::run_cv_sort (const fixture &f)
  {
    const serialized_column &col = f.cols[0];
    struct entry
    {
      const char *data;
      int len;
      std::size_t row;
    };
    std::vector<entry> entries (f.row_count);

    auto t0 = std::chrono::steady_clock::now ();

    std::size_t offset = 0;
    for (std::size_t r = 0; r < f.row_count; ++r)
      {
	char *dest = arena_.data () + offset;
	int len = detoast_into_entry (col.vals[r].data (), col.lengths[r], dest);
	entries[r] = { dest, len, r };
	offset += static_cast<std::size_t> (len) + 1;	/* +1 pad, absorbs the decompress NUL */
      }

    std::stable_sort (entries.begin (), entries.end (), [] (const entry &a, const entry &b)
    {
      return byte_compare (a.data, a.len, b.data, b.len) < 0;
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

  /* ---- CV_MERGE: same detoast-once entries as CV_SORT, then adjacent dedup on entry-owned bytes
   * (no per-comparison re-decompress for the dedup pass either — §s.4's R2 mitigation applies here
   * exactly as it does to the sort compare path) ---- */
  cell_result
  variant_a_slot::run_cv_merge (const fixture &f)
  {
    const serialized_column &col = f.cols[0];
    struct entry
    {
      const char *data;
      int len;
      std::size_t row;
    };
    std::vector<entry> entries (f.row_count);

    auto t0 = std::chrono::steady_clock::now ();

    std::size_t offset = 0;
    for (std::size_t r = 0; r < f.row_count; ++r)
      {
	char *dest = arena_.data () + offset;
	int len = detoast_into_entry (col.vals[r].data (), col.lengths[r], dest);
	entries[r] = { dest, len, r };
	offset += static_cast<std::size_t> (len) + 1;
      }

    std::stable_sort (entries.begin (), entries.end (), [] (const entry &a, const entry &b)
    {
      return byte_compare (a.data, a.len, b.data, b.len) < 0;
    });

    std::vector<char> digest_bytes;
    bool have_prev = false;
    const entry *prev = nullptr;
    for (const entry &e : entries)
      {
	if (have_prev && byte_compare (prev->data, prev->len, e.data, e.len) == 0)
	  {
	    continue;		/* adjacent duplicate, dropped */
	  }
	append_bytes (digest_bytes, col.vals[e.row].data (), static_cast<std::size_t> (col.lengths[e.row]));
	prev = &e;
	have_prev = true;
      }

    auto t1 = std::chrono::steady_clock::now ();

    cell_result res;
    res.elapsed_us = static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
    return res;
  }

  /* ---- UV_PEEK: zero-copy pointer peek + filter, no materialization at all (§a.3 VC_RAW row) ---- */
  cell_result
  variant_a_slot::run_uv_peek (const fixture &f)
  {
    const serialized_column &col = f.cols[0];
    std::vector<char> digest_bytes;

    auto t0 = std::chrono::steady_clock::now ();

    for (std::size_t r = 0; r < f.row_count; ++r)
      {
	varchar_view v = peek_varchar (col.vals[r].data (), col.lengths[r]);
	if (byte_compare (v.data, v.len, bound_varchar_.data (), static_cast<int> (bound_varchar_.size ())) <= 0)
	  {
	    append_bytes (digest_bytes, v.data, static_cast<std::size_t> (v.len));
	  }
      }

    auto t1 = std::chrono::steady_clock::now ();
    cell_result res;
    res.elapsed_us = static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
    return res;
  }

  /* ---- NUM_SORT: decode-once (§a.3) + datum1 int64 proxy compare for p<=18 (§s.3) ---- */
  cell_result
  variant_a_slot::run_num_sort (const fixture &f)
  {
    const serialized_column &col = f.cols[0];
    std::vector<numeric_decoded> decoded (f.row_count);
    struct entry
    {
      std::size_t row;
    };
    std::vector<entry> entries (f.row_count);

    auto t0 = std::chrono::steady_clock::now ();

    for (std::size_t r = 0; r < f.row_count; ++r)
      {
	decode_numeric_once (reinterpret_cast<const unsigned char *> (col.vals[r].data ()), decoded[r]);
	entries[r].row = r;
      }

    std::stable_sort (entries.begin (), entries.end (), [&decoded] (const entry &a, const entry &b)
    {
      return numeric_compare_decoded (decoded[a.row], decoded[b.row]) < 0;
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

  /* ---- NUM_AGG_INPUT: decode-once, accumulate the exact int64 proxy — one-shot digest ---- */
  cell_result
  variant_a_slot::run_num_agg_input (const fixture &f)
  {
    const serialized_column &col = f.cols[0];
    std::int64_t acc = 0;

    auto t0 = std::chrono::steady_clock::now ();

    for (std::size_t r = 0; r < f.row_count; ++r)
      {
	numeric_decoded d;
	decode_numeric_once (reinterpret_cast<const unsigned char *> (col.vals[r].data ()), d);
	acc += d.proxy_valid ? d.int64_proxy : 0;
      }

    auto t1 = std::chrono::steady_clock::now ();
    cell_result res;
    res.elapsed_us = static_cast<std::uint64_t> (std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ());
    res.digest = fnv1a (&acc, sizeof (acc), f.seed);
    return res;
  }

  /* ---- PEEK_VS_COPY: deform-once per row; repeat within-row references are free array reads
   * (§a.4). INT column pays 1 peek + 1 copy-into-slot at first deform; VARCHAR column (VC_RAW) pays
   * 1 peek + 0 copy, ever — the concrete divergence from the legacy 1:1 peek/copy ratio. A second
   * simulated reference to each column (e.g. filter + projection) costs nothing further. ---- */
  cell_result
  variant_a_slot::run_peek_vs_copy (const fixture &f)
  {
    int int_col = -1, vc_col = -1;
    for (int i = 0; i < natts_; ++i)
      {
	if (class_[i] == att_class::FL_INLINE && int_col < 0)
	  {
	    int_col = i;
	  }
	else if (class_[i] == att_class::VC_RAW && vc_col < 0)
	  {
	    vc_col = i;
	  }
      }

    std::vector<char> digest_bytes;
    std::uint64_t peeks = 0, copies = 0;

    auto t0 = std::chrono::steady_clock::now ();

    for (std::size_t r = 0; r < f.row_count; ++r)
      {
	/* first (and only, per design) deform of each column: peek + materialize into slot storage */
	std::uint64_t int_cell = deform_fl_inline (dbtype_[int_col], f.cols[int_col].vals[r].data ());
	peeks += 1;
	copies += 1;		/* FL_INLINE materializes into values[] — one copy, first access only */

	varchar_view vc = peek_varchar (f.cols[vc_col].vals[r].data (), f.cols[vc_col].lengths[r]);
	peeks += 1;		/* VC_RAW: zero-copy peek */
	/* no copies += for VARCHAR: VC_RAW never materializes a DB_VALUE, §a.3/§(c) point 1 */

	/* simulated second reference within the same row (filter already consumed the first; this
	 * models e.g. a projection) — under ValueSlot this is a values[]/pointer re-read: zero calls
	 * into peek/copy primitives, so no counters move here. */
	bool survives = fl_compare (dbtype_[int_col], int_cell, bound_fl_[int_col]) <= 0
			&& byte_compare (vc.data, vc.len, bound_varchar_.data (), static_cast<int> (bound_varchar_.size ())) <= 0;

	if (survives)
	  {
	    append_u64_native (digest_bytes, int_cell);
	    append_bytes (digest_bytes, vc.data, static_cast<std::size_t> (vc.len));
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

  /* ---- ABBREV_SUBCELL (grilled D-G5 upside measurement, campaign-deferred in production, bench-
   * only here): 8B-prefix CONTENT proxy compare with full-compare tiebreak. This is the one cell
   * explicitly licensed to use a content proxy outside stage-1's design (§s.4) — it exists to
   * measure the abbreviated-key upside the production design defers. Output order MUST still equal
   * the reference full byte-compare sort (the proxy accelerates comparisons, never changes the
   * result), so this cell's digest formula matches CV_SORT's exactly. ---- */
  cell_result
  variant_a_slot::run_abbrev_subcell (const fixture &f)
  {
    const serialized_column &col = f.cols[0];
    struct entry
    {
      const char *data;
      int len;
      std::uint64_t prefix;	/* 8B big-endian content proxy, D-G5 upside measurement only */
      std::size_t row;
    };
    std::vector<entry> entries (f.row_count);

    auto t0 = std::chrono::steady_clock::now ();

    std::size_t offset = 0;
    for (std::size_t r = 0; r < f.row_count; ++r)
      {
	char *dest = arena_.data () + offset;
	int len = detoast_into_entry (col.vals[r].data (), col.lengths[r], dest);
	std::uint64_t prefix = 0;
	int n = std::min (len, 8);
	for (int i = 0; i < n; ++i)
	  {
	    prefix = (prefix << 8) | static_cast<unsigned char> (dest[i]);
	  }
	prefix <<= (8 - n) * 8;
	entries[r] = { dest, len, prefix, r };
	offset += static_cast<std::size_t> (len) + 1;
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
      return byte_compare (a.data, a.len, b.data, b.len) < 0;
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
make_variant_a_slot ()
{
  static variant_a_slot instance;
  return &instance;
}

}  // namespace vhb
