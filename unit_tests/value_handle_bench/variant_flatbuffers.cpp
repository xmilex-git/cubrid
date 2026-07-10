/*
 * variant_flatbuffers.cpp — "C-flatbuffers" variant for the P2 value-representation microbench.
 *
 * BENCH-ONLY code (see value_handle_bench.hpp header comment): nothing here ships in the server.
 *
 * Modeling choices (per assignment / IRC with 30-P2-Harness):
 *  - Direct API only (no flatc, no Object API): each row is built by hand via
 *    flatbuffers::FlatBufferBuilder using a manually-assigned vtable-compatible field layout
 *    (voffsets chosen the same way flatc-generated code would assign them: 4 + 2*field_index).
 *    Reading a row is `flatbuffers::GetRoot<Table>(ptr)->GetField<T>(voffset, default)` — a raw
 *    pointer cast plus a vtable indirection, no deserialization.
 *  - prepare() is the "ingestion" step: it decodes the fixture's real on-disk value images
 *    (via CUBRID's own pr_/or_ machinery — pr_type_from_id()->data_readval(), tp_domain_construct())
 *    and re-encodes each row as one small FlatBuffer table. This cost is NOT measured, matching the
 *    contract ("building cost is prepare()-side").
 *  - run_cell() measures ONLY direct field-access + the cell's operation (filter/sort/merge/agg),
 *    reading exclusively from the FlatBuffer tables built in prepare().
 *  - NUMERIC has no FlatBuffers scalar type. The modeling choice is: store the row's raw
 *    OR_NUMERIC disk bytes verbatim in a ubyte vector field at prepare() time (zero decode cost),
 *    and decode-at-use inside run_cell() for every comparison (NUM_SORT, via
 *    tp_Type_numeric->data_cmpdisk()) or every accumulate step (NUM_AGG_INPUT, via
 *    tp_Type_numeric->data_readval() + numeric_db_value_add()). This intentionally bills
 *    FlatBuffers for the cost of NOT having a native NUMERIC type.
 *
 *  Digest convention and predicates — RECONCILED against the landed variant_cmpdisk.cpp header
 *  comment (owned by 30-P2-Harness), which is itself re-derived from variant_valueslot.cpp /
 *  variant_pervalue.cpp (the reference pair). This supersedes an earlier IRC exchange with
 *  30-P2-Harness that assumed a different seed/predicate/chaining scheme before the reference
 *  pair's code existed to read. Per variant_cmpdisk.cpp:
 *    - seed = f.seed (fixture.seed), never a fixed constant.
 *    - ONE fnv1a() call per cell over a single concatenated buffer built from the OUTPUT sequence
 *      in order — not a chained call per output row.
 *    - FL_FILTER/FL_SORT: buffer = native-endian 8B per deformed FL cell (deform_fl_inline,
 *      sign-extended for the I32 family), 5 columns per row in fixture.cols[] order.
 *    - CV_SORT/CV_MERGE/NUM_SORT: buffer = each surviving/ordered row's ORIGINAL
 *      fixture-serialized bytes (fixture.cols[0].vals[row]), in output order.
 *    - UV_PEEK: buffer = the row's PEEKED CONTENT bytes (header-stripped, via
 *      or_get_varchar_compression_lengths) for survivors, in scan order.
 *    - NUM_AGG_INPUT: buffer = the raw 8 bytes of the accumulated int64 unscaled-value proxy
 *      (exact/lossless for NUMERIC(15,2)), computed as an untimed independent re-derivation
 *      straight from the fixture's raw bytes.
 *    - bound row for every filter/peek predicate = the fixture's OWN row at index row_count/3
 *      (or row 0 if row_count<3) — never a synthetic constant. FL_FILTER requires the bound
 *      relation to hold on EVERY one of the 5 FL columns (composite "dominates" filter);
 *      UV_PEEK is a single-column bound comparison.
 *  The actual filter/sort DECISIONS below are made against this variant's own FlatBuffer tables
 *  (that's the mechanism under test), but are numerically/lexicographically equivalent to
 *  variant_cmpdisk.cpp's data_cmpdisk-based decisions over the same domain, and the digest buffer
 *  itself is always independently re-derived from the fixture's raw bytes — never from this
 *  variant's internal representation — so digests match despite the different mechanism.
 *  - Only the 7 "at minimum" cells are implemented (FL_FILTER, FL_SORT, CV_SORT, UV_PEEK, NUM_SORT,
 *    NUM_AGG_INPUT, CV_MERGE). PEEK_VS_COPY and ABBREV_SUBCELL are counter/measurement-only cells the
 *    contract explicitly allows this variant to bow out of; prepare() returns false for both.
 */

#include "value_handle_bench.hpp"

#include "flatbuffers/flatbuffers.h"

#include "dbtype.h"
#include "object_domain.h"
#include "object_primitive.h"
#include "object_representation.h"
#include "numeric_opfunc.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <vector>

namespace
{
  using flatbuffers::DetachedBuffer;
  using flatbuffers::FlatBufferBuilder;
  using flatbuffers::Table;

  /* vtable field offsets — hand-assigned the same way flatc would (4 + 2*field_index). */
  constexpr flatbuffers::voffset_t VT_INT = 4;
  constexpr flatbuffers::voffset_t VT_BIGINT = 6;
  constexpr flatbuffers::voffset_t VT_DOUBLE = 8;
  constexpr flatbuffers::voffset_t VT_DATE = 10;
  constexpr flatbuffers::voffset_t VT_TS = 12;
  constexpr flatbuffers::voffset_t VT_BYTES = 4; /* single-field bytes-vector tables (varchar/numeric) */

  constexpr int k_numeric_header_size = 3; /* NUMERIC_HEADER_SIZE, object_primitive.c */
  constexpr unsigned char k_numeric_sign_bit = 0x80;

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
	return fl_family::I32; /* INTEGER/DATE/TIMESTAMP: 4B disk image */
      }
  }

  /* deform a raw FL_INLINE disk image into a native-endian 8B cell — matches
   * variant_cmpdisk.cpp's deform_fl_inline()/variant_valueslot.cpp's append_u64_native() shape,
   * used ONLY to build the digest buffer independently of this variant's own representation. */
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

  /* header-only peek: never decompresses (matches variant_cmpdisk.cpp's peek_varchar() /
   * variant_valueslot.cpp's peek_varchar()) — for UV_PEEK's always-uncompressed column this
   * already IS the plain content. */
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

  /* int64 unscaled-value proxy, exact for precision<=18 — same byte-level parse as
   * variant_cmpdisk.cpp's numeric_int64_proxy(), used only for NUM_AGG_INPUT's digest. */
  bool
  numeric_int64_proxy (const unsigned char *raw, std::int64_t &out)
  {
    int disk_size = raw[0] & 0x7F;
    bool negative = (raw[0] & k_numeric_sign_bit) != 0;
    int precision = raw[1] & 0x7F;
    if (precision > 18)
      {
	return false;
      }

    unsigned char magnitude[DB_NUMERIC_BUF_SIZE] = { 0 };
    int mag_len = disk_size - k_numeric_header_size;
    if (mag_len > 0)
      {
	std::memcpy (magnitude + (DB_NUMERIC_BUF_SIZE - mag_len), raw + k_numeric_header_size, mag_len);
      }

    std::uint64_t mag = 0;
    for (int i = DB_NUMERIC_BUF_SIZE - 8; i < DB_NUMERIC_BUF_SIZE; i++)
      {
	mag = (mag << 8) | magnitude[i];
      }
    out = negative ? -static_cast<std::int64_t> (mag) : static_cast<std::int64_t> (mag);
    return true;
  }

  /* canonical bound row every filter/peek predicate uses — matches variant_cmpdisk.cpp's
   * bound_row_of() / variant_valueslot.cpp's bound_row_. */
  std::size_t
  bound_row_of (std::size_t row_count)
  {
    return row_count >= 3 ? row_count / 3 : 0;
  }

  DetachedBuffer
  build_fl_row (FlatBufferBuilder &fbb, std::int32_t i, std::int64_t bi, double d, std::int32_t date, std::int32_t ts)
  {
    fbb.Clear ();
    auto start = fbb.StartTable ();
    fbb.AddElement<std::int32_t> (VT_INT, i, 0);
    fbb.AddElement<std::int64_t> (VT_BIGINT, bi, 0);
    fbb.AddElement<double> (VT_DOUBLE, d, 0.0);
    fbb.AddElement<std::int32_t> (VT_DATE, date, 0);
    fbb.AddElement<std::int32_t> (VT_TS, ts, 0);
    auto end = fbb.EndTable (start);
    fbb.Finish (flatbuffers::Offset<Table> (end));
    return fbb.Release ();
  }

  DetachedBuffer
  build_bytes_row (FlatBufferBuilder &fbb, const char *data, std::size_t len)
  {
    fbb.Clear ();
    auto vec = fbb.CreateVector (reinterpret_cast<const std::uint8_t *> (data), len);
    auto start = fbb.StartTable ();
    fbb.AddOffset (VT_BYTES, vec);
    auto end = fbb.EndTable (start);
    fbb.Finish (flatbuffers::Offset<Table> (end));
    return fbb.Release ();
  }

  inline const Table *
  root_of (const DetachedBuffer &buf)
  {
    return flatbuffers::GetRoot<Table> (buf.data ());
  }

  inline const flatbuffers::Vector<std::uint8_t> *
  bytes_vector_of (const Table *t)
  {
    return t->GetPointer<const flatbuffers::Vector<std::uint8_t> *> (VT_BYTES);
  }

  /* decode a single-value disk image (fixed-length FL types) with the plain OR_BUF get_* family */
  int
  decode_int (const vhb::serialized_column &col, std::size_t row)
  {
    OR_BUF buf;
    or_init (&buf, const_cast<char *> (col.vals[row].data ()), col.lengths[row]);
    int err = NO_ERROR;
    return or_get_int (&buf, &err);
  }

  std::int64_t
  decode_bigint (const vhb::serialized_column &col, std::size_t row)
  {
    OR_BUF buf;
    or_init (&buf, const_cast<char *> (col.vals[row].data ()), col.lengths[row]);
    int err = NO_ERROR;
    return or_get_bigint (&buf, &err);
  }

  double
  decode_double (const vhb::serialized_column &col, std::size_t row)
  {
    OR_BUF buf;
    or_init (&buf, const_cast<char *> (col.vals[row].data ()), col.lengths[row]);
    int err = NO_ERROR;
    return or_get_double (&buf, &err);
  }

  std::int32_t
  decode_date (const vhb::serialized_column &col, std::size_t row)
  {
    OR_BUF buf;
    or_init (&buf, const_cast<char *> (col.vals[row].data ()), col.lengths[row]);
    DB_DATE d = 0;
    (void) or_get_date (&buf, &d);
    return static_cast<std::int32_t> (d);
  }

  std::int32_t
  decode_utime (const vhb::serialized_column &col, std::size_t row)
  {
    OR_BUF buf;
    or_init (&buf, const_cast<char *> (col.vals[row].data ()), col.lengths[row]);
    DB_UTIME t = 0;
    (void) or_get_utime (&buf, &t);
    return static_cast<std::int32_t> (t);
  }

  /* decode a VARCHAR disk image (transparently decompresses via pr_ machinery) into owned bytes */
  std::vector<char>
  decode_varchar (const vhb::serialized_column &col, std::size_t row, TP_DOMAIN *domain)
  {
    OR_BUF buf;
    or_init (&buf, const_cast<char *> (col.vals[row].data ()), col.lengths[row]);
    DB_VALUE val;
    db_make_null (&val);
    std::vector<char> out;
    int rc = pr_type_from_id (DB_TYPE_VARCHAR)->data_readval (&buf, &val, domain, -1, true, nullptr, 0);
    if (rc == NO_ERROR && !DB_IS_NULL (&val))
      {
	int len = db_get_string_size (&val);
	const char *s = db_get_string (&val);
	if (s != nullptr && len > 0)
	  {
	    out.assign (s, s + len);
	  }
      }
    pr_clear_value (&val);
    return out;
  }

  bool
  varchar_less (const DetachedBuffer &a, const DetachedBuffer &b)
  {
    auto va = bytes_vector_of (root_of (a));
    auto vb = bytes_vector_of (root_of (b));
    std::size_t na = va != nullptr ? va->size () : 0;
    std::size_t nb = vb != nullptr ? vb->size () : 0;
    std::size_t n = std::min (na, nb);
    int c = n ? std::memcmp (va->data (), vb->data (), n) : 0;
    if (c != 0)
      {
	return c < 0;
      }
    return na < nb;
  }

  bool
  varchar_equal (const DetachedBuffer &a, const DetachedBuffer &b)
  {
    auto va = bytes_vector_of (root_of (a));
    auto vb = bytes_vector_of (root_of (b));
    std::size_t na = va != nullptr ? va->size () : 0;
    std::size_t nb = vb != nullptr ? vb->size () : 0;
    if (na != nb)
      {
	return false;
      }
    return na == 0 || std::memcmp (va->data (), vb->data (), na) == 0;
  }
}

namespace vhb
{
  class variant_c_flatbuffers : public variant
  {
    public:
      const char *
      name () const override
      {
	return "C-flatbuffers";
      }

      bool
      prepare (cell_id c, const fixture &f) override
      {
	rows_.clear ();

	switch (c)
	  {
	  case cell_id::FL_FILTER:
	  case cell_id::FL_SORT:
	    return prepare_fl (f);

	  case cell_id::CV_SORT:
	    return prepare_varchar (f, 300);

	  case cell_id::CV_MERGE:
	    return prepare_varchar (f, 300);

	  case cell_id::UV_PEEK:
	    return prepare_varchar (f, 100);

	  case cell_id::NUM_SORT:
	  case cell_id::NUM_AGG_INPUT:
	    return prepare_numeric_passthrough (f);

	  case cell_id::PEEK_VS_COPY:
	  case cell_id::ABBREV_SUBCELL:
	  default:
	    /* counter/measurement-only cells this variant bows out of, per contract. */
	    return false;
	  }
      }

      cell_result
      run_cell (cell_id c, const fixture &f) override
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
	  default:
	    return {};
	  }
      }

    private:
      /* ---- FL (5 fixed-length columns: INT,BIGINT,DOUBLE,DATE,TIMESTAMP) ---- */

      bool
      prepare_fl (const fixture &f)
      {
	int int_idx = -1, bigint_idx = -1, double_idx = -1, date_idx = -1, ts_idx = -1;
	for (std::size_t i = 0; i < f.cols.size (); i++)
	  {
	    switch (f.cols[i].dbtype)
	      {
	      case DB_TYPE_INTEGER:
		int_idx = (int) i;
		break;
	      case DB_TYPE_BIGINT:
		bigint_idx = (int) i;
		break;
	      case DB_TYPE_DOUBLE:
		double_idx = (int) i;
		break;
	      case DB_TYPE_DATE:
		date_idx = (int) i;
		break;
	      case DB_TYPE_TIMESTAMP:
		ts_idx = (int) i;
		break;
	      default:
		break;
	      }
	  }
	if (int_idx < 0 || bigint_idx < 0 || double_idx < 0 || date_idx < 0 || ts_idx < 0)
	  {
	    return false; /* fixture doesn't carry the expected 5 FL columns */
	  }

	/* per fixture-column-index voffset, so run_cell can compare column ci generically without
	 * caring which of the three I32-family columns (INT/DATE/TIMESTAMP) it is. */
	col_vt_.assign (f.cols.size (), VT_INT);
	col_vt_[int_idx] = VT_INT;
	col_vt_[bigint_idx] = VT_BIGINT;
	col_vt_[double_idx] = VT_DOUBLE;
	col_vt_[date_idx] = VT_DATE;
	col_vt_[ts_idx] = VT_TS;

	FlatBufferBuilder fbb (256);
	rows_.reserve (f.row_count);
	for (std::size_t r = 0; r < f.row_count; r++)
	  {
	    rows_.push_back (build_fl_row (fbb, decode_int (f.cols[int_idx], r), decode_bigint (f.cols[bigint_idx], r),
					   decode_double (f.cols[double_idx], r),
					   decode_date (f.cols[date_idx], r), decode_utime (f.cols[ts_idx], r)));
	  }
	return true;
      }

      /* generic 3-way compare of fixture column `ci` between two prepared rows, dispatching on
       * that column's dbtype family — numerically equivalent to data_cmpdisk() for these types. */
      int
      cmp_fl_field (const fixture &f, std::size_t ci, std::size_t row_a, std::size_t row_b) const
      {
	flatbuffers::voffset_t vt = col_vt_[ci];
	const Table *ta = root_of (rows_[row_a]);
	const Table *tb = root_of (rows_[row_b]);
	switch (fl_family_of (f.cols[ci].dbtype))
	  {
	  case fl_family::I64:
	    {
	      std::int64_t a = ta->GetField<std::int64_t> (vt, 0), b = tb->GetField<std::int64_t> (vt, 0);
	      return a < b ? -1 : (a > b ? 1 : 0);
	    }
	  case fl_family::F64:
	    {
	      double a = ta->GetField<double> (vt, 0.0), b = tb->GetField<double> (vt, 0.0);
	      return a < b ? -1 : (a > b ? 1 : 0);
	    }
	  case fl_family::I32:
	  default:
	    {
	      std::int32_t a = ta->GetField<std::int32_t> (vt, 0), b = tb->GetField<std::int32_t> (vt, 0);
	      return a < b ? -1 : (a > b ? 1 : 0);
	    }
	  }
      }

      cell_result
      run_fl_filter (const fixture &f)
      {
	std::size_t bound_row = bound_row_of (f.row_count);
	std::vector<char> digest_bytes;
	std::uint64_t survivors = 0;

	auto t0 = std::chrono::steady_clock::now ();
	for (std::size_t r = 0; r < f.row_count; r++)
	  {
	    bool ok = true;
	    for (std::size_t ci = 0; ci < f.cols.size (); ci++)
	      {
		if (cmp_fl_field (f, ci, r, bound_row) > 0)
		  {
		    ok = false;
		    break;
		  }
	      }
	    if (ok)
	      {
		for (std::size_t ci = 0; ci < f.cols.size (); ci++)
		  {
		    append_u64_native (digest_bytes, deform_fl_inline (f.cols[ci].dbtype, f.cols[ci].vals[r].data ()));
		  }
		survivors++;
	      }
	  }
	auto t1 = std::chrono::steady_clock::now ();

	cell_result res;
	res.elapsed_us = (std::uint64_t) std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ();
	res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
	res.aux_counter_a = survivors;
	return res;
      }

      cell_result
      run_fl_sort (const fixture &f)
      {
	std::vector<std::size_t> order (f.row_count);
	for (std::size_t r = 0; r < f.row_count; r++)
	  {
	    order[r] = r;
	  }

	auto t0 = std::chrono::steady_clock::now ();
	std::stable_sort (order.begin (), order.end (), [this, &f] (std::size_t a, std::size_t b)
			   {
			     for (std::size_t ci = 0; ci < f.cols.size (); ci++)
			       {
				 int c = cmp_fl_field (f, ci, a, b);
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
	res.elapsed_us = (std::uint64_t) std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ();
	res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
	return res;
      }

      /* ---- VARCHAR (CV_SORT/CV_MERGE compressed 300B, UV_PEEK uncompressed 100B) ---- */

      bool
      prepare_varchar (const fixture &f, int precision)
      {
	if (f.cols.empty ())
	  {
	    return false;
	  }
	const serialized_column &col = f.cols[0];

	TP_DOMAIN *domain = tp_domain_construct (DB_TYPE_VARCHAR, nullptr, precision, 0, nullptr);
	if (domain == nullptr)
	  {
	    return false;
	  }

	FlatBufferBuilder fbb (512);
	rows_.reserve (f.row_count);
	for (std::size_t r = 0; r < f.row_count; r++)
	  {
	    std::vector<char> bytes = decode_varchar (col, r, domain);
	    rows_.push_back (build_bytes_row (fbb, bytes.data (), bytes.size ()));
	  }
	return true;
      }

      cell_result
      run_cv_sort (const fixture &f)
      {
	const serialized_column &col = f.cols[0];
	std::vector<std::size_t> order (f.row_count);
	for (std::size_t r = 0; r < f.row_count; r++)
	  {
	    order[r] = r;
	  }

	auto t0 = std::chrono::steady_clock::now ();
	std::stable_sort (order.begin (), order.end (), [this] (std::size_t a, std::size_t b)
			   {
			     return varchar_less (rows_[a], rows_[b]);
			   });
	auto t1 = std::chrono::steady_clock::now ();

	std::vector<char> digest_bytes;
	for (std::size_t r : order)
	  {
	    append_bytes (digest_bytes, col.vals[r].data (), col.vals[r].size ());
	  }

	cell_result res;
	res.elapsed_us = (std::uint64_t) std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ();
	res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
	return res;
      }

      cell_result
      run_cv_merge (const fixture &f)
      {
	const serialized_column &col = f.cols[0];
	std::vector<std::size_t> order (f.row_count);
	for (std::size_t r = 0; r < f.row_count; r++)
	  {
	    order[r] = r;
	  }

	auto t0 = std::chrono::steady_clock::now ();
	std::stable_sort (order.begin (), order.end (), [this] (std::size_t a, std::size_t b)
			   {
			     return varchar_less (rows_[a], rows_[b]);
			   });

	std::vector<char> digest_bytes;
	std::uint64_t distinct = 0;
	for (std::size_t i = 0; i < order.size (); i++)
	  {
	    bool is_dup = i > 0 && varchar_equal (rows_[order[i]], rows_[order[i - 1]]);
	    if (!is_dup)
	      {
		append_bytes (digest_bytes, col.vals[order[i]].data (), col.vals[order[i]].size ());
		distinct++;
	      }
	  }
	auto t1 = std::chrono::steady_clock::now ();

	cell_result res;
	res.elapsed_us = (std::uint64_t) std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ();
	res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
	res.aux_counter_a = distinct;
	return res;
      }

      cell_result
      run_uv_peek (const fixture &f)
      {
	const serialized_column &col = f.cols[0];
	std::size_t bound_row = bound_row_of (f.row_count);

	std::vector<char> digest_bytes;
	std::uint64_t survivors = 0;

	auto t0 = std::chrono::steady_clock::now ();
	for (std::size_t r = 0; r < f.row_count; r++)
	  {
	    if (varchar_le_bound (r, bound_row))
	      {
		varchar_view v = peek_varchar (col.vals[r].data (), col.lengths[r]);
		append_bytes (digest_bytes, v.data, static_cast<std::size_t> (v.len));
		survivors++;
	      }
	  }
	auto t1 = std::chrono::steady_clock::now ();

	cell_result res;
	res.elapsed_us = (std::uint64_t) std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ();
	res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
	res.aux_counter_a = survivors;
	return res;
      }

      bool
      varchar_le_bound (std::size_t row, std::size_t bound_row) const
      {
	if (row == bound_row)
	  {
	    return true;
	  }
	return varchar_less (rows_[row], rows_[bound_row]) || varchar_equal (rows_[row], rows_[bound_row]);
      }

      /* ---- NUMERIC(15,2) — raw disk bytes carried verbatim, decoded only at use ---- */

      bool
      prepare_numeric_passthrough (const fixture &f)
      {
	if (f.cols.empty ())
	  {
	    return false;
	  }
	numeric_domain_ = tp_domain_construct (DB_TYPE_NUMERIC, nullptr, 15, 2, nullptr);
	if (numeric_domain_ == nullptr)
	  {
	    return false;
	  }

	const serialized_column &col = f.cols[0];
	FlatBufferBuilder fbb (64);
	rows_.reserve (f.row_count);
	for (std::size_t r = 0; r < f.row_count; r++)
	  {
	    rows_.push_back (build_bytes_row (fbb, col.vals[r].data (), (std::size_t) col.lengths[r]));
	  }
	return true;
      }

      cell_result
      run_num_sort (const fixture &f)
      {
	const serialized_column &col = f.cols[0];
	std::vector<std::size_t> order (f.row_count);
	for (std::size_t r = 0; r < f.row_count; r++)
	  {
	    order[r] = r;
	  }

	auto t0 = std::chrono::steady_clock::now ();
	std::stable_sort (order.begin (), order.end (), [this] (std::size_t a, std::size_t b)
			   {
			     auto va = bytes_vector_of (root_of (rows_[a]));
			     auto vb = bytes_vector_of (root_of (rows_[b]));
			     int cmp = tp_Type_numeric->data_cmpdisk (va->data (), vb->data (), numeric_domain_, 0, 1,
									nullptr);
			     return cmp < 0;
			   });
	auto t1 = std::chrono::steady_clock::now ();

	std::vector<char> digest_bytes;
	for (std::size_t r : order)
	  {
	    append_bytes (digest_bytes, col.vals[r].data (), col.vals[r].size ());
	  }

	cell_result res;
	res.elapsed_us = (std::uint64_t) std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ();
	res.digest = fnv1a (digest_bytes.data (), digest_bytes.size (), f.seed);
	return res;
      }

      cell_result
      run_num_agg_input (const fixture &f)
      {
	cell_result res;
	if (f.row_count == 0)
	  {
	    return res;
	  }
	const serialized_column &col = f.cols[0];

	DB_VALUE acc;
	db_make_null (&acc);

	auto t0 = std::chrono::steady_clock::now ();
	for (std::size_t r = 0; r < f.row_count; r++)
	  {
	    auto vec = bytes_vector_of (root_of (rows_[r]));
	    OR_BUF buf;
	    or_init (&buf, reinterpret_cast<char *> (const_cast<std::uint8_t *> (vec->data ())),
		     (int) vec->size ());
	    DB_VALUE cur;
	    db_make_null (&cur);
	    (void) tp_Type_numeric->data_readval (&buf, &cur, numeric_domain_, (int) vec->size (), false, nullptr, 0);

	    if (r == 0)
	      {
		acc = cur;
	      }
	    else
	      {
		DB_VALUE next;
		db_make_null (&next);
		(void) numeric_db_value_add (&acc, &cur, &next);
		pr_clear_value (&acc);
		pr_clear_value (&cur);
		acc = next;
	      }
	  }
	auto t1 = std::chrono::steady_clock::now ();
	res.elapsed_us = (std::uint64_t) std::chrono::duration_cast<std::chrono::microseconds> (t1 - t0).count ();
	pr_clear_value (&acc);

	/* digest is an untimed, independent re-derivation straight from the fixture's raw bytes
	 * (the int64 unscaled-value proxy sum) — matches variant_cmpdisk.cpp bit for bit,
	 * regardless of this variant's own accumulation mechanism above. */
	std::int64_t proxy_sum = 0;
	for (std::size_t r = 0; r < f.row_count; r++)
	  {
	    std::int64_t proxy = 0;
	    if (numeric_int64_proxy (reinterpret_cast<const unsigned char *> (col.vals[r].data ()), proxy))
	      {
		proxy_sum += proxy;
	      }
	  }
	res.digest = fnv1a (&proxy_sum, sizeof (proxy_sum), f.seed);
	res.aux_counter_a = col.vals.size ();
	return res;
      }

      std::vector<DetachedBuffer> rows_;
      std::vector<flatbuffers::voffset_t> col_vt_;
      TP_DOMAIN *numeric_domain_ = nullptr;
  };

  variant *
  make_variant_c_flatbuffers ()
  {
    return new variant_c_flatbuffers ();
  }
}
