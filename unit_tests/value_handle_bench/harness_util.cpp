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
 * harness_util.cpp — vhb::cell_name()/vhb::fnv1a() (value_handle_bench.hpp contract).
 * Owned by 30-P2-Harness.
 *
 * fnv1a() is the digest primitive every variant (A-slot / A-handle / B-cmpdisk / C-flatbuffers)
 * uses to compute cell_result::digest for P2.4 mini-parity. The function itself is a plain
 * chainable FNV-1a 64-bit (any caller may thread a running hash across multiple calls by
 * passing the previous return value back in as `seed`), but the CROSS-VARIANT CONVENTION for
 * *how* it is applied is fixed by variant_valueslot.cpp/variant_pervalue.cpp (the reference
 * pair, which commit to matching each other "bit for bit") and re-derived into
 * variant_cmpdisk.cpp — see that file's header comment for the authoritative, per-cell
 * byte-layout description. Summary:
 *
 *   - ONE fnv1a() call per cell, over a single buffer built from the cell's OUTPUT sequence in
 *     order (not a running chain across many small calls).
 *   - seed = f.seed (the fixture's own deterministic PRNG seed), never a fixed constant.
 *   - the buffer's byte layout is cell-specific (native-endian deformed FL cells for
 *     FL_FILTER/FL_SORT; original fixture-serialized bytes for CV_SORT/CV_MERGE/
 *     ABBREV_SUBCELL/NUM_SORT; header-stripped peeked content for UV_PEEK/PEEK_VS_COPY; the raw
 *     bytes of an accumulated int64 proxy for NUM_AGG_INPUT) — never a variant-INTERNAL
 *     representation, so the digest is comparable across variants with entirely different
 *     mechanisms (ValueSlot arrays / per-value handles / disk-direct cmpdisk / flatbuffers) as
 *     long as each variant's row-identity/order (or aggregate result) is correct, which is
 *     exactly what the P2.4 parity ASSERT_EQ is meant to catch.
 *   - every filter/peek predicate's bound value is the fixture's OWN row at index row_count/3
 *     (or row 0 if row_count<3) — never a synthetic constant.
 */

#include "value_handle_bench.hpp"

namespace vhb
{
  const char *
  cell_name (cell_id c)
  {
    switch (c)
      {
      case cell_id::FL_FILTER:
	return "FL_FILTER";
      case cell_id::FL_SORT:
	return "FL_SORT";
      case cell_id::CV_SORT:
	return "CV_SORT";
      case cell_id::CV_MERGE:
	return "CV_MERGE";
      case cell_id::UV_PEEK:
	return "UV_PEEK";
      case cell_id::NUM_SORT:
	return "NUM_SORT";
      case cell_id::NUM_AGG_INPUT:
	return "NUM_AGG_INPUT";
      case cell_id::PEEK_VS_COPY:
	return "PEEK_VS_COPY";
      case cell_id::ABBREV_SUBCELL:
	return "ABBREV_SUBCELL";
      case cell_id::CELL_COUNT:
      default:
	return "UNKNOWN_CELL";
      }
  }

  std::uint64_t
  fnv1a (const void *data, std::size_t len, std::uint64_t seed)
  {
    /* FNV-1a 64-bit. `seed` doubles as the running hash, so repeated calls with the previous
     * call's return value chain into a single order-sensitive digest over a whole sequence —
     * but the cross-variant convention (see the file header comment) is one call per cell over
     * one concatenated buffer, seeded from fixture.seed, not a running per-item chain. */
    constexpr std::uint64_t FNV_PRIME_64 = 1099511628211ULL;
    std::uint64_t h = seed;
    const unsigned char *p = static_cast<const unsigned char *> (data);
    for (std::size_t i = 0; i < len; i++)
      {
	h ^= static_cast<std::uint64_t> (p[i]);
	h *= FNV_PRIME_64;
      }
    return h;
  }
} // namespace vhb
