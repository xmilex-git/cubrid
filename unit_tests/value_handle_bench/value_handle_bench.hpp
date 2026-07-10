/*
 * value_handle_bench.hpp — shared contract for the 3-variant value-representation microbench.
 *
 * Stage-1 P2 deliverable (ralplan P2.1-P2.4 + grilled P2.2/P2.3 deltas).
 * BENCH-ONLY code: nothing here ships in the server. Numbers produced under this harness are
 * BEST-CASE-UPPER-BOUND by definition (no page I/O, no locking, no server plumbing).
 *
 * CONTRACT OWNERSHIP: this header is the leader-fixed interface. Implementers extend behind it;
 * signature changes require IRC agreement across all bench workers.
 */

#ifndef _VALUE_HANDLE_BENCH_HPP_
#define _VALUE_HANDLE_BENCH_HPP_

#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>

namespace vhb
{
  /* ---- cells (ralplan P2.3 matrix + grilled P2.3+ abbrev sub-cell) ---- */
  enum class cell_id
  {
    FL_FILTER,       /* INT,BIGINT,DOUBLE,DATE,TIMESTAMP — range filter, 1M rows            */
    FL_SORT,         /* same types — full sort, 1M rows                                     */
    CV_SORT,         /* VARCHAR(300) compressed (ratio >= 2:1) — full sort, 500K rows       */
    CV_MERGE,        /* VARCHAR(300) compressed — merge/distinct, 500K rows, 50% dup        */
    UV_PEEK,         /* VARCHAR(100) uncompressed — filter+peek, 500K rows                  */
    NUM_SORT,        /* NUMERIC(15,2) — full sort, 500K rows                                */
    NUM_AGG_INPUT,   /* NUMERIC(15,2) — aggregate input, 500K rows                          */
    PEEK_VS_COPY,    /* INT + VARCHAR(100) uncompressed — filter, 1M rows, counter cell     */
    ABBREV_SUBCELL,  /* grilled D-G5: varchar 8B-prefix proxy vs full compare (upside only) */
    CELL_COUNT
  };

  const char *cell_name (cell_id c);

  /* ---- fixture ----
   * Rows are pre-serialized into CUBRID's real on-disk value encodings (data_writeval /
   * pr_type writeval through an OR_BUF), so every variant consumes genuine disk images.
   * One column per cell type; fixed-length cells carry 5 parallel columns (one per FL type).
   * sort_entropy: LOW = CHR(65+i%26)||LPAD filler (parity/filter fixtures, deterministic
   * row counts); HIGH = leading 8 pseudo-random chars (grilled D-G5 fixture fix, SORT cells only).
   */
  enum class entropy { LOW, HIGH };

  struct serialized_column
  {
    int dbtype;                          /* DB_TYPE enum value                      */
    int precision;                       /* NUMERIC/VARCHAR precision, else 0       */
    std::vector<std::vector<char>> vals; /* per-row serialized disk image           */
    std::vector<int> lengths;            /* per-row image length                    */
  };

  struct fixture
  {
    std::size_t row_count = 0;
    std::vector<serialized_column> cols;
    entropy sort_entropy = entropy::LOW;
    std::uint32_t seed = 20260709;       /* deterministic PRNG seed                 */
  };

  /* build the fixture for a cell (row counts per the matrix above; smoke=1000-row override) */
  fixture build_fixture (cell_id c, entropy e, std::size_t row_override /* 0 = matrix default */);

  /* ---- per-cell result ----
   * digest: order-sensitive FNV-1a over the cell's OUTPUT sequence (sorted rows / filter
   * survivors / merge output / aggregate value bytes). Variant pairs MUST match per cell
   * (P2.4 mini-parity ASSERT_EQ). elapsed_us covers the OPERATION only (fixture build and
   * variant prepare() excluded).
   */
  struct cell_result
  {
    std::uint64_t digest = 0;
    std::uint64_t elapsed_us = 0;
    std::uint64_t aux_counter_a = 0;     /* PEEK_VS_COPY: peek count; else variant-defined */
    std::uint64_t aux_counter_b = 0;     /* PEEK_VS_COPY: copy count; else variant-defined */
  };

  /* ---- variant interface ---- */
  class variant
  {
  public:
    virtual ~variant () = default;
    virtual const char *name () const = 0;             /* "A-slot" | "A-handle" | "B-cmpdisk" | "C-flatbuffers" */
    /* one-time per-fixture preparation (variant-private representation build).
     * Cost is NOT measured — it models scan-open/ingestion setup; per-row ingestion work that
     * the design bills per-row (e.g. A-slot deform, detoast-once at sort entry) MUST happen
     * inside run_cell, not here. Returns false if the variant cannot run this cell
     * (C-flatbuffers is allowed to bow out of counter cells; record as SKIPPED). */
    virtual bool prepare (cell_id c, const fixture &f) = 0;
    virtual cell_result run_cell (cell_id c, const fixture &f) = 0;
  };

  /* registry: implementations self-register via this hook (each variant .cpp defines one) */
  variant *make_variant_a_slot ();      /* variant_valueslot.cpp   */
  variant *make_variant_a_handle ();    /* variant_pervalue.cpp    */
  variant *make_variant_b_cmpdisk ();   /* variant_cmpdisk.cpp     */
  variant *make_variant_c_flatbuffers ();/* variant_flatbuffers.cpp; may return nullptr if the
                                          * 4h-bounded flatbuffers effort failed (R6) — harness
                                          * then records FLATBUFFERS_UNAVAILABLE and continues. */

  /* ---- measurement protocol (SSOT §2) ----
   * warmup 2 (discarded) + measured iterations >= 7; median; CoV = stdev/mean*100 must be
   * <= 15% per (variant, cell); every CSV row carries cubrid_rel sha + BEST-CASE-UPPER-BOUND.
   * CSV schemas (ralplan P2.1):
   *   bench_results.csv: variant,matrix_cell,iteration,elapsed_us,cubrid_rel_sha
   *   bench_summary.csv: variant,matrix_cell,median_us,cov_pct,label
   */
  struct run_options
  {
    int warmup = 2;
    int iterations = 7;
    bool smoke = false;                  /* 1000-row fixtures, parity only            */
    entropy sort_cell_entropy = entropy::HIGH; /* D-G5 default for SORT cells         */
    std::string results_csv = "bench_results.csv";
    std::string summary_csv = "bench_summary.csv";
  };

  std::uint64_t fnv1a (const void *data, std::size_t len, std::uint64_t seed);

  /* CLI (main.cpp): --help | --all | --parity | --smoke | --cell=<name> | --iters=N */
}
#endif /* _VALUE_HANDLE_BENCH_HPP_ */
