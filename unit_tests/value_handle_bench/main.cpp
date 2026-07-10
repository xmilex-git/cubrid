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
 * main.cpp — CLI, variant registry, measurement loop, CSV writers (value_handle_bench.hpp
 * contract). Owned by 30-P2-Harness.
 *
 * cubrid_rel sha choice: release_string.c's own mechanism (makestring()-wrapped, CMake-injected
 * `#define`s such as RELEASE_STRING/BUILD_NUMBER, see cmake/version.h.cmake) never carries an
 * actual git commit SHA — it is a version-number string, not a revision identity. Since the CSV
 * schema explicitly wants a "cubrid_rel sha", this harness follows the *same mechanism*
 * (CMake-time `execute_process()` embeds a value into a compile definition, exactly like
 * `RELEASE_STRING` is embedded) but sources the value from `git describe --always --dirty`
 * inside value_handle_bench's own CMakeLists.txt (falling back to the plain short SHA whenever
 * `--always` itself can't produce anything, e.g. no `.git` at configure time), exposed here as
 * the `VHB_CUBRID_REL_SHA` macro, with a further documented fallback to
 * `rel_build_number()`/`rel_build_os()` (release_string.c's real accessors) if git metadata is
 * unavailable at configure time at all (e.g. a tarball checkout with no `.git`).
 *
 * Variant registry ownership: `make_variant_*()` factories are NOT guaranteed to return a
 * heap-allocated (`new`-owned) object — some implementations return a pointer to a function-local
 * `static` singleton instead. The registry below therefore holds plain non-owning `variant *`
 * and never deletes them (they live for the process's lifetime either way, `static` or
 * intentionally leaked); do not wrap these in `unique_ptr`/`shared_ptr`.
 */

#include "value_handle_bench.hpp"

#include "dbtype.h"
#include "error_code.h"
#include "language_support.h"
#include "object_domain.h"
#include "release_string.h"
#include "thread_compat.hpp"
#include "thread_manager.hpp"

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#if !defined (VHB_CUBRID_REL_SHA)
#define VHB_CUBRID_REL_SHA "unknown"
#endif

namespace
{
  using vhb::cell_id;
  using vhb::variant;

  struct cli_options
  {
    bool help = false;
    bool run_all = false;
    bool parity = false;
    bool smoke = false;
    bool cell_selected = false;
    cell_id selected_cell = cell_id::FL_FILTER;
    int iters = 0; /* 0 = leave run_options' protocol default (7) untouched */
    bool strict_cov = false; /* --strict-cov: COV_VIOLATION rows make the process exit nonzero */
    bool reverse = false; /* --reverse: run variants in reverse registry order (order-balanced headline runs) */
  };

  void
  print_help ()
  {
    std::cout
      << "test_value_handle_bench -- P2 value-representation microbench (value_handle_bench.hpp)\n"
      << "\n"
      << "Usage: test_value_handle_bench [options]\n"
      << "  --help          show this help and exit 0\n"
      << "  --all           run every cell against every registered variant (default mode when\n"
      << "                  no other mode flag is given)\n"
      << "  --parity        run every registered/available variant against every selected cell\n"
      << "                  at smoke (1000-row) fixture size and ASSERT digest equality per cell\n"
      << "                  (P2.4 mini-parity); prints 'PARITY: N/N cells PASS'\n"
      << "  --smoke         use the 1000-row smoke fixture override instead of matrix row counts\n"
      << "                  (--parity always implies this, regardless of whether it is also given)\n"
      << "  --cell=<name>   restrict the run to one cell (e.g. --cell=FL_FILTER); last one wins\n"
      << "  --iters=N       measured iterations per (variant,cell), N >= 1 (protocol default is\n"
      << "                  7, always preceded by 2 discarded warmup runs)\n"
      << "  --strict-cov    exit nonzero if any (variant,cell) row's CoV exceeds the 15% protocol\n"
      << "                  ceiling (rows are always flagged ';COV_VIOLATION' in the summary CSV\n"
      << "                  and warned about on stderr regardless of this flag; this flag only\n"
      << "                  changes the process exit code)\n"
      << "  --reverse       run the variants in REVERSE registry order (C,B,A-handle,A-slot); the\n"
      << "                  official headline protocol pools forward and reverse process runs\n"
      << "                  (e.g. F,R,R,F) so no variant's number depends on a fixed position\n";
  }

  bool
  cell_from_name (const std::string &name, cell_id &out)
  {
    for (int i = 0; i < static_cast<int> (cell_id::CELL_COUNT); i++)
      {
	cell_id c = static_cast<cell_id> (i);
	if (name == vhb::cell_name (c))
	  {
	    out = c;
	    return true;
	  }
      }
    return false;
  }

  /* strtol()-based --iters=N parser: rejects empty input, trailing garbage, out-of-int-range
   * values, and non-positive counts, instead of atoi()'s silent-0-on-garbage behavior. */
  bool
  parse_iters (const std::string &val, int &out)
  {
    if (val.empty ())
      {
	return false;
      }
    errno = 0;
    char *endp = nullptr;
    long parsed = std::strtol (val.c_str (), &endp, 10);
    if (endp != val.c_str () + val.size () || errno == ERANGE || parsed < 1
	|| parsed > static_cast<long> (std::numeric_limits<int>::max ()))
      {
	return false;
      }
    out = static_cast<int> (parsed);
    return true;
  }

  bool
  parse_args (int argc, char **argv, cli_options &opts)
  {
    for (int i = 1; i < argc; i++)
      {
	std::string arg = argv[i];
	if (arg == "--help")
	  {
	    opts.help = true;
	  }
	else if (arg == "--all")
	  {
	    opts.run_all = true;
	  }
	else if (arg == "--parity")
	  {
	    opts.parity = true;
	  }
	else if (arg == "--smoke")
	  {
	    opts.smoke = true;
	  }
	else if (arg == "--strict-cov")
	  {
	    opts.strict_cov = true;
	  }
	else if (arg == "--reverse")
	  {
	    opts.reverse = true;
	  }
	else if (arg.rfind ("--cell=", 0) == 0)
	  {
	    std::string name = arg.substr (7);
	    cell_id c;
	    if (!cell_from_name (name, c))
	      {
		std::cerr << "test_value_handle_bench: unknown cell '" << name << "'\n";
		return false;
	      }
	    opts.cell_selected = true;
	    opts.selected_cell = c;
	  }
	else if (arg.rfind ("--iters=", 0) == 0)
	  {
	    std::string val = arg.substr (8);
	    if (!parse_iters (val, opts.iters))
	      {
		std::cerr << "test_value_handle_bench: --iters must be an integer >= 1 (got '" << val << "')\n";
		return false;
	      }
	  }
	else
	  {
	    std::cerr << "test_value_handle_bench: unrecognized option '" << arg << "'\n";
	    return false;
	  }
      }
    return true;
  }

  int
  init_cubrid_modules ()
  {
    THREAD_ENTRY *thread_p = NULL;
    lang_init ();
    tp_init ();
    lang_set_charset_lang ("en_US.iso88591");
    cubthread::initialize (thread_p);
    return cubthread::initialize_thread_entries ();
  }

  std::vector<variant *>
  make_registry ()
  {
    std::vector<variant *> reg;
    variant *v;
    if ((v = vhb::make_variant_a_slot ()) != nullptr)
      {
	reg.push_back (v);
      }
    if ((v = vhb::make_variant_a_handle ()) != nullptr)
      {
	reg.push_back (v);
      }
    if ((v = vhb::make_variant_b_cmpdisk ()) != nullptr)
      {
	reg.push_back (v);
      }
    if ((v = vhb::make_variant_c_flatbuffers ()) != nullptr)
      {
	reg.push_back (v);
      }
    else
      {
	std::cout << "FLATBUFFERS_UNAVAILABLE: make_variant_c_flatbuffers() returned nullptr; "
		     "continuing without C-flatbuffers\n";
      }
    return reg;
  }

  double
  median_of (std::vector<std::uint64_t> v)
  {
    std::sort (v.begin (), v.end ());
    std::size_t n = v.size ();
    if (n == 0)
      {
	return 0.0;
      }
    if (n % 2 == 1)
      {
	return static_cast<double> (v[n / 2]);
      }
    return (static_cast<double> (v[n / 2 - 1]) + static_cast<double> (v[n / 2])) / 2.0;
  }

  double
  cov_pct_of (const std::vector<std::uint64_t> &v)
  {
    if (v.size () < 2)
      {
	return 0.0;
      }
    double mean = 0.0;
    for (std::uint64_t x : v)
      {
	mean += static_cast<double> (x);
      }
    mean /= static_cast<double> (v.size ());
    if (mean == 0.0)
      {
	return 0.0;
      }
    double var = 0.0;
    for (std::uint64_t x : v)
      {
	double d = static_cast<double> (x) - mean;
	var += d * d;
      }
    var /= static_cast<double> (v.size () - 1);
    return std::sqrt (var) / mean * 100.0;
  }

  constexpr double COV_PROTOCOL_CEILING_PCT = 15.0;

  /* D-G5 default: SORT cells use HIGH entropy; every other cell uses LOW (parity/filter
   * fixtures, per value_handle_bench.hpp's `entropy` doc comment).
   *
   * [HIGH-2 fix, architect finding] CV_MERGE now routes to the same configured entropy as the
   * SORT cells (HIGH by default) instead of being forced to LOW. LOW entropy's fixture content is
   * `CHR(65 + content_key % 26) + filler` (fixture.cpp's fill_compressible_content()) — only 26
   * distinct leading bytes — which silently collapsed CV_MERGE's 500K/250K (smoke: 1000/500)
   * *distinct* duplicate-pair content_keys down to 26 *actually distinct* row values well before
   * the merge/distinct operation under test ever ran, making the cell's real 50%-duplicate-pair
   * shape unmeasurable. HIGH entropy's 8B pseudo-random prefix is already seeded from
   * `content_key` (fixture.cpp's fill_compressible_content(), HIGH branch), which is exactly
   * `i/2` for CV_MERGE's `duplicate_pairs=true` fixture — so switching entropy here preserves the
   * 50%-dup-pair structure (rows 2k/2k+1 still get byte-identical content) while giving the merge
   * a realistic, non-degenerate distinct-value cardinality (~rows/2). */
  vhb::entropy
  entropy_for_cell (cell_id c, const vhb::run_options &opt)
  {
    switch (c)
      {
      case cell_id::CV_SORT:
      case cell_id::CV_MERGE:
      case cell_id::NUM_SORT:
      case cell_id::ABBREV_SUBCELL:
	return opt.sort_cell_entropy;
      default:
	return vhb::entropy::LOW;
      }
  }

  struct measured
  {
    std::string variant_name;
    std::string cell;
    std::vector<std::uint64_t> iter_us;
    double median_us = 0.0;
    double cov_pct = 0.0;
    std::uint64_t digest = 0;
    std::uint64_t aux_a = 0; /* last iteration's cell_result::aux_counter_a (variant/cell-defined) */
    std::uint64_t aux_b = 0; /* last iteration's cell_result::aux_counter_b (variant/cell-defined) */
    bool skipped = false;
  };

  measured
  measure_one (variant &v, cell_id c, const vhb::fixture &f, const vhb::run_options &opt)
  {
    measured m;
    m.variant_name = v.name ();
    m.cell = vhb::cell_name (c);

    if (!v.prepare (c, f))
      {
	m.skipped = true;
	return m;
      }

    for (int i = 0; i < opt.warmup; i++)
      {
	(void) v.run_cell (c, f); /* warmup: discarded by protocol */
      }

    for (int i = 0; i < opt.iterations; i++)
      {
	vhb::cell_result r = v.run_cell (c, f);
	m.iter_us.push_back (r.elapsed_us);
	m.digest = r.digest;
	m.aux_a = r.aux_counter_a;
	m.aux_b = r.aux_counter_b;
      }
    m.median_us = median_of (m.iter_us);
    m.cov_pct = cov_pct_of (m.iter_us);
    return m;
  }

  void
  append_results_csv (std::ofstream &out, const measured &m)
  {
    for (std::size_t i = 0; i < m.iter_us.size (); i++)
      {
	out << m.variant_name << "," << m.cell << "," << (i + 1) << "," << m.iter_us[i] << ","
	    << VHB_CUBRID_REL_SHA << "\n";
      }
  }

  /* [MED-2, LOW-a fixes, architect findings] summary label composition:
   *   - 'INGESTION-EXEMPT' is appended for C-flatbuffers on CV_SORT/CV_MERGE/UV_PEEK — the three
   *     cells whose prepare() fully decodes/decompresses the fixture's VARCHAR content (untimed)
   *     on top of the FlatBuffer-building cost every cell already excludes from timing (see
   *     variant_flatbuffers.cpp's file-header comment for the full rationale).
   *   - 'COV_VIOLATION' is appended whenever the row's CoV exceeds the SSOT §2 protocol ceiling
   *     (15%) — always, independent of --strict-cov (which only controls the process exit code).
   * Neither addition changes the CSV's column count — both live inside the existing `label`
   * column, semicolon-joined. */
  std::string
  summary_label (const std::string &variant_name, cell_id c, double cov_pct)
  {
    std::string label = "BEST-CASE-UPPER-BOUND";

    bool ingestion_exempt = variant_name == "C-flatbuffers"
      && (c == cell_id::CV_SORT || c == cell_id::CV_MERGE || c == cell_id::UV_PEEK);
    if (ingestion_exempt)
      {
	label += ";INGESTION-EXEMPT";
      }

    if (cov_pct > COV_PROTOCOL_CEILING_PCT)
      {
	label += ";COV_VIOLATION";
      }

    return label;
  }

  void
  append_summary_csv (std::ofstream &out, const measured &m, cell_id c)
  {
    if (m.skipped)
      {
	out << m.variant_name << "," << m.cell << ",SKIPPED,SKIPPED,BEST-CASE-UPPER-BOUND\n";
	return;
      }
    out << m.variant_name << "," << m.cell << "," << m.median_us << "," << m.cov_pct << ","
	<< summary_label (m.variant_name, c, m.cov_pct) << "\n";
  }

  int
  run_parity (std::vector<variant *> &registry, const std::vector<cell_id> &cells, const vhb::run_options &run_opt)
  {
    std::size_t pass = 0;
    std::size_t total = 0;

    for (cell_id c : cells)
      {
	vhb::entropy e = entropy_for_cell (c, run_opt);
	vhb::fixture f = vhb::build_fixture (c, e, /* row_override */ 1000); /* parity is always smoke-sized */

	bool have_reference = false;
	std::uint64_t reference_digest = 0;
	bool cell_pass = true;
	bool any_ran = false;

	for (variant *v : registry)
	  {
	    if (!v->prepare (c, f))
	      {
		continue; /* variant bows out of this cell (e.g. C-flatbuffers on a counter cell) */
	      }
	    vhb::cell_result r = v->run_cell (c, f);
	    any_ran = true;
	    if (!have_reference)
	      {
		reference_digest = r.digest;
		have_reference = true;
	      }
	    else if (r.digest != reference_digest)
	      {
		cell_pass = false;
		std::cerr << "PARITY MISMATCH: cell=" << vhb::cell_name (c) << " variant=" << v->name ()
			  << " digest=" << r.digest << " reference=" << reference_digest << "\n";
	      }
	  }

	total++;
	if (any_ran && cell_pass)
	  {
	    pass++;
	  }
      }

    std::cout << "PARITY: " << pass << "/" << total << " cells PASS\n";
    return pass == total ? 0 : 1;
  }
} // namespace

int
main (int argc, char **argv)
{
  cli_options opts;
  if (!parse_args (argc, argv, opts))
    {
      print_help ();
      return 1;
    }
  if (opts.help)
    {
      print_help ();
      return 0;
    }
  if (!opts.run_all && !opts.parity && !opts.cell_selected)
    {
      opts.run_all = true; /* bare invocation defaults to running everything */
    }

  if (init_cubrid_modules () != NO_ERROR)
    {
      std::cerr << "test_value_handle_bench: CUBRID module init failed\n";
      return 1;
    }

  vhb::run_options run_opt;
  run_opt.smoke = opts.smoke;
  if (opts.iters >= 1)
    {
      run_opt.iterations = opts.iters;
    }

  std::vector<cell_id> cells;
  if (opts.cell_selected)
    {
      cells.push_back (opts.selected_cell);
    }
  else
    {
      for (int i = 0; i < static_cast<int> (cell_id::CELL_COUNT); i++)
	{
	  cells.push_back (static_cast<cell_id> (i));
	}
    }

  std::vector<variant *> registry = make_registry ();
  if (registry.empty ())
    {
      std::cerr << "test_value_handle_bench: no variants registered (every factory returned nullptr)\n";
      return 1;
    }
  if (opts.reverse)
    {
      std::reverse (registry.begin (), registry.end ());
    }

  if (opts.parity)
    {
      return run_parity (registry, cells, run_opt);
    }

  std::ofstream results_csv (run_opt.results_csv);
  std::ofstream summary_csv (run_opt.summary_csv);
  if (!results_csv.good () || !summary_csv.good ())
    {
      std::cerr << "test_value_handle_bench: failed to open output CSV file(s) ('" << run_opt.results_csv << "', '"
		<< run_opt.summary_csv << "') for writing\n";
      return 1;
    }
  results_csv << "variant,matrix_cell,iteration,elapsed_us,cubrid_rel_sha\n";
  summary_csv << "variant,matrix_cell,median_us,cov_pct,label\n";

  std::size_t row_override = opts.smoke ? 1000 : 0;
  bool any_cov_violation = false;

  for (cell_id c : cells)
    {
      vhb::entropy e = entropy_for_cell (c, run_opt);
      vhb::fixture f = vhb::build_fixture (c, e, row_override);

      for (variant *v : registry)
	{
	  measured m = measure_one (*v, c, f, run_opt);
	  if (m.skipped)
	    {
	      std::cout << vhb::cell_name (c) << " / " << v->name () << " : SKIPPED\n";
	    }
	  else
	    {
	      std::cout << vhb::cell_name (c) << " / " << v->name () << " : median=" << m.median_us
			<< "us CoV=" << m.cov_pct << "% digest=" << m.digest << " aux_a=" << m.aux_a
			<< " aux_b=" << m.aux_b << "\n";
	      if (m.cov_pct > COV_PROTOCOL_CEILING_PCT)
		{
		  std::cerr << "COV_VIOLATION: variant=" << m.variant_name << " cell=" << m.cell
			    << " cov_pct=" << m.cov_pct << "% (exceeds " << COV_PROTOCOL_CEILING_PCT
			    << "% protocol ceiling)\n";
		  any_cov_violation = true;
		}
	    }
	  append_results_csv (results_csv, m);
	  append_summary_csv (summary_csv, m, c);
	}
    }

  if (opts.strict_cov && any_cov_violation)
    {
      return 1;
    }
  return 0;
}
