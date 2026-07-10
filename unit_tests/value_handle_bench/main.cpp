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
 * `RELEASE_STRING` is embedded) but sources the value from `git rev-parse --short HEAD` inside
 * value_handle_bench's own CMakeLists.txt, exposed here as the `VHB_CUBRID_REL_SHA` macro, with a
 * documented fallback to `rel_build_number()`/`rel_build_os()` (release_string.c's real accessors)
 * if git metadata is unavailable at configure time (e.g. a tarball checkout with no `.git`).
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
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iostream>
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
      << "                  7, always preceded by 2 discarded warmup runs)\n";
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
	    opts.iters = std::atoi (arg.substr (8).c_str ());
	    if (opts.iters < 1)
	      {
		std::cerr << "test_value_handle_bench: --iters must be >= 1\n";
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

  /* D-G5 default: SORT cells use HIGH entropy; every other cell uses LOW (parity/filter
   * fixtures, per value_handle_bench.hpp's `entropy` doc comment). */
  vhb::entropy
  entropy_for_cell (cell_id c, const vhb::run_options &opt)
  {
    switch (c)
      {
      case cell_id::CV_SORT:
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

  void
  append_summary_csv (std::ofstream &out, const measured &m)
  {
    if (m.skipped)
      {
	out << m.variant_name << "," << m.cell << ",SKIPPED,SKIPPED,BEST-CASE-UPPER-BOUND\n";
	return;
      }
    out << m.variant_name << "," << m.cell << "," << m.median_us << "," << m.cov_pct
	<< ",BEST-CASE-UPPER-BOUND\n";
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

  if (opts.parity)
    {
      return run_parity (registry, cells, run_opt);
    }

  std::ofstream results_csv (run_opt.results_csv);
  std::ofstream summary_csv (run_opt.summary_csv);
  results_csv << "variant,matrix_cell,iteration,elapsed_us,cubrid_rel_sha\n";
  summary_csv << "variant,matrix_cell,median_us,cov_pct,label\n";

  std::size_t row_override = opts.smoke ? 1000 : 0;

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
			<< "us CoV=" << m.cov_pct << "% digest=" << m.digest << "\n";
	    }
	  append_results_csv (results_csv, m);
	  append_summary_csv (summary_csv, m);
	}
    }

  return 0;
}
