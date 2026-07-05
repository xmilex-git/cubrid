#!/usr/bin/env python3
"""
Word-boundary identifier rename for issue #143 slice S1 (and S1b).

Applies the exact OLD -> NEW identifier substitutions decided in issue #143
(design doc §1-A/§1-C, decision R8: OLD -> PGBUF) across an explicit file
whitelist. No fuzzy/broad regexes: only the exact identifiers below are
touched, each matched on word boundaries (\\b) so partial-token collisions
(e.g. unrelated "new_page"-style identifiers) are never affected.

Usage:
    python3 contrib/scripts/rename_wm143_s1.py --commit s1   # pure internal rename
    python3 contrib/scripts/rename_wm143_s1.py --commit s1b  # externally-visible rename

Kept for reproducibility; not part of the build, not invoked by any Makefile
or CI job.
"""
import argparse
import re
import sys

# --- S1: pure internal identifier rename -----------------------------------
S1_PAIRS = [
    ("QFILE_BACKING_OLD", "QFILE_BACKING_PGBUF"),
    ("QFILE_BACKING_NEW", "QFILE_BACKING_TAPESET"),
    ("qfile_list_has_new_backing", "qfile_list_has_tapeset"),
    ("qfile_list_has_old_backing", "qfile_list_has_pgbuf_backing"),
    ("QFILE_GUARD_OLD_MECHANISM", "QFILE_GUARD_PGBUF_MECHANISM"),
    ("QFILE_GUARD_NEW_MECHANISM", "QFILE_GUARD_TAPESET_MECHANISM"),
    ("qfile_list_make_new_backed", "qfile_list_make_tapeset_backed"),
    ("suppress_new_backing", "suppress_tapeset_backing"),
    ("qfile_new_backed_record_create", "qfile_tapeset_backed_record_create"),
    ("qfile_new_backed_create_count", "qfile_tapeset_backed_create_count"),
    ("qfile_new_backed_reset_create_count", "qfile_tapeset_backed_reset_create_count"),
    ("qfile_ae_record_old_touch", "qfile_ae_record_pgbuf_touch"),
    ("qfile_ae_old_touch_count", "qfile_ae_pgbuf_touch_count"),
    ("qfile_ae_reset_old_touch_count", "qfile_ae_reset_pgbuf_touch_count"),
    ("qfile_Ae_old_touch", "qfile_Ae_pgbuf_touch"),
    ("new_contains_overflow_", "tapeset_contains_overflow_"),
    ("QFILE_LIST_ID_NEW_CONTAINS_OVERFLOW", "QFILE_LIST_ID_TAPESET_CONTAINS_OVERFLOW"),
    ("is_new_backing", "has_tapeset"),
    ("m_new_tuple_source", "m_tapeset_source"),
    ("SPILL_OVERFLOW", "PAGE_SPILL"),
    ("spill_flush_page", "page_spill_flush_page"),
    ("qfile_list_is_spill_overflowed", "qfile_list_is_page_spilled"),
]

S1_FILES = [
    "src/query/list_file.c",
    "src/query/list_file.h",
    "src/query/query_list.h",
    "src/query/qfile_tape.cpp",
    "src/query/query_executor.c",
    "src/query/query_hash_join.c",
    "src/query/query_manager.c",
    "src/query/query_manager.h",
    "src/query/temp_page_store.cpp",
    "src/query/temp_page_store.hpp",
    "src/storage/external_sort.c",
    "src/query/parallel/px_hash_join/px_hash_join.cpp",
    "src/query/parallel/px_hash_join/px_hash_join_task_manager.cpp",
    "src/query/parallel/px_scan/px_scan.cpp",
    "src/query/parallel/px_scan/px_scan_input_handler_list.cpp",
    "src/query/parallel/px_scan/px_scan_result_handler.cpp",
    "src/query/parallel/px_scan/px_scan_slot_iterator_list.cpp",
    "src/query/parallel/px_scan/px_scan_slot_iterator_list.hpp",
    "unit_tests/tapeset/test_tapeset_scan.cpp",
    "bench/harness/queries/wmloc_fixture_setup.sql",
]

# --- S1b: externally-visible rename -----------------------------------------
S1B_PAIRS = [
    ("Num_qfile_new_backed_create", "Num_qfile_tapeset_create"),
    ("Num_qfile_old_touch_on_new", "Num_qfile_pgbuf_touch_on_tapeset"),
    ("CUBRID_BUFFILE_SELFTEST", "CUBRID_WM_BUFFILE_SELFTEST"),
    ("CUBRID_HELDTAPE_SELFTEST", "CUBRID_WM_HELDTAPE_SELFTEST"),
    ("CUBRID_TAPEREAD_SELFTEST", "CUBRID_WM_TAPEREAD_SELFTEST"),
    ("CUBRID_PRODUCER_SELFTEST", "CUBRID_WM_PRODUCER_SELFTEST"),
]

S1B_FILES = [
    "src/base/perf_monitor.c",
    "src/query/query_manager.c",
    "bench/harness/parity.sh",
    "bench/harness/gate_tapeset_scan.sh",
    "bench/harness/preflight.sh",
]


def apply(pairs, files, repo_root):
    for rel in files:
        path = repo_root / rel
        if not path.exists():
            print(f"skip (not found): {rel}", file=sys.stderr)
            continue
        text = path.read_text()
        new_text = text
        for old, new in pairs:
            new_text = re.sub(rf"\b{re.escape(old)}\b", new, new_text)
        if new_text != text:
            path.write_text(new_text)
            print(f"rewrote: {rel}")


def main():
    from pathlib import Path

    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", choices=["s1", "s1b"], required=True)
    args = ap.parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    if args.commit == "s1":
        apply(S1_PAIRS, S1_FILES, repo_root)
    else:
        apply(S1B_PAIRS, S1B_FILES, repo_root)


if __name__ == "__main__":
    main()
