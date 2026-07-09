# CONTEXT.md — Value-Representation Stage-1 Canonical Terminology

Canonical terms for the CUBRID 값 표현 통일 (heap/mem/temp 물질화 왕복 제거) workstream, per grilled_plan §6.
Design docs: `docs/value-handle/` (P0 evidence: `p0/`, stage-1 design: `p1/`, gate packet: `ug1-packet.md`).
Branch: `feature/value-handle-stage1` (base ebf08d080, origin/develop).

| Term | Definition | Canonical source |
|---|---|---|
| **ValueSlot** | Per-scan deform target: `values[natts]` 8B array + bound-bit array + type vector fixed at scan open; incremental deform-once per row (PG `TupleTableSlot`/`tts_nvalid` analog). Replaces per-value fat handles (D-G1). Release struct carries NO generation field. | `docs/value-handle/p1/p1.1-valueslot-design.md` §a |
| **ValueHandle** | Historical name for the rejected per-value `{kind, type, len, ≤8B, generation}` design (~24B/value, DB_VALUE-lite). In stage-1 usage, "handle" means a ValueSlot entry: a bare 8B value or pointer with no self-describing metadata. | `p1.1-valueslot-design.md` §a.0 |
| **CopiedFrame** | Scan-owned page copy — CUBRID's translation of PG pin discipline (page-fix extension is the fixed_scan deadlock class). Copy alive ⇒ pointers valid; copy replacement is the only heap-side bump. Basis: issue154-page-copy-heap-scan ("cached scan") workstream. | `docs/value-handle/p1/p1.1-lifetime-contract.md` §1 |
| **LifetimeContract** | Structural validity guarantee for pointer-type slot entries: bump points = {page/copy transition, scan close} + list-scan page transition; `tplrec` forbidden-reference rule; debug-only generation backstop; forced materialization on scan-boundary crossing. | `p1.1-lifetime-contract.md` §2–§5 |
| **class-(d) list** | Forward-only server-internal intermediate `QFILE_LIST_ID`: never reaches cursor.c, never cached, never backward-scanned, never a raw cross-list copy source (FORBID rule). 10 confirmed sites at ebf08d080; sole candidates for `format_version = 1` framing. | `docs/value-handle/p0/p0.2-list-class-audit.md` §3 |
| **Datum1Cache** | First-sort-key 8B inline cache in the sort entry (PG `SortTuple.datum1` analog): most comparisons never touch the tuple body. Fixed-length 9 types free; NUMERIC exact int64 proxy for precision ≤ 18. | `p1.1-valueslot-design.md` §sort-entry |
