# UG1 Verdict — User Gate 1 Decisions (2026-07-09)

- **Task**: record the binding user verdict on `ug1-packet.md`'s 6 agenda items, closing UG1 (ralplan.md
  §3 User Gates "UG1 안건 재편": "P2 CANNOT begin until UG1 approval"). This document is the authoritative
  record of what was decided; `ug1-packet.md` itself is left as the historical agenda (unedited body, see
  its prepended status block) and downstream P1 docs are annotated in-place to point back here rather than
  rewritten wholesale.
- **Decided by**: user, 2026-07-09.
- **Base**: worktree `/home/cubrid/dev/cubrid-vh`, branch `feature/value-handle-stage1`.
- **Gate status**: **UG1 = PASSED** (6/6 items resolved: 4 APPROVE, 2 MODIFY — no REJECT). **P2 (bench
  harness) is unblocked** per `ug1-packet.md`'s own gate rule ("UG1 승인 → P2"); nothing below reopens
  P1-exit or defers the gate.

---

## ① ValueSlot + structural lifetime contract — **MODIFY**

- **Asked**: approve the ValueSlot/CopiedFrame design (`p1.1-valueslot-design.md`, `p1.1-lifetime-contract.md`)
  together with its page-copy landing-order plan, which was a two-branch decision — (1) primary: rebase
  onto `issue154-page-copy-heap-scan` *after* it lands on `develop`; (2) fallback: if unlanded by P3 start,
  implement a minimal scan-owned page copy confined to the slice-scan path (5-boundary AC,
  `p1.1-lifetime-contract.md` §4).
- **Decided**: the ValueSlot/CopiedFrame design itself is retained as designed. The landing-order plan is
  **superseded**: instead of waiting for `develop` or building the minimal self-implementation fallback,
  **merge `issue154-page-copy-heap-scan` ("cached scan", 6 slices, tip `be256ac12`) directly into this
  branch**, and proceed with the rest of stage-1 on top of that merged base. Neither the "wait for develop"
  primary path nor the "minimal scan-owned page copy" fallback path is exercised — both are superseded by
  the direct merge.
- **Implementation**: the merge itself is a peer task (branch-surgery scope, not this document's). This
  verdict records the decision; the mechanical result is expected to be reflected in
  **`p1.1-lifetime-contract.md` §4 revised** (the landing-order section — owned by the lifetime-contract
  peer, not edited here) once the merge lands, replacing the primary/fallback branch with "consumes
  `HEAP_SCANCACHE.copied_frame`/`PEEK_FROM_COPIED_PAGE` unconditionally, basis always present post-merge."
- **Downstream impact (P2/P3)**:
  - P2 bench harness: no change to bench-matrix shape: cached-scan availability was already an assumed
    P3-time precondition for the CopiedFrame basis; the merge just makes that precondition true earlier
    (at P1-exit) instead of conditionally true at P3 start.
  - P3: the fallback's 5-boundary minimal-scope AC (`p1.1-lifetime-contract.md` §4, points 1-5) and its
    named P3.1 AC line (`Num_valueslot_heap_pointer_without_copied_frame_basis`-class counter) become
    **dead branches** once the merge lands — the basis invariant ("`VC_RAW`/`VC_COMPRESSED` heap columns
    enabled iff a CopiedFrame basis is present") still holds, but is now unconditionally satisfied via the
    merged `issue154` primitives rather than conditionally satisfied via one of two mechanisms. P3 no
    longer needs to plan for the fallback-triggered code path at all; that scope simply does not get built.
  - Risk retired: the "page-copy landing trigger judged at P3 start" open item
    (`ug1-packet.md` "P1-exit 개방 항목") is resolved now rather than left open into P3.

## ② B안 스케치 + 비용 추정 (native-endian sketch + cost estimate) — **MODIFY**

- **Asked**: approve (a) the B안 (native-endian on-disk format) design sketch, (b) its 9-engineer-month
  ±50% cost estimate, and (c) the 2 migration strategies sketched as its rollout mechanism
  (`p1.2-b-sketch.md` §2/§3).
- **Decided**: the cost estimate (9 EM ±50%) is **SKIPPED — not approved, informational only**. It is not
  rejected as wrong; it simply carries no approval weight and does not gate anything (B-entry remains
  gated on the `[Z]` threshold measured in P3.8, per §2.3 of the sketch — the cost estimate was never part
  of that gate condition, only a UG1-evaluation input for weighing B against A). The **2 migration
  strategies (§2.1 version-tagged lazy rewrite, §2.2 offline migration tool) ARE approved, as
  documentation** — i.e. approved as a design-record artifact describing candidate B-stage rollout
  mechanisms, not as an implementation commitment (B-stage itself is not entered by this decision; entry
  still requires its own future gate per `p1.3-proposals.md`'s Stage B row).
- **Downstream impact (P2/P3)**: none directly — B안 is out of stage-1 scope regardless (A안 is what P2/P3
  build). The practical effect is bookkeeping: `p1.2-b-sketch.md` §3 stays in the document (superseded, not
  deleted — see item 3 of this task's annotation below) so a future Stage-B cost re-estimate has a
  documented starting point and known-informational status, and the 2 migration strategies remain
  citable as pre-vetted options if/when Stage B is ever entered.

## ③ Per-cell thresholds [X]×3 + [Y]/[Z] + prediction table — **APPROVE**

- **Asked**: approve the proposed UG1-policy thresholds — `[X]_FL-filter = 3%`, `[X]_CV-sort = 38%`,
  `[X]_NUM-sort = 18%`, `[Y] = 10%`, `[Z] = 40%` — and the UG1.5 gate rule / prediction table
  (`ug1-packet.md` ③, `p1.3-proposals.md` (ii)).
- **Decided**: **approved as proposed**, numbers unchanged. Additionally, the user directs a **NL-join
  copy-elimination review** as a companion item — evaluate whether nested-loop join's inner-side
  DB_VALUE-boundary copy pattern is itself eligible for the same peek/copy-elimination treatment
  ValueSlot/CopiedFrame gives the scan path, and whether that changes any of the per-cell cost models the
  thresholds above were derived from. This review is tracked at
  **`docs/value-handle/p1/p1.3a-nljoin-review.md`** (peer deliverable, not authored by this task).
- **Downstream impact (P2/P3)**:
  - P2: bench harness proceeds against the approved thresholds unchanged — no matrix-shape change from
    this item.
  - P3/UG1.5: the NL-join review is a **new, separate input** to the UG1.5 gate judgment. If it finds a
    materially different copy-elimination profile for NL-join than what CV-sort/NUM-sort/FL-filter assume,
    that is `[P0-CONDITIONAL]`-class evidence requiring the affected threshold(s) to be re-derived at
    UG1.5 or UG3 rather than taken as closed — this item does not itself change any threshold, it opens a
    tracked follow-up that could.

## ④ Temp framing go/no-go (+ client-visible extension) — **APPROVE**

- **Asked** *(clarified per architect review 2026-07-09 — this item covers **two** framing surfaces)*:
  (1) the packet-④ question proper: approve excluding **temp class-(d) recdes-style framing** from the
  stage-1/P3 slice per the D-G6.2 volume condition (measured class-(d) share **12.12% < 30%** baseline,
  `p0.2b-byte-volume.md` §9); and (2) by extension, keep the **client-visible framing extension**
  (Option 2/3 of `p1.2-b-sketch.md` §4.2) likewise out of stage-1, per the sketch's recommendation.
- **Decided**: **approved — framing extension excluded from stage-1**, deferred to B/campaign per the
  sketch's own Option-1-now/Option-3-as-the-only-path-to-Option-2 recommendation (`p1.2-b-sketch.md` §4.3).
  **Directive attached**: stage-1's implementation MUST route tuple/value read-write through **one common
  format-boundary API surface**, so that a future `format_version = 1` framing extension can be added as a
  second table entry to that API rather than requiring call-site surgery across every consumer. This
  directive is worked out concretely in `p1.1-format-selection.md`'s new "Common format-boundary API
  directive (UG1 ④)" section.
- **Downstream impact (P2/P3)**:
  - P2: no bench-matrix change (framing stays excluded either way).
  - P3: **new AC** — stage-1's format-0 (legacy) read/write paths must be implemented *through* the common
    API's format-0 table entry, not as ad hoc direct byte access, so that the deferred framing item costs
    "add a table entry" later instead of "re-open every call site." Concretely: no direct legacy-format
    byte access outside the API in new stage-1 code (see the format-selection doc's new section for the
    exact AC line and the 4 named touchpoints it applies to).
  - Campaign/B: the eventual framing-extension work (whenever it is picked up) inherits a pre-built seam
    instead of starting from zero call-site discovery.

## ⑤ 4B length-prefix — **APPROVE (rejection)**

- **Asked**: approve rejecting the 4B length-prefix optimization for stage-1, per the computed ~2.0%
  (16.5% mix-conditioned ceiling) share of total temp write volume (`p1.1-format-selection.md` §4.3).
- **Decided**: **approved — 4B-prefix rejected for stage-1**, matching D-G6.4's auto-reject condition
  (volume too small to justify the format change, and reverse-scan is actively used per P0.2, which the
  4B-prefix change would complicate). No MODIFY to the computed numbers; the 16.5%-canonical /
  16.6%-rounding-corrected reconciliation already recorded in `p1.1-format-selection.md` §4.3 stands as-is.
- **Downstream impact (P2/P3)**: none — this closes a design branch that was never going to be built in
  stage-1 either way; no bench cell, AC, or API surface is affected. Recorded here purely so UG1 has a
  complete, closed 6/6 item count.

## ⑥ pr_type extension hook list (D-G8) — **APPROVE**

- **Asked**: approve the 3-hook `pr_type` extension list — (1) slot deform function, (2) datum1-extraction
  hook, (3) abbreviated-key converter (campaign-reserved slot) — as the retain+extend answer to whether
  `pr_type`/`DB_TYPE` dispatch changes under this project (`ug1-packet.md` ⑥, `p1.3-proposals.md` (i), PG
  `sortsupport`-pattern precedent).
- **Decided**: **approved as proposed**, 3-hook list unchanged. `DB_TYPE` removal remains explicitly out of
  scope, permanently (not stage-scoped) — D-G8's own framing.
- **Downstream impact (P2/P3)**:
  - P2/P3: stage-1 implements hooks (1) and (2) (slot deform, datum1 extraction); hook (3) (abbreviated-key
    converter) stays a reserved slot — declared in the dispatch shape but not implemented until the
    Stage-C abbreviated-key/radix work (`p1.3-proposals.md` Stage C row).
  - This item's approved pattern (a per-`DB_TYPE` function-pointer table with a reserved-but-unimplemented
    slot for future work) is the direct precedent cited by item ④'s new common format-boundary API
    directive (`p1.1-format-selection.md`'s new section) — the same "table entry now, table entry later"
    shape is reused there for `format_version` dispatch instead of `DB_TYPE` dispatch.

---

## Summary table

| # | Item | Verdict | Gates opened/closed |
|---|---|---|---|
| ① | ValueSlot/CopiedFrame + page-copy landing order | MODIFY | Landing-order branch closed (merge-in-branch replaces primary+fallback); `p1.1-lifetime-contract.md` §4 revision tracked as peer follow-up |
| ② | B안 sketch + cost estimate | MODIFY | Cost estimate → informational-only, not a gate input; 2 migration strategies → approved as documentation only |
| ③ | Thresholds + prediction table | APPROVE | NL-join copy-elimination review opened as a tracked UG1.5/UG3 input (`p1.3a-nljoin-review.md`) |
| ④ | Framing extension exclusion | APPROVE | Stage-1 exclusion confirmed; common format-boundary API directive opened, with a new P3 AC |
| ⑤ | 4B length-prefix | APPROVE (rejection) | Closed, no further action |
| ⑥ | pr_type 3-hook list | APPROVE | Closed; pattern reused as precedent for item ④'s API directive |

**Net gate state: UG1 PASSED (2026-07-09). P2 is unblocked and may begin.**
