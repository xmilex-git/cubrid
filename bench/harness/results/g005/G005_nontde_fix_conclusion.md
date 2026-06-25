# G005 — non-TDE regression FIXED (per leader decision: accept TDE-slow, make non-TDE fast)

## Leader decision
Accept the TDE temp-encryption slowdown; ensure NON-TDE workloads are fast (no regression).

## Root cause (corrected via VTune on a non-TDE workload)
The raw-fd overflow was gated on `raw_fd_writes_enabled()`, whose `tde_wired` term effectively reduces to
`tde_is_loaded()`: a plain `cubrid createdb` ALWAYS creates a master keys file (baseline does this too), and
`tde_default_algorithm` has its NONE keyword commented out (system_parameter.c:5563) so it is always AES. Thus
`tde_is_loaded()` is true for essentially every DB. The redesign therefore routed EVERY spill through the
encrypted raw-fd path and forced `tfile.tde_encrypted=true` -- even for PLAINTEXT (non-TDE) tables. VTune on a
non-TDE DB confirmed the spill hot path was `rawfd_flush_page -> tde_encrypt_data_page -> aesni_ctr32_encrypt_blocks`
even though the table data was plaintext on disk. (tpch_sf10 used in R6 is itself plaintext: 30M+ readable
tpch strings on disk -> the R6 env-A "regression" was this bug, not a justified security cost.)

## Fix (temp_page_store.cpp:2403)
Route MEMBUF overflow to encrypted raw-fd ONLY when `tfile_p->tde_encrypted` is true. `tde_encrypted` is set
from the query's `includes_tde_class` (query_manager.c:3814 = XASL_INCLUDES_TDE_CLASS), i.e. only when the
query actually touches a genuinely TDE-encrypted class. Non-TDE temp falls through to the develop temp-volume
path (PRIVATE_SPILL_FALLBACK) -- no needless AES on plaintext temp.

## Verification (release, env-A 512M buffer, 1,048,576 rows)
| workload | baseline | redesign pre-fix | redesign post-fix |
|---|---|---|---|
| non-TDE GBY (plaintext t) | 697ms | 27401ms (41x) | 662ms  PARITY |
| non-TDE HJ  (plaintext t) | 677ms | 6480ms (9.6x) | 975ms  (~1.4x residual) |
- non-TDE plaintext: rawfd_decrypt delta = 0 -> raw-fd OFF (develop path).
- genuine TDE table (CREATE TABLE te ... ENCRYPT=AES, same DB): rawfd_decrypt delta 4611 (GBY) / 11462 (HJ),
  results = 1048576 -> raw-fd still encrypts TDE temp; decrypt round-trip correct. SECURITY PRESERVED.
- TDE-encrypted workloads remain the accepted-slower path; only genuinely-encrypted data pays AES.

## Residual (minor, candidate future optimization)
non-TDE HJ self-join is ~1.4x baseline (down from 9.6x). VTune shows the residual is core hash-join +
heaplayers allocator churn (mspace_free / hl_fixed_alloc / mht_*_hls / hjoin_*) from the per-worker work_mem
MEMBUF tier's alloc/free pattern -- NOT raw-fd/AES/accountant/contention. GBY is already at parity. Reducing
the HJ residual would mean pooling/reusing per-worker work_mem buffers (deeper work_mem-tier change). Left as a
documented follow-up; the catastrophic non-TDE regression is resolved.

Commit: bccab3df2 (fix + evidence).
