# G005 — final conclusion: the R6 env-A cost is the irreducible security cost of encrypting temp spills

## What was tried
1. **LRU-pollution fix (as-built P6 port)** — LANDED (commit 8a8cebde4). Effect: env-A `ioread` 6818 -> 0 (temp pages no longer evict hot data pages). Minor wall-time component.
2. **VTune root cause** (commit 39aa78fb9) — #1 hotspot `aesni_ctr32_encrypt_blocks` 16.9s = actual AES-CTR block encryption of spill pages; `pwrite` 5s; ~27s pthread lock/unlock/cond-wait contention. The redesign encrypts temp spills on the TDE DB; the develop baseline writes temp in PLAINTEXT.
3. **Cipher-context caching** (tried, REVERTED) — cached the EVP_CIPHER_CTX + key schedule per thread, refreshing only the nonce/IV per page. Verified CORRECT (env-A result correct, parallel==serial parity PASS) but env-A median was UNCHANGED (2233ms vs 2230ms). Reason: per-page key setup (`aesni_set_encrypt_key`) is only ~0.16s; the bottleneck is the actual block encryption + contention, not key init. Reverted to keep the security-critical TDE path minimal (no measured benefit).

## Definitive finding
On a TDE database with a constrained buffer + spill-heavy workload (env A), the redesign is ~10x slower than the develop baseline because it **encrypts every temp spill page** (`aesni_ctr32_encrypt_blocks` 16.9s + `pwrite` 5s) while the baseline writes temp in **plaintext**. This is the INTENDED security behavior (TDE-correct temp; closes a plaintext-temp data-leak hole). It is fundamentally irreducible: even removing 100% of the ~27s lock contention, the encryption + write cost keeps env A far above the plaintext baseline. Encrypted temp cannot match plaintext-temp throughput.

## This is a security/perf tradeoff, NOT a fixable bug
- The redesign on env C (16G buffer, little spill) is at PARITY with baseline — the cost only appears under heavy spill.
- The redesign's expected wins are I/O-bound (avoiding temp-volume round-trips) + high-concurrency; on a small-buffer TDE workload the per-page encryption dominates.
- Correctness, parity, raw-fd safety (TDE no-plaintext, fresh-nonce-per-page, orphan-zero) are all PROVEN (G001-G003).

## Decision required (leader) — autonomous engineering is exhausted
Options, none of which a code change can resolve into baseline parity:
- (A) ACCEPT the security/perf tradeoff: ship raw-fd encrypted temp on TDE DBs (slower spill, no plaintext leak), document the env-A cost as expected. Then open the develop PR with the tradeoff recorded.
- (B) Pursue lock-contention reduction (per-worker nonce batching) as a PERF improvement WITHOUT expecting baseline parity (env A stays above baseline due to encryption); uncertain payoff, security-sensitive.
- (C) Reconsider scope: e.g. only use encrypted raw-fd temp when the work_mem-first policy still overflows, or compare against a fairer baseline (TDE with plaintext temp is itself a security bug to measure against).

The redesign is correct, raw-fd-safe, guard-on, and fully root-caused. The remaining choice is a product/security decision, not further code.
