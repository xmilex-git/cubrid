# G005 — VTune root cause of the R6 env-A "regression" (corrects the earlier LRU-only hypothesis)

Tooling: Intel VTune Profiler 2025.0.1 hotspots, attached to the redesign RELEASE cub_server during the env-A query (USE_HASH PARALLEL hash join, data_buffer_size=512M, parallelism=8) on tpch_sf10.

## KEY FACT: tpch_sf10 is a TDE database (tpch_sf10_keys present)
So with the guard TRUE (G003), the redesign routes temp spills through the raw-fd path, which ENCRYPTS every spilled page (full FILEIO_PAGE + TDE, fresh nonce per physical page). The develop baseline has NO raw-fd and spills temp in PLAINTEXT to temp volumes. The env-A comparison is therefore redesign(encrypted temp) vs baseline(plaintext temp) on a TDE DB — i.e., the redesign closes a real plaintext-temp security hole, at a CPU cost.

## VTune top hotspots (self CPU time, ~24s window, env-A loop)
- aesni_ctr32_encrypt_blocks  16.92s   (libcubrid)   <-- #1: AES-CTR encryption of temp spill pages
- __GI___pthread_mutex_lock   10.67s
- __pthread_mutex_unlock_usercnt 9.37s
- pthread_cond_timedwait      7.10s
- pwrite                      4.97s     (writing encrypted spill pages)
- __memmove / __memset (evex) 3.69s     (crypto buffer moves / CRYPTO_zalloc)
- parallel_query thread_safe_queue try_pop_fast/is_empty/try_pop ~1.2s  (parallel job-queue)
- pgbuf_bcb_register_fix / pgbuf_fix_release ~0.5s

## Call tree (the actionable inefficiency)
temp_page_store::rawfd_flush_page -> rawfd_write_page -> tde_encrypt_data_page -> tde_encrypt_internal
  -> EVP_CipherInit_ex -> aesni_init_key -> aesni_set_encrypt_key (+ CRYPTO_zalloc + malloc + ENGINE_get_cipher_engine)
  -> evp_EncryptDecryptUpdate -> aes_ctr_cipher -> aesni_ctr32_encrypt_blocks

## Root cause = THREE components (not the LRU pollution alone)
1. SECURITY-FEATURE COST (inherent): encrypting temp spills at all. The baseline leaves temp plaintext; the redesign encrypts it. This AES + pwrite cost is the price of TDE-correct temp on a TDE DB under a spill-heavy constrained buffer. NOT a bug.
2. FIXABLE — per-page cipher key re-init: tde_encrypt_data_page calls EVP_CipherInit_ex / aesni_set_encrypt_key (full AES key expansion + CRYPTO_zalloc/malloc + ENGINE lookup) on EVERY page. In AES-CTR the key is constant; only the nonce/counter changes per page. Caching the EVP cipher context per worker/file (set key once, set a FRESH nonce per page) removes the per-page key-setup overhead while preserving the fresh-nonce-per-physical-page invariant. High-value, security-preserving optimization.
3. FIXABLE — lock contention (~27s lock/unlock/cond_wait): consistent with (a) the single GLOBAL nonce counter taken under a mutex per page across all parallel workers (serializes parallel encryption) and (b) the parallel_query job queue. Mitigation: allocate nonce ranges per worker from the global counter in batches (still globally unique, still fresh-per-page), reduce per-page global locking; review the parallel job-queue contention.

## Corrected verdict
The earlier R6 ADR attributed env A solely to data-LRU pollution. VTune shows the LRU pollution (ioread 6818->0, fixed by the as-built P6 cherry-pick in G005-partial) was a MINOR component; the dominant cost is (1) the security feature (encrypt temp) plus (2) per-page cipher re-init and (3) global-nonce + queue lock contention. The redesign is correct + raw-fd-safe; its env-A slowdown on a TDE DB is the cost of encrypting previously-plaintext temp, with two concrete, security-preserving optimizations available (cipher-context caching; per-worker nonce batching).

## Decision needed (leader) / next steps for G005
- Implement (2) cipher-context caching + (3) per-worker nonce batching in temp_page_store's raw-fd encryption path, then RE-RUN the G003 TDE security gate (no (key,nonce) reuse) + re-measure env A. These are security-critical hot-path changes -> must re-verify nonce uniqueness.
- Decide whether the residual AES cost (component 1, inherent to encrypting temp) is acceptable vs baseline's plaintext temp (a security-vs-perf product tradeoff). The redesign's gains were expected in I/O-bound/high-concurrency via avoiding temp-volume round-trips; on this small-buffer TDE workload the encryption cost dominates.
