# G2 report — P3 전 비용 정리 + Q12 재검증 + MIDXKEY 보존 증명

> Date: 2026-07-10 (afternoon)
> Handoff §8. UG1.5 verdict: PROCEED (`ug15-verdict.md`, 82aa032bc).

## 1. Part A — cached-scan cost cleanup (handoff §4)

Commits on `feature/value-handle-stage1` (post-cleanup HEAD `c135c397a`, cubrid_rel `11.5.0.2341-c135c39`):

- `5c836f670` VH-G2: remove temporary scan diagnostics
  - VH-P0-TEMP-COUNTER atomics/dump 완전 제거 (heap_file.c + list_file.c의 P0 list-byte 카운터까지);
    `grep -rn 'VH_P0|vh_p0' src/` = 0건.
  - copy-buffer 할당 성공 er_log_debug 제거 (page_buffer.c). OOM degrade 로그는 cold 경로 계약 진단으로 유지.
- `c135c397a` VH-G2: streamline cached scan runtime state
  - eligibility 1회 계산: `qexec_is_cached_scan_eligible()` 호출은 qexec_open_scan 한 곳; parallel은
    boolean 전달(worker의 `m_is_cached_scan && !fixed_scan`는 안전 교집합으로 유지).
  - `ACCESS_SPEC_TYPE.cached_scan` 삭제 → `SCAN_ID.cached_scan` 단일 runtime source
    (partition reopen은 SCAN_ID 재사용; stream_to_xasl.c/query_dump.c 갱신).
  - read_mode/handle 이중 표현 제거: LOCAL_CACHE 설정 시 non-NULL handle+page assert, per-row는 read_mode만 검사.
  - per-row `pgbuf_copy_buffer_get_page_ptr()` 제거: 할당 시 1회 취득해 `HEAP_SCANCACHE.local_cache_page`에 보관.
  - 빈 format dispatcher/conversion-zero counter: 부재 확인(삭제 대상 없음).
- 유지(불변): per-page full copy, ordered-fix, per-scan private buffer, 모든 correctness fallback.
- 게이트: release 풀빌드 green(`logs/build-feature2.log`는 측정용 빌드), debug 풀빌드 green
  (`logs/build-debug-g2.log`, post-cleanup HEAD에서 재실행·보존), bench `--parity` 9/9 PASS
  (executor 1차 + QA lane 독립 재실행).
- short/reopen scan allocation: 계측 우선 원칙 유지 — 이번에 bounded reuse 미도입(병목 실측 없음).

## 2. Part B — Q12 재검증 (성능 무손실 증명)

ABBA(B A A B), warmup 2 + measured 4/block, 8 measured/arm, env -i, 동일 conf(paramdump 저장):

| arm | median | mean | CoV |
|---|---|---|---|
| develop merge-base (`2311-ebf08d0`) | 5.635 s | 5.739 s | 5.42% |
| **feature post-cleanup (`2341-c135c39`)** | **3.131 s** | 3.230 s | 10.26% |

- G0 기준 feature median 3.109 s → post-cleanup 3.131 s (+0.7%, 잡음 범위; feature2 b2에
  4.035 s outlier 1건 포함 — 제외 없이 보고). speedup 1.80x. **정리로 인한 성능 손실 없음.**
- parity: 전 run MAIL 62071/93045, SHIP 62426/93261. plan/trace: workers 10, lineitem seq scan 지배
  (`feature2-q12-trace.txt`, `develop-g2-q12-trace.txt`).
- 참고: feature2 b2 직전의 start 호출 1회는 wrapper가 비제로 종료를 보고했으나 실체는
  "server 'tpch_sf10' is running" — 직전에 중단된 시도에서 살아남은 서버가 그대로 서빙
  (트랜스크립트: `logs/server-ctl/start-tpch_sf10-20260710-163624.log`; wrapper의 RESULT
  요약줄은 stdout 전용이라 로그 파일에는 없음). 측정값 유효.

## 3. Part C — MIDXKEY 보존 검증 (handoff §8.2)

SQL: `perf/g2_midxkey_regression.sql` (M1 composite range / M2 composite equality+filter /
M3 ORDER BY=index order / M4 **강제 index skip scan**(composite PK `USING INDEX`; 단일컬럼 FK 인덱스가
아니면 최적화기가 ISS를 안 탐) / M5 composite FK range + heap lookup).

### 판정 (대칭 부하 라운드 = block 6, 7; n=7/arm/cell/block)

| cell | develop | feature2 | delta | 판정 |
|---|---|---|---|---|
| M1 range | 0.151 / 0.153 s | 0.141 / 0.145 s | −6.6% / −5.2% | OK |
| M2 equality | 0.348 / 0.376 s | 0.351 / 0.342 s | +0.9% / −9.0% | OK |
| M3 order-by-index | 0.440 / 0.441 s | 0.442 / 0.435 s | +0.5% / −1.4% | OK |
| M4 skip scan (12M fetch) | 10.072 / 10.101 s | 10.089 / 10.021 s | +0.2% / −0.8% | OK |
| M5 FK range (v3 warm, b7) | — / 0.0430 s | — / 0.0420 s | — / −2.3% | OK |
| M5 FK range (IO형 v2, 재측정 §3.1) | 1.376 s (n=20) | 1.301 s (n=20) | **−5.45%**, CI[−6.6,−0.6] | OK |
| index build 3M×(a,b,c) (재측정 §3.2) | 4.691 s (n=16) | 4.816 s (n=16) | +2.69% (median), paired +1.83% CI[−0.6,+4.3] | **확정 회귀 아님** (CI가 0 포함) |

- **확인된(>2% + CI>0) 회귀 없음. MIDXKEY 고속 경로 보존 증명 완료. P3 진입 차단 요인 없음.**
- index build는 점추정 +1.8~2.7%로 작은 회귀 가능성을 배제하지 못함 — **UG3 1급 입력으로 기록**
  (§8.2의 차단 기준은 '확인된' >2% 회귀이며, 쌍별 95% CI [−0.62%, +4.27%]는 0을 포함).

### 3.1 M5 IO형(v2 범위) 재측정 — block-6 +26.6%의 처리

리뷰 지적(정당): 최초 보고는 대칭 부하 block 6의 M5(v2) **+26.6% (CoV 유효)** 를 표에서
누락한 채 '전 셀 무회귀'라고 썼다. 이는 규칙 위반이므로 다음과 같이 정정한다:
- 해당 수치는 공개하며(develop 1.104 s vs feature 1.398 s, block 6), 사전 선언된 제외 규칙이
  아니었음을 인정한다.
- 판정은 **재측정으로 대체**: 순서균형(ABBA BAAB, 블록당 warmup 1 + measured 5, **n=20/arm**),
  블록별 1분 loadavg 실측 3.05–6.26 범위에서 arm별 평균이 대칭(develop 4.30 vs feature 4.46;
  균형 배치가 드리프트를 양 arm에 균등 분배). 결과 **feature −5.45%, mean-diff 95% CI
  [−6.58%, −0.57%] 전체가 0 미만** — feature가 유의하게 빠름. 따라서 block-6의 +26.6%는
  균형 재측정에서 **재현되지 않았고 판정은 본 재측정이 대체**한다. 원인에 대해서는: 이 셀(v2)이
  실행당 ~579K 물리 ioread로 IO-bound라는 사실(v2 trace)에서 코-테넌트 IO 간섭 민감성을
  가설로 두되, block 6 자체의 로그상 부하는 대칭이었으므로 **원인 확정은 하지 않는다**.
  원자료 `m5io-runs.csv`, loadavg는 `logs/block-loadavg.log`의 m5io 행.

### 3.2 index build 재측정 — 최초 n=9 단일 시퀀스 판정의 대체

리뷰 지적(정당): 최초 n=9는 develop→feature 단일 시퀀스라 9/9 쌍별 열세가 순서 편향인지
판별 불가. 재측정: 두 scratch DB를 동시 유지하고 **CREATE를 arm 교차(ABBA 패턴 왕복)로
n=16/arm**, warmup 2/arm 선행. 결과: median develop 4.691 s vs feature 4.816 s (+2.69%);
**쌍별(인접 교차쌍) 분석 +1.83%, 95% CI [−0.62%, +4.27%] — 0 포함, 확정 회귀 아님.**
교차 조건에서 쌍별 방향은 6/16이 음수로 일관되지 않음 → 최초 9/9 열세는 시퀀스 편향으로
판정. **`logs/index-build-timings.txt`의 당초 판정문 "delta(median) = +1.41% (<= 2% threshold,
OK)"는 본 절이 명시적으로 대체(supersede)한다** — 당초 실험은 설계 결함(단일 시퀀스)으로
판정 능력이 없었다. 원자료 `idxbuild-runs.csv` (per-run loadavg 포함). 잔여 +1.8~2.7%
점추정은 UG3에서 실제 index-build 워크로드로 재평가할 1급 입력으로 남긴다.
- plan 동일성: 두 arm trace가 인덱스 선택·key range·covered·filteredkeys까지 일치(readkeys ±1은
  경계 프로브 잡음). 결과 fingerprint 동일(v1 `2805129870`, v2 `2669763051`, v3 `3798931025`).
- ISS 실증: M4 trace = composite PK에서 readkeys 4,008,000 / fetch 12,011,292 (skip-scan 특유의
  그룹별 재탐색 시그니처), 양 arm 동일.

### 측정 위생 기록 (중요)

- **간섭 오염 블록**: block 4/5는 동거 에이전트 부하(loadavg 4→23 요동, `logs/block-loadavg.log`)와
  겹침 — 이 블록들에서만 +5~+60% 편차가 나타났고 부호가 블록마다 반전. block 3의 오염 귀속은
  loadavg 로그가 block 4부터 시작하므로 **값 패턴에서 추론한 것**임을 명시한다. per-cell CoV>15%
  행은 프로토콜대로 INVALID 처리, 오염 블록 원자료는 CSV에 보존. block 6의 M5(v2) +26.6%는
  §3.1에서 공개·대체(균형 재측정이 판정을 대체).
- **M5 v1/v2 (l_partkey 20001키, 실행당 ~579K 물리 ioread)는 2% 질문을 판별 불가능한 IO-bound 셀**로
  판명되어 v3(버퍼 수용 범위, warm CPU-bound)로 교체. v1/v2 원자료 보존.
- index build는 SA 모드(csql -S) 측정 — 비교기(element별 index_cmpdisk) 경로는 CS와 동일하며,
  scratch DB(g2midx, 3M행)는 각 arm의 자체 install로 생성 후 deletedb로 제거. tpch_sf10 무접촉.
- Q12/MIDX 측정 전후 `~/CUBRID` 심링크 복원 및 cub_* 0 — `logs/restoration-proof.txt`.
- conf 공정성: `develop-g2-paramdump.txt` == `feature2-paramdump.txt` (error_log 파일명 제외 diff 0).
- wrapper 트랜스크립트: `logs/server-ctl/` (b2 건은 `start-...-163624.log`의 already-running 기록).

## 4. Artifacts

```
g2-results/
  develop-g2-runs.csv feature2-runs.csv           # Q12 re-verify
  develop-g2-midx.csv feature2-midx.csv           # MIDX blocks 3-7 (v2/v3)
  m5io-runs.csv                                   # §3.1 IO형 M5 재측정 (n=20/arm)
  idxbuild-runs.csv                               # §3.2 index build 교차 재측정 (n=16/arm)
  develop-g2-paramdump.txt                        # conf 공정성 증거
  *-midx-trace-v3.txt                             # v3 plan proofs
  logs/*-midx.v1.csv                              # v1 blocks 1-2
  *-q12-trace.txt *-midx-trace-v2.txt             # plan proofs
  feature2-paramdump.txt                          # conf proof
  logs/block-loadavg.log                          # interference evidence
  logs/build-feature2.log                         # measurement build
  ../g2_midxkey_regression.sql                    # v3 canonical SQL
  ../../..//scratch/g2midx/{schema,buildidx}.sql  # index-build fixture (data regenerable)
```

## 5. 다음 단계

G3 (P3 env-gated vertical slice, handoff §9). FL-filter는 UG1.5 verdict대로 적용 축소 후보:
FL 단순 필터 경로는 ValueSlot 미적용(기존 disk-compare 유지).
