# UG1.5 Decision Packet — P2 마이크로벤치 헤드라인 셀 판정

- **Base**: feature/value-handle-stage1 @ d7700338b (= 9d86f4840 + bench hygiene L1/L2 + `--reverse`), 2026-07-10
- **Gate**: ug1-packet §③의 UG1.5 게이트 규칙 — 헤드라인 3셀 {FL-filter, CV-sort, NUM-sort},
  `PASS_cell ≡ (median_B-cmpdisk − median_A-slot) / median_B-cmpdisk ≥ [X]_cell`.
  **STOP은 3셀 전부 VALID-FAIL일 때만**; 부분 pass는 진행, FAIL 셀은 P3 축소 후보로 기록.
- **근거 데이터**: `p2-results/bench_summary.csv`, `p2-results/bench_results.csv`, `p2-results/run-log.txt`
  (order-balanced F,R,R,F 4패스 풀링, n=28/row, 전 row CoV≤15%, 전 셀 full-size digest 동등성 PASS).
- **숫자 성격**: 전부 **BEST-CASE-UPPER-BOUND** (마이크로벤치 상한). 실제 질의 채택 판단은 P3의
  OBSERVED 실측(UG3)이 기준이며, 실질 참고치는 G0 보고서의 Q12 1.88x
  (`workspace/.not_git_tracking/cubrid_heap_mem_temp_sync/perf/current_gain_results/current_gain_report.md`).

---

## 헤드라인 3셀 판정

| cell | [X] 문턱 | A-slot median | B-cmpdisk median | (B−A)/B | 판정 |
|---|---|---|---|---|---|
| FL-filter (`FL_FILTER`) | 3% (policy) | 35,058 µs (CoV 2.38%) | 26,790 µs (CoV 14.72%) | **−30.9%** | **VALID-FAIL** |
| CV-sort (`CV_SORT`) | 38% | 395,348 µs (CoV 1.92%) | 4,689,660 µs (CoV 0.68%) | **+91.6%** | **VALID-PASS** |
| NUM-sort (`NUM_SORT`) | 18% | 116,748 µs (CoV 3.79%) | 707,956 µs (CoV 6.71%) | **+83.5%** | **VALID-PASS** |

**진리표 적용: 2/3 VALID-PASS → STOP 아님. P3 진행. FL-filter는 P3 축소 후보로 기록.**

(ug1-packet §③ / handoff §7.9에 따라 3% 문턱을 바꾸지 않고 FL을 매트릭스에서 빼지도 않는다.)

### FL-filter VALID-FAIL 해석 (P3 축소 후보 기록)

- fixed-length 단순 범위 필터에서 B의 `data_cmpdisk`는 disk image 직접 비교(변환 없음)로 이미
  최소 비용이다. A-slot은 deform(슬롯 적재) 비용을 선불하는데, FL 필터는 각 값을 행당 1회만
  만지므로 deform-once의 회수 기회가 없다 — ug1-packet §③이 "[X]_FL-filter는 미유도 정책값"으로
  둔 바로 그 불확실성이 음수로 실측된 것.
- P3 함의: **FL-only 단순 필터 경로는 ValueSlot 적용을 강제하지 않는다**(legacy/disk-compare 경로
  유지가 이미 설계 원칙 — handoff §0). 반면 FL-sort(+67.0%)는 정렬 재접근 구조라 이득이 실재.
- 주의: B-cmpdisk FL_FILTER 풀링 CoV 14.72%는 상한(15%) 근접. 각 공식 패스 개별 CoV는 ≤15%였고
  판정 방향(−30.9% vs +3%)은 CoV 여유와 무관하게 결정적.

## 전체 매트릭스 (참고, BEST-CASE-UPPER-BOUND)

| cell | A-slot | A-handle | B-cmpdisk | C-flatbuffers | A-slot vs B |
|---|---|---|---|---|---|
| FL_FILTER | 35,058 | 55,389 | 26,790 | 41,629 | −30.9% |
| FL_SORT | 225,176 | 244,676 | 682,409 | 1,722,161 | +67.0% |
| CV_SORT | 395,348 | 536,489 | 4,689,660 | 958,736† | +91.6% |
| CV_MERGE | 419,784 | 579,553 | 5,077,640 | 901,245† | +91.7% |
| UV_PEEK | 10,773 | 33,555 | 14,582 | 35,103† | +26.1% |
| NUM_SORT | 116,748 | 153,576 | 707,956 | 1,263,391 | +83.5% |
| NUM_AGG_INPUT | 21,000 | 28,448 | 43,596 | 55,114 | +51.8% |
| PEEK_VS_COPY | 87,642 | 190,617 | 247,468 | SKIPPED | +64.6% |
| ABBREV_SUBCELL | 162,095 | 198,977 | 4,696,973 | SKIPPED | +96.5% |

(µs, 풀링 median, n=28. † = INGESTION-EXEMPT: C의 prepare()가 VARCHAR 디코드/해제를 untimed로
선수행 — C의 CV/UV 숫자는 다른 variant와 직접 비교 불가.)

- **A-slot vs A-handle**: 전 셀에서 A-slot 우세 (per-value 자기서술 handle 기각의 실측 재확인).
- **C-flatbuffers**: ingestion 비용 제외 조건에서도 A-slot에 전 셀 열세 — framing 제외 결정 지지.
- **ABBREV_SUBCELL +96.5%**: upside-only 서브셀; varchar 8B 프록시는 Stage-1 범위 아님(기록만).

## P2 종결 상태 (handoff §7 체크리스트)

1. `variant_cmpdisk.cpp` 위생 diff 검토·커밋 — 82f64dafe (L1 owned-domain free / L2 timed-loop hoist)
2. 벤치 target-only release rebuild — done (run-log)
3. smoke/parity 9/9 — PASS (forward + reverse)
4. 헤드라인 order-balance — `--reverse` 추가(d7700338b), 공식 4패스 F,R,R,F 풀링
5. full matrix 조용한 머신 — done; pass3-R 1회 INVALID(CoV 17.4%) → 프로토콜대로 재측정
6. NUM_AGG_INPUT full-size digest 전 variant 동일 — PASS (14621492692458723252)
7. p2-results 0-byte 파일 교체 — bench_results.csv / bench_summary.csv / run-log.txt
8. 본 packet 작성
9. commit·push — 본 packet 커밋에 포함
10. **UG1.5 사람 판정** — 아래

## 판정 요청

- 게이트 규칙상 자동 결론: **진행(P3)** — STOP 조건(3셀 전부 VALID-FAIL) 불충족.
- 판정: `PROCEED(P3 진행, FL-filter 축소 후보 기록) / MODIFY / STOP`

## 승인 후 다음 단계

UG1.5 승인 → G2 (develop 대비 추가 비용 제거 + MIDXKEY 보존 검증, handoff §8) →
P3 env-gated vertical slice (§9) → UG3 (§10).
