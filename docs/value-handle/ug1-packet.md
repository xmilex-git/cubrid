# UG1 Decision Packet — 값 표현 통일 1단계 설계 승인

> **DECIDED 2026-07-09 — see `ug1-verdict.md`.** Per-item outcome (one line each; full rationale,
> downstream impact, and gate status live in `ug1-verdict.md` — this packet's body below is left
> unedited as the historical agenda record):
> - **①** MODIFY — ValueSlot/CopiedFrame design retained; page-copy landing-order plan superseded:
>   merge `issue154-page-copy-heap-scan` ("cached scan") directly into this branch instead of
>   wait-for-develop/minimal-self-impl.
> - **②** MODIFY — 9 EM cost estimate SKIPPED (not approved, informational only); the 2 migration
>   strategies are approved as documentation only.
> - **③** APPROVE — thresholds approved as proposed; NL-join copy-elimination review directed
>   (`p1.3a-nljoin-review.md`).
> - **④** APPROVE — framing extension excluded from stage-1 (as recommended); common
>   format-boundary API directive attached.
> - **⑤** APPROVE — 4B length-prefix rejection confirmed for stage-1.
> - **⑥** APPROVE — pr_type 3-hook list approved as proposed.
>
> **Gate: UG1 PASSED. P2 is unblocked.**

---

- **Base**: feature/value-handle-stage1 @ ebf08d080 (origin/develop), 2026-07-09
- **Gate**: grilled_plan §User Gates "UG1 안건 재편" — 아래 6건을 승인해야 P2(마이크로벤치)가 시작된다. **P2 CANNOT begin until UG1 approval** (ralplan Option A).
- **근거 문서**: P0 증거 5건(`docs/value-handle/p0/`), P1 설계 6건(`docs/value-handle/p1/`), 용어 정본(repo-root `CONTEXT.md`).

---

## ① ValueSlot + 구조적 수명계약 설계 승인

- **내용**: per-scan ValueSlot(deform-once, 타입벡터 스캔오픈 1회, release 구조체 generation 없음) + CopiedFrame 1급 수명계약(bump 2종+list-scan 전환, PEEK→COPY 강등 본선 제외, tplrec 금지규칙 추가).
- **핵심 P0 반영**: peek/copy 비율이 구조적으로 0.5(동어반복) → 제거 대상은 "행당 반복 접근 + DB_VALUE 경계 복사 형태"로 재정위 (`p1.1-valueslot-design.md` §a.4). 강등 패턴 8개소 전수 검증 후 제외 논증 (`p1.1-lifetime-contract.md` §2.1).
- **page-copy 착지 순서**: 1순위 = issue154(cached scan) 랜딩 후 rebase; P3 착수 시점까지 미랜딩이면 슬라이스 스캔 경로 한정 최소 자체 구현(5개 경계 AC로 범위 침식 방지).
- **판정**: `APPROVE / MODIFY / REJECT`

## ② B안 스케치 + 비용 추정

- **내용**: native-endian 온디스크 포맷 개념 + 마이그레이션 전략 2종(version-tagged lazy rewrite vs 오프라인 툴) + 클라이언트 가시 framing 확장 검토(Option 1 유지 권고, Option 3=프로토콜 버저닝만이 Option 2로 가는 유일 경로).
- **비용 추정**: **9 engineer-months, ±50% (4.5–13.5)** — 입력: Shape A 블라스트 반경 15–20 파일 × 마이그레이션 계수 3–4 + 온디스크 타입 38종 테스트 매트릭스 (`p1.2-b-sketch.md` §3, 하위 서브토탈-포인트추정 정합 근거는 §3.2 재조정 문단 참조).
- **판정**: `APPROVE / MODIFY / REJECT`

## ③ 셀별 문턱값 [X]×3 + [Y]/[Z] + 예측표 (D-G3)

- **재접지 사유**: 원계획의 "[X]는 P0.3 headroom 상한에서 유도"는 P0.3의 동어반복 발견으로 사용 불가 → cmpdisk 비용 구조로 재유도 (`p1.3-proposals.md` (ii)).
- **정렬키 저장 결정 (LEADER, binding — ①/③ 공통 컨텍스트)**: CV-sort 정렬키는 정렬 진입 시 **1회 detoast**하여 **정렬 소유 per-entry storage**에 둔다(비교당 재해제 금지, spec f17/사용자 확인 R14) — `datum1`은 그 storage로의 **포인터**(varchar 8B 콘텐츠-프록시는 1단계에 없음 — abbrev sub-cell 전용); 튜플 본체는 temp pass-through에서 압축 유지(R2는 key-only 인플레이션으로 계상). 폴백 트리거는 **entry-build 시점의 per-entry `area_size` 사전 검사**(`expected_decompressed_size`를 압축 헤더에서 선판독) — 실패 시 해당 entry만 legacy 비교 경로(승리 소실, `Num_sort_key_decompress_fallback`로 계상); 외부정렬 spill은 이미 만들어진 bytes를 run에 그대로 기록. **A_sort_key 모드 주의**(키 집합=출력 집합, W1이 정확히 이 형태): 출력 튜플이 SORT_REC에서 재구성되므로 첫 키 컬럼은 출력 리스트에 **비압축 legal form으로 재직렬화**되어 실린다(출력 인플레이션은 LIMIT 바운드, R2 출력항으로 별도 계상) — 상세는 `p1.1-valueslot-design.md` §s.2/§s.2.1/§s.2.2/§s.4 revised.
- **제안 숫자** (전건 proposal-pending-UG1; 재유도 근거는 `p1.3-proposals.md` (ii)(b) 비용모형 참조):
  | 항목 | 제안값 | 유도 근거 |
  |---|---|---|
  | [X]_FL-filter | 3% — **UG1 policy threshold (not derived)** | dispatch+bswap 비용은 변형 불변(1단계 미제거, P-6/D-G8) → 잔여 이득은 within-row 중복접근 제거뿐이며 그 빈도가 미측정이라 정책값으로 하향 고정 |
  | [X]_CV-sort | **38%** (재유도, 이전 35%) | T_old≈N·log₂N·(2·C_decompress+C_cmp) → T_new≈N·C_decompress+N·log₂N·C_cmp (detoast-once per-entry 정정 기전); r=C_decompress/C_cmp≈50, 비교경로 몫 f_cmp≈40% 가정 |
  | [X]_NUM-sort | **18%** (재유도, 이전 15%) | 동일 모형, r=C_readval/C_cmp≈10, f_cmp≈20% 가정 |
  | [Y] (슬라이스 채택) | 10% — **UG1 policy threshold (not derived)** | 측정식(env-ON/OFF, single-thread W1 median)은 확정; 희석비 자체는 P1 판단, 미유도 |
  | [Z] (B진입) | 40% — **UG1 policy threshold (not derived)** | 측정 프로토콜(perf/VTune inline-aware bswap 귀속, 분모=서버 쿼리 CPU−I/O wait)은 확정; FL-filter/FL-sort 가중치(0.6/0.4)도 정책값 |
- **UG1.5 게이트 규칙**: `PASS_cell ≡ (median_cmpdisk+lazy − median_A-slot) / median_cmpdisk+lazy ≥ [X]_cell`, {FL-filter, CV-sort, NUM-sort} per-cell 측정(median ≥7회, 2회 warmup 제외, 양쪽 variant CoV≤15%, 동일 cubrid_rel+conf/paramdump diff empty, env -i). FL-filter comparator = filter/predicate 비교 경로(정렬 cmpdisk 아님, 인용 정정). 3셀 STOP 진리표: **VALID-PASS**(양쪽 CoV≤15%, 비율≥[X]) / **VALID-FAIL**(양쪽 CoV≤15%, 비율<[X]) / **INVALID**(CoV>15% 또는 오류 → 1회 재측정, 재측정도 무효면 게이트 보류·FAIL로 집계 안 함). **STOP은 3셀 전부 VALID-FAIL일 때만**; 부분 pass는 진행, FAIL 셀은 P3 축소 후보로 기록.
- **예측표**: W1 주 이득처 / W2 중간 / W3 소폭 / W4 ≈0 — P3.8 실측과 사전 대조, 괴리는 UG3 1급 입력.
- **판정**: `APPROVE / MODIFY / REJECT` (숫자 단위 MODIFY 가능)

## ④ Temp framing go/no-go — 실측 class-(d) 볼륨 기반

- **실측**: class-(d) 쓰기 바이트 점유율 = **12.12%** (< 기준선 30%; 커버리지 워크로드, 믹스 민감 — Q1+Q2만이면 99.2%) (`p0.2b` §9).
- **권고**: **framing을 P3 슬라이스에서 제외**, B/캠페인 이연 (D-G6.2; 이 범위 축소는 2026-07-09 사용자 사전 수용됨). 슬라이스는 슬롯+수명계약+정렬 항목(Datum1Cache)에 집중.
- **번복 조건**: 사용자가 DISTINCT/GROUP-BY-heavy 대상 워크로드를 지정하고 그 믹스에서 점유율 ≥30% 실측 시.
- **판정**: `APPROVE(제외) / MODIFY(기준선 변경) / REJECT(framing 유지)`

## ⑤ 4B length-prefix — 볼륨 종속 자동 기각

- **계산**: class-(d) 튜플당 24B 중 4B ≈ 로컬 16.7% × 점유율 12.12% ⇒ 전체 temp 쓰기의 **~2.0%** (믹스 상한 **16.5%** — canonical 값, `p1.1-format-selection.md` §4.3; 이전 16.6%은 반올림 오차로 정정) (`p1.1-format-selection.md` §4.3).
- **권고**: 1단계 **REJECT** (D-G6.4 자동 기각 조건 충족; 역방향 스캔 활성 사용 확인 P0.2).
- **판정**: `APPROVE(기각) / MODIFY / REJECT`

## ⑥ pr_type 확장 훅 목록 (D-G8: retain+extend, 존폐 논의 아님)

- **3-훅**: slot deform 함수 / datum1 추출 훅 / (캠페인 예약) abbreviated-key 변환기 — PG sortsupport 패턴 (`p1.3-proposals.md` (i)). DB_TYPE 제거는 캠페인에서도 논외.
- **판정**: `APPROVE / MODIFY / REJECT`

---

## 기록 전용 (재론 아님)

- **Shape A 확정** (D-G7): 병렬 handle-fetch 경로, ~15–20 파일, ABI 리스크 없음. 비교표는 `p1.1-format-selection.md` §5 — 결정은 닫혀 있음.
- **P1-exit 개방 항목**: P3.0a S_BACKWARD-reachability trace (VERIFY(shape) 12행 업그레이드), page-copy 랜딩 트리거 판정 (P3 착수 시).

## 승인 후 다음 단계

UG1 승인 → P2 (bench harness `unit_tests/value_handle_bench/`, 3-variant × 8-cell 매트릭스 + abbrev sub-cell + 고엔트로피 정렬 픽스처) → UG1.5 (3-헤드라인 셀 판정) → P3 (env-gated 수직 슬라이스) → UG3 (도입/B진입/중단 판정).
