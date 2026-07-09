# UG1 Decision Packet — 값 표현 통일 1단계 설계 승인

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
- **비용 추정**: **9 engineer-months, ±50% (4.5–13.5)** — 입력: Shape A 블라스트 반경 15–20 파일 × 마이그레이션 계수 3–4 + 온디스크 타입 38종 테스트 매트릭스 (`p1.2-b-sketch.md` §3).
- **판정**: `APPROVE / MODIFY / REJECT`

## ③ 셀별 문턱값 [X]×3 + [Y]/[Z] + 예측표 (D-G3)

- **재접지 사유**: 원계획의 "[X]는 P0.3 headroom 상한에서 유도"는 P0.3의 동어반복 발견으로 사용 불가 → cmpdisk 비용 구조로 재유도 (`p1.3-proposals.md` (ii)).
- **제안 숫자** (전건 proposal-pending-UG1):
  | 항목 | 제안값 | 유도 근거 |
  |---|---|---|
  | [X]_FL-filter | 3% | dispatch+bswap만, 곱셈 효과 없음 (최약체 셀) |
  | [X]_CV-sort | 35% | 비교당 malloc+decompress 제거 × O(N log N) |
  | [X]_NUM-sort | 15% | 비교당 double-readval 제거 × O(N log N) |
  | [Y] (슬라이스 채택) | 10% | [X]_CV의 end-to-end 희석분 |
  | [Z] (B진입) | 40% | A안 이득 차감 후 잔여 오버헤드 중 bswap 점유율 |
- **UG1.5 게이트 규칙**: {FL-filter, CV-sort, NUM-sort} per-cell 판정, STOP은 **3셀 전부 미달 시만**.
- **예측표**: W1 주 이득처 / W2 중간 / W3 소폭 / W4 ≈0 — P3.8 실측과 사전 대조, 괴리는 UG3 1급 입력.
- **판정**: `APPROVE / MODIFY / REJECT` (숫자 단위 MODIFY 가능)

## ④ Temp framing go/no-go — 실측 class-(d) 볼륨 기반

- **실측**: class-(d) 쓰기 바이트 점유율 = **12.12%** (< 기준선 30%; 커버리지 워크로드, 믹스 민감 — Q1+Q2만이면 99.2%) (`p0.2b` §9).
- **권고**: **framing을 P3 슬라이스에서 제외**, B/캠페인 이연 (D-G6.2; 이 범위 축소는 2026-07-09 사용자 사전 수용됨). 슬라이스는 슬롯+수명계약+정렬 항목(Datum1Cache)에 집중.
- **번복 조건**: 사용자가 DISTINCT/GROUP-BY-heavy 대상 워크로드를 지정하고 그 믹스에서 점유율 ≥30% 실측 시.
- **판정**: `APPROVE(제외) / MODIFY(기준선 변경) / REJECT(framing 유지)`

## ⑤ 4B length-prefix — 볼륨 종속 자동 기각

- **계산**: class-(d) 튜플당 24B 중 4B ≈ 로컬 16.7% × 점유율 12.12% ⇒ 전체 temp 쓰기의 **~2.0%** (믹스 상한 16.6%) (`p1.1-format-selection.md` §4.3).
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
