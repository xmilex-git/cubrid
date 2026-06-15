---
name: cubrid-jira-issue-write
description: Write a CUBRID JIRA issue report in Korean, lean and paste-ready in JIRA wiki markup (tone_guide.md is authoritative). The body is classic bug-report sections only (no Issue Triage / AI-Generated Context / Summary scaffolding); Test Build is the cubrid_rel output string; heavy detail (backtrace, call path, root cause, core paths) goes to an attached analysis file. Writes to .not_git_tracking/jira/issues. Use when the user wants to write up a JIRA issue, document a bug finding, or create a feature/task report for CUBRID.
---

# CUBRID JIRA Issue Writer

Write structured JIRA issue reports for the CUBRID project. Output is a markdown file saved to `.not_git_tracking/jira/issues`.

> **AUTHORITATIVE TONE & OUTPUT: read [`tone_guide.md`](./tone_guide.md) first.** It governs body
> structure, markup, and verbosity, and **overrides the templates/rules below where they conflict.**
> The short version: output **JIRA wiki markup** (not Markdown); the JIRA body has **NO Issue Triage,
> NO AI-Generated Context, NO Summary** block — classic bug-report sections only; **Test Build = the
> `cubrid_rel` output string**; keep the body minimal and push backtrace / call path / root-cause /
> core paths into the attached analysis file. The `Common Header`, `AI-Generated Context`, and
> `Top-of-Issue Triage Rules` sections below are **superseded for the JIRA body** by `tone_guide.md`
> and are retained only as historical reference — do not emit them into the ticket.

## When to Use

- User says "write a jira issue", "jira로 작성", "이슈 작성", "리포트 작성"
- User has analysis results or findings to document as a JIRA issue
- User wants to formalize a bug report, feature request, or task

## Output Format

The issue file MUST follow these conventions:

### Language Rules

- **Section headers (`##`)**: Always in English
- **Subsection headers (`###`) and body text**: Always in Korean
- **Code snippets, function names, file paths**: Keep as-is (English/code)
- **Tables**: Korean content, English column headers are OK

### Character Restrictions

- **NO emoji** (e.g., no ✅, ❌, 🚀, 📝, ⚠️, etc.)
- **NO non-BMP Unicode characters** or special symbols (e.g., no →, ←, ✓, ✗, ★, ☆, ※, ▶, ◆, ●, ■, □)
- Use ASCII alternatives instead: `->` instead of `→`, `[x]`/`[ ]` instead of `✅`/`❌`, `*` or `-` instead of `●`/`■`
- **Reason**: The CUBRID JIRA API rejects requests containing emoji and many non-ASCII symbol characters. Stick to plain ASCII punctuation, Korean Hangul, and standard CJK characters only.

### File Naming

`CBRD-XXXXX-short-slug.md` where XXXXX is the JIRA ticket number and short-slug is a brief English descriptor.

If no JIRA ticket number is provided, ask the user for it or use a descriptive name.

## Issue Types

Reference: https://dev.cubrid.org/dev-process/jira/open

Always determine the issue type first — section structure depends on it. The four most commonly used types:

| Type | When to use | Korean |
|------|-------------|--------|
| **Correct Error** | Bug or error fix | 버그/에러 수정 |
| **Improve Function/Performance** | Enhance existing feature, perf tuning | 기능/성능 개선 |
| **Development Subject** | Add a new feature | 신규 기능 개발 |
| **Internal Management** | Internal-only work (version bumps, infra) | 내부 관리 |

Other types (use only if above don't fit):

- **Refactoring** — code cleanup / restructuring (uses the Improve template)
- **Task** — fallback when nothing else fits (discouraged)
- **Sub-task** — child of a parent issue

If the type is unclear, ask the user before drafting.

### Required Sections by Issue Type

Every issue starts with the **Issue Triage** block (three required fields for fast triage), followed by an explicitly labeled **AI-Generated Context** block, then the official section list for that type.

The triage block exists because AI-generated issue bodies are often too long to read end-to-end during triage. Reviewers must be able to grasp 목적 / 이유 / 방안 in 10 seconds without entering the AI-written context.

#### Common Header (all types)

Fill the slots in `<...>`. Detail rules live in **Top-of-Issue Triage Rules** below — do not copy the prose hints from there back into the issue.

```markdown
# [TAG] 한국어 제목

## Issue Triage

**이슈 수행 목적** (필수): <결과 상태 1-2 문장>

**이슈 수행 이유** (필수):

- **현재 동작 / 배경**: <기존 매커니즘 + 한계. 임계치는 코드의 상수/매크로 이름으로 인용>
- **영향**: <위 한계가 만드는 실제 문제. 해당되는 한 가지를 골라 구체 예시와 함께>

**이슈 수행 방안**:

- <합의된 결정 1>
- <합의된 결정 2>
- <기존 정책과의 관계 — 혼용 / 대체 / 점진적 마이그레이션>
- <미결정 슬롯: `TBD - ANALYSIS 단계에서 결정` 또는 `TBD - 합의 미확인`>

---

## AI-Generated Context

> 아래 내용은 AI 가 코드/맥락을 분석해 작성한 상세 자료입니다. 빠른 triage에는 위 **Issue Triage** 블록만으로 충분하며, 본문은 구현/리뷰 단계에서 참고하시면 됩니다.

### Summary

- **문제 / 목적**: 한 줄 요약
- **원인 / 배경**: 한 줄 요약
- **제안 / 변경**: 한 줄 요약
- **영향 범위**: 영향받는 모듈, 사용자, 호환성

---
```

#### Correct Error template

```markdown
## Description
(간결하게 "어떤 상황에서 이런 오류가 발생한다" 1-3 문장. 상세 분석은 여기 두지 않는다 — 아래 Correct Error: Description 규칙 참고)

## Test Build
(예: `CUBRID-11.0.0.0248-b53ae4a`, OS 정보 포함. develop 기준 커밋 해시를 명시)

## Repro
(복붙으로 재현 가능한 단계. 서술이 아닌 실행 가능한 명령/SQL)

## Expected Result
(정상 동작 시 기대 결과)

## Actual Result
(실제 관찰된 잘못된 동작)

## Additional Information
(요약 수준의 로그/assert 위치/관련 이슈 링크. 전체 backtrace, 호출 경로, 근본 원인 가설 등 상세 분석은 별도 분석 `.md` 로 분리하고 여기서는 그 파일을 가리킨다)
```

##### Correct Error: Description 규칙 (간결 설명 vs 상세 분석 분리)

Correct Error 이슈의 `## Description` 은 **"어떤 상황에서 이런 오류가 발생한다"** 를 간결히(보통 1-3 문장) 적는다. backtrace 전문, 호출 경로 다이어그램, 코드 인용, 근본 원인 가설은 Description 에 넣지 않는다.

- **Description 에 넣는 것**: 오류를 유발하는 상황(부하/동시성/입력 조건), 관찰되는 실패 모드(crash/assert/오결과), 재현 범위(어떤 조건에서 되고 어떤 조건에서 안 되는지). 핵심 함수/assert 위치는 한두 개만 backtick 으로 짚고 끝낸다.
- **Description 에 넣지 않는 것**: 전체 backtrace, 호출 체인 ASCII 다이어그램, 코드 블록 인용, offset/메모리 디코딩, race 지점 추정 등.
- **상세 분석은 별도 `.md` 파일로 분리한다.** 파일명은 이슈 파일과 같은 디렉터리에 `<이슈-slug>-analysis.md` 로 둔다. 분석 문서에는 backtrace 전문, 호출 경로, 코드 인용, 근본 원인 가설, 재현/비재현 대조표, 환경 상세를 담는다.
- **이슈 본문에서 분석 문서를 가리킨다.** `## Additional Information` 에 "상세 분석: `<slug>-analysis.md` 참고" 한 줄을 둔다. 분석 문서 첫머리에는 어느 이슈의 분석인지 역참조를 적는다.
- **왜 나누나**: Description 이 backtrace 와 분석으로 부풀면 triage/QA/CS 가 "무슨 상황에서 터지는지"를 한눈에 못 본다. 간결한 상황 설명은 모든 독자가, 상세 분석은 구현/디버깅하는 사람만 보면 된다.
- **develop 커밋 해시**: 발생 빌드는 `## Test Build` 에 develop 기준 커밋 해시(짧은 해시 + 풀 해시)로 기록한다. 포크/브랜치 빌드면 "develop `<해시>` 위에 올린 `<브랜치 SHA>`" 형태로 둘 다 적는다.

#### Improve Function/Performance, Development Subject, Refactoring template

```markdown
## Description
(배경, 목적, 문제 정의)

## Specification Changes
(변경되는 스펙. QA/매뉴얼 갱신을 위해 명시. 변경 없으면 N/A)

## Implementation
(설계 및 구현 방법. 코드 흐름, 자료구조, 알고리즘)

## Acceptance Criteria
- [ ] 수락 조건 1
- [ ] 수락 조건 2

## Definition of done
- [ ] 위 A/C 충족
- [ ] QA 통과
- [ ] 문서/매뉴얼 반영
```

#### Internal Management / Task template

```markdown
## Description
(작업의 목적과 설명)
```

### Section Rules

- **Patch/Revision versions** must be written explicitly in the description (JIRA UI only shows Major.Minor).
- Do **not** delete unused sections — replace contents with `N/A` instead.
- Use `TBD` for fields that are not yet known.
- Optional add-ons (project convention, append at the end if useful):
  - `## 참고 코드` — key source file references
  - `## Remarks` — follow-up work, PR links, related tickets

### Top-of-Issue Triage Rules

The `## Issue Triage` block is **required** and must be the first content after the title. It exists so a reviewer can decide priority/assignment in under 10 seconds without reading any AI-generated context.

**Issue Triage block — three fields:**

The Common Header above defines the schema. The rules below define what counts as a *good* fill.

- **이슈 수행 목적 (필수)**: 결과 상태 1-2 문장. 분석·배경은 금지 — 그건 이유로 간다.
- **이슈 수행 이유 (필수)**: 두 축(**현재 동작·한계** + **영향**)을 모두 짚는다.
  - **현재 동작 / 배경 요건**: 임계치·매개변수·조건은 코드의 매크로/상수/함수 이름으로 인용한다 (예: `DB_PAGESIZE/8`, `pgbuf_fix`). 파일·라인 번호도 곁들이면 가장 좋다. "현재 약 512 바이트" 같은 어림 표기 금지.
  - **영향 요건**: 해당되는 한 가지(고객 장애 · QA 실패 · 성능 저하 · 설계 의도 훼손 · 기술 부채 중 하나)를 골라 구체 예시와 함께 적는다. 다섯 가지를 모두 늘어놓으면 menu-padding 이 된다.
  - 추상적 한 줄("일관성 유지", "성능 개선 필요") 금지.
  - **Correct Error 특례**: 버그 티켓은 두 축이 짧게 collapse 한다 — 현재 동작 = 한 줄 재현 요약, 영향 = 사용자가 보는 실패 모드. 그래도 두 항목은 분리해서 적는다.
- **이슈 수행 방안**: 결정된 스펙은 구체적으로 적고, 미결정만 TBD 로 남긴다.
  - 합의된 결정(임계치, 알고리즘, 적용 옵션, 외부 레퍼런스, 기존 정책과의 관계)은 bullet 로 명시. 합의된 내용을 "TBD" 로 덮어쓰지 말 것.
  - **무엇이 "합의된" 것인가**: 이 세션 사용자 메시지의 인용 가능한 구체 결정 · 인용 가능한 JIRA 코멘트 · 명시적 설계 문서 — 이 세 출처만 합의로 간주한다. 사용자 메시지를 근거로 들 때는 원문 일부를 큰따옴표로 함께 적는다 ("사용자 인용: \"...\""). 유사 티켓과의 유추, AI 의 그럴듯한 추론은 합의가 아니다.
  - **TBD 마커 선택**: 분석 단계로 미루는 것이 명시적으로 합의된 영역은 `TBD - ANALYSIS 단계에서 결정`. 결정 존재 여부 자체가 불확실하면 `TBD - 합의 미확인` 을 쓴다. 헷갈리면 `TBD - 합의 미확인` 으로 보수적으로 표기해서 리뷰어가 명시적으로 잡아내도록 한다. 대화형 세션이면 사용자에게 직접 묻는다.
  - 세부 코드 흐름/자료구조는 `## Implementation` 으로 미룬다. 방안에는 "무엇을 결정했는가" 만.

**AI-Generated Context block — separation rule:**

모든 AI 분석 결과(Summary 불릿, Description, Implementation, 흐름도, 코드 인용 등)는 `## AI-Generated Context` 헤더 이후에 둔다. 이 분리는 리뷰어가 "사람이 직접 작성한 triage 요약"과 "AI 가 채운 상세 맥락"을 구분할 수 있게 해 준다.

**Anti-patterns:**

- TL;DR 부활시키기: `> **TL;DR**:` 블록은 더 이상 쓰지 않으므로, 대신 `## Issue Triage` 의 세 필드를 채운다.
- 목적/이유 합치기: 목적과 이유는 별개 필드라, "X 를 Y 하기 위해 Z 한다" 한 문장으로 두 필드를 한꺼번에 메우지 말 것.
- 이유 빈약 작성: "성능 개선이 필요하다" 같은 추상적 한 줄로 끝내지 말 것. 구체적 수치/임계치/조건이 빠진 이유는 미흡한 이유다.
- 방안 양극단 — 둘 다 reject: (1) 합의되지 않은 구현 계획을 방안에 추가 (과잉 추측), (2) 합의된 스펙을 "TBD" 로 덮음 (과소 작성). 무엇이 합의인지 모르겠으면 위 "무엇이 합의된 것인가" 항을 따른다.
- 컨텍스트 누수: AI 분석 결과를 triage 블록 안에 끌어다 두지 말 것. AI 분석은 `## AI-Generated Context` 아래에만 둔다.

**구조 라벨 예외 (triage 블록 한정)**: 다음 다섯 개 라벨만 "Avoid translationese" 의 영문 직역 라벨 금지 규정에서 예외다 — `**이슈 수행 목적**`, `**이슈 수행 이유**`, `**이슈 수행 방안**`, `**현재 동작 / 배경**`, `**영향**`. 이외의 영문/혼용 라벨(`**Fact**:`, `**Risk**:`, `**Mitigation**:`, `**무엇을**:` 등)은 본문 어디에서도 금지 규정을 따른다. 본문 산문이 아니라 triage 슬롯 식별자라서 예외를 둔다.

**Worked example (OOS migration policy change — Improve Function):**

```markdown
## Issue Triage

**이슈 수행 목적**: heap 레코드의 큰 가변 컬럼이 OOS 의도대로 일관되게 외부로 이관되도록 한다.

**이슈 수행 이유**:

- **현재 동작 / 배경**: 현재 코드는 레코드 총 길이가 `DB_PAGESIZE/8` 을 넘는 경우에만 512 바이트 초과 가변 컬럼을 OOS 로 보내므로, 511 바이트 가변 컬럼만 있는 레코드는 임계치에 못 미친 채 overflow 경로로 빠진다 (개발 편의 목적의 임시 임계치).
- **영향**: 설계 의도 훼손 — OOS 도입 의도와 달리 511 바이트 가변 컬럼 페이로드는 OOS 대상에서 누락된 채 heap 내부 overflow 로 빠지므로 OOS 도입 효과가 무력화된다.

**이슈 수행 방안**:

- 레코드 총 길이가 `DB_PAGESIZE/4` 를 넘으면 가장 큰 가변 컬럼부터 순차적으로 OOS 로 이관하며, `DB_PAGESIZE/4` 이하가 될 때까지 반복한다.
- OOS 이관 시 lz4 압축을 적용하되, P 사 기본값인 EXTENDED 모드를 차용한다.
- P 사의 다른 정책(MAIN, EXTERNAL, PLAIN 등)은 본 이슈 범위 밖이며 CBRD-26536 으로 분리한다.
- btree/schema 등 기존 overflow 정책은 유지하고, heap 의 고정 컬럼 overflow 만 점진적으로 대체한다.
- lz4 압축 레벨 세부값: `TBD - 합의 미확인`.
```

핵심: **이유** 가 임계치를 매크로 이름으로 인용하고 영향을 한 카테고리(설계 의도 훼손) + 구체 시나리오로 좁혔으며, **방안** 이 합의된 결정만 bullet 로 적되 범위 밖 항목은 별도 티켓으로 분리하고 미확인 항목은 보수적 마커로 표기했다.

### Style Guide

1. **Title format**: `# [TAG] 한국어 설명` — TAG is a short category like `[OOS]`, `[BTREE]`, `[BROKER]`
2. **Lead with Issue Triage block** (목적/이유/방안) — human-readable triage summary before any AI-generated context
3. **Separate AI context** with `## AI-Generated Context` header — all detailed analysis lives below this divider
4. **Use `---` horizontal rules** between major sections
5. **Tables** for structured data (function lists, format changes, comparison)
6. **Code blocks** with language annotation for source code
7. **Flow diagrams** using ASCII art in code blocks for call chains
8. **Bold** for emphasis on key terms
9. **Backticks** for all function names, variable names, file paths, and code references
10. Keep paragraphs concise — prefer bullet points and tables over long prose
11. Acceptance criteria as markdown checkboxes (`- [ ]`)

### Plain Language

Write the issue so a teammate from a different module — QA, customer support, a new hire — can read it once and understand. JIRA tickets travel far beyond the original author.

- **Short sentences.** One idea per sentence. If a sentence runs past two lines, split it.
- **Plain Korean over jargon.** Use ordinary words; only keep CUBRID-internal terms (function names, file paths, protocol acronyms) when they're load-bearing. Don't translate well-known English code identifiers (`pgbuf_fix`, `MVCC`, `WAL`) — keep them in code-style as-is, and gloss them on first use per the rule above.
- **Lead with what changed, then where, then why.** "heap 의 OOS 이관 임계치를 `DB_PAGESIZE/8` 에서 `DB_PAGESIZE/4` 로 올린다 (`heap_file.c:12300` 부근) — 511 바이트 가변 컬럼이 OOS 대상에서 빠지는 문제 때문" reads in one pass; the same facts in scrambled order do not.
- **Concrete over abstract.** "에러 코드 6곳을 모두 갱신해야 한다" beats "전반적인 일관성을 유지해야 한다." Name the file, the function, the number.
- **No filler.** Drop phrases like "본 이슈에서는...", "필요에 따라...", "전반적으로...". State the fact directly.
- **Reproducible Repro.** The Repro section should be copy-pasteable commands or SQL, not narrative prose.
- **One-pass readability check.** After drafting, re-read each paragraph and ask: "Could a new hire who knows C/C++ but has never opened this file follow this sentence?" If not, either gloss the term or restructure.

### Local-only tooling (justfile, personal aliases, dotfiles)

JIRA issues are read by every dev, QA, and CS person — most of them do not share the author's personal tooling. Keep commands in the issue portable.

- **Never write `just <recipe>` in an issue body, Repro, Acceptance Criteria, or table.** The `justfile` lives in the author's local workspace; a reader running `just shell-debug` gets `command not found`. Substitute the underlying command the recipe wraps (e.g., `ctp.sh shell -c shell_ci.conf`, plus a 1-line note on how to point the conf's `scenario` at the test path if relevant).
- **Same for personal aliases / functions** (`my-rerun`, `cb`, custom shell helpers, sourced dotfiles). If it isn't in the public CTP/CUBRID toolchain or shipped with the project, it doesn't belong in the issue body.
- **Acceptable wrappers** (these are universal to a CUBRID engineer's environment): `ctp.sh ...`, `cubrid ...`, `csql ...`, `make ...`, `cmake ...`, `gh ...`, raw `bash ...`, `sh ...`. Prefer these over anything custom.
- **If a personal recipe is the easiest repro path for the author**, paraphrase the underlying command in the issue and keep the `just`/alias form in private notes only. Do not put both — readers will copy-paste the unportable one.
- **Pre-upload scan**: `rg -nP '\bjust\s+\w' file.md` must return zero hits. Same for any other author-local tool the reviewer flags (project-specific aliases, wrapper scripts not in `$PATH` of a fresh CUBRID dev VM).

### Audience: any CUBRID engineer, including new hires

Readers include the CTO, team lead, and senior peers — but also QA, customer support, and engineers who joined last month and have never opened this module. JIRA tickets travel far beyond the original author. Write so a new hire who can read C/C++ but has not internalized this subsystem's jargon can follow on one read.

- **Gloss internal terms on first use.** Acronyms and module-specific identifiers (`OOS`, `recdes`, `attrepr`, `pgbuf_*`, `OR_VAR_*`, `assert_release`, `WAL`, `MVCC`, `latch`, `OID`, `heap`/`btree` policy names, build-mode names) get a short inline aside on first mention — one clause, not a paragraph. Examples:
  - "`OOS` (Out-of-row Storage — heap 의 큰 가변 컬럼을 외부 페이지로 분리하는 저장 방식)"
  - "`pgbuf_fix` (페이지 버퍼 풀에서 페이지를 잠가 가져오는 함수)"
  - "`recdes` (heap 레코드 디스크립터 구조체)"
- **Once is enough.** After the first gloss, use the term raw — do not re-define it in every section. If a term appears once and is universal C/DB knowledge (`malloc`, `free`, `assert`, `mutex`), skip the gloss.
- **Explain the "왜 중요한지" for non-obvious thresholds and policies.** A magic number with no rationale is unreadable. "`DB_PAGESIZE/8` 미만이면 OOS 이관이 일어나지 않아 큰 가변 컬럼이 heap 내부로 흘러들어간다" beats a bare "`DB_PAGESIZE/8`".
- **Still no tutorial mode.** A 1-line gloss is fine; a paragraph explaining what a heap is, is not. Readers know relational databases — they just do not know *this* codebase's spelling.
- **No meta-labels in headers.** `### 왜 (한 번만 설명)`, `### \`*is_oos\` 계약 (호출자가 알아야 할 것)` — the parenthetical is an author's note to self. Drop it.
- **No obvious-statement filler.** If reading the diff or running the Repro makes it obvious, do not say it.

### Avoid translationese and AI cadence

The biggest tell of LLM-written prose is rhythm. Hunt for these and rewrite.

**Translationese — word-for-word English idioms:**

| Avoid | Use |
|---|---|
| "에러가 ... 흘러간다" | "결과셋에 섞여 나간다", "그대로 반환된다" |
| "측면도 문제다" / "측면에서는" | restructure to drop "측면" |
| "수용한다" (limitation) | "그대로 둔다", "받아들인다" |
| "이렇다." (앞에 두고 코드 블록) | colon `:` 찍고 코드로 |
| "위함이다" / "기 위함이다" | "위해서다", "기 위해서다" |
| "그렇게도 안 한다" | "그것조차 하지 않는다" |

**AI lockstep cadence — multiple short "한다." sentences in a row.** Vary endings with `-므로`, `-기 때문에`, `-라`, `-도록`, longer subordinate clauses.

**Structural patterns to avoid:**

- **English-direct labels.** `**무엇을**:` / `**어떻게**:` / `**왜**:` are direct renderings of "What:/How:/Why:". Use Korean: `**변경**:`, `**부수 수정**:`, `**영향**:`. Same for header `### 왜` — use `### 배경` / `### 발단`.
- **Sentence-fragment 명사구 종결 ("...없음.", "...적용.", "...불필요.") in body prose.** OK in table cells, NOT in paragraphs.
- **`Fact: / Effect: / Ops 결론:` style bullet labels** — ITIL/RFC parody tone. Use peer-to-peer prose.
- **`->` arrows in subheaders.** Use descriptive Korean (`#### Case 4 — 메모리 부족`).
- **Self-narration filler.** Drop "다음과 같이 정의한다", "위 사항을 반영하여...", "본 티켓에서는 ... 한다".
- **존댓말 leak.** JIRA bodies are 평어 (한다체). Fix any `합니다` / `입니다`.

### Avoid duplication across sections

The same rationale appearing in Issue Triage + Summary + Description + Implementation + A/C erodes trust fast.

- **Issue Triage 목적/이유**: 결과 상태 + 근거. 메커니즘/구현은 NO.
- **Issue Triage 방안**: 작성 시점에 아는 수준만. 상세 설계는 `## Implementation` 으로.
- **Summary bullets**: ≤ 1 line each, triage 블록과는 다른 정보를 추가 (예: 영향 범위, 호환성).
- **Description**: 정식 "why". 위에서 이미 한 말을 그대로 복붙하지 말고 깊이를 더한다.
- **A/C**: checklist 항목. 산문 재서술 NO.
- **Out-of-scope vs. A/C**: 각 사실은 한 곳에만 등장.

After drafting, grep for sentences appearing in 2+ sections; pick the strongest location.

## Reference Examples

Refer to existing issues in `.not_git_tracking/jira/issues/` for style consistency. Currently present:

- `parallel-groupby-partial-hash-aggregate-assert.md` + `parallel-groupby-partial-hash-aggregate-assert-analysis.md` — **Correct Error, canonical example of the Description-split rule.** 이슈 본문은 간결한 상황 설명(`## Description` 은 "어떤 상황에서 오류" 만)이고, 전체 backtrace / 호출 경로 / core 별 offset signature 는 companion `-analysis.md` 로 분리. 새 Correct Error 이슈는 이 쌍을 따른다.
- `CBRD-XXXXX-cas-isnotnull-fold-crash.md` — Correct Error (`[PARSER]` CAS SIGSEGV). 권한 토글로 인과를 증명/반증하는 결정적(10/10) Repro 가 좋은 본보기다. 단, 이 파일은 Description-split 규칙 이전 작성물이라 Root cause/호출 그래프를 `## Description` 안에 인라인으로 둔다 — 분리 레이아웃은 위 parallel-groupby 쌍을 따를 것.

## Execution Steps

1. **Check output directory**: Verify that `.not_git_tracking/jira/issues/` exists. If it does NOT exist, **stop immediately** and tell the user: "Error: Issue directory `.not_git_tracking/jira/issues/` does not exist. Please create the directory first (e.g. `mkdir -p .not_git_tracking/jira/issues/`)." Do NOT create the directory automatically.
2. **Determine the issue type**: Pick from `Correct Error`, `Improve Function/Performance`, `Development Subject`, `Internal Management` (or `Refactoring` / `Task`). Section structure depends on it. If unclear, ask the user.
3. **Gather context**: Read relevant source code, prior analysis, or conversation context
4. **Write the issue body in JIRA wiki markup per `tone_guide.md`**: classic bug-report sections only — NO Issue Triage, NO AI-Generated Context, NO Summary. Lead with unlabeled description prose, then `*bold*` section labels. For **Correct Error**, keep the lead description concise ("어떤 상황에서 이런 오류가 발생한다") and push full backtrace / call path / root-cause into the companion analysis file (Correct Error: Description 규칙). `Test Build` = the `cubrid_rel` output string. Keep `Actual Result`/`Additional Information` minimal — detail goes to attachments.
5. **Save the file(s)**: Write the issue to `.not_git_tracking/jira/issues/CBRD-XXXXX-slug.md` (JIRA markup inside a `.md` file). If a companion analysis was split out, save it as `CBRD-XXXXX-slug-analysis.md` in the same directory and point to it from the body's `*Additional Information*`.
6. **Show the user**: Print the file path(s), the chosen issue type, and a 2-3 line gist so the user can sanity-check the framing at a glance.

## Arguments

Pass the JIRA ticket number and/or topic as arguments:

- `/write-jira-issue CBRD-26583 OOS compact analysis` — Write issue for specific ticket
- `/write-jira-issue` — Interactive mode, ask user for details

## Mandatory: Iterate with Grill-with-Docs

Every JIRA issue draft must go through `/grill-with-docs` before being filed. Do not post a single-pass issue. Single-pass issues drift toward hand-wavy filler, missing or non-executable Repro steps, unsupported root-cause claims, and TL;DRs that just restate the body. JIRA tickets are read across QA, dev, and customer support by people with no other context, so unclear writing has a long blast radius.

This step is required, not optional. It applies to every issue. No agent-side judgment — including size, scope, perceived triviality, or perceived risk — is a valid skip criterion. The only legitimate skip is when the user, in the message that triggered this skill, explicitly says "skip grill" or "don't grill this" (or unambiguous equivalent: "no grill", "skip the grill loop", "just push it"). If in doubt, do the grill loop.

**How to hand off:**

After saving the initial draft to `.not_git_tracking/jira/issues/CBRD-XXXXX-slug.md`, invoke `/grill-with-docs` with:

- **Topic & purpose**: JIRA ticket number, issue type (Correct Error / Improve / Development Subject / etc.), audience (CUBRID dev team, QA, customer-facing)
- **Output path**: the same file path so the loop revises in place
- **Source material**: relevant source files, prior analysis, `/jira CBRD-XXXXX` output, repro logs
- **Review angle**:
  - Technical accuracy, reproducibility (Repro section is executable).
  - CUBRID conventions: Korean body, English `##` headers, NO emoji, NO non-BMP unicode.
  - **Issue Triage block** present at the top with all three fields (목적/이유/방안) filled and not collapsed into one sentence.
  - **Triage depth — 이유**: cites current behavior with code-named thresholds/macros/functions AND names the resulting impact. Abstract one-liners ("성능 개선 필요", "일관성 유지") are reject criteria.
  - **Triage depth — 방안**: already-decided spec listed as concrete bullets (thresholds, algorithms, options, external references, scope splits with ticket numbers). Pure-TBD 방안 when decisions exist is a reject. AI-invented implementation details are a reject.
  - **AI-Generated Context divider** clearly separates AI-written detail from the triage summary.
  - Summary/Description don't duplicate the triage block verbatim.
  - **New-hire readability**: every CUBRID-internal acronym or module-specific identifier on first use has a one-clause inline gloss; every threshold/magic number has a one-clause rationale. A junior engineer who can read C/C++ but has not opened this file should be able to follow the issue on one read. Untreated insider shorthand is a reject.
  - **Natural Korean prose**: the "Audience: any CUBRID engineer, including new hires" and "Avoid translationese and AI cadence" sections above must be passed to the reviewer verbatim.
- **Round cap**: default 5
