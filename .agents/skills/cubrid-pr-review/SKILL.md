---
name: cubrid-pr-review
description: "Review a CUBRID pull request and write a code review report. Use this when the user shares a CUBRID GitHub PR link or asks to review CUBRID code changes."
argument-hint: "<pr-url>"
allowed-tools: Bash(gh *), Bash(git *), Bash(jq *), Bash(scripts/*), Read, Write, Glob, Grep, mcp__plugin_oh-my-claudecode_t__lsp_diagnostics, mcp__plugin_oh-my-claudecode_t__lsp_diagnostics_directory, mcp__plugin_oh-my-claudecode_t__lsp_hover, mcp__plugin_oh-my-claudecode_t__lsp_goto_definition, mcp__plugin_oh-my-claudecode_t__lsp_find_references, mcp__plugin_oh-my-claudecode_t__lsp_document_symbols
---

# CUBRID PR Reviewer

Review CUBRID database engine pull requests and produce a concise Korean review report. CUBRID is a multi-threaded, open-source RDBMS with a large C/C++ codebase, so reviews focus on correctness, memory safety, and concurrency.

## When to Use

- User shares a CUBRID GitHub PR URL (e.g., `https://github.com/CUBRID/cubrid/pull/6950`)
- User says "review this PR", "PR 리뷰", "코드 리뷰 부탁", "리뷰해줘"
- User requests LSP/clangd analysis of PR changes
- Even if the user just pastes a CUBRID PR link without explicit instructions, this skill applies.

## Arguments

- `/cubrid-pr-review <pr-url>` — Review the given PR
- `/cubrid-pr-review` — Ask the user for a PR URL

## Output Format

Write the report to `.claude/review/PR-<NUMBER>/PR-<NUMBER>-report.md` in the repo root (or current directory). The report is **local-only** — never post it to GitHub. A second artifact, `PR-<NUMBER>-comments.md`, holds the post-ready review comments rendered in CUBRID human-reviewer voice (see `voice-guide.md` and Step 6); it is generated after the grill loop and is the only thing meant to be pasted into a GitHub review — and only after explicit user confirmation.

### Language Rules

- **Section headers (`##`)**: English
- **Subsection headers (`###`)**: English (Note: `cubrid-jira-issue-write` uses Korean `###`; for review reports we keep `###` English to match the Findings category names like `Blocking (must fix)`.)
- **Body text**: Korean
- **Tables**: Korean content, English column headers OK
- **Code snippets, function names, file paths**: keep as-is

### Character Restrictions

- **NO emoji** (no checkmarks, crosses, rockets, warnings, etc.) (These examples appear here only as illustrations of what NOT to put in the report.)
- **NO non-BMP Unicode** or special symbols (no `->`, `<-`, check/cross marks, stars, bullets like `●`/`■`)
- Use ASCII alternatives: `->` instead of arrows, `[x]`/`[ ]` for checkboxes, `*`/`-` for bullets
- **Reason**: matches house style across `cubrid-pr-create` and `cubrid-jira-issue-write`, and avoids encoding issues if the report is later pasted into a ticket comment.

### Length Budget

- **Hard cap: 80 lines** for typical PRs, **200 lines** for large multi-module PRs.
- If the report would exceed 80 lines, first try to compress: drop low-signal findings, shorten code excerpts to the smallest illustrative span, collapse adjacent items. Going over 96 lines is permitted only when the PR has 5+ Blocking findings.

### Report Template

```markdown
# PR #<NUMBER> 코드 리뷰 보고서

**PR:** [<OWNER>/<REPO>#<NUMBER>](https://github.com/<OWNER>/<REPO>/pull/<NUMBER>)
**제목:** <PR title>
**작성자:** <author>
**HEAD SHA:** `<head_sha>`
**리뷰 일시:** <today's date>

> **TL;DR** (<Verdict>): 1-3 문장으로 결론과 핵심 이슈 1-2개.

## Summary

- **변경 요약**: 한 줄로 PR이 무엇을 바꾸는지
- **주요 이슈**: 가장 중요한 1-2 항목 (없으면 "없음")
- **확인 필요 사항**: 작성자가 확인/응답해야 할 질문 (없으면 "없음")

---

## Findings

<see Findings Rules below. 발견 사항이 전혀 없으면 이 섹션 본문을 `없음` 한 줄로 대체하고 아래 세 subsection은 모두 생략한다. 일부 카테고리만 비어 있다면 채워진 카테고리만 남기고 비어 있는 subsection은 통째로 생략한다.>

### Blocking (must fix)
<버그, 메모리/동시성 안전성 위반 등 머지 전에 반드시 수정되어야 하는 항목.>

### Non-blocking (should consider)
<수정을 권하지만 머지를 막지는 않는 사항.>

### Questions for the author
<작성자에게 확인이 필요한 질문.>

## JIRA Context
<JIRA 티켓 정보 요약 및 PR이 티켓 의도와 일치하는지 한두 줄. JIRA 없으면 섹션 생략.>

## Existing Comments
<PR에 달려 있고 작성자/메인테이너 답변이 없는 top-level 코멘트(`in_reply_to_id == null` 인 것 중 author/maintainer 답글이 없는 것)만 짧은 표로 정리. 없으면 섹션 생략.>
```

### Top-of-Report Summary Rules

The `> **TL;DR**` blockquote and `## Summary` block are **required** for every report. They exist so a reader can decide in 20 seconds whether the PR is shippable.

- **TL;DR carries the verdict.** TL;DR 라벨은 `**TL;DR**` 뒤 괄호 안에 `Blocking` / `Non-blocking` / `작성자 확인 필요` 중 하나로 적고, 그 뒤 1-3 문장 평문 한국어로 핵심 이슈를 요약한다. 같은 결론을 Summary에 다시 쓰지 않는다.
- **Summary bullets**: 각 항목 한 줄. 자세한 내용은 `## Findings`에서 풀어 쓴다.
- 사소한 PR(주석/typo)에서도 TL;DR 한 줄은 항상 포함한다.

### Findings Rules

This is the single source of truth for how findings are written. The template above just refers here.

- **Signal over volume.** 발견 사항이 없으면 본문을 `없음` 한 줄로 끝낸다. 채우기용 항목을 만들지 않는다. 30줄짜리 보고서가 300줄짜리 보고서보다 낫다. TL;DR과 Summary는 본문의 **요약**이지 본문 자체가 아니다 — 같은 문장을 그대로 복붙하지 않는다.
- **One sentence per finding** is the default — `파일:라인 + 한 문장 설명 + 근거 코드/진단`. 코드 인용은 1-5줄로 충분하다.
- **Every finding needs evidence**: 코드 스니펫(파일:라인)이나 LSP/clangd 진단. "might be wrong" 같은 모호한 지적은 금지.
- **Only flag issues introduced by this PR.** 이미 존재하던 문제, 이미 다른 코멘트에서 지적된 항목, 수정되지 않은 라인은 제외한다.
- **Skip what CI catches.** 포맷팅, astyle, cppcheck 경고 등 CI가 잡는 항목은 보고하지 않는다.

### Plain Language

The report is read by the PR author under time pressure — and the author may be a recent hire who hasn't memorized every acronym in the module. Write so a junior engineer who can read C/C++ but hasn't lived in this file can act on the report in one pass.

- **One idea per sentence.** Short, declarative Korean. If a finding needs three sentences, the second and third belong as evidence (code excerpt) — not as more prose.
- **Lead with the defect, then the cause, then the consequence.** "에러 경로에서 `pgbuf_unfix` 누락 -> 페이지 핀이 영구히 잠긴 채 남아 다른 트랜잭션이 해당 페이지를 잡을 수 없음" beats "전반적으로 살펴보니 ... 가능성이 있어 보입니다." The consequence clause is what tells the author *why* this is blocking, not just *what* is wrong.
- **No hedging or filler.** Drop "~인 것 같습니다", "혹시", "전반적으로", "본 리뷰에서는". State the fact: "에러 경로에서 `pgbuf_unfix` 누락."
- **Keep code identifiers as-is.** Function names, file paths, macros stay in their original English form inside backticks. Don't translate them.
- **Show, don't summarize.** When a finding hinges on a few lines of code, paste those lines (1-5 lines) instead of describing them.
- **Gloss CUBRID-internal terms on first use.** On the first mention of an internal-only concept in the report (`OOS`, `pgbuf_*`, `recdes`, `OR_VAR_*`, "6곳 룰", build-mode names like `SERVER_MODE`/`SA_MODE`, latch protocols), add a one-clause aside in parentheses: "`pgbuf_unfix` (페이지 버퍼 핀 해제)", "6곳 룰 (새 에러 코드는 `error_code.h`, `error_code.c` 등 6개 파일을 모두 갱신해야 함)", "`SERVER_MODE` (서버 프로세스 빌드 모드)". After the first gloss, use the term raw. Universal C/DB vocabulary (`malloc`, `mutex`, `assert`) does not need glossing. If `reference.md` already has the long-form explanation, gloss in one clause and link by name.

### Voice: report vs. post-ready comments

Two artifacts, two voices — do not mix them:

- **`PR-<NUMBER>-report.md`** (analysis, local-only): the terse, declarative **Plain Language** style above — defect -> cause -> consequence, no hedging. For the reviewer/maintainer to assess shippability.
- **`PR-<NUMBER>-comments.md`** (post-ready, Step 6): the CUBRID human-reviewer voice defined in **`voice-guide.md`** — question / assertion / suggestion endings (`~지 않나요?`, `~인 것 같습니다`, `~는 어떨까요?`), `NIT:` for minor items, identifiers kept English, no AI tells / emoji / English verdict labels. The hedged tone that Plain Language bans in the report is *expected* here, because these are the words the author actually reads.

`voice-guide.md` is the single source of truth for comment voice. When the two conflict, Plain Language governs the report and `voice-guide.md` governs the comments.

## Execution Steps

### Step 1: Setup

Parse the PR URL with the helper script and capture metadata in one shot:

```bash
scripts/check-prereqs.sh "$PR_URL"
```

The script prints JSON with `owner`, `repo`, `number`, `head_sha`, `base_ref`, `title`, `body`, `author`, `state`, `draft`. If it exits non-zero, surface the message and stop. If the PR is not open, or is marked draft, warn the user once and ask whether to proceed before continuing.

### Step 2: Gather Context (parallel)

Run these in parallel:

1. **PR diff:** `gh pr diff <NUMBER> -R <OWNER>/<REPO>`. If `gh pr diff` returns empty or fails, surface the error and stop — there is nothing to review.
2. **Existing PR comments**:
   ```bash
   gh api "repos/<OWNER>/<REPO>/pulls/<NUMBER>/comments" --jq '.[] | {id, user: .user.login, path, line: .original_line, in_reply_to_id, body}'
   gh api "repos/<OWNER>/<REPO>/issues/<NUMBER>/comments" --jq '.[] | {id, user: .user.login, body}'
   ```
3. **JIRA context** (if PR title contains `CBRD-XXXXX`): invoke `/jira CBRD-XXXXX` to fetch ticket context.
4. **Read `reference.md`** (sibling file in this skill's directory) for CUBRID-specific review knowledge: error-code six-place rule, memory/error-handling conventions, lock/page-buffer/WAL/MVCC protocols, build-mode guards, key data structures, false-positive guidance. If `reference.md` is missing on this checkout, warn the user once and proceed using only the review categories listed in Step 3 below — do not invent CUBRID-specific rules.
5. **Read any CLAUDE.md / AGENTS.md** in directories containing changed files. Use Glob to walk ancestor directories of each changed file looking for these context files.
6. **Read `voice-guide.md`** (sibling file) for the CUBRID human-reviewer tone applied when rendering post-ready comments in Step 6. If `voice-guide.md` is missing, render comments in the report's Plain Language style instead and note the fallback once.

### Step 3: Review

Read the **full functions** surrounding each diff hunk, not just the hunk. Trace call chains where it matters. Aim for **signal**: prefer reading one suspicious function carefully over skimming ten safe ones. Investigate broadly, report narrowly — deep reading is for filtering findings, not justifying long reports.

Focus on these high-signal categories for CUBRID. One sentence each — the detailed sub-checklists live in `reference.md`:

- **Logic & correctness**: did the change introduce a wrong branch, a missing error path, or an uninitialized read?
- **Memory safety**: are CUBRID's allocator/free-and-init conventions followed and are all error paths leak-free?
- **Concurrency & thread safety**: is shared state protected, and is lock/latch ordering preserved across all paths?
- **Architecture vs JIRA intent**: does the implementation match the ticket's stated goal, and are build-mode guards (`SERVER_MODE`/`SA_MODE`/`CS_MODE`) and updated callers correct?

If the diff touches the SQL parser, broker protocol, or CCI client interface, also check buffer/integer overflow and unchecked input. Skip generic security checklists otherwise.

**LSP analysis (optional).** If `compile_commands.json` is available, run `lsp_diagnostics` on changed files for clangd warnings on changed lines, `lsp_hover`/`lsp_goto_definition` on suspicious types, and `lsp_find_references` if a function signature changed. Skip silently if LSP is unavailable.

If `reference.md` is present and flags a rule (e.g., new error code -> 6 places), don't restate the rule body — link to it by name and point to the file:line that needs updating.

### Step 3b: Sanity Check (mandatory sub-agent)

After the main review pass, spawn a **separate sub-agent** (`subagent_type: "code-reviewer"`) to run a focused sanity scan on the diff. This step is mandatory and cannot be skipped — it catches convention violations that `codestyle.sh` and CI do not enforce.

The sub-agent receives:
- The full PR diff
- The `reference.md` "Comment & Convention Hygiene" section
- Any `CLAUDE.md` from directories of changed files

The sub-agent checks **only** these categories (no logic/concurrency/memory — the main review covers those):

1. **Stale file:line references in comments** — any new comment that pairs a filename with a line number. Flag with: "`<file>:<line>` — 주석에 파일명+라인번호 참조 금지. 심볼 이름으로 교체 필요."
2. **Incomprehensible comments** — comments that only make sense at write-time ("added for the refactor", "fixes last week's bug") or require opening another file to understand.
3. **License header on new files** — every new `.c`/`.h`/`.cpp`/`.hpp` file must have the standard license header.
4. **Commented-out code blocks** — `#if 0` or large `/* ... */` dead code blocks in new code.
5. **`#include` order violations** — `config.h` must come first, then system, then CUBRID headers.
6. **Magic numbers** — bare numeric literals (other than 0, 1, -1, NULL) without a named constant.

The sub-agent returns a flat list of findings (file:line + one-sentence description). Merge these into the main report's Findings section under the appropriate severity:
- Stale file:line refs and incomprehensible comments -> **Non-blocking (should consider)**
- Missing license header -> **Blocking (must fix)**
- Commented-out code, include order, magic numbers -> **Non-blocking (should consider)**

If the sub-agent finds nothing, it returns an empty list and no sanity items appear in the report.

### Step 4: Filter

Drop findings that are:

- **Pre-existing** (not introduced by this PR)
- **Already raised** in existing PR comments
- **Stylistic** (formatting, naming) unless CLAUDE.md mandates it — but **never drop Step 3b sanity findings** (comment hygiene, license headers, include order, etc.); those exist precisely because CI does not catch them
- **On unmodified lines**
- **Out of the PR's stated scope** — don't critique the design of code the PR didn't intend to change, even if the diff exposed it

Every surviving finding needs a code snippet or diagnostic as evidence.

### Step 5: Write the Report

1. **Draft the TL;DR + Summary first.** Write the verdict label and conclusion before the body — this forces a clear stance and reveals whether the rest of the report supports it.
2. **Write Findings tightly.** One sentence per item. Group as Blocking / Non-blocking / Questions, omitting any subsection that has no items. If no category has any items, replace the section body with a single `없음` line and omit all three subsections.
3. **Add JIRA Context and Existing Comments only if useful.** Omit empty sections.
4. **Save** to `PR-<NUMBER>-report.md` in the repo root (or cwd).
5. **Print** three things so the user can sanity-check the call at a glance: (1) the saved file path, (2) the verdict label extracted from the TL;DR (`Blocking` / `Non-blocking` / `작성자 확인 필요`), (3) the TL;DR sentence(s) without the label prefix.

## Example Output

A short finished report looks like this — use it as a shape anchor, not a template to fill out verbatim. The example below is about 30 lines, well under half the 80-line budget; for a typical PR this is plenty.

```markdown
# PR #6950 코드 리뷰 보고서

**PR:** [CUBRID/CUBRID#6950](https://github.com/CUBRID/CUBRID/pull/6950)
**제목:** [CBRD-26583] Re-enable OOS OID replacement in heap records
**작성자:** xmilex-git
**HEAD SHA:** `abc1234`
**리뷰 일시:** 2026-05-06

> **TL;DR** (Blocking): `heap_record_replace_oos_oids` 에러 경로에서 `pgbuf_unfix` 누락으로 페이지 핀 누수 가능. 해당 한 곳만 고치면 머지 가능.

## Summary

- **변경 요약**: heap 레코드의 OOS OID 치환 로직을 `feat/oos`에 다시 활성화
- **주요 이슈**: `heap_file.c:12345` 에러 경로 페이지 핀 누수
- **확인 필요 사항**: 없음

---

## Findings

### Blocking (must fix)
- `src/storage/heap_file.c:12345` — `er_set` 후 `goto exit` 전에 `pgbuf_unfix(thread_p, page_p)` 누락. 해당 함수 진입에서 `pgbuf_fix` 했으므로 모든 종료 경로에서 unfix 필요. 근거: 같은 함수 line 12302의 정상 종료 경로에는 unfix 존재.

## JIRA Context
CBRD-26583 의 목표는 OOS OID 치환 재활성화. 본 PR은 그 범위 안.
```

(JIRA Context only; Existing Comments was omitted because no unresolved top-level comments existed. `Non-blocking` and `Questions for the author` subsections are also omitted because they had no items.)

## Mandatory: Iterate with Grill-with-Docs

Every review report must go through `/grill-with-docs` before being posted or shared. Do not deliver a single-pass review. Single-pass reviews drift toward weak evidence ("might be wrong" hedges), pre-existing-issue leakage, mis-scoped findings, and verdict labels that don't match the Findings.

This step is required, not optional. It applies to every review. No agent-side judgment — including size, scope, perceived triviality, or perceived risk — is a valid skip criterion. The only legitimate skip is when the user, in the message that triggered this skill, explicitly says "skip grill" or "don't grill this" (or unambiguous equivalent: "no grill", "skip the grill loop", "just push it"). If in doubt, do the grill loop.

**How to hand off:**

After saving the initial review to `PR-<NUMBER>-report.md`, invoke `/grill-with-docs` with:

- **Topic & purpose**: PR review report for `<OWNER>/<REPO>#<NUMBER>`, audience is the PR author and CUBRID maintainers
- **Output path**: the same report file (the loop revises in place)
- **Source material**: the PR diff, JIRA ticket context, this skill's `reference.md`, any `CLAUDE.md` / `AGENTS.md` in directories of changed files
- **Review angle**: every Finding has file:line evidence (no "might be wrong" hedging), no pre-existing / CI-caught / out-of-scope items leaked through, TL;DR verdict label matches the Findings, length budget respected (80-line target, 200 hard cap), no emoji or non-BMP unicode, every CUBRID-internal term on first use has a one-clause inline gloss (a junior engineer who has not opened this file should follow the report on one read), every blocking finding spells out the consequence (defect -> cause -> impact), not just the symptom
- **Round cap**: default 5

## Step 6: Render Post-Ready Comments (voice-guide)

After the grill loop finalizes `PR-<NUMBER>-report.md`, convert the surviving findings into review comments a CUBRID maintainer could paste as-is, following `voice-guide.md`. Save them to `PR-<NUMBER>-comments.md` in the same `PR-<NUMBER>/` directory. Do **not** auto-post to GitHub — posting is outward-facing and requires explicit user confirmation. After saving, print the comments file path.

Mapping from report to comments:

- Each **Blocking / Question** finding -> one comment. Lead with `file:line` (inline target), then a 1-3 line body ending in a human reviewer ending: a real bug is short and assertive (`~해야 하는 것으로 보입니다`), a doubt is a question (`~지 않나요?`), a proposal is a suggestion (`~는 어떨까요?`). Cite an existing function / macro / pattern instead of explaining abstractly; attach a short reproducible SQL or a ` ```suggestion ` block when it helps.
- Each **Non-blocking** finding -> a `NIT:` comment (`NIT:` is the only severity label allowed in comment bodies).
- **Existing Comments** the report listed as unresolved: do **not** re-post a near-duplicate. Note in `PR-<NUMBER>-comments.md` that they should be answered as a reply on the existing thread instead.
- Optional top-level comment: one short paragraph summarizing the PR in plain human tone (no `##` headers, no verdict label, no lane / meta terms), followed by `확인 부탁드립니다` and `추가로 논의가 필요해 보이는 항목` groupings for documentation / scope items.

Before saving, run the `voice-guide.md` Section 7 self-checklist (no English headers in bodies, no `Why:` / `Fix:` labels, every comment ends in a human ending, identifiers English, only `NIT:` as a label, no emoji / `LGTM` / `Strongly recommend`, no lane / meta terms, no comment body over 6 lines).
