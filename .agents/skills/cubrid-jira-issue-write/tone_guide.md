# CUBRID JIRA Issue — Tone & Output Guide

This file is **authoritative for tone, body structure, output markup, and verbosity.** Where it
conflicts with the templates/rules in `SKILL.md`, this guide wins. Derived from user-reviewed,
actually-filed issues (exemplar: `parallel-groupby-partial-hash-aggregate-assert-user-reviewed.md`,
= the body of CBRD-26927).

The orthogonal rules in `SKILL.md` still apply unchanged: NO emoji, NO non-BMP unicode/symbols,
no `just`/local-only tooling in the body, 평어(한다체), and the "Avoid translationese / AI cadence"
and "Plain Language" sections. This guide only governs *structure, markup, and how much goes in the
body vs. attachments*.

---

## 1. Output is JIRA wiki markup, not Markdown

The issue is pasted straight into jira.cubrid.org, so write JIRA wiki notation, not Markdown.

| Need | Use | NOT |
|------|-----|-----|
| Section label | `*Test Build*` (bold line) | `## Test Build` |
| SQL block | `{code:sql} ... {code}` | ```` ```sql ```` |
| Shell block | `{code:bash} ... {code}` | ```` ```sh ```` |
| Config / plain text block | `{noformat} ... {noformat}` | ```` ``` ```` |
| Numbered steps | `1.` `2.` `3.` | — |
| Inline code identifier | plain text, e.g. `or_advance` | `{{identifier}}`, `_identifier_`, backticks |

- In prose, plain text for code identifiers is fine (e.g. write `or_advance`, `parallelism=0` as
  plain words). Do not wrap function names, file names, macros, parameters, SQL identifiers, or trace
  labels in inline markup.
- Do **not** use JIRA wiki monospace `{{...}}` in issue bodies. The CUBRID JIRA version is old enough
  that this syntax does not render reliably; it may be pasted as literal braces.
- Do **not** use JIRA wiki italic `_..._` for inline code identifiers. Many CUBRID identifiers contain
  underscores, and old JIRA parses those underscores as italic delimiters, producing broken text such
  as partially italicized `fetch_peek_dbval` or literal leading/trailing underscores.
- Use `_..._` only for ordinary prose emphasis without underscores, and only when the sentence is still
  clear if the emphasis is lost.
- The file on disk stays `*.md` (same `.not_git_tracking/jira/issues/` dir), but its **contents are
  JIRA markup** — paste-ready, no conversion step.

## 2. The JIRA body has NO AI scaffolding

NEVER put any of these in the JIRA body, for **any** issue type:

- `Issue Triage` block (목적 / 이유 / 방안)
- `AI-Generated Context` divider / caveat line
- `Summary` bullet block

The body is the classic bug-report sections only. (Triage thinking, if you do it, stays in your
head or in scratch — it does not ship in the ticket.)

## 3. Section set — Correct Error

In order. The leading description has **no label** (it is the JIRA Description field's opening prose):

```
<lead prose: 어떤 상황에서 오류가 나는지 2-3 문장. 상세는 첨부로 미룬다.>

*Test Build*
<cubrid_rel one-liner — see section 4>

*Repro*
<copy-paste 가능한 재현 절차. 자동화 스크립트가 있으면 "첨부 스크립트 실행" 한 줄 + 수동 절차.>

*Expected Result*
<정상 동작 한두 줄.>

*Actual Result*
<사용자가 보는 결과만: SIGABRT + coredump 수준. assert file:line / backtrace 금지 — 첨부로.>

*Additional Information*
<"첨부파일 참고" 또는 한 줄 포인터.>
```

Other issue types (Improve / Development / Refactoring / Internal Management) keep their own
section set from `SKILL.md` (Description / Specification Changes / Implementation / Acceptance
Criteria / Definition of done, etc.) — but still with **no triage/AI-context wrapper** and in JIRA
markup.

## 4. Test Build = `cubrid_rel` output

Paste the string that `cubrid_rel` prints. Format:

```
CUBRID 11.5.0 (11.5.0.2204-ffa4846)
```

- The build number (`2204`) and short git hash (`ffa4846`) are already in that string, so do **not**
  add a separate full 40-char hash, internal fork SHA, `[CBRD-xxxxx]` ref, or OS/container line.
- If it is a debug build, say so plainly (e.g. note `(debug build)`), since asserts only fire in debug.

## 5. Body minimal — detail lives in attachments

- **Actual Result**: user-visible outcome only. No assert expression, no `file:line`, no backtrace.
- **Additional Information**: `첨부파일 참고`, or a single pointer line. Do not enumerate core paths,
  reproduction metadata, or related-issue links in the body.
- Attachments carry everything heavy: the analysis `.md` (full backtrace, call path, root-cause
  hypothesis, per-core signatures, core file paths, reproduction-confirmation metadata) and the
  repro `.sh`.

This reinforces the Correct Error Description-split rule in `SKILL.md`: the body says *what situation
breaks*; the attached analysis says *why and how*.

## 6. Prose tone

Terse and lean. Short sentences, plain Korean, peer-to-peer. No defensive over-glossing in the body
(the analysis attachment is where a term can be explained if needed). State the fact and move on.
