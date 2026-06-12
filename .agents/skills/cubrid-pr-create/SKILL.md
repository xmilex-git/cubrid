---
name: cubrid-pr-create
description: Open a GitHub pull request for the CUBRID project. Use this when the user wants to create a PR for their CUBRID changes.
---

# CUBRID PR Creator

Create GitHub pull requests for the CUBRID project following team conventions.

## When to Use

- User says "create pr", "make pr", "PR 만들어", "PR 올려", "풀리퀘"
- User wants to push changes and open a PR against CUBRID/CUBRID or a fork

## Arguments

Pass optional arguments to customize:

- `/cubrid-pr-create CBRD-26583` — Use this JIRA ticket number
- `/cubrid-pr-create CBRD-26583 feat/oos` — Ticket + base branch
- `/cubrid-pr-create` — Interactive: detect from branch name or ask

## Conventions

### Title Format

```
[CBRD-XXXXX] Short English description
```

- The JIRA ticket number is **required**. Extract from branch name (e.g., `cbrd-26583-oos-compact` → `CBRD-26583`) or ask the user.
- Description should be concise (<60 chars after the tag), in English.
- Use imperative mood: "Fix", "Add", "Refactor", "Support", not "Fixed", "Adding".

### Body Format

- **Section headers (`##`)**: Always in **English**
- **Body text**: Always in **Korean**
- **Code snippets, function names, file paths**: Keep as-is (English/code)

### Required Sections

The JIRA issue link **must** appear at the very top of the PR body, before any section headers. Immediately below the link, include a human-readable `## Summary` block so a reviewer can understand the PR in under 30 seconds without scrolling.

```markdown
https://jira.cubrid.org/browse/CBRD-XXXXX

> **TL;DR**: 1-3 문장으로 이 PR이 무엇을 바꾸는지, 왜 바꾸는지 요약.

## Summary

- **변경**: 한 줄로 무엇을 했는지
- **이유**: 한 줄로 왜 했는지 (관련 이슈/배경)
- **영향**: 한 줄로 영향 범위 (모듈, 호환성, 성능)
- **리뷰 포인트**: 리뷰어가 특히 봐야 할 부분 1-2가지

---

## Description
(변경 사항에 대한 배경 및 설명)

## Implementation
(구현 방법 및 주요 변경 사항 요약)

## Remarks
(참고 사항, 주의점, 후속 작업 등)
```

### Top-of-PR Summary Rules

The `> **TL;DR**` blockquote and `## Summary` block are **required** for every PR. Reviewers have limited time — a focused summary at the top dramatically increases review quality and turnaround.

- **TL;DR**: 1-3 문장, 평문 한국어. 무엇을 바꾸고 왜 바꾸는지 결론부터.
- **Summary bullets**: 각 항목 한 줄. 자세한 내용은 `## Description` / `## Implementation`에서 풀어 쓴다.
- TL;DR과 Summary는 본문의 **요약**이지 본문 자체가 아니다. 같은 문장을 그대로 복붙하지 않는다.
- 단순 typo/주석 수정 같은 사소한 PR에서는 Summary bullets는 생략 가능하나 TL;DR 한 줄은 항상 포함한다.

### Plain Language

PR descriptions are read by reviewers (and later by anyone doing `git log` archaeology). Reviewers may include engineers who joined recently or who own a neighboring module but not this one. Write so a reviewer who hasn't read the JIRA ticket and hasn't opened this file can grasp the change in one pass.

- **Short sentences.** One idea per sentence. Split anything that runs past two lines.
- **TL;DR must stand alone.** A reviewer who never clicks the JIRA link should still know what changed and why. Bad: "CBRD-26583 의 후속 작업으로 OOS 이관 로직 재활성화." Good: "heap 의 OOS (Out-of-row Storage — 큰 가변 컬럼을 외부 페이지로 분리) 이관 로직을 `feat/oos` 브랜치에서 다시 켠다. 직전에 안전을 위해 꺼 두었고 부수 회귀가 없음을 확인했기 때문."
- **Gloss internal terms on first use.** On the first mention in the PR body of a CUBRID-internal concept (`OOS`, `pgbuf_*`, `recdes`, `OR_VAR_*`, `latch`, build-mode names like `SERVER_MODE`), add a one-clause aside in parentheses. After the first gloss, use the term raw. Universal C/DB vocabulary (`malloc`, `mutex`, `assert`) does not need glossing.
- **Concrete over abstract.** Name the file, function, and behavior that changed. "`heap_record_replace_oos_oids` 에러 경로에 `pgbuf_unfix` 추가" beats "에러 처리 안정성 개선."
- **No filler.** Drop "본 PR은...", "전반적으로...", "필요에 따라...". State the fact directly.
- **Bullets over prose** in `## Implementation`. Each bullet = one observable change with a file or function reference.
- **Keep code identifiers in English code-style.** `pgbuf_unfix`, `MVCC`, `feat/oos` — don't translate or paraphrase.
- **Reviewer pointers must be specific.** "리뷰 포인트" 항목은 "`heap_file.c:12345` 에러 경로 정합성"처럼 파일/함수까지 짚는다. "전반적인 흐름 확인"은 안 된다.

### Optional Sections

Add when relevant:

- `## Test Plan` — 테스트 방법 및 검증 계획
- `## Related Issues` — 관련 JIRA 이슈 또는 PR 링크

## Execution Steps

### Step 1: Gather Context

Run these in parallel:

1. `git status` — check for uncommitted changes
2. `git branch -vv` — current branch and tracking info
3. `git remote -v` — available remotes

If there are uncommitted changes, warn the user and ask whether to proceed or commit first.

### Step 2: Determine PR Parameters

1. **JIRA ticket**: Extract from arguments, branch name (`cbrd-XXXXX` or `CBRD-XXXXX` pattern), or ask.
2. **Base branch**: If not specified, detect:
   - For `feat/oos*` branches → base is `feat/oos`
   - For `CBRD-*` branches → base is `develop`
   - For `cubvec/*` branches → base is `cubvec/cubvec`
   - Otherwise ask the user
3. **Target repo**: Default `CUBRID/CUBRID`. Use `--repo` if different.
4. **Source**: Determine the user's fork remote (typically `xmilex` for `xmilex-git/cubrid`). The head ref format is `<github-user>:<branch>`.

### Step 3: Analyze Changes

1. Fetch the base branch: `git fetch <upstream-remote> <base-branch>`
2. Show commits: `git log --oneline <upstream>/<base>..HEAD`
3. Show diff stat: `git diff <upstream>/<base>...HEAD --stat`
4. Read the full diff to understand all changes.
5. If a JIRA ticket was identified, fetch context with `/jira CBRD-XXXXX` for richer description.

### Step 4: Draft PR Content

Based on the diff analysis:

1. **Title**: `[CBRD-XXXXX] Imperative English summary`
2. **Body**: Start with the JIRA link, then a TL;DR + Summary block, then detailed sections:
   - `https://jira.cubrid.org/browse/CBRD-XXXXX` — 맨 위에 JIRA 이슈 링크
   - `> **TL;DR**: ...` — 1-3 문장 요약 (필수)
   - `## Summary` — 변경/이유/영향/리뷰 포인트 bullet (필수, trivial PR 제외)
   - `## Description` — 왜 이 변경이 필요한지 배경 설명
   - `## Implementation` — 주요 변경 내용을 bullet points로 정리. 파일명, 함수명 포함.
   - `## Remarks` — 리뷰어가 알아야 할 참고 사항, 제한 사항, 후속 작업

**Draft the TL;DR + Summary first**, before writing the detailed sections. This forces a clear thesis and reveals when the PR is doing too many unrelated things.

Show the draft to the user and ask for confirmation before creating.

### Step 5: Push and Create PR

1. Push the branch to the user's fork:
   ```bash
   git push <fork-remote> <branch> -u
   ```
2. Create the PR using `gh`:
   ```bash
   gh pr create --repo CUBRID/CUBRID \
     --draft \
     --base <base-branch> \
     --head <user>:<branch> \
     --assignee xmilex-git \
     --title "[CBRD-XXXXX] Title" \
     --body "$(cat <<'EOF'
   https://jira.cubrid.org/browse/CBRD-XXXXX

   > **TL;DR**: 한 줄 요약...

   ## Summary

   - **변경**: ...
   - **이유**: ...
   - **영향**: ...
   - **리뷰 포인트**: ...

   ---

   ## Description
   한국어 설명...

   ## Implementation
   한국어 구현 내용...

   ## Remarks
   한국어 참고 사항...
   EOF
   )"
   ```
3. Print the resulting PR URL.

## Example Output

```
PR created: https://github.com/CUBRID/cubrid/pull/6950

Title: [CBRD-26583] Re-enable OOS OID replacement in heap records
Base:  feat/oos
Head:  xmilex-git:feat/oos-replace-oos-oid
```

## Tips

- If the branch has already been pushed, skip the push step.
- If a PR already exists for the branch, show it instead of creating a duplicate.
- For multi-commit PRs, summarize the overall change rather than listing each commit message.
- Always use `gh pr create` with heredoc for the body to handle multi-line Korean text correctly.

## Mandatory: Iterate with Grill-with-Docs

Every PR description must go through `/grill-with-docs` before `gh pr create`. Do not post a single-pass body. Single-pass PR descriptions drift toward hand-wavy filler, vague TL;DRs, and `## Implementation` bullets that hide what actually changed.

This step is required, not optional. It applies to every PR. No agent-side judgment — including size, scope, perceived triviality, or perceived risk — is a valid skip criterion. The only legitimate skip is when the user, in the message that triggered this skill, explicitly says "skip grill" or "don't grill this" (or unambiguous equivalent: "no grill", "skip the grill loop", "just push it"). If in doubt, do the grill loop.

**How to hand off:**

1. **Draft to a local file first.** Instead of going straight to `gh pr create`, write the PR body to a temp file like `./pr-body-draft.md`.
2. **Invoke `/grill-with-docs`** with:
   - **Topic & purpose**: PR title, JIRA ticket, target reviewers (CUBRID maintainers)
   - **Output path**: the temp draft file (the loop revises in place)
   - **Source material**: the diff (`git diff <upstream>/<base>...HEAD`), `/jira CBRD-XXXXX` output, related issues/PRs
   - **Review angle**: clarity for reviewers, completeness of `## Description` / `## Implementation` / `## Remarks`, adherence to CUBRID PR conventions (Korean body, English `##` headers, JIRA link at the very top, TL;DR carries a clear thesis), TL;DR stands alone without requiring the reviewer to open the JIRA ticket first, every CUBRID-internal term on first use has a one-clause inline gloss so a recently-onboarded reviewer can follow the body on one read
   - **Round cap**: default 5
3. **After approval, create the PR** by passing the polished body to `gh pr create --body "$(cat ./pr-body-draft.md)"`.
