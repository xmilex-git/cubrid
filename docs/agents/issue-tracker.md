# Issue tracker: GitHub (xmilex-git/cubrid fork)

Issues and PRDs for this repo live as GitHub issues **in the fork
`xmilex-git/cubrid`**, NOT in `origin` (`CUBRID/cubrid`).

⚠️ Never create, comment on, label, or close issues on `CUBRID/cubrid`.
Every `gh` command MUST target the fork explicitly with `--repo xmilex-git/cubrid`
(the cwd's default remote is `origin`, so omitting `--repo` writes to the wrong repo).

## Issue language

Write issue **bodies in Korean** so the maintainer can read them easily.

- Body prose: Korean.
- Section headers (`##`): English is fine (matches the repo's existing convention).
- Keep verbatim, do NOT translate: code identifiers, file paths, function/variable
  names, error codes (e.g. `ER_...`), JIRA keys (`CBRD-XXXXX`), CLI commands, and log
  excerpts.
- Titles: Korean is preferred for readability, but a leading `[CBRD-XXXXX]` /
  `[area]` tag in the repo's usual form may stay as-is.

## Conventions

- **Create**:  `gh --repo xmilex-git/cubrid issue create --title "..." --body "..."`
  (use a heredoc for multi-line Korean bodies).
- **Read**:    `gh --repo xmilex-git/cubrid issue view <n> --comments`
- **List**:    `gh --repo xmilex-git/cubrid issue list --state open \
                 --json number,title,body,labels,comments \
                 --jq '[.[] | {number, title, body, labels:[.labels[].name], comments:[.comments[].body]}]'`
  with appropriate `--label` / `--state` filters.
- **Comment**: `gh --repo xmilex-git/cubrid issue comment <n> --body "..."`
- **Label**:   `gh --repo xmilex-git/cubrid issue edit <n> --add-label "..."` /
               `--remove-label "..."`
- **Close**:   `gh --repo xmilex-git/cubrid issue close <n> --comment "..."`

Triage labels may not exist in the fork yet; create them once before first use:
`gh --repo xmilex-git/cubrid label create <name>`.

## When a skill says "publish to the issue tracker"

Create a GitHub issue in `xmilex-git/cubrid` (Korean body).

## When a skill says "fetch the relevant ticket"

Run `gh --repo xmilex-git/cubrid issue view <number> --comments`.
