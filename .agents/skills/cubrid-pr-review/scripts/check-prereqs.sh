#!/usr/bin/env bash
# check-prereqs.sh — Parse PR URL and fetch metadata
#
# On success: prints JSON metadata to stdout
# On failure: prints error to stderr and exits non-zero

set -euo pipefail

PR_URL="${1:-}"

if [[ -z "$PR_URL" ]]; then
  echo "FAIL: No PR URL provided." >&2
  echo "Usage: cubrid-pr-review https://github.com/CUBRID/cubrid/pull/6930" >&2
  exit 1
fi

if [[ ! "$PR_URL" =~ ^https://github\.com/([^/]+)/([^/]+)/pull/([0-9]+)$ ]]; then
  echo "FAIL: Invalid PR URL format: $PR_URL" >&2
  echo "Expected: https://github.com/OWNER/REPO/pull/NUMBER" >&2
  exit 1
fi

OWNER="${BASH_REMATCH[1]}"
REPO="${BASH_REMATCH[2]}"
PR_NUMBER="${BASH_REMATCH[3]}"

PR_JSON=$(gh api "repos/$OWNER/$REPO/pulls/$PR_NUMBER" 2>&1) || {
  echo "FAIL: Could not fetch PR #$PR_NUMBER from $OWNER/$REPO" >&2
  echo "$PR_JSON" >&2
  exit 1
}

echo "$PR_JSON" | jq '{
  status: "OK",
  owner: "'"$OWNER"'",
  repo: "'"$REPO"'",
  number: '"$PR_NUMBER"',
  pr_url: "'"$PR_URL"'",
  head_sha: .head.sha,
  base_ref: .base.ref,
  state: .state,
  title: .title,
  draft: .draft,
  author: .user.login,
  body: (.body // "")
}'
