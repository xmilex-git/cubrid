---
name: cubrid-qa-fetch
description: "Fetch an authenticated page from CUBRID's internal QA portal (qahome.cubrid.org) using credentials from environment variables, save the HTML locally, and report the path for downstream parsing. Use when the user pastes a qahome.cubrid.org URL — typically a shell/SQL/medium test result page like viewShellTestResult.nhn, viewSqlTestResult.nhn, viewMediumTestResult.nhn, or showfile.nhn — and asks to read, parse, analyze, summarize, triage, or extract from it. The portal is behind a login wall; WebFetch and naked curl return only the 2.9 KB login form. Triggers on phrases like 'fetch qahome', 'qahome.cubrid.org', 'qa result page', 'viewShellTestResult', 'shellTestId', 'failed tests at <qa url>', 'NOK list from <qa url>', or any URL on qahome.cubrid.org."
argument-hint: "<qahome-url>"
---

# Fetch an authenticated qahome.cubrid.org page

`qahome.cubrid.org/qaresult/*.nhn` requires a logged-in `JSESSIONID`. The login form posts `m=login&user_id=…&pwd=…` to `/qaresult/user.nhn`; on success the server sets a `JSESSIONID` cookie scoped to `/qaresult`. After that, any `viewShellTestResult.nhn`, `viewSqlTestResult.nhn`, `showfile.nhn`, etc. returns the real HTML.

This skill performs that login flow, fetches the requested URL, and saves the response to a known path so the parent conversation can parse it locally.

## When to use

- User pastes a `https://qahome.cubrid.org/qaresult/...` URL and asks to read or analyze it
- User says "the failed tests are at <url>", "parse the NOK list from <url>", "what core dumped in <url>"
- User asks to re-fetch a previously-tried qahome URL that returned a login form via WebFetch

Do **not** use for:

- Public pages (github.com, jira.cubrid.org public REST). Use WebFetch or `gh` / `/jira`.
- Atlassian Crowd-protected pages outside `qahome.cubrid.org`. The cookie/flow is different.

## Required inputs

| Input | Source | Notes |
|-------|--------|-------|
| URL | argument | Must be on host `qahome.cubrid.org`. Refuse anything else (the credentials are scoped to this site). |
| `CUBRID_QA_USER` | env var | The portal username. |
| `CUBRID_QA_PASS` | env var | The portal password. Never echo, never write to disk, never paste into chat. |

If either env var is missing, **stop and instruct the user how to set them** instead of asking for the password in the conversation:

```sh
# bash/zsh — add to ~/.bashrc or ~/.zshrc
export CUBRID_QA_USER='<your-qa-id>'
export CUBRID_QA_PASS='…'

# nushell — add to env.nu
$env.CUBRID_QA_USER = '<your-qa-id>'
$env.CUBRID_QA_PASS = '…'
```

After they set the vars, the env in *this* Claude Code session may already have them (depending on how the session was launched). If `env | grep CUBRID_QA_` from a Bash tool call shows them, you're ready; otherwise tell the user to restart the session so the new vars are inherited.

## Step 1: Validate inputs

```bash
URL='<the-argument>'
case "$URL" in
  https://qahome.cubrid.org/*|http://qahome.cubrid.org/*) ;;
  *) echo "refusing: URL must be on qahome.cubrid.org"; exit 2 ;;
esac
: "${CUBRID_QA_USER:?CUBRID_QA_USER not set — see skill docs}"
: "${CUBRID_QA_PASS:?CUBRID_QA_PASS not set — see skill docs}"
```

The host check matters: these credentials must not leak to a typo'd domain. Refuse anything that isn't exactly `qahome.cubrid.org`.

## Step 2: Log in and fetch

Run as one Bash tool call so the cookie jar and the protected fetch share state. Pass the password via `--data-urlencode` (no shell quoting issues with special chars) and never echo it:

```bash
JAR=$(mktemp /home/cubrid/dev/cubrid/.claude/scratch/cubrid_qa_cookies.XXXXXX.txt)
# Derive a stable output filename from the URL (host stripped, slashes flattened, query kept)
NAME=$(printf '%s' "$URL" \
  | sed -E 's|^https?://qahome\.cubrid\.org/||; s|[/?&=]|_|g; s|_+|_|g')
OUT="/home/cubrid/dev/cubrid/.claude/scratch/cubrid_qa_${NAME}.html"

# Step 2a: prime a session
curl -sS -c "$JAR" -b "$JAR" -o /dev/null \
  -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64) cubrid-qa-fetch/1' \
  'https://qahome.cubrid.org/qaresult/index.nhn'

# Step 2b: POST credentials. -L follows the 0-second meta refresh to index.nhn on success.
LOGIN_RESP=$(mktemp /home/cubrid/dev/cubrid/.claude/scratch/cubrid_qa_login.XXXXXX.html)
curl -sS -c "$JAR" -b "$JAR" -o "$LOGIN_RESP" -L \
  -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64) cubrid-qa-fetch/1' \
  -H 'Referer: https://qahome.cubrid.org/qaresult/user.nhn' \
  --data-urlencode 'm=login' \
  --data-urlencode "user_id=${CUBRID_QA_USER}" \
  --data-urlencode "pwd=${CUBRID_QA_PASS}" \
  --data-urlencode 'backUrl=' \
  'https://qahome.cubrid.org/qaresult/user.nhn'

# Step 2c: did login work? On success the response is a 340-byte meta-refresh page;
# on failure it's the ~3 KB login form, often with an "alert($('#message').val())" line.
if grep -q 'name="loginform"' "$LOGIN_RESP"; then
  echo "login failed — check CUBRID_QA_USER / CUBRID_QA_PASS"
  rm -f "$JAR" "$LOGIN_RESP"
  exit 3
fi
rm -f "$LOGIN_RESP"

# Step 2d: fetch the target URL with the authenticated jar
curl -sS -c "$JAR" -b "$JAR" -o "$OUT" -L \
  -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64) cubrid-qa-fetch/1' \
  -H 'Referer: https://qahome.cubrid.org/qaresult/index.nhn' \
  -w 'HTTP=%{http_code} size=%{size_download}\n' \
  "$URL"

# Step 2e: post-fetch verification — login page is ~2948 bytes and contains "loginform"
SZ=$(stat -c %s "$OUT")
if [ "$SZ" -lt 4096 ] && grep -q 'name="loginform"' "$OUT"; then
  echo "got login form back from $URL — session was rejected mid-flow"
  rm -f "$JAR"
  exit 4
fi

rm -f "$JAR"
echo "OUT=$OUT  size=$SZ"
```

The two checks (login-form after login POST, login-form after fetch) catch both wrong-password and session-rejected-by-server cases. Without them you can silently parse the login form and report nonsense.

## Step 3: Report and hand off

Tell the user: the saved path (`$OUT`), the size, and one line about what the page looks like (e.g. "results page, ~38 K lines, 32 `[NOK]` markers"). Then proceed to whatever they actually asked for — parsing the failure list, extracting a stack trace, etc.

If the URL is `viewShellTestResult.nhn?...resultType=NOK`, the page structure is:

- Each failing test is a numbered `<b>N. <a ... filePath=...>FULL_PATH</a></b>` with `EnvId=` and a `<pre>…</pre>` block
- Core dumps include a `CORE ANALYZER` section with `SUMMARY : [...]` and a `DETAIL STACK:`
- The "ALL / OK / NOK" links at the top let you swap views by changing `resultType=`

A workable parser (run from a Bash tool call):

```python
python3 - <<'PYEOF'
import re, html
from pathlib import Path
text = Path("/home/cubrid/dev/cubrid/.claude/scratch/cubrid_qa_qaresult_viewShellTestResult.nhn_shellTestId_56485_resultType_NOK.html").read_text(errors="replace")
pat = re.compile(
    r'<b>(\d+)\. <a[^>]*filePath=([^&"]+)[^>]*>([^<]+)</a></b>\s*\(<i>EnvId=([^[]+)\[([^\]]+)\]</I>\)\s*</span>\s*<pre>(.*?)</pre>',
    re.DOTALL,
)
for m in pat.finditer(text):
    n, _fp, full, _e, host, body = m.groups()
    body = html.unescape(body)
    summary = (re.search(r'SUMMARY\s*:\s*\[([^\]]+)\]', body) or [None, ''])[1]
    first_nok = next((ln.strip() for ln in body.splitlines() if 'NOK' in ln), '')[:200]
    print(f"{int(n):2}. {full}")
    if first_nok: print(f"    nok: {first_nok}")
    if summary:   print(f"    CRASH: {summary}")
PYEOF
```

## Failure modes and what they mean

| Symptom | Cause | Fix |
|---------|-------|-----|
| `login failed — check CUBRID_QA_USER / CUBRID_QA_PASS` | Wrong creds, or password contains chars that break shell quoting (this skill uses `--data-urlencode`, so that should not happen). | Verify in a browser. Rotate the env var. |
| `got login form back from $URL — session was rejected mid-flow` | Some QA endpoints additionally check `X-Forwarded-For` or rate-limit by IP; rare but seen. | Retry once. If it persists, fall back to "save the page from your logged-in browser". |
| HTML downloaded but `size < 10 KB` and not a login form | Empty/restricted result. The `shellTestId` may not exist or you lack access to that build. | Confirm the ID in the browser. |
| `curl: (60) SSL certificate problem` | Stale CA bundle or corporate MITM proxy. | `export CURL_CA_BUNDLE=...` or add `--cacert`. Don't paper over with `-k`. |

## Operational notes

- **Never** write `CUBRID_QA_PASS` into the conversation transcript, a log file, the cookie jar, or the output HTML name. The skill reads it via `--data-urlencode "pwd=${CUBRID_QA_PASS}"` from the parent env and that's it.
- The cookie jar is removed at the end of each invocation. If you need to fetch multiple URLs in one session, batch the curl calls inside one Bash tool call (re-use `$JAR` between them) rather than re-running the whole skill — re-running re-logs-in each time and wastes a round trip.
- Output paths are deterministic (derived from the URL). Re-fetching the same URL overwrites the previous file, which is what you want during iteration.
- The user has not asked for an aggressive cache. Don't add one. The QA results for a given `shellTestId` change rarely, but the "currently failing" view of an in-progress build *does* change, and a stale cache will lie.
