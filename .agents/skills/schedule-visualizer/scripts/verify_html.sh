#!/usr/bin/env bash
# Verify a generated schedule HTML file before delivering it.
#
# Catches the two failure modes that have bitten this design repeatedly:
#   1. Corrupted hex color tokens (e.g. #5b6personally) — autocomplete/edit gremlins
#      that the browser silently ignores, leaving an element rendered wrong.
#   2. Unbalanced block tags (div/td/tr) — a dropped close tag collapses the grid.
#
# Usage: verify_html.sh <file.html>
# Exit:  0 = clean, 1 = problems found (details printed).

set -u

file="${1:-}"
if [[ -z "$file" || ! -f "$file" ]]; then
  echo "usage: verify_html.sh <file.html>" >&2
  exit 2
fi

status=0

# Strip HTML comments before counting — templates legitimately document tags
# (e.g. "<td>", "<tr>") inside <!-- ... --> comment blocks, and those must not
# affect tag-balance or color checks. Work against this comment-free copy.
stripped="$(perl -0777 -pe 's/<!--.*?-->//gs' "$file")"

echo "== corrupted hex colors =="
# A valid hex token is # followed by exactly 3-8 hex digits. Anything with a
# letter g-z inside a #... token is corrupted. Skip the literal "#29"-style
# issue refs by requiring the token to look like a color attempt (>=3 chars,
# starts with hex) — but report anything suspicious so a human can eyeball.
bad="$(printf '%s' "$stripped" | grep -noE '#[0-9a-fA-F]*[g-zG-Z][0-9a-zA-Z]*' | grep -viE ':#[0-9a-f]{3,8}$' || true)"
if [[ -n "$bad" ]]; then
  echo "  FOUND corrupted/suspicious hex tokens:"
  echo "$bad" | sed 's/^/    /'
  status=1
else
  echo "  clean"
fi

echo "== tag balance =="
for tag in div td tr; do
  open="$(printf '%s' "$stripped" | grep -oE "<$tag( |>)" | wc -l | tr -d ' ')"
  close="$(printf '%s' "$stripped" | grep -oE "</$tag>" | wc -l | tr -d ' ')"
  if [[ "$open" == "$close" ]]; then
    echo "  $tag: $open/$close  ok"
  else
    echo "  $tag: $open/$close  MISMATCH"
    status=1
  fi
done

echo "== self-contained check =="
if printf '%s' "$stripped" | grep -qiE '<(link|script)[^>]+(href|src)=["'"'"']https?://'; then
  echo "  WARNING: external http(s) resource referenced — file is not offline-safe"
  printf '%s' "$stripped" | grep -niE '<(link|script)[^>]+(href|src)=["'"'"']https?://' | sed 's/^/    /'
  status=1
else
  echo "  no external resources"
fi

if [[ "$status" -eq 0 ]]; then
  echo "RESULT: clean ✓"
else
  echo "RESULT: problems found — fix before delivering ✗"
fi
exit "$status"
