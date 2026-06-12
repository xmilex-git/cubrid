# Dependency preflight — HARD GATE

This skill needs two things. Check **both** before doing any work. **This is blocking, not advisory:** if either is missing, show the install steps below, push for installation, and **do not run**. If the user installs, re-check and continue. **If the user refuses or cannot install, ABORT** — generate no HTML, run no further steps, offer no degraded fallback. End with: `필수 의존성(frontend-design 플러그인 / Playwright MCP)이 없어 이 스킬을 실행할 수 없습니다. 설치 후 다시 불러주세요.`

Rationale: frontend-design is the design engine, and Playwright is the only check that the deck isn't visually broken (the skill's whole purpose). A deck produced without both is exactly the kind of silently-broken output this skill exists to prevent — so it refuses rather than ship one.

## 1. frontend-design plugin (provides `/frontend-design:frontend-design`)

Check (any one is sufficient):

```bash
ls -d ~/.claude/plugins/cache/*/frontend-design 2>/dev/null   # non-empty path => installed
```
or confirm `/frontend-design:frontend-design` appears in the available-skills list in your context.

If missing, tell the user (Korean is fine):

> frontend-design 플러그인이 없습니다. 설치해 주세요:
> 1. `/plugin` 실행 → 마켓플레이스 **claude-plugins-official** 에서 **frontend-design** 설치
> 2. `/reload-plugins` 로 적용
> 설치 후 다시 요청해 주세요.

## 2. Playwright MCP server (provides `mcp__playwright__playwright_*`)

Check (any one):

```bash
claude mcp list 2>/dev/null | grep -i playwright
```
or `ToolSearch` for `mcp__playwright__playwright_navigate` (if it resolves, it's available).

If missing, tell the user:

> Playwright MCP 서버가 없습니다. 추가해 주세요 (예시):
> `claude mcp add playwright -- npx -y @executeautomation/mcp-playwright`
> 그런 다음 세션을 재시작/리로드한 뒤 다시 요청해 주세요.
> (검증 단계에서 `mcp__playwright__playwright_navigate / playwright_resize / playwright_evaluate / playwright_screenshot / playwright_console_logs` 를 사용합니다.)

## Notes

- The exact Playwright MCP package may differ per machine; if the user already has a different Playwright MCP server exposing `mcp__playwright__playwright_*` tools, that's fine.
- Only `/frontend-design:frontend-design` and the Playwright tools are external. Everything else (the HTML/CSS/JS skeleton, the highlighter, the layout) is self-contained in this skill.
