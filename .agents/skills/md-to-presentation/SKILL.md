---
name: md-to-presentation
description: Turn a markdown file (code-review walkthrough, spec, or notes) into one self-contained, keyboard-navigable HTML slide deck (VS Code Dark Modern theme); deck output text is Korean. Requires the frontend-design plugin + a Playwright MCP server (hard-gated) and verifies the result before finishing. Use when the user wants to turn an .md into a presentation / slide deck / 발표자료, build presentation.html from a walkthrough or review md, or says "md를 발표 html로", "코드리뷰 슬라이드/발표자료 만들어", "md→html deck".
---

# md → presentation (HTML slide deck)

Turn an input `.md` into a single self-contained `presentation.html` (no CDN, system fonts only), saved **next to the input file**.

**Output language:** the deck's body text is **Korean**; code **comments** inside code panels are **English**. (This SKILL and its references are English for maintenance; the artifact you produce is Korean.)

**Prime directive — get it right on the FIRST write.** Do not treat the Playwright step as a design loop. Build correctly up front by **cloning the lean, already-verified** [reference/skeleton.html](reference/skeleton.html) and replacing only its *data* (`META` + `DECK`); keep its CSS + JS skeleton intact. The verify step should then pass with zero or near-zero fixes.

## ⛔ Step 0 — HARD GATE: required dependencies (no deps → do NOT run)

**This is a blocking gate. Before reading the md, before writing a single byte of HTML, before invoking anything, verify BOTH dependencies are installed. If either is missing, STOP and require installation. If the user declines or cannot install, ABORT the skill — produce nothing, never fall back to a degraded/partial deck.** Both are mandatory: frontend-design supplies the design craft, and Playwright is the *only* way this skill proves the deck isn't visually broken — its whole reason to exist. Running without them defeats the skill, so it does not run.

Run both checks **now**:

1. **frontend-design plugin** — present if either is true:
   - `ls -d ~/.claude/plugins/cache/*/frontend-design 2>/dev/null` returns a path, **or**
   - `/frontend-design:frontend-design` appears in the available-skills list.
2. **Playwright MCP** — present if either is true:
   - `ToolSearch` for `mcp__playwright__playwright_navigate` resolves, **or**
   - `claude mcp list 2>/dev/null | grep -i playwright` matches.

Decide:

- **Both present** → continue to Step 1.
- **Either missing** → do NOT proceed. Show the install steps from [reference/dependencies.md](reference/dependencies.md) **firmly** ("이 스킬은 frontend-design 과 Playwright MCP 없이는 실행할 수 없습니다"), tell the user to install + reload, and **stop this run** (on the next invocation, re-run this preflight from the top).
  - **If the user refuses / says "skip"·"그냥 해"·"없이 진행" / cannot install → ABORT.** Reply with one line — `필수 의존성(frontend-design 플러그인 / Playwright MCP)이 없어 이 스킬을 실행할 수 없습니다. 설치 후 다시 불러주세요.` — and **stop. Generate no HTML, offer no workaround, do not partially run.**

## 1. Parse the input md

Map the md to deck structure: **cover facts** (title, ids, counts, modules) → `META`; each **numbered section** → one slide; **code/diff blocks** → left panel (split a slide into *steps* when it has several diffs — one visible at a time); **notes / tables / labels** → right panel. ASCII tables/matrices become **HTML grid/table components**, never raw text in a code block. The clone target's data shape (Step 3) is `META` (deck-wide copy) + a `DECK` array (one object per content slide: `{id, num, cat, title, sub, file, pills, steps:[{code,right}]}`). Cover / TOC / closing are generated from `META`+`DECK` — do not hand-write them.

## 2. Invoke frontend-design (for craft, within the fixed skeleton)

Call `/frontend-design:frontend-design` to inform craft choices. **Boundary (strict):** it may advise palette/accent, typography, copy density, visual hierarchy, and component usage — it may **not** rewrite the layout CSS, the navigation JS, the scroll mechanics, or the footer/viewport behavior. Those are fixed by [reference/layout-spec.md](reference/layout-spec.md) and the skeleton.

## 3. Build presentation.html — clone the skeleton, swap only data

Start from [reference/skeleton.html](reference/skeleton.html) — a lean, **already-verified** deck driven entirely by a `META` object + a `DECK` array (cover/TOC/closing/footer/start are derived). Replace **only** `META` and `DECK` (and the `:root` palette if a different vibe is wanted). **Do not touch** the CSS, the syntax highlighter, `slideHTML`, or the navigation — that's the part that's easy to get wrong, and it's already right. The normative layout rules and the "what breaks if you skip it" checklist live in [reference/layout-spec.md](reference/layout-spec.md) — read it once and keep the checklist open while you write.

Need a richer worked example (multi-step diffs, pseudocode, data-flow grids, findings tables)? Consult [reference/example-deck.html](reference/example-deck.html) — a full real deck — **only when needed**; it is not the default clone target (it carries domain-specific content).

Output **next to the input md**. Output text **Korean**; code comments **English**.

## 4. Verify with Playwright — a gate, not a redesign

Run [reference/verify.md](reference/verify.md) at **1440×900** and a zoomed **1000×620**. The checks are data-driven (they iterate the actual slides; no hardcoded ids): page scroll = 0 on every slide; every `.code-wrap` bottom stays above the footer (overflowing ones scroll internally); footer full-width pinned; **console errors = 0**; plus screenshots. If something fails it almost always means you edited the skeleton — diff against `skeleton.html` and restore. Re-verify until clean.

## 5. Report

Give the output path. Optionally offer a live preview: a **threaded** `python3` HTTP server from the folder (single-threaded `http.server` hangs when a browser holds a keep-alive — see [reference/verify.md](reference/verify.md)).
