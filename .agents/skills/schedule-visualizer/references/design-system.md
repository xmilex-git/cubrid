# Schedule Visualizer — Design System & Data Model

This document is the single source of truth for the visual language and the data
you need to collect before filling a template. Read it once, then work from the
templates in `../assets/`.

## Table of contents
1. Two output formats (when to use which)
2. Data model — what to collect from the user
3. Buckets & color palette
4. Chip anatomy (calendar)
5. Bar/row anatomy (Gantt)
6. Special cells: holiday, milestone, buffer
7. Layout rules & gotchas
8. Verification (always run before delivering)

---

## 1. Two output formats

| Format | File | Best for | Granularity |
|---|---|---|---|
| **Daily calendar** | `calendar-template.html` | "엑셀처럼 칸 나눠서", day-by-day plans, holidays, who-does-what-when | one cell per working day (Mon–Fri) |
| **Gantt timeline** | `gantt-template.html` | high-level overview, overlapping workstreams, critical-path emphasis | bars spanning week columns |

They share one palette and one mental model (buckets → colors). You can produce
either or both. When the user wants "한 페이지 요약" → Gantt. When they want
"날짜별로 뭘 하는지" → calendar. When unsure, ask, or produce the calendar
(it carries more concrete information).

---

## 2. Data model — collect this first

Before touching a template, pin down the inputs. Ask the user only for what you
can't infer from the conversation/repo.

**Project frame**
- Start date + weekday (e.g., "2026-06-01, Monday"). All cells derive from this.
- Working week: assume Mon–Fri unless told otherwise.
- Holidays / non-working days (exact dates) — these become shaded cells.
- Target/milestone date (e.g., merge date) — becomes the 💎 milestone cell.
- Buffer days (after the milestone).

**Work items** (each becomes a chip / bar + a dictionary row)
- `id` — exact identifier (JIRA key, PR #, ticket). Never invent or approximate.
- `summary` — one short line, in the user's language.
- `bucket` — one of the buckets in §3 (drives color).
- `status` — e.g., Develop / Confirmed / Open / Done / 미머지. Optional but useful.
- `day(s)` — when it's worked / reviewed / merged.
- `excluded?` — items explicitly out of scope go in a greyed dictionary row, not on the calendar.

**Schedule logic** (ask, don't guess — these swing the dates the most)
- What's the critical path? (often human review, not coding)
- Velocity: people, days-per-item, review throughput (items merged per week).
- What gates "done"? (CI green, manual test, review sign-off)

If the user corrects an assumption (e.g., "manual test is 2h not 2 days",
"remove sync"), regenerate — don't leave stale rows. A schedule the user has
to mentally patch is worse than no schedule.

---

## 3. Buckets & color palette

Buckets are semantic categories, not arbitrary colors. Pick the bucket that
matches the *nature* of the work so the colors carry meaning at a glance.

| Bucket | CSS class | Accent | Use for |
|---|---|---|---|
| dev | `c-dev` / `a-dev` | blue `#2f6df0` | feature work, in-progress issues being finalized |
| refactor | `c-refactor` / `a-refactor` | purple `#7c3aed` | refactors, cleanups, signature changes |
| regress | `c-regress` / `a-regress` | orange `#e8590c` | bug/regression fixes, failing-test triage |
| test | `c-test` / `a-test` | green `#0ca678` | CI, manual test, verification |
| sync | `c-sync` / `a-sync` | amber `#d9a000` | branch sync / rebase / merge-from-upstream |
| proc | `c-proc` / `a-proc` | teal `#0b7285` | process: kickoff, review coordination, ticket close |
| merge | `c-merge` / `a-merge` | green `#2f9e44` | the actual merge / release / milestone |
| buffer | `c-buffer` / `a-buffer` | grey `#868e96` | contingency / slack days |

The palette is defined once in each template's `:root`. **Do not invent new hex
values inline** — reuse the CSS variables/classes. (Inline ad-hoc colors are the
#1 source of the corrupted-hex bug; see §8.) If a project genuinely needs a new
category, add a new `--var` + `.c-x`/`.a-x` pair in `:root` and the class block,
not a one-off color on a single element.

Only show legend entries for buckets you actually use — drop the rest.

---

## 4. Chip anatomy (calendar)

A chip is one unit of work on one day. Structure:

```html
<div class="chip c-dev">
  <div class="top">
    <span class="iss">CBRD-26668</span>      <!-- exact id -->
    <span class="act a-dev">PR</span>          <!-- short action verb -->
  </div>
  <div class="desc">vacuum이 OOS 정리 — 리뷰 요청</div>  <!-- one-line summary -->
</div>
```

- `.iss` = exact identifier (monospace). For grouped merges use `26729·26776`.
- `.act` = action badge: `PR` / `리뷰` / `머지` / `fix` / `재현` / `~2h` / `green` / `클로즈` / `조율`. Keep it ≤ 3 chars where possible; the badge color must match the chip bucket (`a-*` == `c-*`).
- `.desc` = the summary. This is what makes the calendar self-explanatory — never leave a chip as just a number.

Multiple chips stack vertically in a day cell. A cell with no work is left empty
(just the date header).

---

## 5. Bar/row anatomy (Gantt)

Each row is a workstream. Bars are positioned with `left:%` and `width:%` over a
5-column (week) grid; 0% = start of W1, 100% = end of the last column.

```html
<div class="row">
  <div class="rlab">🔵 개발중 4건<span class="tag">26668·26729·26776·26830</span></div>
  <div class="lane">
    <span class="cellguide"></span> ... (one per week column)
    <div class="bar b-dev" style="left:1%;width:37%">마무리 → 리뷰 → 머지</div>
  </div>
</div>
```

- Put the **critical path row first**, with the `critrow` modifier and `b-crit`
  bar, so the eye lands on what actually governs the end date.
- `rlab .tag` carries the exact issue ids.
- Milestones use the `milestone` diamond; recurring events (e.g., weekly sync)
  use small `dot`s on a `ghost` bar.
- Bar width represents the review/merge flow, not keyboard time — note this in a
  legend/footnote so nobody reads a 1-day task as a 1-week bar.

---

## 6. Special cells

- **Holiday**: add class `holiday` to the `<td>`; body is a single
  `<div class="holidaychip">공휴일</div>`. Hatched background signals "no work".
  Remember a holiday shrinks that week's capacity — reflect it in the plan.
- **Milestone / merge day**: add class `mergeday` to the `<td>`; use a `c-merge`
  chip with `💎` in `.iss`. This is the payoff cell — make it unmissable.
- **Buffer day**: class `bufferday` on the `<td>`, `c-buffer` chip. Label the last
  one "한계 / fallback" so the buffer reads as a deadline cushion, not free time.

---

## 7. Layout rules & gotchas

- **Single self-contained file.** No external CSS/JS/fonts/CDN. It must open
  offline, attach to email, and print (Ctrl+P) without breaking. This is a hard
  requirement — these schedules get forwarded to managers on locked-down networks.
- Korean-friendly font stack is already in the template; keep it.
- "한 페이지에 다 안 보여도 돼" — vertical scroll is fine; don't cram.
- Keep the **issue dictionary** table at the bottom: exact id · bucket · full
  summary · status, plus greyed `excl` rows for out-of-scope items. The cells
  show short chips; the dictionary is where the full detail and the "what we
  deliberately excluded" lives.
- Add a one-line footer noting key assumptions baked into the build
  (e.g., "공휴일 6/3 반영 · 매뉴얼 2h · 26383 제외") so the artifact is
  self-documenting when it's read out of context.

---

## 8. Verification (ALWAYS run before delivering)

Two failure modes have bitten this design repeatedly. Run
`../scripts/verify_html.sh <file>` before you hand the file over.

1. **Corrupted hex colors.** Editors/autocomplete sometimes mangle a hex value
   into garbage like `#5b6personally`, `#3a4considerable`, `#6b7policy`. The
   browser silently ignores the broken property and the element renders wrong.
   The script greps for any `#…` token containing letters outside `a–f`.
2. **Unbalanced tags.** A dropped `</div>`/`</td>`/`</tr>` collapses the grid.
   The script counts open vs close for `div`, `td`, `tr`.

If the script reports anything, fix it before delivering. Don't rely on visual
inspection alone — the corrupted-hex bug is invisible until you look at the exact
property, and a human glancing at the page may not catch a subtly broken cell.
