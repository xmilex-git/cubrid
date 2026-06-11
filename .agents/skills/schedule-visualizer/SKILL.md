---
name: schedule-visualizer
description: Generate a polished, single-file HTML project schedule — either a daily Excel-like calendar grid or a Gantt timeline (or both) — from a set of work items, dates, and milestones. Use this whenever the user wants to visualize, lay out, or "예쁘게 뽑아줘" a project plan, merge/release schedule, roadmap, sprint, or 일정표 as HTML; whenever they ask for a Gantt chart, timeline, calendar view, or day-by-day plan; or whenever a planning/estimation discussion produces issues + durations + a target date that would be clearer as a visual. Trigger even if the user doesn't say "HTML" — if they're organizing a schedule to send to a manager or team, offer this. Especially fits CUBRID merge/milestone planning (e.g., feat branch → develop) with JIRA issue numbers, holidays, and review-gated timelines.
argument-hint: "[gantt|calendar|both] [optional: project/branch or path to a plan doc]"
---

# Schedule Visualizer

Turn a project plan into a polished, self-contained HTML artifact a manager can
open offline, forward by email, or print. Two formats, one design system:

- **Daily calendar** — Excel-like Mon–Fri grid, one chip per task per day. Best
  for concrete day-by-day plans, holidays, "엑셀처럼 칸 나눠서".
- **Gantt timeline** — week-column bars across overlapping workstreams, with the
  critical path called out. Best for a one-glance overview.

The look is deliberately *not* generic-AI: a tuned palette, semantic color
buckets, an issue dictionary, holiday/milestone/buffer cells. The proven CSS and
structure live in the templates — your job is to gather the data and fill them
faithfully, not to redesign from scratch.

## When to use

- "일정표 HTML로 뽑아줘", "간트 차트 만들어줘", "스케줄 시각화", "달력처럼 보여줘",
  "이 계획 예쁘게 정리해서 팀장한테 보낼 수 있게"
- A planning/estimation thread just settled on issues + durations + a target date.
- CUBRID milestone/merge planning with CBRD-XXXXX issues, holidays, CI gates.

If it's a one-line "when's the deadline" question, just answer — don't build an
artifact. Reach for this when there's enough structure (several items, dates,
a milestone) that a picture genuinely beats prose.

## Workflow

### 1. Read the design system first
Read `references/design-system.md`. It defines the data model, the buckets and
palette, chip/bar anatomy, special cells, and the verification step. Don't skip
it — the templates assume you know the bucket semantics.

### 2. Gather the inputs
Collect the project frame (start date + weekday, working week, holidays, target/
milestone, buffer) and the work items (exact id, summary, bucket, status, day(s),
excluded?). Pull as much as you can from the current conversation and the repo
(git log, JIRA via the `jira` skill, PRs via `gh`) before asking the user.

The schedule logic — **what's the critical path, velocity, what gates "done"** —
swings the dates more than anything. If it's not already established in the
conversation, ask. A common and important finding: when most work is already in
draft PRs, the critical path is *human review throughput*, not coding — model the
timeline around that, not around keyboard time.

Never invent or approximate an issue id. If you're unsure of an exact JIRA
summary or status, look it up.

### 3. Pick the format
- User asked for one → do that one.
- "both" → produce two files.
- Unsure → produce the **calendar** (it carries more concrete information), and
  offer the Gantt as a follow-up.

### 4. Fill the template
Copy the chosen template from `assets/` to the working directory (name it for the
project, e.g. `oos-m2-merge-calendar.html`). Then:
- Replace header/cards/meta with project facts.
- Build the week rows / workstream bars from the work items.
- Mark holidays, the milestone day, and buffer days with their cell classes.
- Fill the issue dictionary; put out-of-scope items in greyed `excl` rows.
- Trim the legend to only the buckets you used.
- Add a footer line recording the assumptions baked in ("공휴일 6/3 반영 ·
  매뉴얼 2h · 26383 제외") so the file is self-documenting out of context.

Reuse the CSS variables/classes — never hand-write new inline hex colors (that's
the corrupted-hex bug waiting to happen). If a project needs a new category, add
a `--var` + `.c-x`/`.a-x` (calendar) or `.b-x` (Gantt) pair, not a one-off color.

### 5. Verify before delivering — required
Run the bundled check:

```bash
bash <skill-dir>/scripts/verify_html.sh <output.html>
```

It catches corrupted hex colors (e.g. `#5b6personally`), unbalanced
`div`/`td`/`tr` tags, and accidental external resource references. Fix anything it
flags. These failures are invisible on a casual glance but break the render —
don't rely on eyeballing.

### 6. Deliver
Hand over the file path. If running in a UI that surfaces files, surface it.
Give a 2–3 line summary of what's in it, then offer the obvious next moves:
- the other format (Gantt ↔ calendar) for consistency,
- adjusting assumptions (velocity, holidays, scope) and regenerating,
- a manager-ready one-paragraph summary to paste into a reply.

## Keeping it in sync
When the user changes an assumption — removes an issue, compresses a phase,
shifts a date — **regenerate the affected file fully**. A schedule the reader has
to mentally patch is worse than none. If you produced both formats, update both
so they don't contradict each other.

## Notes
- These are static artifacts, not apps. No build step, no dependencies.
- The templates are Korean-first (font stack + example copy) but work in any
  language — swap the text, keep the structure.
- If the `frontend-design` skill is available and the user wants a bolder or
  rebranded look, you can layer it on top — but preserve the single-file,
  offline-safe, print-friendly constraints above.
