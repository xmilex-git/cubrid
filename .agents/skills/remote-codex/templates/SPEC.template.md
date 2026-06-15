# <one-line task title>

<!--
  This SPEC is executed start-to-finish, with no human in the loop, by **codex alone** on a remote
  worker (cubrid@192.168.6.30/.32/.33), driven by its `/goal` long-running-task mode. So:
    - codex cannot ask questions — put every bit of needed context, decision, and acceptance
      criterion directly in the body.
    - codex reads the repo's AGENTS.md automatically; you may rely on that, but do not depend on any
      local-only skill/plugin/MCP server — assume a plain codex CLI with shell access (YOLO mode).
  File locations (remote): work repo = ~/dev/cubrid, control = ~/rcx (SPEC.md/PROGRESS.md/BLOCKED.md)
-->

## Goal (Why)
<What is different once this is done — 1-3 sentences, no ambiguity.>

## Context (What you need to know)
- Work repo: `~/dev/cubrid` (branch: `<branch this task uses — differs per worker>`)
- Relevant files/modules: `<paths>`
- Background/constraints: `<build system, language, coding rules, things not to touch, etc.>`
- Assumptions/decisions made up front: `<decisions pinned here — codex cannot ask back>`

## Work items (checklist — flip to [x] and log one line to PROGRESS.md as each is done)
- [ ] 1. <concrete step>
- [ ] 2. <concrete step>
- [ ] 3. <...>

## Verification (Acceptance — all of these must pass to count as "done")
```bash
cd ~/dev/cubrid
# e.g. build
# <build command>
# e.g. test
# <test command>
```
- Pass criteria: `<what indicates success — exit 0, specific output, N tests passing, etc.>`

## Out of scope (Do NOT)
- `<things not to touch / not to do>`

## Completion protocol (codex must follow this)
- On finishing each item: check `[x]` in this file + add `- <item> done: <summary>` to `~/rcx/PROGRESS.md`
- Hard blockers: write to `~/rcx/BLOCKED.md` and **keep going with other items**
- Only when everything is done AND verification passes: run `echo done > ~/rcx/DONE` (the completion
  signal — never create it before then)
- If you must stop mid-run: `echo "<reason>" > ~/rcx/PAUSED`
