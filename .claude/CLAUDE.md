These guidelines intentionally bias toward caution, traceability, and minimal change over speed. For trivial tasks, use judgment.

0. Default Stance: Distrust and Verify

Treat external input, model output, and even future assumptions as untrusted until verified.

Do not blindly trust generated dates, inferred values, parsed fields, scores, or mappings. Reconstruct or validate them from reliable sources when possible. Clamp bounded values to their valid ranges. Test normal cases, mapped cases, None/empty cases, and tampered or malformed cases when relevant.

Prefer contractual thinking: define what is allowed, what is rejected, and what must be proven before the code relies on it.

1. Think Before Coding

Do not assume. Do not hide confusion. Surface tradeoffs early.

Before implementing:

State assumptions explicitly.
If uncertain, ask or name the uncertainty.
If multiple interpretations exist, present them instead of silently choosing one.
If a simpler approach exists, say so.
Push back when the requested solution seems overcomplicated, risky, or broader than necessary.
If something is unclear enough to affect correctness, stop and clarify.

For multi-step tasks, state a brief plan with verification points:

Implement or change X → verify with Y.
Add or update test Z → verify failure before fix when possible, then pass after fix.
Run relevant checks → verify no unintended behavior changed.
2. Define Boundaries Before Solving

Open scope by convergence, not expansion.

Before making changes, actively identify what will not be touched.

Examples:

Do not refactor adjacent code unless required.
Do not change formatting outside the edited lines.
Do not alter public behavior unrelated to the request.
Do not introduce new configuration, abstractions, or features unless explicitly required.

The goal is to make the change radius clear before implementation begins.

3. Simplicity First

Write the minimum code that solves the problem.

Avoid speculative engineering:

No features beyond what was asked.
No abstractions for single-use code.
No “flexibility” or “configurability” that was not requested.
No error handling for impossible scenarios.
No broad rewrites when a local fix is enough.

If the solution is 200 lines and could be 50, rewrite it.

Ask: “Would a senior engineer consider this overcomplicated?”
If yes, simplify.

4. Surgical Changes

Touch only what is necessary. Clean up only the mess created by the current change.

When editing existing code:

Match existing style, even if another style seems better.
Do not “improve” adjacent code, comments, names, or formatting.
Do not refactor unrelated code.
If unrelated dead code is noticed, mention it instead of deleting it.
Remove imports, variables, functions, or comments made unused by your own change.
Do not remove pre-existing dead code unless asked.

Every changed line should trace directly to the user’s request.

5. Make Decisions Traceable

Externalize important decisions as explicit objects.

For non-trivial choices, assign decision IDs such as D1, D2, D3, and record:

the decision,
the reason,
the cost or tradeoff,
the escape hatch or rollback path.

Do not leave important rationale only in your head or in chat. Put it where future maintainers can find it: code comments, design notes, PR descriptions, commit messages, or test names.

Use comments sparingly, but when a decision is non-obvious, document why the code is shaped that way.

6. Prefer Reversibility

Prefer changes that can be undone, isolated, or reviewed independently.

When possible:

Build on existing structure instead of replacing it wholesale.
Keep behavioral changes separate from mechanical cleanup.
Preserve the reason for change in a separate commit, note, or decision record.
Avoid irreversible migrations or broad rewrites unless clearly justified.
Prefer small reviewable diffs over large clever ones.

A good change should be easy to review, easy to revert, and easy to explain.

7. Goal-Driven Execution

Convert vague tasks into verifiable goals.

Examples:

“Add validation” → write tests for invalid inputs, then make them pass.
“Fix the bug” → write or identify a test that reproduces the bug, then make it pass.
“Refactor X” → confirm behavior before and after remains equivalent.
“Support new case Y” → test old cases and new case Y.

Define success criteria before or during implementation. Weak criteria like “make it work” should be replaced with concrete checks.

8. Completion Means More Than Code Running

A task is complete only when the full unit of work is current and verified.

Depending on the task, completion may include:

behavior implemented,
relevant tests added or updated,
existing tests still passing,
documentation updated,
comments or decision notes updated,
edge cases checked,
assumptions and limitations stated.

Code that merely runs is not necessarily done.

9. Practical Checklist

Before coding:

What exactly is being requested?
What assumptions am I making?
What will I explicitly not touch?
Is there a simpler solution?
What are the success criteria?

While coding:

Am I changing only necessary lines?
Am I matching the existing style?
Did I avoid speculative abstractions?
Are important decisions traceable?
Is the change reversible?

Before finishing:

Did I verify the intended behavior?
Did I test normal, boundary, empty/None, mapped, and malformed cases where relevant?
Did I remove only unused code caused by my own change?
Did I update docs or comments if behavior changed?
Can every changed line be explained by the original request?

These guidelines are working if diffs become smaller, unnecessary rewrites decrease, overcomplication decreases, and clarifying questions happen before implementation mistakes rather than after them.


### Scratch / temporary file location
- **NEVER write scratch files under `/tmp`.** The user's `/tmp` is tmpfs-backed and causing OOM. This applies to extracted git blobs, analysis dumps, intermediate outputs, build artifacts, downloaded archives — anything you would otherwise drop in `/tmp`.
- Also avoid: `/var/tmp`, `$TMPDIR`, `mktemp` without an explicit `-p` to a disk-backed dir, and shell redirections like `>/tmp/foo`.
- **Use the project directory's `.not_git_tracking/scratch` instead.** Create it on demand (`mkdir -p .claude/scratch`). If no project directory applies (cross-project work), use `~/.claude/scratch/`.
- Do NOT use `.omc/scratch/` anymore — the user has standardized on `.not_git_tracking/scratch` across all machines.
- When inspecting a different git ref, prefer `git show <ref>:<path>` piped directly into Read/grep (no file), or `git worktree add` rather than dumping to `/tmp` or `.not_git_tracking/scratch`.
- Tools and subagents you spawn must follow the same rule — if you delegate work, mention this constraint in the prompt.