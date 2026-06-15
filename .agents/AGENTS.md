Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

Tradeoff: These guidelines bias toward caution over speed. For trivial tasks, use judgment.

1. Think Before Coding
Don't assume. Don't hide confusion. Surface tradeoffs.

Before implementing:

State your assumptions explicitly. If uncertain, ask.
If multiple interpretations exist, present them - don't pick silently.
If a simpler approach exists, say so. Push back when warranted.
If something is unclear, stop. Name what's confusing. Ask.
2. Simplicity First
Minimum code that solves the problem. Nothing speculative.

No features beyond what was asked.
No abstractions for single-use code.
No "flexibility" or "configurability" that wasn't requested.
No error handling for impossible scenarios.
If you write 200 lines and it could be 50, rewrite it.
Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

3. Surgical Changes
Touch only what you must. Clean up only your own mess.

When editing existing code:

Don't "improve" adjacent code, comments, or formatting.
Don't refactor things that aren't broken.
Match existing style, even if you'd do it differently.
If you notice unrelated dead code, mention it - don't delete it.
When your changes create orphans:

Remove imports/variables/functions that YOUR changes made unused.
Don't remove pre-existing dead code unless asked.
The test: Every changed line should trace directly to the user's request.

4. Goal-Driven Execution
Define success criteria. Loop until verified.

Transform tasks into verifiable goals:

"Add validation" → "Write tests for invalid inputs, then make them pass"
"Fix the bug" → "Write a test that reproduces it, then make it pass"
"Refactor X" → "Ensure tests pass before and after"
For multi-step tasks, state a brief plan:

1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

These guidelines are working if: fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.


### Scratch / temporary file location
- **NEVER write scratch files under `/tmp`.** The user's `/tmp` is tmpfs-backed and causing OOM. This applies to extracted git blobs, analysis dumps, intermediate outputs, build artifacts, downloaded archives — anything you would otherwise drop in `/tmp`.
- Also avoid: `/var/tmp`, `$TMPDIR`, `mktemp` without an explicit `-p` to a disk-backed dir, and shell redirections like `>/tmp/foo`.
- **Use the project directory's `.not_git_tracking/scratch` instead.** Create it on demand (`mkdir -p .claude/scratch`). If no project directory applies (cross-project work), use `~/.claude/scratch/`.
- Do NOT use `.omc/scratch/` anymore — the user has standardized on `.not_git_tracking/scratch` across all machines.
- When inspecting a different git ref, prefer `git show <ref>:<path>` piped directly into Read/grep (no file), or `git worktree add` rather than dumping to `/tmp` or `.not_git_tracking/scratch`.
- Tools and subagents you spawn must follow the same rule — if you delegate work, mention this constraint in the prompt.