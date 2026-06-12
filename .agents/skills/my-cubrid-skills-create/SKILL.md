---
name: my-cubrid-skills-create
description: "Create a new skill in the my-cubrid-skills collection. Use when the user wants to turn a workflow into a reusable skill, capture what they just did as a skill, or add a new skill to their CUBRID toolbox. Triggers on phrases like 'make this a skill', 'create a skill for this', 'save this as a skill', 'add a new skill to my-cubrid-skills'."
argument-hint: "<skill-name-or-description>"
---

# Create a New Skill in my-cubrid-skills

Scaffolds a new skill directory and writes a complete `SKILL.md` inline from gathered context.

## Steps

### Step 1: Determine the collection root

Walk up from cwd looking for a `justfile` containing `npx skills add`. If found, that directory is the collection root. Otherwise ask the user for the path.

### Step 2: Derive a candidate skill name

If `$ARGUMENTS` is empty, or would produce an empty or tautological name (e.g. `"save this as a skill"` collapses to nothing after filler removal), skip naming for now and proceed to Step 4 to gather workflow context first — then derive the name from the workflow's primary verb + object.

Otherwise, from `$ARGUMENTS`:

- Drop filler words: `a`, `the`, `this`, `that`, `which`, `skill`, `save`, `as`.
- Convert verbs to imperative form and lowercase-hyphenate — e.g. `"a skill that closes greptile comments"` → `close-greptile-comments`.
- Apply the `cubrid-` prefix when the skill touches CUBRID source, tests, JIRA tooling, or CI. The prefix rule applies even when the input is already kebab-case.

**Propose** the candidate name and wait for confirmation or correction before continuing.

### Step 3: Check for conflicts

Confirm `<collection-root>/<skill-name>/` does not already exist. If it does, ask: update the existing skill, or pick a different name?

### Step 4: Gather context

From the current conversation collect:
- **Workflow**: the sequence of commands, tools, or actions the skill automates
- **Tools**: CLI tools, APIs, or Claude skills involved
- **Edge cases**: failure modes or conditions the user mentioned

If Step 2 was deferred, derive the skill name now from the workflow's verb + object, confirm with the user, then run Step 3's conflict check before continuing.

### Step 5: Write the SKILL.md

Create `<collection-root>/<skill-name>/SKILL.md` with complete content — no placeholder comments. Follow the valid skill structure below.

Note: `SKILL.md` is installed to both Claude Code and Codex via `just install`. Use `$ARGUMENTS` for input; avoid Claude-Code-only constructs unless explicitly flagged.

### Step 6: Grill the SKILL.md (mandatory)

Before installing, run the freshly written `SKILL.md` through `/grill-with-docs`. Single-pass skill descriptions and trigger lists drift toward generic verbs, over-broad triggers, missing `Triggers on phrases like '...'` clauses, and steps that reference tools without exact CLI commands. Skills are loaded by description and trigger phrases that LLMs match against, so unclear writing here means the skill never fires.

This step is required, not optional. It applies to every new skill. No agent-side judgment — including size, scope, perceived triviality, or perceived risk — is a valid skip criterion. The only legitimate skip is when the user, in the message that triggered this skill, explicitly says "skip grill" or "don't grill this" (or unambiguous equivalent: "no grill", "skip the grill loop", "just push it"). If in doubt, do the grill loop.

Invoke `/grill-with-docs` with:

- **Topic & purpose**: a new skill in the my-cubrid-skills collection, audience is Claude Code (LLM-triggered) and the user
- **Output path**: `<collection-root>/<skill-name>/SKILL.md` (the loop revises in place)
- **Source material**: the workflow context gathered in Step 4, real examples (`resolve-greptile-comments/SKILL.md`, `cubrid-pr-create/SKILL.md`), the valid skill structure below
- **Review angle**: description is imperative and ends with `Triggers on phrases like '...'`; trigger phrases are concrete and not over-broad; steps are numbered, executable, and reference exact CLI commands; no placeholders, no `<!-- ... -->` comments; CUBRID prefix applied where relevant
- **Round cap**: default 5

After the reviewer approves, proceed to install.

### Step 7: Install, verify, and commit

```bash
just install
just list | grep <frontmatter-name>
```

`just list` outputs frontmatter `name:` values, so grep on the name you set in the SKILL.md front matter. If the skill appears, prompt the user to commit:

```bash
git add <skill-name>/ && git commit -m "feat(<scope>): add <skill-name> skill"
```

Match `<scope>` to the style of `git log --oneline -10` (scope is often a category, not the literal skill name).

## Valid skill structure

- Frontmatter: `name` (kebab-case) and `description`.
- `description`: imperative phrase, "Use when …", ends with `Triggers on phrases like 'X', 'Y', 'Z'.`
- Body: `# Title` heading and numbered execution steps.
- No placeholders or `<!-- ... -->` comments in the final file.

Real examples: `resolve-greptile-comments/SKILL.md`, `cubrid-pr-create/SKILL.md`.
