# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

This repo is **multi-context**: a `CONTEXT-MAP.md` at the root points to one
`CONTEXT.md` per engine module under `src/<module>/`.

## Before exploring, read these

- **`CONTEXT-MAP.md`** at the repo root — it points at one `CONTEXT.md` per context.
  Read each one relevant to the topic you're about to work in.
- **`src/<module>/CONTEXT.md`** — the context-scoped glossary for that module.
- **`docs/adr/`** — engine-wide architectural decisions.
- **`src/<module>/docs/adr/`** — context-scoped decisions for that module.

If any of these files don't exist, **proceed silently**. Don't flag their absence;
don't suggest creating them upfront. The producer skill (`/grill-with-docs`) creates
them lazily when terms or decisions actually get resolved.

## File structure

Multi-context layout (CUBRID engine modules):

```
/
├── CONTEXT-MAP.md                     ← points to each src/<module>/CONTEXT.md
├── docs/adr/                          ← engine-wide decisions
└── src/
    ├── query/
    │   ├── CONTEXT.md
    │   └── docs/adr/                  ← context-specific decisions
    ├── optimizer/
    │   ├── CONTEXT.md
    │   └── docs/adr/
    ├── storage/
    │   ├── CONTEXT.md
    │   └── docs/adr/
    ├── transaction/
    │   ├── CONTEXT.md
    │   └── docs/adr/
    └── ...                            ← parser/, object/, thread/, etc.
```

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in the relevant `src/<module>/CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal — either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/grill-with-docs`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0007 (parallel heap scan mergeable list) — but worth reopening because…_
