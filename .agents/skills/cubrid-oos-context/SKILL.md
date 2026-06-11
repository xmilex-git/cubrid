---
name: cubrid-oos-context
description: Load CUBRID OOS (Out-of-row Overflow Storage) project context from the local knowledge base. Use this skill whenever OOS is mentioned, when working on the feat/oos branch, when touching oos_file.cpp or OOS-related heap_file.c code, when CBRD-26517/26458/26516 or other OOS JIRA tickets come up, or when you need to understand the OOS architecture, record format, CRUD flows, or MVCC integration. Also trigger when you see OOS-related identifiers like OR_MVCC_FLAG_HAS_OOS, OR_VAR_BIT_OOS, FILE_OOS, oos_insert, oos_read, oos_delete, or heap_record_replace_oos_oids.
---

# CUBRID OOS Context

This skill loads context about the **OOS (Out-of-row Overflow Storage)** project — a CUBRID feature that separates large variable-length columns from heap records into dedicated OOS files to reduce unnecessary disk I/O.

$ARGUMENTS

## Step 0: Validate environment

Before loading context, verify the workspace is ready for OOS development. Run the bundled validation script against the current working directory:

```bash
bash <skill-path>/scripts/validate-env.sh "$PWD"
```

This checks that:
- You're in a **git repository** (or worktree) — needed for code search, grep, and git features
- The directory is a **CUBRID source tree** (has CMakeLists.txt + CMakePresets.json)
- **OOS source files** exist (oos_file.cpp — indicates you're on a feat/oos branch)
- **PRESET_MODE** is set and a **build directory** exists (build_preset_*)
- **compile_commands.json** is present at the project root — this is essential for LSP features (hover, goto-definition, find-references via clangd)
- **clangd** and **just** are available

**If the script reports errors (exit code 1):**
- Warn the user about what's missing and how to fix it
- If `compile_commands.json` is missing, tell the user to run `just build` (which generates it via CMake's `CMAKE_EXPORT_COMPILE_COMMANDS=ON`) and then symlink it: `ln -sf build_preset_${PRESET_MODE}/compile_commands.json .`
- If not in a git repo, suggest switching to a CUBRID worktree (e.g., one under `~/gh/cb/`)
- Do NOT skip context loading — proceed with Step 1, but note the limitations

**If the script reports only warnings (exit code 0):**
- Note the warnings but proceed normally — the environment is usable

## Step 1: Load OOS context

Read the single source of truth for OOS knowledge:

```
/home/vimkim/gh/cubrid-oos-context/OOS-CONTEXT.md
```

This file contains everything needed for OOS implementation and debugging:
- Quick reference (trigger conditions, flags, key source files)
- Architecture & design (record format, binary layout, multi-chunk chain, comparison with other DBs)
- CRUD flows (INSERT, SELECT, UPDATE, DELETE step-by-step)
- Recovery, replication & MVCC invariants
- Known bugs, limitations & optimization ideas
- Test scenarios and SQL patterns

**This single file is sufficient for most OOS-related questions.** Read it before answering.

## Quick Reference

| Concept | Detail |
|---------|--------|
| OOS trigger | record > `DB_PAGESIZE/8` (2KB on 16KB pages) AND column > 512B |
| OOS file type | `FILE_OOS`, one per heap file (1:1 mapping) |
| OOS pointer | 8-byte OOS OID (volid, pageid, slotid) in variable area |
| MVCC flags | `OR_MVCC_FLAG_HAS_OOS` (bit 3), `OR_VAR_BIT_OOS` (bit 0) |
| Key sources | `heap_file.c`, `oos_file.cpp`, `object_representation.h` |
| Branch | `feat/oos` |
| Current milestone | M2 (3/10-4/17): bestspace, compaction, vacuum, drop table |
