---
name: cubrid-build
description: CUBRID build and test workflow using the portable justfile. Use when building, compiling, installing, or testing CUBRID source code in any CUBRID worktree or source directory.
---

# CUBRID Build & Test

Build, install, and test CUBRID via the repo's portable `justfile` (drives the tracked
`CMakePresets.json`; installs to a per-mode versioned dir and repoints `~/CUBRID`).

## When to Use

- User says "build", "compile", "install", "test", "빌드", "테스트"
- After code edits, to verify compilation or run tests
- Switching the active build between debug and release
- Any time you're in a CUBRID source tree/worktree (contains `src/storage/`, `src/parser/`, a `justfile`)

## Prerequisites

- Working directory is a CUBRID source tree/worktree with `justfile` + `CMakePresets.json` at the root
- Toolchain: `just`, `cmake` (>=3.21), `ninja`, `gcc`/`g++` (8+)
- `$CUBRID` set to the runtime dir (e.g. `~/CUBRID`); `$HOME` set
- First build auto-inits the `cubrid-cci` submodule (needs network once)
- No `$PRESET_MODE` / `$CUBRID_BUILD_DIR` / direnv needed — the justfile is self-contained

## Build Modes

- **debug** — `CMAKE_BUILD_TYPE=Debug`, assertions on. Use for correctness / stability / crash work.
- **release** — `RelWithDebInfo`, optimized. Use for performance measurement.

Each mode installs to its OWN dir `~/<mode>/CUBRID-<version>` (default version `11.5.develop`,
override with `CUBRID_VERSION`) and repoints `~/CUBRID` -> that dir. debug and release never
clobber each other; switch the active one with `just use <mode>` (no rebuild).

## Commands

### Build + install (default: debug)
```bash
just build               # debug
just build release       # release
just debug               # alias for: just build debug
just release             # alias for: just build release
just build debug 11.5.x  # explicit version label
```
Builds `build_preset_<mode>/` and installs to `~/<mode>/CUBRID-<version>`, then points
`~/CUBRID` there. **Use this to verify edits compile.** Never call `cmake --build` directly.

### Switch active install (no rebuild)
```bash
just use release         # repoint ~/CUBRID -> ~/release/CUBRID-<version>
just use debug
```

### Fresh rebuild (wipes the build tree first)
```bash
just rebuild             # debug
just rebuild release
```

### Configure only
```bash
just configure           # debug
just configure release
```

### Apply campaign test conf (idempotent)
```bash
just conf                # server=demodb, thread_worker_timeout_seconds=4, double_write_buffer_size=0
```

### Locale files (auto-handled by build)
`just build` / `just rebuild` automatically copy the prebuilt locale files from
`.claude/locale/` into the install (`libcubrid_all_locales.so` -> `lib/`,
`make_locale.sh` -> `bin/`) — the all-locales lib is needed for CTP execution and this
avoids the slow `make_locale` rebuild. To (re)copy manually into the current `$CUBRID`
(or a given dir):
```bash
just install-locale          # into $CUBRID
just install-locale <dir>
```

### Full local refresh
```bash
just deploy              # stop server (if any) -> build debug -> conf
just deploy release
```

### Tests
```bash
just ctest               # ctest (unit + sql-level) against build_preset_debug
just ctest release
```

## Typical Workflow

1. Edit code
2. `just build` (or `just build release`) — verify it compiles + installs
3. `just ctest` — run unit + sql-level tests
4. Switch modes anytime with `just use <mode>` (no rebuild)

## Important

- **Always use `just`**, never raw `cmake --build` / `ctest`.
- Run long builds with `run_in_background`.
- `just build` / `rebuild` / `deploy` **REPOINT `~/CUBRID`**. To preserve the current target,
  note `readlink ~/CUBRID` first, or build with a throwaway `CUBRID_VERSION=<label>` and restore after.
- If a build fails, read the error output carefully before attempting fixes.
- `just --list` shows all available recipes.
