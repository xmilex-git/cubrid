# CUBRID Code Review Reference

Domain knowledge for logic, safety, and memory bug detection in CUBRID PR reviews.

---

## Memory Management (MUST flag)

- **`free()` without `free_and_init()`** — CUBRID nullifies pointers after free to prevent use-after-free. Bare `free()` is never allowed.
- **Bare `malloc`/`free` in server code** — Use `db_private_alloc(thread_p, size)` for server-side allocation and `free_and_init()` for deallocation.
- **Parser memory** — Use `parser_alloc(parser, len)` for parser-lifetime allocations.

## Error Handling

- **`er_set()` without propagation** — After calling `er_set(ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CODE, ...)`, the error code must be returned or the function must jump to a cleanup label (`goto exit`/`goto end`).
- **Unchecked return values** — Functions that return error codes must have their return values checked against `NO_ERROR`.
- **C++ exceptions in engine C code** — `throw`, `try`, `catch` are forbidden in engine C code. Use `er_set` + return codes.

---

## Concurrency & Transaction Safety

### Lock Protocol
- Locks must be acquired in a consistent order to prevent deadlocks.
- Check for: acquiring lock A then B in one path, but B then A in another.
- `pthread_mutex_destroy()` must only be called on initialized mutexes.

### Page Buffer Protocol
- Every `pgbuf_fix()` must have a corresponding `pgbuf_unfix()` on all code paths (including error paths).
- Modified pages must be marked dirty before unfix.
- Don't hold page latches across blocking operations.

### WAL (Write-Ahead Logging)
- Data modifications must be logged before the data page is flushed.
- Log records must be appended before marking pages dirty.

### MVCC
- `mvcc_satisfies_snapshot()` must be called with the correct snapshot for the operation.
- Don't mix snapshots from different transactions.

### Thread Safety
- Static/global variables accessed from multiple threads need synchronization.
- Counters shared across threads need atomic operations.

---

## Build Mode Awareness

The same source compiles into 3 binaries via preprocessor guards:

| Guard | Binary | Context |
|-------|--------|---------|
| `SERVER_MODE` | `cub_server` | Server process |
| `SA_MODE` | `cubridsa` | Standalone (client+server in-process) |
| `CS_MODE` | `cubridcs` | Client library |

- Parser/optimizer code is **client-side**: `#if !defined(SERVER_MODE)`
- Watch for: new code that should be guarded but isn't, or wrong guard applied

---

## Error Code Rules

Adding a new error code requires updates in **6 places**:
1. `src/base/error_code.h` — define the code
2. `src/compat/dbi_compat.h` — client-visible copy
3. `msg/en_US.utf8/cubrid.msg` — English message
4. `msg/ko_KR.utf8/cubrid.msg` — Korean message
5. `ER_LAST_ERROR` constant update
6. CCI's `base_error_code.h` if client-facing

---

## Key Data Structures

| Structure | Purpose | Watch For |
|-----------|---------|-----------|
| `PT_NODE` | Parse tree node (union-based, linked list) | Wrong union member access, null `next` |
| `XASL_NODE` | Executable query plan | Serialization/deserialization mismatch |
| `DB_VALUE` | Universal value container | Wrong type tag, missing `db_value_clear()` |
| `PAGE_BUFFER` | Buffer pool page | Missing unfix, dirty not set |
| `LOCK_RESOURCE` | Lock with owners/waiters | Ordering violations |
| `LOG_RECORD_HEADER` | WAL record | LSN ordering, missing log records |

---

## Comment & Convention Hygiene (CI-invisible)

Rules below are NOT enforced by `codestyle.sh` or any CI check. The review must catch them.

### No Stale File:Line References in Source Comments

- **Ban `filename:line` or `filename line NNNN` references in source-code comments.** These become wrong the moment any line is added or removed above the target. Flag any new comment that names a file path together with a line number.
- **Exception: stable header data-structure docs.** A comment in a rarely-changing header (e.g., `or_mvcc.h` documenting `OR_MVCC_FLAG_*` bit layout) may reference another header by name (without line number) because the target is a named symbol, not a position.
- **What to write instead:** reference the *symbol name* (`see pgbuf_unfix()`, `defined in OR_MVCC_FLAG_HAS_OOS`). Symbol names survive refactors; line numbers do not.

### Comment Understandability

- A comment must be understandable on its own without opening another file. If a reader needs to `grep` to understand the comment, the comment is incomplete.
- Do not write comments that only make sense at the time of writing (e.g., "added for the OOS refactor", "fixes the bug from last week"). These rot as context fades.
- Comments referencing JIRA tickets (`CBRD-XXXXX`) are acceptable only as a *why* annotation on a non-obvious workaround, not as a substitute for explaining the code.

### License / Copyright Headers

- Every new `.c` / `.h` / `.cpp` / `.hpp` file must carry the project's standard license header. Flag new files missing it.
- Do not flag modifications to existing files that already have (or already lack) the header — that is pre-existing.

### Conventions `codestyle.sh` Misses

- **`#include` order**: CUBRID convention is `config.h` first (for `HAVE_*` macros), then system headers, then CUBRID headers. `codestyle.sh` does not enforce order.
- **Magic numbers without named constants**: bare numeric literals in logic (other than 0, 1, -1, `NULL`) should be a `#define` or `enum`. CI does not flag this.
- **Commented-out code blocks**: dead code left in `#if 0` or `/* ... */` blocks should be removed, not preserved "for reference". Version control is the reference.

---

## False Positive Guidance

Do NOT flag:
- Pre-existing issues on unchanged lines
- Issues already raised in PR comments
- Style/formatting caught by CI (indent, astyle, google-java-format)
- Linter/compiler issues (cppcheck runs in CI)
- General code quality unless CLAUDE.md requires it
- Functionality changes clearly intentional per PR description
- Large file sizes (10K+ lines are intentional)
