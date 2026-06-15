# CUBRID build/deploy — self-contained & portable (parallel-test campaign).
#
# Drives the repo's TRACKED CMakePresets.json (presets: debug | release | profile).
# Replicates ~/bin/build_cubrid.sh + set_cubrid_ver.sh INSTALL-PATH behavior:
#   - installs to a per-mode versioned dir   ~/<mode>/CUBRID-<version>
#   - repoints the ~/CUBRID symlink to it     (so $CUBRID reflects the active build)
#   - debug and release live in SEPARATE dirs; switch the active one with `just use <mode>`
#     (or by building it) — no clobbering between modes.
# The prebuilt locale files (.claude/locale/) are copied into EVERY build — the all-locales
# lib is needed for CTP execution and rebuilding it via make_locale is slow.
# No machine-local scripts (~/bin/*.sh) or CMakeUserPresets.json required.
# The cubrid-cci submodule auto-inits on first build (needs network for the initial clone).
#
# Usage:
#   just build [debug|release] [version]   build + install to ~/<mode>/CUBRID-<version>, repoint ~/CUBRID
#   just debug | just release              aliases (default version)
#   just use   [debug|release] [version]   only repoint ~/CUBRID to an already-installed dir
#   just rebuild [mode] [version]          fresh configure + build + install + repoint
#   just conf                              apply campaign test conf to $CUBRID/conf/cubrid.conf (idempotent)
#   just install-locale [dest]             copy prebuilt locale files (lib+bin); auto-run by build/rebuild
#   just deploy [mode] [version]           stop server (if any) -> build -> conf
#   just ctest [mode]                      ctest against the build tree
#
# Campaign: debug install for D1/D2/D3, release install for D4 — switch via `just use <mode>`.

set shell := ["bash", "-cu"]

jobs := env_var_or_default("JOBS", num_cpus())
ver  := env_var_or_default("CUBRID_VERSION", "11.5.develop")

# Default: list recipes.
default:
    @just --list

# Ensure build-critical git submodules are present (cubrid-cci). Inits on first build.
_submodules:
    @[ -f cubrid-cci/CMakeLists.txt ] || git submodule update --init cubrid-cci

# Configure a preset's build tree with install prefix = ~/<mode>/CUBRID-<version>.
configure mode="debug" version=ver: _submodules
    cmake --preset {{mode}} -DCMAKE_INSTALL_PREFIX="$HOME/{{mode}}/CUBRID-{{version}}"

# Build + install to ~/<mode>/CUBRID-<version>, copy locale files, then repoint ~/CUBRID -> there
# (mirrors build_cubrid.sh + set_cubrid_ver.sh install-path + locale behavior).
build mode="debug" version=ver: _submodules
    #!/usr/bin/env bash
    set -eu
    [ -n "${HOME:-}" ] || { echo "ERROR: \$HOME not set." >&2; exit 1; }
    dest="$HOME/{{mode}}/CUBRID-{{version}}"
    mkdir -p "$dest"
    cmake --preset {{mode}} -DCMAKE_INSTALL_PREFIX="$dest"
    cmake --build "build_preset_{{mode}}" -j {{jobs}} --target install
    just install-locale "$dest"
    ln -sfn "$dest" "$HOME/CUBRID"
    echo "installed {{mode}} -> $dest"
    echo "~/CUBRID -> $(readlink "$HOME/CUBRID")"

# Convenience aliases (default version).
debug: (build "debug")
release: (build "release")

# Only repoint ~/CUBRID to an already-installed versioned dir (set_cubrid_ver.sh equivalent).
use mode="debug" version=ver:
    #!/usr/bin/env bash
    set -eu
    dest="$HOME/{{mode}}/CUBRID-{{version}}"
    [ -d "$dest" ] || { echo "ERROR: not installed: $dest (build it first)" >&2; exit 1; }
    ln -sfn "$dest" "$HOME/CUBRID"
    echo "~/CUBRID -> $(readlink "$HOME/CUBRID")"

# Force a fresh configure + build + install (+ locale) + repoint.
rebuild mode="debug" version=ver: _submodules
    #!/usr/bin/env bash
    set -eu
    dest="$HOME/{{mode}}/CUBRID-{{version}}"
    rm -rf "build_preset_{{mode}}"
    mkdir -p "$dest"
    cmake --preset {{mode}} -DCMAKE_INSTALL_PREFIX="$dest"
    cmake --build "build_preset_{{mode}}" -j {{jobs}} --target install
    just install-locale "$dest"
    ln -sfn "$dest" "$HOME/CUBRID"
    echo "~/CUBRID -> $(readlink "$HOME/CUBRID")"

# Copy the prebuilt locale files into an install's lib/ & bin/ (build_cubrid.sh behavior).
# The all-locales lib is needed for CTP execution; shipping it avoids the slow make_locale rebuild.
# Auto-run at the end of `just build` / `just rebuild`. dest defaults to $CUBRID.
install-locale dest=env_var_or_default("CUBRID", ""):
    #!/usr/bin/env bash
    set -eu
    dest="{{dest}}"
    [ -n "$dest" ] || { echo "ERROR: no dest given and \$CUBRID not set." >&2; exit 1; }
    so=".claude/locale/libcubrid_all_locales.so"
    sh=".claude/locale/make_locale.sh"
    if [ -f "$so" ]; then cp -f "$so" "$dest/lib/" && echo "locale: libcubrid_all_locales.so -> $dest/lib/"; else echo "locale: $so missing (skipped)"; fi
    if [ -f "$sh" ]; then cp -f "$sh" "$dest/bin/" && echo "locale: make_locale.sh -> $dest/bin/";          else echo "locale: $sh missing (skipped)"; fi

# Apply campaign test conf to $CUBRID/conf/cubrid.conf (idempotent; from build_cubrid.sh defaults).
conf:
    #!/usr/bin/env bash
    set -eu
    [ -n "${CUBRID:-}" ] || { echo "ERROR: \$CUBRID not set." >&2; exit 1; }
    f="$CUBRID/conf/cubrid.conf"
    [ -f "$f" ] || { echo "ERROR: $f not found (build/install first)." >&2; exit 1; }
    grep -q '^server=' "$f" || sed -i -e 's/^#server=foo,bar/server=demodb/' "$f"
    grep -q '^thread_worker_timeout_seconds=' "$f" || echo 'thread_worker_timeout_seconds=4' >> "$f"
    grep -q '^double_write_buffer_size=' "$f"      || echo 'double_write_buffer_size=0'      >> "$f"
    echo "applied campaign conf to $f"

# Full local refresh: stop server (if any) -> build -> conf.
# `cubrid service stop` output is detached to avoid the known pipe-hang under captured shells.
deploy mode="debug" version=ver:
    -cubrid service stop </dev/null >/dev/null 2>&1
    just build {{mode}} {{version}}
    just conf

# ctest (unit + sql-level) against a build tree.
ctest mode="debug":
    ctest --test-dir "build_preset_{{mode}}" --output-on-failure
