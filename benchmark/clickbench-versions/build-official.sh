#!/bin/bash
# Build databend-query the way the x86_64 nightly release does, from a clean worktree.
# usage: build-official.sh NAME GIT_REV
# Mirrors .github/workflows/release.yml (build_default, x86_64-unknown-linux-gnu) and
# .github/actions/build_linux: profile `release` from Cargo.toml (thin LTO, codegen-units 1),
# CARGO_PROFILE_RELEASE_OPT_LEVEL=s, RUSTFLAGS="-C target-feature=+sse4.2", features `default`,
# CARGO_INCREMENTAL=0, then debug info split off and stripped.
# One deviation: RUSTFLAGS also passes `-C link-arg=-fuse-ld=mold`. Setting RUSTFLAGS drops the
# rustflags of .cargo/config.toml (mold), and the release image links with its own toolchain;
# the linker does not change generated code.
set -euo pipefail
NAME=$1 REV=$2
ROOT=/home/kould/tmp-build/clickbench-versions
WT=/home/kould/tmp-build/wt-$NAME
OUT=$ROOT/bin/$NAME
TARGET=x86_64-unknown-linux-gnu
export CARGO_TARGET_DIR=/home/kould/tmp-build/target-official
export TMPDIR=/home/kould/tmp-build
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_RELEASE_OPT_LEVEL=s
export RUSTFLAGS="-C target-feature=+sse4.2 -C link-arg=-fuse-ld=mold"
unset CARGO_PROFILE_RELEASE_LTO CARGO_PROFILE_RELEASE_CODEGEN_UNITS CARGO_PROFILE_RELEASE_DEBUG CARGO_BUILD_JOBS

cd /home/kould/databend
git worktree remove --force "$WT" 2>/dev/null || true
git worktree add --detach "$WT" "$REV" >/dev/null
cd "$WT"
SHA=$(git rev-parse HEAD)
mkdir -p "$OUT"
START=$(date -u +%FT%TZ)
cargo build --locked --target $TARGET --features default --profile release --bin databend-query \
  > "$OUT/build.log" 2>&1 || { tail -40 "$OUT/build.log"; exit 1; }
END=$(date -u +%FT%TZ)
B=$CARGO_TARGET_DIR/$TARGET/release
cp "$B/databend-query" "$OUT/databend-query"
cd "$OUT"
objcopy --only-keep-debug databend-query databend-query.debug
strip --strip-debug --remove-section=.comment --remove-section=.note databend-query
objcopy --add-gnu-debuglink databend-query.debug databend-query
rm databend-query.debug
python3 - "$OUT/build-info.json" <<PY
import json, subprocess, sys
sh = lambda c: subprocess.run(c, shell=True, capture_output=True, text=True).stdout.strip()
json.dump({
    'name': '$NAME', 'git_rev_arg': '$REV', 'git_sha': '$SHA',
    'git_log': sh('git -C $WT log --oneline -12'),
    'rustc': sh('cd $WT && rustc -Vv'), 'toolchain_file': sh('cat $WT/rust-toolchain.toml'),
    'cargo_cmd': 'cargo build --locked --target $TARGET --features default --profile release --bin databend-query',
    'env': {'CARGO_INCREMENTAL': '0', 'CARGO_PROFILE_RELEASE_OPT_LEVEL': 's', 'RUSTFLAGS': '$RUSTFLAGS'},
    'profile_release': sh("sed -n '/^\\\\[profile.release\\\\]/,/^codegen-units/p' $WT/Cargo.toml"),
    'cargo_config': sh('cat $WT/.cargo/config.toml'),
    'post': 'objcopy --only-keep-debug; strip --strip-debug --remove-section=.comment --remove-section=.note',
    'build_start': '$START', 'build_end': '$END',
    'sha256': sh('sha256sum $OUT/databend-query').split()[0],
    'version': sh('$OUT/databend-query --version'),
}, open(sys.argv[1], 'w'), indent=1)
PY
cat "$OUT/build-info.json" | head -5
cd /home/kould/databend && git worktree remove --force "$WT"
