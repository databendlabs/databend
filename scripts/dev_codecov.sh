#!/bin/bash

# Copyright 2020-2021 The FuseQuery Authors.
#
# SPDX-License-Identifier: Apache-2.0.

SCRIPT_PATH="$( cd "$( dirname "$0" )" >/dev/null 2>&1 && pwd )"
cd "$SCRIPT_PATH/.." || exit

export CARGO_INCREMENTAL=0
export RUSTFLAGS="-Zprofile -Ccodegen-units=1 -Copt-level=0 -Clink-dead-code -Coverflow-checks=off"
cargo +"$(cat ./cargo-toolchain)" test
grcov ./target/debug/ -s . -t lcov --llvm --branch --ignore-not-existing --ignore "/*"  -o ./target/debug/lcov.info
genhtml -o ./target/debug/coverage/ --show-details --highlight --ignore-errors source --legend ./target/debug/lcov.info

cat <<EOF
Finished code coverage.

You should now be able to view the code coverage report:
open ./target/debug/coverage/index.html
EOF

exit 0
