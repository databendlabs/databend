#!/bin/sh

set -o errexit

{
# echo "src/meta/types/Cargo.toml"
# echo "src/meta/protos/Cargo.toml"
# echo "src/meta/app/Cargo.toml"
# echo "src/meta/stoerr/Cargo.toml"
# echo "src/meta/ee/Cargo.toml"
# echo "src/meta/sled-store/Cargo.toml"
# echo "src/meta/kvapi/Cargo.toml"
# echo "src/meta/raft-store/Cargo.toml"
# echo "src/meta/api/Cargo.toml"
# echo "src/meta/proto-conv/Cargo.toml"
echo "src/meta/service/Cargo.toml"
# echo "src/meta/client/Cargo.toml"
# echo "src/meta/embedded/Cargo.toml"
# echo "src/meta/process/Cargo.toml"
# echo "src/meta/store/Cargo.toml"
} | while read x; do

    # --no-fail-fast \
RUST_TEST_THREADS=2 RUST_BACKTRACE=full cargo test \
    --color=always \
    --manifest-path $x --  --exact -Z unstable-options --show-output
done

