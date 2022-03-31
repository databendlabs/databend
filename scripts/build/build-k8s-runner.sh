#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../.." || exit

HUB=${HUB:-datafuselabs}
TAG=${TAG:-latest}

version="${VERSION:-v0.7.3-nightly}"
declare -A platform_targets=(["arm64"]="aarch64-unknown-linux-gnu" ["amd64"]="x86_64-unknown-linux-gnu")

mkdir -p ./distro/
for platform in ${!platform_targets[@]}; do
    target=${platform_targets[$platform]}
    if [[ $version == "" ]]; then
        ./scripts/setup/run_build_tool.sh cargo build --target ${target} --release
    else
        wget -P distro -qc https://repo.databend.rs/databend/${version}/databend-${version}-${target}.tar.gz
        mkdir -p ./target/${target}/release
        tar xC ./target/${target}/release -f ./distro/databend-${version}-${target}.tar.gz
    fi
    mkdir -p ./distro/linux/${platform}
    cp ./target/${target}/release/databend-query ./distro/linux/${platform}
    cp ./target/${target}/release/databend-meta ./distro/linux/${platform}
done

# docker buildx build . -f ./docker/debian/meta.Dockerfile --platform linux/amd64,linux/arm64 -t ${HUB}/databend-meta:${TAG} --push
# docker buildx build . -f ./docker/debian/query.Dockerfile --platform linux/amd64,linux/arm64 -t ${HUB}/databend-query:${TAG} --push
# docker buildx build . -f ./docker/distroless/meta.Dockerfile --platform linux/amd64,linux/arm64 -t ${HUB}/databend-meta:${TAG}-distroless --push
# docker buildx build . -f ./docker/distroless/query.Dockerfile --platform linux/amd64,linux/arm64 -t ${HUB}/databend-query:${TAG}-distroless --push
