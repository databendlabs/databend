#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../.." || exit

#	cargo build --target x86_64-unknown-linux-gnu --release
#	cross build --target aarch64-unknown-linux-gnu --release
mkdir -p ./distro/linux/amd64
mkdir -p ./distro/linux/arm64
cp ./target/x86_64-unknown-linux-gnu/release/databend-meta ./distro/linux/amd64
cp ./target/x86_64-unknown-linux-gnu/release/databend-query ./distro/linux/amd64
cp ./target/aarch64-unknown-linux-gnu/release/databend-meta ./distro/linux/arm64
cp ./target/aarch64-unknown-linux-gnu/release/databend-query ./distro/linux/arm64
mkdir -p ./distro/linux/arm64
docker buildx build . -f ./docker/meta/Dockerfile --platform ${PLATFORM} --allow network.host --builder host -t ${HUB}/databend-meta:${TAG} --push
docker buildx build . -f ./docker/query/Dockerfile --platform ${PLATFORM} --allow network.host --builder host -t ${HUB}/databend-query:${TAG} --push
