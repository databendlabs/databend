#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../.." || exit

HUB="datafuselabs"

docker build . -t ${HUB}/build-tool:aarch64-unknown-linux-gnu --file ./docker/build-tool/aarch64/Dockerfile
docker push ${HUB}/build-tool:aarch64-unknown-linux-gnu
