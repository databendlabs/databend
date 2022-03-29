#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../.." || exit

IMAGE="${IMAGE:-datafuselabs/build-tool:dev-debian-amd64}"
INTERACTIVE="${INTERACTIVE:-false}"
COMMAND="$@"

_UID=$(id -u)
if [[ ${_UID} != "501" ]] && [[ $_UID != "1000" ]] && [[ $_UID != "1001" ]]; then
    echo "warning: You might encounter permission issues when running this script, since the current uid is ${_UID}, not in [501,1000,1001]."
    echo ":) feel free to ignore this warning if you do not need sudo."
fi

if [[ $INTERACTIVE == "true" ]]; then
    echo "running interactive..."
    EXTRA_ARGS="--interactive"
fi

# NOTE: create first to avoid permission issues
mkdir -p "${HOME}/.cargo/registry"
mkdir -p "${HOME}/.cargo/git"

exec docker run --rm --tty --net=host \
    ${EXTRA_ARGS} \
    -u $(id -u):$(id -g) \
    -e RUSTFLAGS \
    -v "${HOME}/.cargo/registry:/opt/rust/.cargo/registry" \
    -v "${HOME}/.cargo/git:/opt/rust/.cargo/git" \
    -v "${PWD}:/workspace" \
    --workdir "/workspace" \
    "${IMAGE}" \
    ${COMMAND}
