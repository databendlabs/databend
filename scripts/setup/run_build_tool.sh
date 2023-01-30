#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../.." || exit

IMAGE="${IMAGE:-datafuselabs/build-tool:dev}"
INTERACTIVE="${INTERACTIVE:-false}"
CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"
BYPASS_ENV_VARS="${BYPASS_ENV_VARS:-RUSTFLAGS,RUST_LOG}"
ENABLE_SCCACHE="${ENABLE_SCCACHE:-false}"
COMMAND="$@"

_UID=$(id -u)
if [[ ${_UID} != "501" ]] && [[ $_UID != "1000" ]] && [[ $_UID != "1001" ]]; then
	echo "warning: You might encounter permission issues when running this script, since the current uid is ${_UID}, not in [501,1000,1001]." >&2
	echo ":) feel free to ignore this warning if you do not need sudo." >&2
fi

# NOTE: create with runner user first to avoid permission issues
mkdir -p "${CARGO_HOME}/git"
mkdir -p "${CARGO_HOME}/registry"

if [[ $INTERACTIVE == "true" ]]; then
	echo "running interactive..." >&2
	EXTRA_ARGS="--interactive --env TERM=xterm-256color"
fi

for var in ${BYPASS_ENV_VARS//,/ }; do
	EXTRA_ARGS="${EXTRA_ARGS} --env ${var}"
done

if [[ $ENABLE_SCCACHE == "true" ]]; then
	env | grep -E "^SCCACHE_" >"${CARGO_HOME}/sccache.env"
	EXTRA_ARGS="${EXTRA_ARGS} --env RUSTC_WRAPPER=/opt/rust/cargo/bin/sccache --env-file ${CARGO_HOME}/sccache.env"
	COMMAND="${COMMAND} && sccache --show-stats"
fi

TOOLCHAIN_VERSION=$(awk -F'[ ="]+' '$1 == "channel" { print $2 }' rust-toolchain.toml)

exec docker run --rm --tty --net=host ${EXTRA_ARGS} \
	--user $(id -u):$(id -g) \
	--volume "${CARGO_HOME}/registry:/opt/rust/cargo/registry" \
	--volume "${CARGO_HOME}/git:/opt/rust/cargo/git" \
	--volume "${PWD}:/workspace" \
	--workdir "/workspace" \
	"${IMAGE}-${TOOLCHAIN_VERSION}" \
	/bin/bash -c "${COMMAND}"
