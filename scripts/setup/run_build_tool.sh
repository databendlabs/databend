#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../.." || exit

TARGET="${TARGET:-dev}"
INTERACTIVE="${INTERACTIVE:-true}"
CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"
BYPASS_ENV_VARS="${BYPASS_ENV_VARS:-RUSTFLAGS,RUST_LOG}"
ENABLE_SCCACHE="${ENABLE_SCCACHE:-false}"
COMMAND=$1
COMMAND_ARGS="$*"

TOOLCHAIN_VERSION=$(awk -F'[ ="]+' '$1 == "channel" { print $2 }' rust-toolchain.toml)

_UID=$(id -u)
_GID=$(id -g)
USER=${USER:-$(whoami)}

case $TARGET in
x86_64-unknown-linux-gnu | aarch64-unknown-linux-gnu)
	RUNNER_BASE="debian"
	;;
x86_64-unknown-linux-musl | aarch64-unknown-linux-musl)
	RUNNER_BASE=${TARGET}
	;;
*)
	RUNNER_BASE="dev"
	TARGET=x86_64-unknown-linux-gnu
	;;
esac

# skip building temporary image for root
if [[ ${_UID} == 0 ]]; then
	IMAGE="datafuselabs/build-tool:${RUNNER_BASE}-${TOOLCHAIN_VERSION}"
else
	IMAGE="${USER}/build-tool:${RUNNER_BASE}-${TOOLCHAIN_VERSION}"

	if [[ $(docker image ls "${IMAGE}" --format="true") ]]; then
		echo "==> build-tool using image ${IMAGE}"
	else
		echo "==> preparing temporary build-tool image ${IMAGE} ..."
		tmpdir=$(mktemp -d)
		cat >"${tmpdir}/Dockerfile" <<EOF
FROM datafuselabs/build-tool:${RUNNER_BASE}-${TOOLCHAIN_VERSION}
RUN useradd -u ${_UID} -M -s /bin/bash ${USER}
RUN chown -R ${USER} /opt/rust/
RUN printf "${USER} ALL=(ALL:ALL) NOPASSWD:ALL\\n" > /etc/sudoers.d/databend
EOF
		docker build -t "${IMAGE}" "${tmpdir}"
		rm -rf "${tmpdir}"
	fi
fi

# NOTE: create with runner user first to avoid permission issues
mkdir -p "${CARGO_HOME}/git"
mkdir -p "${CARGO_HOME}/registry"

if [[ $INTERACTIVE == "true" ]]; then
	echo "==> build-tool with interactive..." >&2
	EXTRA_ARGS="--interactive --env TERM=xterm-256color"
fi

for var in ${BYPASS_ENV_VARS//,/ }; do
	EXTRA_ARGS="${EXTRA_ARGS} --env ${var}"
done

if [[ $ENABLE_SCCACHE == "true" ]]; then
	env | grep -E "^SCCACHE_" >"${CARGO_HOME}/sccache.env"
	EXTRA_ARGS="${EXTRA_ARGS} --env RUSTC_WRAPPER=sccache --env-file ${CARGO_HOME}/sccache.env"
	if [[ $COMMAND == "cargo" ]]; then
		COMMAND_ARGS="${COMMAND_ARGS} && sccache --show-stats"
	fi
fi

# shellcheck disable=SC2086
exec docker run --rm --tty ${EXTRA_ARGS} \
	--user "${_UID}:${_GID}" \
	--volume "${CARGO_HOME}/registry:/opt/rust/cargo/registry" \
	--volume "${CARGO_HOME}/git:/opt/rust/cargo/git" \
	--volume "${PWD}:/workspace" \
	--workdir "/workspace" \
	"${IMAGE}" /bin/bash -c "${COMMAND_ARGS}"
