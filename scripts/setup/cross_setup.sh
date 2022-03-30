#!/bin/bash
# Copyright (c) The Diem Core Contributors.
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../.." || exit

if [[ $(uname -m) != "x86_64" ]]; then
	echo "Cross-compilation is only supported on x86_64"
	exit 1
fi

if [[ ! $(grep /etc/os-release -e "ID=debian") ]]; then
	echo "Cross-compilation is only supported on Debian"
	exit 1
fi

if [[ ! $(id -u) -eq 0 ]]; then
	echo "Cross-compilation requires root"
	exit 1
fi

dpkg --add-architecture arm64
apt-get update -yq
apt-get install -yq libc6-arm64-cross libc6-dev-arm64-cross gcc-aarch64-linux-gnu
apt-get install -yq libdbus-1-dev libdbus-1-dev:arm64
apt-get install -yq libssl-dev libssl-dev:arm64 zlib1g-dev zlib1g-dev:arm64

rustup target add x86_64-unknown-linux-gnu
rustup target add aarch64-unknown-linux-gnu

cat <<EOF >${CARGO_HOME}/config
[target.aarch64-unknown-linux-gnu]
linker = "aarch64-linux-gnu-gcc"
EOF
