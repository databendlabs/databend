#!/usr/bin/env bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -euo pipefail

cargo_home="${CARGO_HOME:-${HOME}/.cargo}"

if [[ -f /usr/local/include/google/protobuf/timestamp.proto ]]; then
	exec protoc "$@"
fi

for protobuf_include in "${cargo_home}"/registry/src/*/protobuf-src-*/protobuf/src; do
	if [[ -f "${protobuf_include}/google/protobuf/timestamp.proto" ]]; then
		exec protoc "$@" "-I${protobuf_include}"
	fi
done

exec protoc "$@"
