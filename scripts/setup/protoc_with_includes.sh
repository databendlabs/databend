#!/usr/bin/env bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -euo pipefail

cargo_home="${CARGO_HOME:-${HOME}/.cargo}"

protoc_bin=$(command -v protoc || true)

protobuf_include_dirs=(/usr/local/include /usr/include)
if [[ -n "${protoc_bin}" ]]; then
	protoc_prefix=$(cd "$(dirname "${protoc_bin}")/.." && pwd)
	protobuf_include_dirs=("${protoc_prefix}/include" "${protobuf_include_dirs[@]}")
fi

for protobuf_include in "${protobuf_include_dirs[@]}"; do
	if [[ -f "${protobuf_include}/google/protobuf/timestamp.proto" ]]; then
		[[ -n "${protoc_bin}" ]] || break
		exec "${protoc_bin}" "$@" "-I${protobuf_include}"
	fi
done

# rust-protobuf vendors the well-known type definitions needed by protoc. Use
# them only when the protoc installation and system include paths lack them.
for protobuf_include in "${cargo_home}"/registry/src/*/protobuf-parse-*/src/proto; do
	if [[ -f "${protobuf_include}/google/protobuf/timestamp.proto" ]]; then
		[[ -n "${protoc_bin}" ]] || break
		exec "${protoc_bin}" "$@" "-I${protobuf_include}"
	fi
done

for protobuf_include in "${cargo_home}"/registry/src/*/protobuf-src-*/protobuf/src; do
	if [[ -f "${protobuf_include}/google/protobuf/timestamp.proto" ]]; then
		[[ -n "${protoc_bin}" ]] || break
		exec "${protoc_bin}" "$@" "-I${protobuf_include}"
	fi
done

if [[ -z "${protoc_bin}" ]]; then
	echo "error: protoc was not found in PATH" >&2
	exit 127
fi

exec "${protoc_bin}" "$@"
