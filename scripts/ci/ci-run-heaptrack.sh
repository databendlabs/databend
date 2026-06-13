#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -euo pipefail

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../" || exit 1

usage() {
	cat <<'USAGE'
Usage:
  make heaptrack

Environment variables:
  HEAPTRACK_TARGET            Target to profile: meta|query. Default: meta
  HEAPTRACK_PROFILE           Build profile: release|debug|<custom>. Default: debug
  HEAPTRACK_TIMEOUT_SECONDS   Auto-stop after N seconds. 0 disables timeout. Default: 120
  HEAPTRACK_OUTPUT_DIR        Output directory. Default: target/heaptrack
  HEAPTRACK_PREBUILD          Build binary before profiling: 1|0. Default: 1

Single-target overrides:
  HEAPTRACK_BIN               Cargo bin target name
  HEAPTRACK_PACKAGE           Cargo package name
  HEAPTRACK_BIN_ARGS          Arguments passed to the profiled binary
  HEAPTRACK_OUTPUT            Output base path passed to cargo-heaptrack

Per-target overrides (fallback in single mode):
  HEAPTRACK_META_BIN          Default: databend-meta
  HEAPTRACK_META_PACKAGE      Default: databend-meta-binaries
  HEAPTRACK_META_BIN_ARGS     Default: --single --log-level=ERROR
  HEAPTRACK_META_OUTPUT       Output base path for meta

  HEAPTRACK_QUERY_BIN         Default: databend-query
  HEAPTRACK_QUERY_PACKAGE     Default: databend-binaries
  HEAPTRACK_QUERY_BIN_ARGS    Default: -c scripts/ci/deploy/config/databend-query-node-1.toml --internal-enable-sandbox-tenant
  HEAPTRACK_QUERY_OUTPUT      Output base path for query

Notes:
  cargo-heaptrack writes raw data by default. The generated file is typically:
  <output>.raw.zst

Examples:
  make heaptrack
  HEAPTRACK_TARGET=query make heaptrack
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
	usage
	exit 0
fi

if ! command -v heaptrack >/dev/null 2>&1; then
	echo "heaptrack not found in PATH"
	echo "Install it first (Ubuntu/Debian): sudo apt-get install heaptrack"
	exit 1
fi

if ! cargo heaptrack --version >/dev/null 2>&1; then
	echo "cargo-heaptrack not found"
	echo "Install it first: cargo install cargo-heaptrack --locked"
	exit 1
fi

HEAPTRACK_TARGET=${HEAPTRACK_TARGET:-meta}
HEAPTRACK_PROFILE=${HEAPTRACK_PROFILE:-debug}
HEAPTRACK_TIMEOUT_SECONDS=${HEAPTRACK_TIMEOUT_SECONDS:-120}
HEAPTRACK_OUTPUT_DIR=${HEAPTRACK_OUTPUT_DIR:-target/heaptrack}
HEAPTRACK_PREBUILD=${HEAPTRACK_PREBUILD:-1}

mkdir -p "$HEAPTRACK_OUTPUT_DIR"

resolve_package_from_bin() {
	local bin="$1"
	case "$bin" in
	databend-meta | databend-meta-oss | databend-metabench | databend-metactl | databend-metaverifier)
		echo "databend-meta-binaries"
		;;
	databend-query | databend-query-oss | table-meta-inspector)
		echo "databend-binaries"
		;;
	*)
		echo "$bin"
		;;
	esac
}

default_bin_for_target() {
	local target="$1"
	case "$target" in
	meta)
		echo "databend-meta"
		;;
	query)
		echo "databend-query"
		;;
	*)
		echo ""
		;;
	esac
}

default_args_for_target() {
	local target="$1"
	case "$target" in
	meta)
		echo "--single --log-level=ERROR"
		;;
	query)
		echo "-c scripts/ci/deploy/config/databend-query-node-1.toml --internal-enable-sandbox-tenant"
		;;
	*)
		echo ""
		;;
	esac
}

run_one_target() {
	local target="$1"

	local default_bin
	default_bin="$(default_bin_for_target "$target")"
	if [[ -z "$default_bin" ]]; then
		echo "unsupported target: $target"
		exit 1
	fi

	local bin=""
	local package=""
	local bin_args=""
	local output_base=""

	if [[ "$target" == "meta" ]]; then
		bin=${HEAPTRACK_META_BIN:-$default_bin}
		bin_args=${HEAPTRACK_META_BIN_ARGS:-$(default_args_for_target "$target")}
		if [[ -n "${HEAPTRACK_META_PACKAGE:-}" ]]; then
			package="$HEAPTRACK_META_PACKAGE"
		fi
		if [[ -n "${HEAPTRACK_META_OUTPUT:-}" ]]; then
			output_base="$HEAPTRACK_META_OUTPUT"
		fi
	else
		bin=${HEAPTRACK_QUERY_BIN:-$default_bin}
		bin_args=${HEAPTRACK_QUERY_BIN_ARGS:-$(default_args_for_target "$target")}
		if [[ -n "${HEAPTRACK_QUERY_PACKAGE:-}" ]]; then
			package="$HEAPTRACK_QUERY_PACKAGE"
		fi
		if [[ -n "${HEAPTRACK_QUERY_OUTPUT:-}" ]]; then
			output_base="$HEAPTRACK_QUERY_OUTPUT"
		fi
	fi

	bin=${HEAPTRACK_BIN:-$bin}
	if [[ -n "${HEAPTRACK_BIN_ARGS:-}" ]]; then
		bin_args="$HEAPTRACK_BIN_ARGS"
	fi
	if [[ -n "${HEAPTRACK_PACKAGE:-}" ]]; then
		package="$HEAPTRACK_PACKAGE"
	fi
	if [[ -n "${HEAPTRACK_OUTPUT:-}" ]]; then
		output_base="$HEAPTRACK_OUTPUT"
	fi

	if [[ -z "$package" ]]; then
		package="$(resolve_package_from_bin "$bin")"
	fi

	if [[ -z "$output_base" ]]; then
		local ts
		ts="$(date +%Y%m%d-%H%M%S)"
		output_base="$HEAPTRACK_OUTPUT_DIR/heaptrack-${bin}-${ts}"
	fi

	# shellcheck disable=SC2206
	local -a bin_args_arr=($bin_args)
	local -a cmd=(cargo heaptrack --package "$package" --bin "$bin" --output "$output_base")

	case "$HEAPTRACK_PROFILE" in
	release) ;;
	debug)
		cmd+=(--dev)
		;;
	*)
		cmd+=(--profile "$HEAPTRACK_PROFILE")
		;;
	esac

	if [[ ${#bin_args_arr[@]} -gt 0 ]]; then
		cmd+=(-- "${bin_args_arr[@]}")
	fi

	echo "Running heaptrack target=$target (${package}/${bin}) ..."
	echo "Output base: ${output_base}"
	if [[ "$target" == "query" ]]; then
		echo "Tip: execute workload queries in another terminal while profiling query."
	fi

	if [[ "$HEAPTRACK_PREBUILD" == "1" ]]; then
		local -a build_cmd=(cargo build --package "$package" --bin "$bin")
		case "$HEAPTRACK_PROFILE" in
		release)
			build_cmd+=(--release)
			;;
		debug) ;;
		*)
			build_cmd+=(--profile "$HEAPTRACK_PROFILE")
			;;
		esac
		echo "Prebuilding ${package}/${bin} with profile=${HEAPTRACK_PROFILE} ..."
		"${build_cmd[@]}"
	fi

	local status=0
	if [[ "$HEAPTRACK_TIMEOUT_SECONDS" =~ ^[0-9]+$ ]] && [[ "$HEAPTRACK_TIMEOUT_SECONDS" -gt 0 ]]; then
		set +e
		timeout --signal=INT --kill-after=15 "${HEAPTRACK_TIMEOUT_SECONDS}s" "${cmd[@]}"
		status=$?
		set -e
		if [[ "$status" -ne 0 && "$status" -ne 124 && "$status" -ne 130 ]]; then
			echo "heaptrack command failed with exit code ${status}"
			exit "$status"
		fi
	else
		"${cmd[@]}"
	fi

	local final_output=""
	for candidate in \
		"${output_base}.raw.zst" \
		"${output_base}.zst" \
		"${output_base}.gz" \
		"${output_base}"; do
		if [[ -f "$candidate" ]]; then
			final_output="$candidate"
			break
		fi
	done

	if [[ -n "$final_output" ]]; then
		echo "heaptrack run completed for $target."
		echo "Inspect data with: heaptrack --analyze ${final_output}"
	else
		echo "heaptrack run completed for $target, but no output file was found at expected paths:"
		echo "  ${output_base}.raw.zst"
		echo "  ${output_base}.zst"
		echo "  ${output_base}.gz"
		echo "  ${output_base}"
		echo "This can happen if timeout expires during build before profiling starts."
	fi
}

case "$HEAPTRACK_TARGET" in
meta)
	run_one_target meta
	;;
query)
	run_one_target query
	;;
*)
	echo "invalid HEAPTRACK_TARGET: ${HEAPTRACK_TARGET}"
	echo "valid values: meta, query"
	exit 1
	;;
esac
