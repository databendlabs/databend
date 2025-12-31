#!/bin/bash
# Copyright (c) The Diem Core Contributors.
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../.." || exit

# NOTE: never use sudo under macos
PRE_COMMAND=()
if [[ "$(whoami)" != 'root' ]] && [[ ${PACKAGE_MANAGER} != "brew" ]]; then
	PRE_COMMAND=(sudo)
fi

function add_to_profile {
	eval "$1"
	FOUND=$(grep -c "$1" "${HOME}/.profile" || true)
	if [ "$FOUND" == "0" ]; then
		echo "$1" >>"${HOME}"/.profile
	fi
}

function update_path_and_profile {
	touch "${HOME}"/.profile
	mkdir -p "${HOME}"/bin
	if [ -n "$CARGO_HOME" ]; then
		add_to_profile "export CARGO_HOME=\"${CARGO_HOME}\""
		add_to_profile "export PATH=\"${HOME}/bin:${CARGO_HOME}/bin:\$PATH\""
	else
		add_to_profile "export PATH=\"${HOME}/bin:${HOME}/.cargo/bin:\$PATH\""
	fi
}

function install_pkg {
	package=$1
	PACKAGE_MANAGER=$2
	if which "$package" &>/dev/null; then
		echo "$package is already installed"
	else
		echo "Installing ${package}."
		case "$PACKAGE_MANAGER" in
		apt-get)
			"${PRE_COMMAND[@]}" apt-get install --no-install-recommends -yq "${package}"
			;;
		yum)
			"${PRE_COMMAND[@]}" yum install -y -q "${package}"
			;;
		pacman)
			"${PRE_COMMAND[@]}" pacman --quiet --noconfirm -Syu "$package"
			;;
		apk)
			apk --quiet --update add --no-cache "${package}"
			;;
		dnf)
			dnf --quiet install "$package"
			;;
		brew)
			brew install --quiet "$package"
			;;
		*)
			echo "Unable to install ${package} package manager: $PACKAGE_MANAGER"
			exit 1
			;;
		esac
	fi
}

function install_build_essentials {
	PACKAGE_MANAGER=$1

	echo "==> installing build essentials..."

	case "$PACKAGE_MANAGER" in
	apt-get)
		install_pkg build-essential "$PACKAGE_MANAGER"
		;;
	pacman)
		install_pkg base-devel "$PACKAGE_MANAGER"
		;;
	apk)
		install_pkg alpine-sdk "$PACKAGE_MANAGER"
		install_pkg coreutils "$PACKAGE_MANAGER"
		;;
	yum | dnf)
		install_pkg gcc "$PACKAGE_MANAGER"
		install_pkg gcc-c++ "$PACKAGE_MANAGER"
		install_pkg make "$PACKAGE_MANAGER"
		;;
	brew)
		# skip
		;;
	*)
		echo "Unable to install build essentials with package manager: $PACKAGE_MANAGER"
		exit 1
		;;
	esac
}

function install_python3 {
	PACKAGE_MANAGER=$1

	if python3 --version; then
		echo "==> python3 is already installed"
		return
	fi
	echo "==> installing python3..."

	case "$PACKAGE_MANAGER" in
	apt-get)
		install_pkg python3-all-dev "$PACKAGE_MANAGER"
		install_pkg python3-setuptools "$PACKAGE_MANAGER"
		install_pkg python3-pip "$PACKAGE_MANAGER"
		install_pkg libcairo2-dev "$PACKAGE_MANAGER"
		;;
	apk)
		install_pkg python3-dev "$PACKAGE_MANAGER"
		install_pkg py3-pip "$PACKAGE_MANAGER"
		install_pkg libffi-dev "$PACKAGE_MANAGER"
		;;
	brew | pacman)
		install_pkg python3 "$PACKAGE_MANAGER"
		install_pkg cairo "$PACKAGE_MANAGER"
		;;
	yum | dnf)
		install_pkg python3-devel "$PACKAGE_MANAGER"
		install_pkg cairo-devel "$PACKAGE_MANAGER"
		;;
	*)
		echo "Unable to install python3 with package manager: $PACKAGE_MANAGER"
		exit 1
		;;
	esac
}

function install_openssl {
	PACKAGE_MANAGER=$1

	echo "==> installing openssl libs..."

	install_pkg perl-core "$PACKAGE_MANAGER" || echo "perl-core skipped"
	install_pkg perl-CPAN "$PACKAGE_MANAGER" || echo "perl-CPAN skipped"
	install_pkg perl-devel "$PACKAGE_MANAGER" || echo "perl-devel skipped"

	PERL_MM_USE_DEFAULT=1 perl -MCPAN -e 'install IPC::Cmd'

	case "$PACKAGE_MANAGER" in
	apt-get)
		install_pkg libssl-dev "$PACKAGE_MANAGER"
		;;
	pacman)
		install_pkg openssl "$PACKAGE_MANAGER"
		;;
	apk)
		install_pkg openssl-dev "$PACKAGE_MANAGER"
		install_pkg openssl-libs-static "$PACKAGE_MANAGER"
		;;
	yum)
		install_pkg openssl-devel "$PACKAGE_MANAGER"
		;;
	dnf)
		install_pkg openssl-devel "$PACKAGE_MANAGER"
		;;
	brew)
		install_pkg openssl "$PACKAGE_MANAGER"
		;;
	*)
		echo "Unable to install openssl with package manager: $PACKAGE_MANAGER"
		exit 1
		;;
	esac
}

function install_protobuf {
	PACKAGE_MANAGER=$1

	if protoc --version; then
		echo "==> protoc is already installed"
		return
	fi
	echo "==> installing protobuf compiler..."

	case "$PACKAGE_MANAGER" in
	brew | apk)
		install_pkg protobuf "$PACKAGE_MANAGER"
		;;
	*)
		arch=$(uname -m)
		arch=${arch/aarch64/aarch_64}
		PB_REL="https://github.com/protocolbuffers/protobuf/releases"
		curl -LO $PB_REL/download/v3.15.8/protoc-3.15.8-linux-${arch}.zip
		unzip protoc-3.15.8-linux-${arch}.zip -d protoc-3.15.8
		"${PRE_COMMAND[@]}" cp protoc-3.15.8/bin/protoc /usr/local/bin/
		"${PRE_COMMAND[@]}" rm -rf protoc-3.15.8*
		"${PRE_COMMAND[@]}" chmod +x /usr/local/bin/protoc
		;;
	esac

	protoc --version
}

function ensure_clang_toolchain {
	PACKAGE_MANAGER=$1

	case "$PACKAGE_MANAGER" in
	brew)
		BREW_LLVM_PATHS=(
			/opt/homebrew/opt/llvm/bin/clang
			/usr/local/opt/llvm/bin/clang
		)
		for brew_clang in "${BREW_LLVM_PATHS[@]}"; do
			if [[ -x "$brew_clang" ]]; then
				echo "==> Homebrew llvm already installed (${brew_clang})"
				return
			fi
		done
		echo "==> Homebrew llvm clang not found, installing with brew"
		;;
	*)
		if command -v clang >/dev/null 2>&1; then
			clang_version=$(clang --version | head -n 1)
			echo "==> clang is already installed (${clang_version})"
			return
		fi

		echo "==> clang not found, installing with ${PACKAGE_MANAGER}"
		;;
	esac

	install_pkg clang "$PACKAGE_MANAGER"
	install_pkg llvm "$PACKAGE_MANAGER"
}

function install_jdk {
	PACKAGE_MANAGER=$1

	echo "==> installing java development kit..."

	case "$PACKAGE_MANAGER" in
	apt-get)
		install_pkg openjdk-11-jdk-headless "$PACKAGE_MANAGER"
		;;
	pacman)
		install_pkg jre11-openjdk-headless "$PACKAGE_MANAGER"
		;;
	apk)
		install_pkg openjdk11 "$PACKAGE_MANAGER"
		;;
	yum)
		install_pkg java-11-openjdk "$PACKAGE_MANAGER"
		;;
	dnf)
		install_pkg java-11-openjdk "$PACKAGE_MANAGER"
		;;
	brew)
		install_pkg java11 "$PACKAGE_MANAGER"
		;;
	*)
		echo "Unable to install jdk with package manager: $PACKAGE_MANAGER"
		exit 1
		;;
	esac
}

function install_lapack {
	PACKAGE_MANAGER=$1

	echo "==> installing lapack library..."

	case "$PACKAGE_MANAGER" in
	apt-get)
		install_pkg liblapack-dev "$PACKAGE_MANAGER"
		;;
	pacman)
		install_pkg lapack "$PACKAGE_MANAGER"
		;;
	apk)
		install_pkg lapack-dev "$PACKAGE_MANAGER"
		;;
	yum)
		install_pkg lapack-devel "$PACKAGE_MANAGER"
		;;
	dnf)
		install_pkg lapack-devel "$PACKAGE_MANAGER"
		;;
	brew)
		install_pkg lapack "$PACKAGE_MANAGER"
		;;
	*)
		echo "Unable to install lapack with package manager: $PACKAGE_MANAGER"
		exit 1
		;;
	esac
}

function install_pkg_config {
	PACKAGE_MANAGER=$1

	echo "==> installing pkg-config..."

	case "$PACKAGE_MANAGER" in
	apt-get | dnf)
		install_pkg pkg-config "$PACKAGE_MANAGER"
		;;
	pacman)
		install_pkg pkgconf "$PACKAGE_MANAGER"
		;;
	apk | brew | yum)
		install_pkg pkgconfig "$PACKAGE_MANAGER"
		;;
	*)
		echo "Unable to install pkg-config with package manager: $PACKAGE_MANAGER"
		exit 1
		;;
	esac
}

function install_mysql_client {
	PACKAGE_MANAGER=$1

	echo "==> installing mysql client..."

	case "$PACKAGE_MANAGER" in
	apt-get)
		install_pkg default-mysql-client "$PACKAGE_MANAGER"
		;;
	pacman)
		install_pkg mysql-clients "$PACKAGE_MANAGER"
		;;
	apk)
		install_pkg mysql-client "$PACKAGE_MANAGER"
		;;
	yum | dnf | brew)
		install_pkg mysql "$PACKAGE_MANAGER"
		;;
	*)
		echo "Unable to install mysql client with package manager: $PACKAGE_MANAGER"
		exit 1
		;;
	esac
}

function install_sqlite3 {
	PACKAGE_MANAGER=$1

	echo "==> installing sqlite3..."

	case "$PACKAGE_MANAGER" in
	apt-get)
		install_pkg libsqlite3-dev "$PACKAGE_MANAGER"
		install_pkg sqlite3 "$PACKAGE_MANAGER"
		;;
	pacman)
		install_pkg sqlite "$PACKAGE_MANAGER"
		;;
	apk)
		install_pkg sqlite-dev "$PACKAGE_MANAGER"
		;;
	yum | dnf)
		install_pkg sqlite-devel "$PACKAGE_MANAGER"
		install_pkg sqlite "$PACKAGE_MANAGER"
		;;
	brew)
		install_pkg sqlite "$PACKAGE_MANAGER"
		;;
	*)
		echo "Unable to install sqlite3 with package manager: $PACKAGE_MANAGER"
		exit 1
		;;
	esac
}

function install_libtiff {
	PACKAGE_MANAGER=$1

	echo "==> installing libtiff..."

	case "$PACKAGE_MANAGER" in
	apt-get)
		install_pkg libtiff-dev "$PACKAGE_MANAGER"
		;;
	pacman)
		install_pkg libtiff "$PACKAGE_MANAGER"
		;;
	apk)
		install_pkg tiff-dev "$PACKAGE_MANAGER"
		;;
	yum | dnf)
		install_pkg libtiff-devel "$PACKAGE_MANAGER"
		;;
	brew)
		install_pkg libtiff "$PACKAGE_MANAGER"
		;;
	*)
		echo "Unable to install libtiff with package manager: $PACKAGE_MANAGER"
		exit 1
		;;
	esac
}

function install_binutils {
	PACKAGE_MANAGER=$1

	echo "==> installing binutils..."

	case "$PACKAGE_MANAGER" in
	apt-get)
		install_pkg binutils "$PACKAGE_MANAGER"
		;;
	yum | dnf)
		install_pkg binutils "$PACKAGE_MANAGER"
		;;
	brew)
		# skip
		;;
	*)
		echo "Unable to install binutils with package manager: $PACKAGE_MANAGER"
		exit 1
		;;
	esac
}

function install_rustup {
	RUST_TOOLCHAIN=$1

	echo "==> Installing Rust......"
	if rustup --version &>/dev/null; then
		echo "Rust is already installed"
	else
		curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain "${RUST_TOOLCHAIN}" --profile minimal
		PATH="${HOME}/.cargo/bin:${PATH}"
		source "$HOME"/.cargo/env
	fi

	rustup show
}

function install_sccache {
	local version="$1"

	echo "==> installing sccache..."
	if sccache --version &>/dev/null; then
		echo "sccache is already installed"
		return
	fi

	if [[ -z "$version" ]]; then
		echo "Missing sccache version"
		return 1
	fi

	local os
	os="$(uname -s)"
	if [[ "$os" == "Darwin" ]]; then
		brew install sccache
		sccache --version
		return
	fi

	if [[ "$os" != "Linux" ]]; then
		echo "Unsupported operating system for sccache: $(uname -s)"
		return 1
	fi

	local arch triple asset url tmpdir extract_dir cargo_bin
	case "$(uname -m)" in
	x86_64 | amd64)
		triple="x86_64-unknown-linux-musl"
		;;
	aarch64 | arm64)
		triple="aarch64-unknown-linux-musl"
		;;
	*)
		echo "Unsupported architecture for sccache: $(uname -m)"
		return 1
		;;
	esac

	asset="sccache-${version}-${triple}.tar.gz"
	url="https://github.com/mozilla/sccache/releases/download/${version}/${asset}"
	tmpdir=$(mktemp -d)
	if ! curl -fsSL "$url" -o "${tmpdir}/${asset}"; then
		rm -rf "$tmpdir"
		echo "Failed to download sccache from ${url}"
		return 1
	fi

	if ! tar -xzf "${tmpdir}/${asset}" -C "$tmpdir"; then
		rm -rf "$tmpdir"
		echo "Failed to extract sccache archive"
		return 1
	fi

	CARGO_HOME="${CARGO_HOME:-${HOME}/.cargo}"
	cargo_bin="${CARGO_HOME}/bin"
	mkdir -p "$cargo_bin"
	extract_dir="${tmpdir}/sccache-${version}-${triple}"
	if [[ ! -f "${extract_dir}/sccache" ]]; then
		rm -rf "$tmpdir"
		echo "sccache binary not found in archive"
		return 1
	fi

	install -m 755 "${extract_dir}/sccache" "${cargo_bin}/sccache"
	rm -rf "$tmpdir"

	sccache --version
}

function install_cargo_nextest {
	CARGO_HOME="${CARGO_HOME:-${HOME}/.cargo}"
	if [[ "$(uname -s)" == "Darwin" ]]; then
		brew install cargo-nextest
	else
		if [[ "$(uname -s)" != "Linux" ]]; then
			echo "Unsupported operating system for cargo-nextest: $(uname -s)"
			return 1
		fi
		local nextest_url
		case "$(uname -m)" in
		x86_64 | amd64)
			nextest_url="https://get.nexte.st/latest/linux"
			;;
		aarch64 | arm64)
			nextest_url="https://get.nexte.st/latest/linux-arm"
			;;
		*)
			echo "Unsupported architecture for cargo-nextest: $(uname -m)"
			return 1
			;;
		esac
		mkdir -p "${CARGO_HOME}/bin"
		curl -LsSf "${nextest_url}" | tar zxf - -C "${CARGO_HOME}/bin"
	fi
	cargo nextest --version
}

function install_taplo_cli {
	echo "==> installing taplo CLI..."
	if taplo --version &>/dev/null; then
		echo "taplo CLI is already installed"
		return
	fi

	local os
	os="$(uname -s)"
	if [[ "$os" == "Darwin" ]]; then
		brew install taplo
		taplo --version
		return
	fi

	if [[ "$os" != "Linux" ]]; then
		echo "Unsupported operating system for taplo CLI: $(uname -s)"
		return 1
	fi

	local taplo_bin arch asset url tmpfile
	CARGO_HOME="${CARGO_HOME:-${HOME}/.cargo}"
	taplo_bin="${CARGO_HOME}/bin"
	mkdir -p "$taplo_bin"

	case "$(uname -m)" in
	x86_64 | amd64)
		arch="x86_64"
		;;
	aarch64 | arm64)
		arch="aarch64"
		;;
	*)
		echo "Unsupported architecture for taplo CLI: $(uname -m)"
		return 1
		;;
	esac

	asset="taplo-linux-${arch}.gz"
	url="https://github.com/tamasfe/taplo/releases/latest/download/${asset}"

	tmpfile=$(mktemp)
	if ! curl -fsSL "$url" | gzip -dc >"$tmpfile"; then
		rm -f "$tmpfile"
		echo "Failed to download taplo CLI from ${url}"
		return 1
	fi

	chmod +x "$tmpfile"
	install -m 755 "$tmpfile" "${taplo_bin}/taplo"
	rm -f "$tmpfile"

	"${taplo_bin}/taplo" --version
}

function install_typos_cli {
	echo "==> installing typos CLI..."
	if typos --version &>/dev/null; then
		echo "typos CLI is already installed"
		return
	fi

	local os
	os="$(uname -s)"
	if [[ "$os" == "Darwin" ]]; then
		brew install typos-cli
		typos --version
		return
	fi

	if [[ "$os" != "Linux" ]]; then
		echo "Unsupported operating system for typos CLI: $(uname -s)"
		return 1
	fi

	local typos_bin arch triple version tag package url tmpdir
	CARGO_HOME="${CARGO_HOME:-${HOME}/.cargo}"
	typos_bin="${CARGO_HOME}/bin"
	mkdir -p "$typos_bin"

	case "$(uname -m)" in
	x86_64 | amd64)
		triple="x86_64-unknown-linux-musl"
		;;
	aarch64 | arm64)
		triple="aarch64-unknown-linux-musl"
		;;
	*)
		echo "Unsupported architecture for typos CLI: $(uname -m)"
		return 1
		;;
	esac

	version="1.40.0"
	tag="v${version}"
	package="typos-${tag}-${triple}.tar.gz"
	url="https://github.com/crate-ci/typos/releases/download/${tag}/${package}"

	tmpdir=$(mktemp -d)
	if ! curl -fsSL "$url" -o "${tmpdir}/${package}"; then
		rm -rf "$tmpdir"
		echo "Failed to download typos CLI from ${url}"
		return 1
	fi

	if ! tar -xzf "${tmpdir}/${package}" -C "$tmpdir"; then
		rm -rf "$tmpdir"
		echo "Failed to extract typos CLI package"
		return 1
	fi

	if [[ ! -f "${tmpdir}/typos" ]]; then
		rm -rf "$tmpdir"
		echo "typos binary not found in package ${package}"
		return 1
	fi

	install -m 755 "${tmpdir}/typos" "${typos_bin}/typos"
	rm -rf "$tmpdir"

	"${typos_bin}/typos" --version
}

function usage {
	cat <<EOF
    usage: $0 [options]

    options:
        -y Auto approve installation
        -b Install build tools
        -d Install development tools
        -p Install profile
        -s Install codegen tools
        -t Install tpch data set
        -v Verbose mode
EOF
}

function welcome_message {
	cat <<EOF
Welcome to DatabendQuery!

This script will download and install the necessary dependencies needed to
build, test and inspect DatabendQuery.

Based on your selection, these tools will be included:
EOF

	if [[ "$INSTALL_BUILD_TOOLS" == "true" ]]; then
		cat <<EOF
Build tools (since -b or no option was provided):
  * Rust (and the necessary components, e.g. rust-fmt, clippy)
  * build-essential
  * pkg-config
  * libssl-dev
  * protobuf-compiler
  * openjdk
  * python3-dev
EOF
	fi

	if [[ "$INSTALL_CHECK_TOOLS" == "true" ]]; then
		cat <<EOF
Check tools (since -c was provided):
  * lcov
  * taplo CLI
  * typos CLI
EOF
	fi

	if [[ "$INSTALL_DEV_TOOLS" == "true" ]]; then
		cat <<EOF
Development tools (since -d was provided):
  * mysql client
  * python3 (boto3, ruff, yamllint, ...)
  * python database drivers (mysql-connector-python, pymysql, sqlalchemy, clickhouse_driver)
EOF
	fi

	if [[ "$INSTALL_CODEGEN" == "true" ]]; then
		cat <<EOF
Codegen tools (since -s was provided):
  * Python3 (numpy, pyre-check)
EOF
	fi

	if [[ "$INSTALL_PROFILE" == "true" ]]; then
		cat <<EOF
Moreover, ~/.profile will be updated (since -p was provided).
EOF
	fi

	if [[ "$INSTALL_TPCH_DATA" == "true" ]]; then
		cat <<EOF
Tpch dataset (since -t was provided):
EOF
	fi

	cat <<EOF
If you'd prefer to install these dependencies yourself, please exit this script
now with Ctrl-C.
EOF
}

AUTO_APPROVE=false
VERBOSE=false
INSTALL_BUILD_TOOLS=false
INSTALL_CHECK_TOOLS=false
INSTALL_DEV_TOOLS=false
INSTALL_PROFILE=false
INSTALL_CODEGEN=false
INSTALL_TPCH_DATA=false
TPCH_SCALE_FACTOR=$2

# parse args
while getopts "ybcdpstv" arg; do
	case "$arg" in
	y)
		AUTO_APPROVE="true"
		;;
	b)
		INSTALL_BUILD_TOOLS="true"
		;;
	c)
		INSTALL_CHECK_TOOLS="true"
		;;
	d)
		INSTALL_DEV_TOOLS="true"
		;;
	p)
		INSTALL_PROFILE="true"
		;;
	s)
		INSTALL_CODEGEN="true"
		;;
	v)
		VERBOSE="true"
		;;
	t)
		INSTALL_TPCH_DATA="true"
		;;

	*)
		usage
		exit 0
		;;
	esac
done

if [[ "$VERBOSE" == "true" ]]; then
	set -x
fi

if [[ "$INSTALL_BUILD_TOOLS" == "false" ]] &&
	[[ "$INSTALL_CHECK_TOOLS" == "false" ]] &&
	[[ "$INSTALL_DEV_TOOLS" == "false" ]] &&
	[[ "$INSTALL_PROFILE" == "false" ]] &&
	[[ "$INSTALL_TPCH_DATA" == "false" ]] &&
	[[ "$INSTALL_CODEGEN" == "false" ]]; then
	INSTALL_BUILD_TOOLS="true"
fi

if [ ! -f rust-toolchain.toml ]; then
	echo "Unknown location. Please run this from the databend repository. Abort."
	exit 1
fi
RUST_TOOLCHAIN="$(awk -F'[ ="]+' '$1 == "channel" { print $2 }' rust-toolchain.toml)"

PACKAGE_MANAGER=
if [[ "$(uname)" == "Linux" ]]; then
	if command -v yum &>/dev/null; then
		PACKAGE_MANAGER="yum"
	elif command -v apt-get &>/dev/null; then
		PACKAGE_MANAGER="apt-get"
	elif command -v pacman &>/dev/null; then
		PACKAGE_MANAGER="pacman"
	elif command -v apk &>/dev/null; then
		PACKAGE_MANAGER="apk"
	elif command -v dnf &>/dev/null; then
		echo "WARNING: dnf package manager support is experimental"
		PACKAGE_MANAGER="dnf"
	else
		echo "Unable to find supported package manager (yum, apt-get, dnf, apk, or pacman). Abort"
		exit 1
	fi
elif [[ "$(uname)" == "Darwin" ]]; then
	if which brew &>/dev/null; then
		PACKAGE_MANAGER="brew"
	else
		echo "Missing package manager Homebrew (https://brew.sh/). Abort"
		exit 1
	fi
else
	echo "Unknown OS. Abort."
	exit 1
fi

if [[ "$AUTO_APPROVE" == "false" ]]; then
	welcome_message
	printf "Proceed with installing necessary dependencies? (y/N) > "
	read -e -r input
	if [[ "$input" != "y"* ]]; then
		echo "Exiting..."
		exit 0
	fi
fi

case "$PACKAGE_MANAGER" in
apt-get)
	"${PRE_COMMAND[@]}" apt-get update
	install_pkg ca-certificates "$PACKAGE_MANAGER"
	;;
yum | dnf)
	install_pkg epel-release "$PACKAGE_MANAGER"
	;;
*) ;;
esac

[[ "$INSTALL_PROFILE" == "true" ]] && update_path_and_profile

install_pkg curl "$PACKAGE_MANAGER"

if [[ "$INSTALL_BUILD_TOOLS" == "true" ]]; then
	install_rustup "$RUST_TOOLCHAIN"

	install_pkg unzip "$PACKAGE_MANAGER"
	install_build_essentials "$PACKAGE_MANAGER"
	install_pkg_config "$PACKAGE_MANAGER"
	install_openssl "$PACKAGE_MANAGER"
	install_protobuf "$PACKAGE_MANAGER"
	install_jdk "$PACKAGE_MANAGER"
	install_lapack "$PACKAGE_MANAGER"

	install_pkg cmake "$PACKAGE_MANAGER"
	ensure_clang_toolchain "$PACKAGE_MANAGER"
	install_python3 "$PACKAGE_MANAGER"
	install_sqlite3 "$PACKAGE_MANAGER"
	install_libtiff "$PACKAGE_MANAGER"
	install_binutils "$PACKAGE_MANAGER"

	# Any call to cargo will make rustup install the correct toolchain
	cargo version

	install_sccache "v0.12.0"
	install_cargo_nextest
fi

if [[ "$INSTALL_CHECK_TOOLS" == "true" ]]; then
	if [[ "$PACKAGE_MANAGER" == "apk" ]]; then
		# needed by lcov
		echo http://nl.alpinelinux.org/alpine/edge/testing >>/etc/apk/repositories
	fi
	install_pkg lcov "$PACKAGE_MANAGER"

	install_taplo_cli
	install_typos_cli
fi

if [[ "$INSTALL_DEV_TOOLS" == "true" ]]; then
	install_mysql_client "$PACKAGE_MANAGER"
	install_pkg git "$PACKAGE_MANAGER"
	install_python3 "$PACKAGE_MANAGER"
	if [[ "$PACKAGE_MANAGER" == "apt-get" ]]; then
		# for killall & timeout
		install_pkg psmisc "$PACKAGE_MANAGER"
		install_pkg coreutils "$PACKAGE_MANAGER"
		# for graphviz, that fuzzingbook depends on
		install_pkg graphviz "$PACKAGE_MANAGER"
		install_pkg graphviz-dev "$PACKAGE_MANAGER"
	fi
	python3 -m pip install --quiet boto3 "moto[all]" shfmt-py toml yamllint ruff
	# drivers
	python3 -m pip install --quiet pymysql sqlalchemy clickhouse_driver
	# sqllogic dependencies
	python3 -m pip install --quiet mysql-connector-python==8.0.30
fi

if [[ "$INSTALL_CODEGEN" == "true" ]]; then
	ensure_clang_toolchain "$PACKAGE_MANAGER"
	install_python3 "$PACKAGE_MANAGER"
	"${PRE_COMMAND[@]}" python3 -m pip install --quiet coscmd PyYAML
fi

if [[ "$INSTALL_TPCH_DATA" == "true" ]]; then
	# Construct a docker imagine to generate tpch-data
	if [[ -z ${TPCH_SCALE_FACTOR} ]]; then
		docker build -f scripts/setup/tpchdata.dockerfile -t databend:latest .
	else
		docker build -f scripts/setup/tpchdata.dockerfile -t databend:latest \
			--build-arg scale_factor="${TPCH_SCALE_FACTOR}" .
	fi
	# Generate data into the ./data directory if it does not already exist
	FILE=benchmark/tpch/data/customer.tbl
	if test -f "$FILE"; then
		echo "$FILE exists."
	else
		mkdir "$(pwd)/benchmark/tpch/data" 2>/dev/null
		docker run -v "$(pwd)/benchmark/tpch/data:/data" --rm databend:latest
	fi
fi

if [[ "${AUTO_APPROVE}" == "false" ]]; then
	if [[ "$INSTALL_BUILD_TOOLS" == "true" ]]; then
		cat <<EOF
Finished installing all build dependencies.

You should now be able to build the project by running:
	cargo build
EOF
	fi

	if [[ "$INSTALL_DEV_TOOLS" == "true" ]]; then
		cat <<EOF
Finished installing all dev dependencies.

You should now be able to run tests with:
	make xxx-test (check Makefile for detailed target)
EOF
	fi

fi

exit 0
