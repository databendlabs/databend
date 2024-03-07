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

function install_ziglang {
	PACKAGE_MANAGER=$1

	if zig version; then
		echo "==> ziglang is already installed"
		return
	fi
	echo "==> installing ziglang..."

	arch=$(uname -m)
	case "$PACKAGE_MANAGER" in
	apt-get | yum | dnf | pacman)
		curl -sSfLo /tmp/zig.tar.xz "https://ziglang.org/download/0.11.0/zig-linux-${arch}-0.11.0.tar.xz"
		tar -xf /tmp/zig.tar.xz -C /tmp
		"${PRE_COMMAND[@]}" mv "/tmp/zig-linux-${arch}-0.11.0/zig" /usr/local/bin/
		"${PRE_COMMAND[@]}" chmod +x /usr/local/bin/zig
		"${PRE_COMMAND[@]}" mv "/tmp/zig-linux-${arch}-0.11.0/lib" /usr/local/lib/zig
		rm -rf /tmp/zig*
		;;
	brew)
		install_pkg zig "$PACKAGE_MANAGER"
		;;
	apk)
		echo "TODO: install ziglang for alpine"
		;;
	*)
		echo "Unable to install ziglang with package manager: $PACKAGE_MANAGER"
		exit 1
		;;
	esac
}

function install_python3 {
	PACKAGE_MANAGER=$1

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

function install_rustup {
	RUST_TOOLCHAIN=$1

	echo "==> Installing Rust......"
	if rustup --version &>/dev/null; then
		echo "Rust is already installed"
	else
		curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain "${RUST_TOOLCHAIN}" --profile minimal
		PATH="${HOME}/.cargo/bin:${PATH}"
		source $HOME/.cargo/env
	fi

	rustup show
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
  * tools from rust-tools.txt ( e.g. cargo-audit, cargo-machete, taplo-cli)
EOF
	fi

	if [[ "$INSTALL_DEV_TOOLS" == "true" ]]; then
		cat <<EOF
Development tools (since -d was provided):
  * mysql client
  * python3 (boto3, black, yamllint, ...)
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
	install_pkg clang "$PACKAGE_MANAGER"
	install_pkg llvm "$PACKAGE_MANAGER"
	install_ziglang "$PACKAGE_MANAGER"
	install_python3 "$PACKAGE_MANAGER"
	install_sqlite3 "$PACKAGE_MANAGER"
	install_libtiff "$PACKAGE_MANAGER"

	# Any call to cargo will make rustup install the correct toolchain
	cargo version
	cargo install cargo-quickinstall
	cargo quickinstall cargo-binstall
	cargo binstall -y sccache
	cargo binstall -y cargo-zigbuild
	cargo binstall -y cargo-nextest

fi

if [[ "$INSTALL_CHECK_TOOLS" == "true" ]]; then
	if [[ -f scripts/setup/rust-tools.txt ]]; then
		while read -r tool; do
			# Use cargo install to prevent downloading the tools with incompatible GLIBC
			cargo install "$tool"
		done <scripts/setup/rust-tools.txt
	fi

	if [[ "$PACKAGE_MANAGER" == "apk" ]]; then
		# needed by lcov
		echo http://nl.alpinelinux.org/alpine/edge/testing >>/etc/apk/repositories
	fi
	install_pkg lcov "$PACKAGE_MANAGER"
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
	python3 -m pip install --quiet boto3 "moto[all]" black shfmt-py toml yamllint
	# drivers
	python3 -m pip install --quiet pymysql sqlalchemy clickhouse_driver
	# sqllogic dependencies
	python3 -m pip install --quiet mysql-connector-python==8.0.30
fi

if [[ "$INSTALL_CODEGEN" == "true" ]]; then
	install_pkg clang "$PACKAGE_MANAGER"
	install_pkg llvm "$PACKAGE_MANAGER"
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
