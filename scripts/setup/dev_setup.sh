#!/bin/bash
# Copyright (c) The Diem Core Contributors.
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../.." || exit

function add_to_profile {
	eval "$1"
	FOUND=$(grep -c "$1" <"${HOME}/.profile")
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
	PRE_COMMAND=()
	if [ "$(whoami)" != 'root' ]; then
		PRE_COMMAND=(sudo)
	fi
	if which "$package" &>/dev/null; then
		echo "$package is already installed"
	else
		echo "Installing ${package}."
		case "$PACKAGE_MANAGER" in
		apt-get)
			"${PRE_COMMAND[@]}" apt-get install --no-install-recommends -yq "${package}"
			;;
		yum)
			"${PRE_COMMAND[@]}" yum install -yq "${package}"
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

	# Differently packages for build essentials
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

	echo "==> installing protobuf compiler..."

	case "$PACKAGE_MANAGER" in
	apt-get)
		install_pkg protobuf-compiler "$PACKAGE_MANAGER"
		;;
	pacman)
		install_pkg protoc "$PACKAGE_MANAGER"
		;;
	apk)
		install_pkg protoc "$PACKAGE_MANAGER"
		;;
	yum)
		install_pkg protobuf "$PACKAGE_MANAGER"
		;;
	dnf)
		install_pkg protobuf-compiler "$PACKAGE_MANAGER"
		;;
	brew)
		install_pkg protobuf "$PACKAGE_MANAGER"
		;;
	*)
		echo "Unable to install protobuf with package manager: $PACKAGE_MANAGER"
		exit 1
		;;
	esac
}

function install_pkg_config {
	PACKAGE_MANAGER=$1

	echo "==> installing pkg-config..."

	# Differently named packages for pkg-configs
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
}

function install_cargo_binary {
	BIN_NAME=$1
	VERSION=$2
	if cargo install --list | grep "${BIN_NAME}" &>/dev/null; then
		echo "${BIN_NAME} is already installed"
	else
		if [ -z "$VERSION" ]; then
			cargo install "${BIN_NAME}"
		else
			cargo install --version "${VERSION}" "${BIN_NAME}"
		fi
	fi
}

function install_toolchain {
	version=$1
	echo "==> Installing ${version} of rust toolchain..."
	rustup install "$version"
	rustup set profile minimal
	rustup component add rustfmt --toolchain "$version"
	rustup component add rust-src --toolchain "$version"
	rustup component add clippy --toolchain "$version"
	rustup component add miri --toolchain "$version"
	rustup default "$version"
}

function usage {
	cat <<EOF
    usage: $0 [options]"

    options:
        -b Enable BATCH_MODE for installation
        -t Install build tools
        -o Install some operation tools
        -p Install profile
        -v Verbose mode
        -y Install prover
        -s Install codegen tools
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
Build tools (since -t or no option was provided):
  * Rust (and the necessary components, e.g. rust-fmt, clippy)
  * lcov
  * pkg-config
  * libssl-dev
  * if linux, gcc-powerpc-linux-gnu
EOF
	fi

	if [[ "$OPERATIONS" == "true" ]]; then
		cat <<EOF
Operation tools (since -o was provided):
  * python3
  * docker
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

	cat <<EOF
If you'd prefer to install these dependencies yourself, please exit this script
now with Ctrl-C.
EOF
}

BATCH_MODE=true
VERBOSE=false
INSTALL_BUILD_TOOLS=false
OPERATIONS=false
INSTALL_PROFILE=false
INSTALL_PROVER=false
INSTALL_CODEGEN=false

#parse args
while getopts "bmtopvysh" arg; do
	case "$arg" in
	b)
		BATCH_MODE="true"
		;;
	t)
		INSTALL_BUILD_TOOLS="true"
		;;
	o)
		OPERATIONS="true"
		;;
	p)
		INSTALL_PROFILE="true"
		;;
	v)
		VERBOSE=true
		;;
	y)
		INSTALL_PROVER="true"
		;;
	s)
		INSTALL_CODEGEN="true"
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
	[[ "$OPERATIONS" == "false" ]] &&
	[[ "$INSTALL_PROFILE" == "false" ]] &&
	[[ "$INSTALL_PROVER" == "false" ]] &&
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

# NOTE: never use sudo under macos
PRE_COMMAND=()
if [[ "$(whoami)" != 'root' ]] && [[ ${PACKAGE_MANAGER} != "brew" ]]; then
	PRE_COMMAND=(sudo)
fi

if [[ "$BATCH_MODE" == "false" ]]; then
	welcome_message
	printf "Proceed with installing necessary dependencies? (y/N) > "
	read -e -r input
	if [[ "$input" != "y"* ]]; then
		echo "Exiting..."
		exit 0
	fi
fi

if [[ "$PACKAGE_MANAGER" == "apt-get" ]]; then
	[[ "$BATCH_MODE" == "false" ]] && echo "Updating apt-get......"
	"${PRE_COMMAND[@]}" apt-get update
	[[ "$BATCH_MODE" == "false" ]] && echo "Installing ca-certificates......"
	install_pkg ca-certificates "$PACKAGE_MANAGER"
fi

[[ "$INSTALL_PROFILE" == "true" ]] && update_path_and_profile

install_pkg curl "$PACKAGE_MANAGER"

if [[ "$INSTALL_BUILD_TOOLS" == "true" ]]; then
	install_rustup "$RUST_TOOLCHAIN"

	install_build_essentials "$PACKAGE_MANAGER"
	install_pkg_config "$PACKAGE_MANAGER"
	install_openssl "$PACKAGE_MANAGER"
	install_protobuf "$PACKAGE_MANAGER"

	install_pkg cmake "$PACKAGE_MANAGER"
	install_pkg clang "$PACKAGE_MANAGER"
	install_pkg llvm "$PACKAGE_MANAGER"
	install_pkg python3 "$PACKAGE_MANAGER"
	if [[ "$PACKAGE_MANAGER" == "apt-get" ]]; then
		install_pkg python3-all-dev "$PACKAGE_MANAGER"
		install_pkg python3-setuptools "$PACKAGE_MANAGER"
		install_pkg python3-pip "$PACKAGE_MANAGER"
	elif [[ "$PACKAGE_MANAGER" == "apk" ]]; then
		# no wheel package for alpine
		install_pkg python3-dev "$PACKAGE_MANAGER"
		install_pkg py3-pip "$PACKAGE_MANAGER"
		install_pkg libffi-dev "$PACKAGE_MANAGER"
	fi
	python3 -m pip install --quiet boto3 "moto[all]" yapf shfmt-py mysql-connector-python toml

	install_toolchain "$RUST_TOOLCHAIN"

	if [ -f rust-tools.txt ]; then
		if [[ "$PACKAGE_MANAGER" == "brew" ]]; then
			# macos with bash 3.2 has no array support
			echo "Please manually install rust tools in rust-tools.txt if needed."
		else
			declare -A RUST_TOOLS
			while IFS='@' read -r key value; do
				RUST_TOOLS[$key]=$value
			done <rust-tools.txt

			for tool in "${!RUST_TOOLS[@]}"; do
				install_cargo_binary "$tool" "${RUST_TOOLS[$tool]}"
			done
		fi
	fi

	if [[ "$PACKAGE_MANAGER" == "apk" ]]; then
		# needed by lcov
		echo http://nl.alpinelinux.org/alpine/edge/testing >>/etc/apk/repositories
	fi
	install_pkg lcov "$PACKAGE_MANAGER"
fi

if [[ "$OPERATIONS" == "true" ]]; then
	install_pkg python3 "$PACKAGE_MANAGER"
	install_pkg git "$PACKAGE_MANAGER"
	# for timeout
	if [[ "$PACKAGE_MANAGER" == "apt-get" ]]; then
		install_pkg coreutils "$PACKAGE_MANAGER"
	fi
	if [[ "$PACKAGE_MANAGER" == "apk" ]]; then
		install_pkg python3-dev "$PACKAGE_MANAGER"
	fi
	# for mysql client
	case "$PACKAGE_MANAGER" in
	apt-get)
		install_pkg mysql-client "$PACKAGE_MANAGER"
		;;
	pacman)
		install_pkg mysql-clients "$PACKAGE_MANAGER"
		;;
	apk)
		install_pkg mysql-client "$PACKAGE_MANAGER"
		;;
	yum)
		install_pkg mysql "$PACKAGE_MANAGER"
		;;
	dnf)
		install_pkg mysql "$PACKAGE_MANAGER"
		;;
	brew)
		install_pkg mysql "$PACKAGE_MANAGER"
		;;
	*)
		echo "Unable to install mysql client with package manager: $PACKAGE_MANAGER"
		exit 1
		;;
	esac
fi

if [[ "$INSTALL_CODEGEN" == "true" ]]; then
	install_pkg clang "$PACKAGE_MANAGER"
	install_pkg llvm "$PACKAGE_MANAGER"
	if [[ "$PACKAGE_MANAGER" == "apt-get" ]]; then
		install_pkg python3-all-dev "$PACKAGE_MANAGER"
		install_pkg python3-setuptools "$PACKAGE_MANAGER"
		install_pkg python3-pip "$PACKAGE_MANAGER"
	elif [[ "$PACKAGE_MANAGER" == "apk" ]]; then
		install_pkg python3-dev "$PACKAGE_MANAGER"
		install_pkg py3-pip "$PACKAGE_MANAGER"
	else
		install_pkg python3 "$PACKAGE_MANAGER"
	fi
	"${PRE_COMMAND[@]}" python3 -m pip install --quiet coscmd PyYAML
fi

[[ "${BATCH_MODE}" == "false" ]] && cat <<EOF
Finished installing all dependencies.

You should now be able to build the project by running:
	cargo build
EOF

exit 0
