#!/bin/bash
# Copyright 2020-2025 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

# Fails if OpenSSL or aws-lc sneaks back into the dependency graph.
#
# Both are C builds that sit at the very start of the clean-build critical path: everything
# that needs `reqwest` (opendal, databend-common-exception, ...) waits for them, which costs
# ~75s of build time. Cargo features are additive, so one dependency that asks for reqwest's
# default TLS backend (`native-tls`) or for the `aws-lc` rustls provider brings them back for
# the whole graph. See https://github.com/databendlabs/databend/pull/20745 for the details.

set -e

cd "$(dirname "$0")/../.." || exit

banned=(openssl openssl-sys native-tls native-tls-crate hyper-tls aws-lc-sys aws-lc-rs)

found=()
for crate in "${banned[@]}"; do
	if grep -qx "name = \"$crate\"" Cargo.lock; then
		found+=("$crate")
	fi
done

if [[ ${#found[@]} -ne 0 ]]; then
	echo "Found banned crates in Cargo.lock: ${found[*]}" >&2
	echo >&2
	echo "These pull in a C build of OpenSSL or aws-lc, on the critical path of every build." >&2
	echo "To find the dependent crate:" >&2
	echo "  cargo tree --workspace --target x86_64-unknown-linux-gnu -e normal,build,features -i openssl-sys" >&2
	echo >&2
	echo "Fix it by asking the dependency for a rustls backend instead, e.g." >&2
	echo '  reqwest = { version = "0.12", default-features = false, features = ["rustls-tls-native-roots"] }' >&2
	echo "and, if it is a third-party crate that does not allow this, send it upstream or patch it." >&2
	exit 1
fi

echo "No OpenSSL or aws-lc in Cargo.lock"
