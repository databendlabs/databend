#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

if [ -z "${ICEBERG_VERSION}" ]; then
	echo "ICEBERG_VERSION is unset"
	exit 1
else
	echo "var is set to '$ICEBERG_VERSION'"
fi

# Validate version format (e.g., 1.0.0)
if [[ ! "$ICEBERG_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
	echo "Error: ICEBERG_VERSION ($ICEBERG_VERSION) must be in the format: <number>.<number>.<number>"
	exit 1
fi

# tar source code
release_version=${ICEBERG_VERSION}
# rc versions
rc_version="${ICEBERG_VERSION_RC:-rc.1}"
# Corresponding git repository branch
git_branch=release-${release_version}-${rc_version}

rm -rf dist
mkdir -p dist/

echo "> Checkout version branch"
git checkout -B "${git_branch}"

# Run a few checks
echo "> Check license"
docker run -it --rm -v $(pwd):/github/workspace apache/skywalking-eyes header check

echo "> Run dependency license check using cargo-deny"
python3 ./scripts/dependencies.py check

# Generate and verify artifacts
echo "> Start package"
git archive --format=tar.gz --output="dist/apache-iceberg-rust-$release_version.tar.gz" --prefix="apache-iceberg-rust-$release_version/" --add-file=Cargo.toml "$git_branch"

cd dist

echo "> Generate signature"
for i in *.tar.gz; do
	echo "$i"
	gpg --armor --output "$i.asc" --detach-sig "$i"
done
echo "> Check signature"
for i in *.tar.gz; do
	echo "$i"
	gpg --verify "$i.asc" "$i"
done
echo "> Generate sha512sum"
for i in *.tar.gz; do
	echo "$i"
	sha512sum "$i" >"$i.sha512"
done
echo "> Check sha512sum"
for i in *.tar.gz; do
	echo "$i"
	sha512sum --check "$i.sha512"
done
