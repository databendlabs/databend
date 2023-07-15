#!/bin/bash

# Copyright 2021 Datafuse Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

VERSION=$1

if [ -z "$VERSION" ]; then
  echo "Usage: bump.sh <version>"
  exit 1
fi

echo "Bumping version to $VERSION"

if [[ "$OSTYPE" == "darwin"* ]]; then
  sed -i '' "s/version = \".*\"/version = \"$VERSION\"/g" Cargo.toml
  sed -i '' "s/\"version\": \".*\"/\"version\": \"$VERSION\"/g" bindings/nodejs/package.json
  sed -i '' "s/\"version\": \".*\"/\"version\": \"$VERSION\"/g" bindings/nodejs/npm/*/package.json
else
  sed -i "s/version = \".*\"/version = \"$VERSION\"/g" Cargo.toml
  sed -i "s/\"version\": \".*\"/\"version\": \"$VERSION\"/g" bindings/nodejs/package.json
  sed -i "s/\"version\": \".*\"/\"version\": \"$VERSION\"/g" bindings/nodejs/npm/*/package.json
fi

git status
