#!/bin/bash

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
