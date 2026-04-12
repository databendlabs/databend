#!/usr/bin/env bash

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

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
DIST_DIR="${DIST_DIR:-$ROOT_DIR/dist}"
REPOSITORY_URL="${REPOSITORY_URL:-https://upload.pypi.org/legacy/}"
SKIP_BUILD=0
SKIP_UPLOAD=0
CLEAN_DIST=0
VERSION=""
TARGETS=()

usage() {
    cat <<'EOF'
Build and optionally upload bendpy from the local host without Docker.

Usage:
  ./scripts/local_publish.sh [options]

Options:
  --python <path>          Python interpreter to use. Default: python3
  --version <semver>       Override version in Cargo.toml and pyproject.toml for this build
  --target <triple>        Rust target triple to build. Repeatable. Default: host target only
  --dist-dir <path>        Output directory for built wheels. Default: dist
  --repository-url <url>   Twine repository URL. Default: PyPI legacy upload URL
  --skip-build             Upload existing wheels in dist dir without rebuilding
  --skip-upload            Only build wheels, do not upload
  --clean                  Remove existing databend wheel artifacts in dist dir before build
  -h, --help               Show this help

Environment:
  PYPI_TOKEN               Required for upload. Used as twine password.
  TWINE_USERNAME           Optional. Default: __token__
  TWINE_PASSWORD           Optional override for PYPI_TOKEN
  MATURIN_EXTRA_ARGS       Extra args appended to maturin build
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --python)
            PYTHON_BIN="$2"
            shift 2
            ;;
        --version)
            VERSION="$2"
            shift 2
            ;;
        --target)
            TARGETS+=("$2")
            shift 2
            ;;
        --dist-dir)
            DIST_DIR="$2"
            shift 2
            ;;
        --repository-url)
            REPOSITORY_URL="$2"
            shift 2
            ;;
        --skip-build)
            SKIP_BUILD=1
            shift
            ;;
        --skip-upload)
            SKIP_UPLOAD=1
            shift
            ;;
        --clean)
            CLEAN_DIST=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "Missing required command: $1" >&2
        exit 1
    fi
}

ensure_python_pip() {
    if "$PYTHON_BIN" -m pip --version >/dev/null 2>&1; then
        return 0
    fi

    echo "pip is not available for $PYTHON_BIN, bootstrapping with ensurepip"
    "$PYTHON_BIN" -m ensurepip --upgrade

    if ! "$PYTHON_BIN" -m pip --version >/dev/null 2>&1; then
        echo "Failed to bootstrap pip for $PYTHON_BIN" >&2
        exit 1
    fi
}

update_version_file() {
    local file="$1"
    local version="$2"
    local tmp
    tmp="$(mktemp)"
    sed "s/^version = \".*\"/version = \"$version\"/" "$file" >"$tmp"
    mv "$tmp" "$file"
}

restore_version_files() {
    if [[ -n "${CARGO_BACKUP:-}" && -f "${CARGO_BACKUP:-}" ]]; then
        mv "$CARGO_BACKUP" "$ROOT_DIR/Cargo.toml"
    fi
    if [[ -n "${PYPROJECT_BACKUP:-}" && -f "${PYPROJECT_BACKUP:-}" ]]; then
        mv "$PYPROJECT_BACKUP" "$ROOT_DIR/pyproject.toml"
    fi
}

ensure_rust_target() {
    local target="$1"
    if rustup target list --installed | grep -Fx "$target" >/dev/null 2>&1; then
        return 0
    fi

    echo "Installing Rust target: $target"
    rustup target add "$target"
}

require_command "$PYTHON_BIN"
require_command rustup
ensure_python_pip

cd "$ROOT_DIR"
mkdir -p "$DIST_DIR"

if [[ "$CLEAN_DIST" -eq 1 ]]; then
    echo "Cleaning existing wheel artifacts in $DIST_DIR"
    find "$DIST_DIR" -maxdepth 1 \( -name 'databend-*.whl' -o -name 'databend-*.tar.gz' \) -print -delete
fi

if [[ -n "$VERSION" ]]; then
    CARGO_BACKUP="$(mktemp)"
    PYPROJECT_BACKUP="$(mktemp)"
    cp Cargo.toml "$CARGO_BACKUP"
    cp pyproject.toml "$PYPROJECT_BACKUP"
    trap restore_version_files EXIT
    update_version_file Cargo.toml "$VERSION"
    update_version_file pyproject.toml "$VERSION"
    echo "Using overridden version: $VERSION"
fi

if [[ "$SKIP_BUILD" -eq 0 ]]; then
    "$PYTHON_BIN" -m pip install --upgrade pip
    "$PYTHON_BIN" -m pip install maturin twine

    if [[ "${#TARGETS[@]}" -eq 0 ]]; then
        echo "Building bendpy wheel with $PYTHON_BIN for host target"
        # Local host build only. This does not attempt a manylinux or Docker-based build.
        "$PYTHON_BIN" -m maturin build --release --strip --out "$DIST_DIR" ${MATURIN_EXTRA_ARGS:-}
    else
        for target in "${TARGETS[@]}"; do
            ensure_rust_target "$target"
            echo "Building bendpy wheel with $PYTHON_BIN for target $target"
            "$PYTHON_BIN" -m maturin build --release --strip --target "$target" --out "$DIST_DIR" ${MATURIN_EXTRA_ARGS:-}
        done
    fi
fi

if ! find "$DIST_DIR" -maxdepth 1 -name '*.whl' | grep -q .; then
    echo "No wheel files found in $DIST_DIR" >&2
    exit 1
fi

ls -lh "$DIST_DIR"

if [[ "$SKIP_UPLOAD" -eq 1 ]]; then
    echo "Skipping upload"
    exit 0
fi

TWINE_USERNAME="${TWINE_USERNAME:-__token__}"
TWINE_PASSWORD="${TWINE_PASSWORD:-${PYPI_TOKEN:-}}"

if [[ -z "$TWINE_PASSWORD" ]]; then
    echo "Missing PYPI_TOKEN or TWINE_PASSWORD for upload" >&2
    exit 1
fi

echo "Uploading wheel(s) to $REPOSITORY_URL"
"$PYTHON_BIN" -m twine upload \
    --skip-existing \
    --repository-url "$REPOSITORY_URL" \
    -u "$TWINE_USERNAME" \
    -p "$TWINE_PASSWORD" \
    "$DIST_DIR"/*.whl
