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

# OSS Scanner build for Databend (https://github.com/databendlabs/databend).
#
# The scanner builds this image with the databend checkout as the build context and network access,
# then audits the finished image with NO network (2 vCPU / 8 GB RAM). Everything the build or the tests
# need must be fetched here: the pinned nightly toolchain, every crate in Cargo.lock (dev-dependencies
# and git sources included), protoc, and the Python packages the stateless test runner imports.
#
# Build budget is 45 minutes on 16 vCPU / 64 GB, so this follows scripts/build/build-debug.sh (the
# binaries CI actually runs) instead of `cargo build --workspace --all-targets`.

FROM debian:bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

# clang + mold: .cargo/config.toml sets linker = "clang" and -fuse-ld=mold on Linux.
# protobuf-compiler/libprotobuf-dev: prost/tonic build scripts (meta protos, flight).
# libssl-dev/pkg-config: openssl-sys. cmake: a few -sys crates. python3: scripts/ci/wait_tcp.py and
# tests/databend-test. default-mysql-client: hand-driven checks over the MySQL handler.
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl git build-essential clang lld llvm cmake pkg-config \
      libssl-dev protobuf-compiler libprotobuf-dev unzip \
      python3 python3-pip python3-venv default-mysql-client jq procps psmisc \
    && rm -rf /var/lib/apt/lists/*

# mold is not in bookworm's main repo at a usable version; take the upstream static release.
RUN set -e; \
    MOLD_VERSION=2.40.4; \
    curl -fsSLo /tmp/mold.tar.gz "https://github.com/rui314/mold/releases/download/v${MOLD_VERSION}/mold-${MOLD_VERSION}-$(uname -m)-linux.tar.gz" \
    && tar --strip-components=1 -xzf /tmp/mold.tar.gz -C /usr/local && rm -f /tmp/mold.tar.gz \
    && mold --version

# rustup only; the exact nightly comes from rust-toolchain.toml. Copy just that file first so the
# toolchain layer is cached across source changes.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --no-modify-path --profile minimal --default-toolchain none
COPY rust-toolchain.toml /src/rust-toolchain.toml
WORKDIR /src
RUN rustup toolchain install && rustup show active-toolchain

# cargo-nextest: what scripts/ci/ci-run-unit-tests.sh uses.
RUN set -e; \
    case "$(uname -m)" in x86_64) flavour=linux ;; aarch64) flavour=linux-arm ;; *) flavour= ;; esac; \
    if [ -n "$flavour" ] && curl -fsSL "https://get.nexte.st/latest/$flavour" | tar zxf - -C /usr/local/cargo/bin; then \
      cargo nextest --version; \
    else \
      cargo install cargo-nextest --locked; \
    fi

# Python deps for tests/databend-test and ad-hoc HTTP/MySQL checks.
RUN pip3 install --no-cache-dir --break-system-packages \
      pyyaml requests mysql-connector-python databend-driver rich

# Scanner contract: full checkout (with .git) at /src.
COPY . /src

# Keep the repo's own rustflags (clang + mold + thin LTO link args) from .cargo/config.toml.
# CARGO_INCREMENTAL=0 keeps target/ smaller than the 120 GB disk and makes the build reproducible.
ENV CARGO_INCREMENTAL=0 \
    CARGO_NET_GIT_FETCH_WITH_CLI=true \
    CARGO_NET_RETRY=10 \
    RUST_BACKTRACE=1

# Every crate in Cargo.lock (registry + the many git deps), dev-dependencies included.
RUN cargo fetch --locked

# Same binaries as scripts/build/build-debug.sh (default features: simd, script-udf, task-support,
# storage-stage). Debug profile keeps overflow-checks and debug info for the auditor.
RUN cargo build --locked \
      --bin databend-query --bin databend-meta --bin databend-metactl \
      --bin databend-sqllogictests \
    && ./target/debug/databend-query --version \
    && ./target/debug/databend-meta --version

# Pre-compile test binaries for the crates the threat model points at, so `cargo nextest run -p ...`
# works offline without a long compile on the 8 GB audit VM. Workspace-wide --no-run would blow the
# time and disk budget; add crates here as needed.
RUN cargo test --no-run --locked \
      -p databend-common-users \
      -p databend-common-formats \
      -p databend-common-io \
      -p databend-common-column \
      -p databend-common-hashtable \
      -p databend-common-ast \
      -p databend-common-sql \
      -p databend-common-storages-stage \
      -p databend-common-storages-parquet \
    || echo "WARNING: some test binaries failed to pre-compile; cargo will build them on demand"

# Smoke test: start a standalone meta + query on local fs storage and run the smallest sqllogic suite.
# Reports but never aborts the build.
RUN ( set -e; \
      bash ./scripts/ci/deploy/databend-query-standalone.sh; \
      ./target/debug/databend-sqllogictests --handlers http --run_dir base/00_dummy --enable_sandbox; \
      killall databend-query databend-meta || true; \
      rm -rf .databend ) \
    || echo "WARNING: standalone smoke test failed; see threat_model.md for how to start the server by hand"

# The audit VM has 2 vCPUs / 8 GB and no network. Relinking databend-query after a patch is the
# expensive step; cap parallelism so it does not OOM, and tell cargo not to wait on sockets.
ENV CARGO_NET_OFFLINE=true \
    CARGO_BUILD_JOBS=2 \
    RUST_TEST_THREADS=2 \
    STORAGE_ALLOW_INSECURE=true

CMD ["/bin/bash"]
