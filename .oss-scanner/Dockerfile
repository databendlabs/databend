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

# OSS Scanner image for Databend. Built online with the repository as context, then audited offline
# (2 vCPU / 8 GB, no network), so the toolchain, every crate in Cargo.lock and the build artifacts
# all stay in the image. Build budget is 45 minutes on 16 vCPU.

FROM rust:bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates clang cmake git lld libssl-dev pkg-config protobuf-compiler libprotobuf-dev \
      python3 python3-requests default-mysql-client jq procps psmisc \
    && rm -rf /var/lib/apt/lists/*

COPY . /src
WORKDIR /src

# rust-toolchain.toml pins the nightly and components the workspace needs.
RUN rustup toolchain install && rustup show active-toolchain

# Debug profile keeps overflow-checks and debug-assertions, which is what makes crafted-input bugs
# visible. Line-table debug info keeps file:line in panics and gdb while keeping target/ small enough
# to ship in the image. RUSTFLAGS overrides .cargo/config.toml's mold/thin-LTO link args so the image
# only needs lld; the linker is still clang from that config.
ENV CARGO_INCREMENTAL=0 \
    CARGO_PROFILE_DEV_DEBUG=line-tables-only \
    CARGO_PROFILE_DEV_SPLIT_DEBUGINFO=off \
    CARGO_PROFILE_TEST_DEBUG=line-tables-only \
    CARGO_PROFILE_TEST_SPLIT_DEBUGINFO=off \
    CARGO_NET_GIT_FETCH_WITH_CLI=true \
    RUSTFLAGS="-C force-frame-pointers=yes -C link-arg=-fuse-ld=lld" \
    RUST_BACKTRACE=1

RUN cargo fetch --locked

ENV CARGO_NET_OFFLINE=true

# The same binaries CI builds (scripts/build/build-debug.sh), with default features, plus library
# tests for the small crates on the authentication and input-parsing paths so they are compiled and
# known to pass before the offline audit starts.
RUN cargo build --frozen \
      --bin databend-query --bin databend-meta --bin databend-metactl --bin databend-sqllogictests \
    && cargo test --frozen --lib \
      -p databend-common-users -p databend-common-ast -p databend-common-io \
      -p databend-common-formats -p databend-common-column -p databend-common-hashtable

# Bring up meta + query on local fs storage once and run the smallest SQL logic suite, so the
# standalone path described in threat_model.md is known to work in this image.
RUN bash ./scripts/ci/deploy/databend-query-standalone.sh \
    && ./target/debug/databend-sqllogictests --handlers http --run_dir 00_dummy --enable_sandbox \
    && (killall databend-query databend-meta || true) \
    && rm -rf .databend

# Audit VM is 2 vCPU / 8 GB: relinking databend-query after a patch must not OOM.
ENV CARGO_BUILD_JOBS=2 \
    RUST_TEST_THREADS=2 \
    STORAGE_ALLOW_INSECURE=true

CMD ["bash"]
