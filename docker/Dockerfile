FROM rust:1.48.0-buster AS builder

RUN cargo install cargo-build-deps
RUN rustup toolchain install nightly

COPY Makefile /tmp/fusequery
COPY Cargo.toml /tmp/fusequery
COPY src /tmp/fusequery
WORKDIR /tmp/fusequery
RUN make run