FROM rust:1.48.0-buster AS builder

RUN cargo install cargo-build-deps
RUN rustup toolchain install nightly

WORKDIR /app
COPY Makefile /app
COPY Cargo.toml /app
RUN cargo fetch

COPY src/ /app/src
WORKDIR /app
RUN make build