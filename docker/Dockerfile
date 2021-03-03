FROM rust:1.48.0-buster AS builder
RUN rustup toolchain install stable; \
rustup component add rustfmt;

COPY ./ /app
WORKDIR /app
RUN make build

FROM debian:buster
COPY --from=builder /app/target/release/fuse-query /fuse-query
ENTRYPOINT ["/fuse-query"]
