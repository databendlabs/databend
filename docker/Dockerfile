FROM rust:1.48.0-buster AS builder
RUN rustup toolchain install nightly; \
rustup component add rustfmt --toolchain nightly;

COPY ./ /app
WORKDIR /app
RUN make build

FROM debian:buster
COPY --from=builder /app/target/release/fuse-query /fuse-query
ENTRYPOINT ["/fuse-query"]
