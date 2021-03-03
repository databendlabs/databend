FROM rust:1.48.0-buster AS builder

COPY ./ /app
WORKDIR /app
RUN make setup
RUN make build

FROM debian:buster
COPY --from=builder /app/target/release/fuse-query /fuse-query
ENTRYPOINT ["/fuse-query"]
