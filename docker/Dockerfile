FROM rust:1.48.0-buster AS builder

COPY ./ /app
WORKDIR /app
RUN make setup
RUN make build

FROM debian:buster
COPY --from=builder /app/target/release/fuse-query /fuse-query
COPY --from=builder /app/target/release/fuse-store /fuse-store
COPY --from=builder /app/scripts/deploy/config/fusequery-node-1.toml /fuse-query.toml
COPY --from=builder /app/docker/bootstrap.sh /bootstrap.sh
ENTRYPOINT ["/bootstrap.sh"]
