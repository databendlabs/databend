FROM ubuntu:22.04

COPY ./target/release/databend-query /databend-query
COPY ./target/release/databend-meta /databend-meta
COPY ./docker/databend-query-docker.toml /databend-query.toml
COPY ./docker/bootstrap.sh /bootstrap.sh
ENTRYPOINT ["/bootstrap.sh"]
