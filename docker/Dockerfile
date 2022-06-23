FROM ubuntu:22.04

RUN apt-get update -y && \
    apt-get install -y apt-transport-https ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /var/cache/apt/*

COPY ./target/release/databend-query /databend-query
COPY ./target/release/databend-meta /databend-meta
COPY ./docker/databend-query-docker.toml /databend-query.toml
COPY ./docker/bootstrap.sh /bootstrap.sh
ENTRYPOINT ["/bootstrap.sh"]
