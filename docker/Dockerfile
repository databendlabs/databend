FROM ubuntu:22.04

ARG TARGETPLATFORM

RUN apt-get update -y && \
    apt-get install -y apt-transport-https ca-certificates curl xxd dumb-init && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /var/cache/apt/*

COPY ./distro/$TARGETPLATFORM/databend-meta /databend-meta
COPY ./distro/$TARGETPLATFORM/databend-query /databend-query
COPY ./docker/databend-query-docker.toml /databend-query.toml
COPY ./docker/bootstrap.sh /bootstrap.sh
ENTRYPOINT [ "dumb-init", "--", "/bootstrap.sh"]
