FROM debian:bookworm

ARG TARGETPLATFORM

RUN apt-get update -y && \
    apt-get install -y apt-transport-https ca-certificates curl xxd dumb-init && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /var/cache/apt/*

COPY ./distro/$TARGETPLATFORM/databend-meta /usr/bin/databend-meta
COPY ./distro/$TARGETPLATFORM/databend-query /usr/bin/databend-query
COPY ./docker/query-config.toml /etc/databend/query_config_spec.toml
COPY ./docker/bootstrap.sh /bootstrap.sh
ENTRYPOINT [ "dumb-init", "--", "/bootstrap.sh"]
VOLUME [ "/var/log/databend", "/etc/databend", "/var/lib/databend", "/var/lib/minio" ]
EXPOSE 3307 8124 8000 8900
