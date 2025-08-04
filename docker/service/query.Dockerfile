FROM python:3.12-slim-bookworm

ARG TARGETPLATFORM
ENV TERM=dumb
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && \
    apt-get install -y apt-transport-https ca-certificates gdb curl procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /var/cache/apt/*
COPY ./distro/$TARGETPLATFORM/databend-query /usr/bin/databend-query
RUN useradd --uid 1000 --shell /sbin/nologin \
    --home-dir /var/lib/databend --user-group \
    --comment "Databend cloud data analytics" databend && \
    mkdir -p /var/lib/databend && \
    chown -R databend:databend /var/lib/databend
VOLUME ["/var/lib/databend"]
ENTRYPOINT ["/usr/bin/databend-query"]
