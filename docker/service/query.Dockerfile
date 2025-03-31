FROM python:3.12-slim-bookworm

ARG TARGETPLATFORM
ENV TERM=dumb
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && \
    apt-get install -y apt-transport-https ca-certificates gdb curl default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /var/cache/apt/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV LD_LIBRARY_PATH=/usr/lib/jvm/default-java/lib/server
COPY ./distro/$TARGETPLATFORM/databend-query /databend-query
RUN useradd --uid 1000 --shell /sbin/nologin \
    --home-dir /var/lib/databend --user-group \
    --comment "Databend cloud data analytics" databend && \
    mkdir -p /var/lib/databend && \
    chown -R databend:databend /var/lib/databend
ENTRYPOINT ["/databend-query"]
