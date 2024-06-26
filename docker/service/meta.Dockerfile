FROM debian:bookworm

ARG TARGETPLATFORM
ENV TERM=dumb
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && \
    apt-get install -y apt-transport-https ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /var/cache/apt/*
COPY ./distro/$TARGETPLATFORM/databend-meta /databend-meta
COPY ./distro/$TARGETPLATFORM/databend-metactl /databend-metactl
RUN useradd --uid 1000 --shell /sbin/nologin \
    --home-dir /var/lib/databend --user-group \
    --comment "Databend cloud data analytics" databend && \
    mkdir -p /var/lib/databend && \
    chown -R databend:databend /var/lib/databend
ENTRYPOINT ["/databend-meta"]
