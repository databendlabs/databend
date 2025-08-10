FROM debian:bookworm

ARG TARGETPLATFORM
ENV TERM=dumb
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && \
    apt-get install -y apt-transport-https ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /var/cache/apt/*
COPY ./distro/$TARGETPLATFORM/databend-meta /usr/bin/databend-meta
COPY ./distro/$TARGETPLATFORM/databend-metactl /usr/bin/databend-metactl
COPY ./distro/$TARGETPLATFORM/cat-logs.sh /cat-logs.sh
ENTRYPOINT ["databend-meta"]
