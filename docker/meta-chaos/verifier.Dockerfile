FROM debian:bookworm

ARG TARGETPLATFORM
ENV TERM=dumb
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && \
    apt-get install -y apt-transport-https ca-certificates curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /var/cache/apt/*

COPY ./distro/$TARGETPLATFORM/databend-metaverifier /usr/bin/databend-metaverifier
COPY ./distro/$TARGETPLATFORM/start-verifier.sh /start-verifier.sh
COPY ./distro/$TARGETPLATFORM/cat-logs.sh /cat-logs.sh

ENTRYPOINT ["/start-verifier.sh"]
