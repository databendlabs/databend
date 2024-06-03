FROM python:3.12-slim-bookworm

ARG TARGETPLATFORM
ENV TERM=dumb
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && \
    apt-get install -y apt-transport-https ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /var/cache/apt/*
COPY ./distro/$TARGETPLATFORM/databend-query /databend-query
ENTRYPOINT ["/databend-query"]
