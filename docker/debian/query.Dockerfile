FROM debian:bullseye
ARG TARGETPLATFORM
ENV TERM=dumb
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update
RUN apt-get install -y apt-transport-https ca-certificates
COPY ./distro/$TARGETPLATFORM/databend-query /databend-query
ENTRYPOINT ["/databend-query"]
