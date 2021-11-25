FROM debian:bullseye
ARG TARGETPLATFORM
RUN apt-get update
RUN apt-get install -y apt-transport-https ca-certificates
COPY ./distro/$TARGETPLATFORM/databend-query /databend-query
ENTRYPOINT ["bash"]