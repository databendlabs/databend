FROM gcr.io/distroless/cc
ARG TARGETPLATFORM
COPY ./distro/$TARGETPLATFORM/databend-meta /databend-meta
COPY ./distro/$TARGETPLATFORM/databend-metactl /databend-metactl
ENTRYPOINT ["/databend-meta"]
