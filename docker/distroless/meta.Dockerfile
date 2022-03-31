FROM gcr.io/distroless/cc
ARG TARGETPLATFORM
COPY ./distro/$TARGETPLATFORM/databend-meta /databend-meta
ENTRYPOINT ["/databend-meta"]
