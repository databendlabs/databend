FROM gcr.io/distroless/base
ARG TARGETPLATFORM
COPY ./distro/$TARGETPLATFORM/databend-meta /databend-meta
ENTRYPOINT ["/databend-meta"]
