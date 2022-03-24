FROM gcr.io/distroless/base
ARG TARGETPLATFORM
COPY ./distro/$TARGETPLATFORM/databend-query /databend-query
ENTRYPOINT ["/databend-query"]
