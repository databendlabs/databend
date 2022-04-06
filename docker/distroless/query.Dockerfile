FROM debian:bullseye as builder
RUN cp -L /lib/$(uname -m)-linux-gnu/libz.so.1 /lib/libz.so.1

FROM gcr.io/distroless/cc
ARG TARGETPLATFORM
COPY --from=builder /lib/libz.so.1 /lib/libz.so.1
COPY ./distro/$TARGETPLATFORM/databend-query /databend-query
ENTRYPOINT ["/databend-query"]
