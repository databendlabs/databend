ARG RUST_TOOLCHAIN

FROM golang:1.18 AS go-build
RUN go install github.com/apache/skywalking-eyes/cmd/license-eye@latest

FROM datafuselabs/build-tool:base-${RUST_TOOLCHAIN}

COPY --from=go-build /go/bin/license-eye /usr/local/bin/license-eye

ARG ARCH
COPY rust-tools.txt /build/scripts/setup/rust-tools.txt
RUN /build/scripts/setup/dev_setup.sh -yd && \
    rm -rf /opt/rust/cargo/git && \
    rm -rf /opt/rust/cargo/registry && \
    rm -rf /root/.cache/*
