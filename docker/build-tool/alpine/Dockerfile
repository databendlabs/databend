FROM alpine:3.15

ENV PATH=/root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

COPY rust-toolchain.toml /rust-toolchain.toml
COPY rust-tools.txt /rust-tools.txt
COPY scripts/setup/dev_setup.sh /build_setup.sh
RUN apk --update add --no-cache bash && \
    chmod +x /build_setup.sh && \
    /build_setup.sh && \
    rm -rf /root/.cache/* && \
    rm -rf /root/.cargo/registry/*

ENV OPENSSL_STATIC=true
ENV OPENSSL_LIB_DIR=/usr/lib
ENV OPENSSL_INCLUDE_DIR=/usr/include
