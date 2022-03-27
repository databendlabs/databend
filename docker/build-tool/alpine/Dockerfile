FROM alpine:3.15

ENV PATH=/root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

ENV LANGUAGE en_US
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

COPY rust-toolchain.toml /build/rust-toolchain.toml
COPY scripts/setup/rust-tools.txt /build/scripts/setup/rust-tools.txt
COPY scripts/setup/dev_setup.sh /build/scripts/setup/dev_setup.sh
RUN apk --update add --no-cache bash && \
    chmod +x /build/scripts/setup/dev_setup.sh && \
    /build/scripts/setup/dev_setup.sh && \
    rm -rf /root/.cache/* && \
    rm -rf /root/.cargo/registry/*

ENV OPENSSL_STATIC=true
ENV OPENSSL_LIB_DIR=/usr/lib
ENV OPENSSL_INCLUDE_DIR=/usr/include
