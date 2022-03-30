FROM alpine:3.15 as build-base

ENV LANGUAGE en_US
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

RUN apk add --no-cache sudo bash && delgroup dialout && \
    addgroup -g 20 staff && addgroup -g 121 docker && \
    adduser -u 1000 -G users builder -D -H && \
    adduser -u 1001 -G docker runner -D -H && \
    adduser -u 501 -G staff macos -D -H && \
    printf "builder,runner,macos ALL=(ALL:ALL) NOPASSWD:ALL\n" > /etc/sudoers.d/databend

COPY rust-toolchain.toml /build/rust-toolchain.toml
COPY scripts/setup/dev_setup.sh /build/scripts/setup/dev_setup.sh
RUN chmod +x /build/scripts/setup/dev_setup.sh && \
    /build/scripts/setup/dev_setup.sh -yb && \
    rm -rf /root/.cargo/git && \
    rm -rf /root/.cargo/registry && \
    mkdir -p /opt/rust && \
    mv /root/.cargo /opt/rust/cargo && \
    mv /root/.rustup /opt/rust/rustup

ENV OPENSSL_STATIC=true
ENV OPENSSL_LIB_DIR=/usr/lib
ENV OPENSSL_INCLUDE_DIR=/usr/include
ENV RUSTUP_HOME /opt/rust/rustup
ENV CARGO_HOME /opt/rust/cargo
ENV PATH=/opt/rust/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
VOLUME [ "/workspace", "/opt/rust/cargo/registry", "/opt/rust/cargo/git" ]


FROM build-base as dev
COPY scripts/setup/rust-tools.txt /build/scripts/setup/rust-tools.txt
RUN /build/scripts/setup/dev_setup.sh -yd && \
    rm -rf /opt/rust/cargo/git && \
    rm -rf /opt/rust/cargo/registry && \
    rm -rf /root/.cache/*
