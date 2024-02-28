FROM alpine:3.19

ENV PATH=/root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

ENV LANGUAGE en_US
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

RUN apk --update add --no-cache bash sudo

COPY rust-toolchain.toml /build/rust-toolchain.toml
COPY dev_setup.sh /build/scripts/setup/dev_setup.sh
RUN chmod +x /build/scripts/setup/dev_setup.sh && \
    /build/scripts/setup/dev_setup.sh -yb && \
    rm -rf /root/.cargo/git && \
    rm -rf /root/.cargo/registry && \
    mkdir -p /opt/rust && \
    mv /root/.cargo /opt/rust/cargo && \
    mv /root/.rustup /opt/rust/rustup

RUN curl -sSfLo /tmp/mold.tar.gz https://github.com/rui314/mold/releases/download/v2.4.0/mold-2.4.0-$(uname -m)-linux.tar.gz && \
    tar --strip-components=1 -xzf /tmp/mold.tar.gz -C /usr/local && \
    rm -rf /tmp/mold.tar.gz && \
    ln -sf /usr/local/bin/mold /usr/bin/$(uname -m)-linux-gnu-ld

ENV RUSTUP_HOME /opt/rust/rustup
ENV CARGO_HOME /opt/rust/cargo
ENV PATH /opt/rust/cargo/bin:/opt/java/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

VOLUME [ "/workspace", "/opt/rust/cargo/registry", "/opt/rust/cargo/git" ]
