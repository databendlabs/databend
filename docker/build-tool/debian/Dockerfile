FROM debian:bullseye as build-base

ENV TERM=dumb
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -yq && \
    apt-get install -yq locales sudo && \
    printf 'en_US.UTF-8 UTF-8\n' > /etc/locale.gen && \
    locale-gen && \
    rm -rf /var/lib/apt/lists/*

ENV LANGUAGE en_US
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

RUN groupdel dialout && groupdel staff && \
    groupadd -g 20 staff && groupadd -g 121 docker && \
    useradd -u 1000 -g 100 builder && \
    useradd -u 1001 -g 121 runner && \
    useradd -u 501 -g 20 macos && \
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

ENV RUSTUP_HOME /opt/rust/rustup
ENV CARGO_HOME /opt/rust/cargo
ENV PATH /opt/rust/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
VOLUME [ "/workspace", "/opt/rust/cargo/registry", "/opt/rust/cargo/git" ]


FROM build-base as multiarch
ENV PKG_CONFIG_ALLOW_CROSS 1
COPY scripts/setup/cross_setup.sh /build/scripts/setup/cross_setup.sh
RUN chmod +x /build/scripts/setup/cross_setup.sh && /build/scripts/setup/cross_setup.sh


FROM build-base as dev
COPY scripts/setup/rust-tools.txt /build/scripts/setup/rust-tools.txt
RUN /build/scripts/setup/dev_setup.sh -yd && \
    rm -rf /opt/rust/cargo/git && \
    rm -rf /opt/rust/cargo/registry && \
    rm -rf /root/.cache/*
