FROM debian:bullseye

ENV PATH=/root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -yq && \
    apt-get install -yq locales && \
    printf 'en_US.UTF-8 UTF-8\n' > /etc/locale.gen && \
    locale-gen && apt-get remove -yq locales && apt-get autoremove -yq && \
    rm -rf /var/lib/apt/lists/*

ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

COPY rust-toolchain.toml /rust-toolchain.toml
COPY rust-tools.txt /rust-tools.txt
COPY scripts/setup/dev_setup.sh /build_setup.sh
RUN chmod +x /build_setup.sh && \
    /build_setup.sh && \
    rm -rf /root/.cache/* && \
    rm -rf /root/.cargo/registry/*
