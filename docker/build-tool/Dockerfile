FROM debian:buster
COPY ./dev_setup.sh /build_setup.sh
COPY ./rust-toolchain.toml /rust-toolchain.toml
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && \
    apt-get install -yq \
        build-essential \
        curl \
        file \
        git \
        libssl-dev \
        pkgconf \
        sudo \
        unzip \
        xutils-dev \
        protobuf-compiler \
        lcov \
        python3 \
        coreutils \
        default-mysql-client \
        clang \
        llvm \
        python3-all-dev \
        python3-setuptools \
        python3-pip \
        psmisc \
        procps \
        locales \
        && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    useradd rust --user-group --create-home --shell /bin/bash --groups sudo
# We use the instructions at https://github.com/rust-lang/rustup/issues/2383
# to install the rustup toolchain as root.
ENV RUSTUP_HOME=/opt/rust/rustup \
    PATH=/home/rust/.cargo/bin:/opt/rust/cargo/bin:/usr/local/musl/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN curl https://sh.rustup.rs -sSf |\
    env CARGO_HOME=/opt/rust/cargo \
        sh -s -- -y --default-toolchain stable --profile minimal --no-modify-path && \
    export RUST_VERSION="$(awk -F'[ ="]+' '$1 == "channel" { print $2 }' /rust-toolchain.toml)" && \
    env CARGO_HOME=/opt/rust/cargo rustup install $RUST_VERSION && \
    env CARGO_HOME=/opt/rust/cargo rustup set profile minimal && \
    env CARGO_HOME=/opt/rust/cargo rustup component add rustfmt rust-src clippy miri --toolchain $RUST_VERSION && \
    env CARGO_HOME=/opt/rust/cargo rustup default "$RUST_VERSION"


RUN mkdir -p /home/rust/libs /home/rust/src /home/rust/.cargo && \
    ln -s /opt/rust/cargo/config /home/rust/.cargo/config && sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen && \
    python3 -m pip install boto3 "moto[all]" yapf shfmt-py coscmd PyYAML
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
