ARG RUST_TOOLCHAIN
FROM datafuselabs/build-tool:base-${RUST_TOOLCHAIN}

ENV PKG_CONFIG_ALLOW_CROSS 1
RUN dpkg --add-architecture arm64 && \
    apt-get update -yq && \
    apt-get install -yq libc6-arm64-cross libc6-dev-arm64-cross gcc-aarch64-linux-gnu g++-aarch64-linux-gnu && \
    apt-get install -yq libdbus-1-dev libdbus-1-dev:arm64 && \
    apt-get install -yq libssl-dev libssl-dev:arm64 zlib1g-dev zlib1g-dev:arm64 liblzma-dev liblzma-dev:arm64

ARG ARCH
RUN rustup target add ${ARCH}-unknown-linux-gnu
RUN ln -sf /usr/local/bin/mold /usr/bin/${ARCH}-linux-gnu-ld
RUN printf "[target.${ARCH}-unknown-linux-gnu]\nlinker = \"${ARCH}-linux-gnu-gcc\"\n" > ${CARGO_HOME}/config
