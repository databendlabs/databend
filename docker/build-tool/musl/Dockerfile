ARG RUST_TOOLCHAIN
FROM datafuselabs/build-tool:base-${RUST_TOOLCHAIN}

COPY docker/build-tool/musl/musl.sh /musl.sh
RUN chmod +x /musl.sh && /musl.sh TARGET=${ARCH}-linux-musl

ARG ARCH
RUN rustup target add ${ARCH}-unknown-linux-musl
RUN ln -s ${ARCH}-linux-musl-gcc /usr/local/bin/musl-gcc
RUN printf "[target.${ARCH}-unknown-linux-musl]\nlinker = \"${ARCH}-linux-musl-gcc\"\n" > ${CARGO_HOME}/config
