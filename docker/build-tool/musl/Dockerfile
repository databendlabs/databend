ARG RUST_TOOLCHAIN
FROM datafuselabs/build-tool:debian-${RUST_TOOLCHAIN}

ARG ARCH
ENV MUSL_TARGET ${ARCH}-linux-musl
ENV MUSL_RUST_TARGET ${ARCH}-unknown-linux-musl

RUN curl -sSfLo /tmp/musl-cross-make.tar.gz https://github.com/richfelker/musl-cross-make/archive/v0.9.10.tar.gz && \
    tar -xzf /tmp/musl-cross-make.tar.gz -C /tmp && \
    cd /tmp/musl-cross-make-* && \
    make install "-j$(nproc)" \
        OUTPUT=/usr/local/ \
        TARGET=${MUSL_TARGET} \
        DL_CMD='curl --retry 3 -sSfL -C - -o' && \
    rm -rf /tmp/musl-cross-make-* && \
    rm -f /tmp/musl-cross-make.tar.gz

ENV C_INCLUDE_PATH /usr/local/${MUSL_TARGET}/include/
RUN ln -s ${MUSL_TARGET}-gcc /usr/local/bin/musl-gcc

RUN rustup target add ${MUSL_RUST_TARGET}

# HACK: to link with libstdc++ statically
# ref: https://github.com/rust-lang/rust/issues/36710#issuecomment-364623950
COPY linker.sh /usr/local/bin/linker
RUN chmod +x /usr/local/bin/linker
RUN printf "[target.${MUSL_RUST_TARGET}]\nlinker = \"linker\"\n" >${CARGO_HOME}/config
