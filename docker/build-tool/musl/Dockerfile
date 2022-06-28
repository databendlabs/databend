ARG RUST_TOOLCHAIN
FROM datafuselabs/build-tool:base-${RUST_TOOLCHAIN}

ARG ARCH
# TODO:(everpcpc) change to tagged version after next release
# curl -sSfLo /tmp/musl-cross-make.tar.gz https://github.com/richfelker/musl-cross-make/archive/v0.9.9.tar.gz && \
#    tar -xzf /tmp/musl-cross-make.tar.gz -C /tmp
RUN curl -sSfLo /tmp/musl-cross-make.tar.gz https://github.com/richfelker/musl-cross-make/archive/fe915821b652a7fa37b34a596f47d8e20bc72338.zip && \
    unzip /tmp/musl-cross-make.tar.gz -d /tmp && \
    cd /tmp/musl-cross-make-* && \
    make install "-j$(nproc)" \
      OUTPUT=/usr/local/ \
      TARGET=${ARCH}-linux-musl \
      DL_CMD='curl --retry 3 -sSfL -C - -o' && \
    rm -rf /tmp/musl-cross-make-* && \
    rm -f /tmp/musl-cross-make.tar.gz

RUN rustup target add ${ARCH}-unknown-linux-musl
RUN ln -s ${ARCH}-linux-musl-gcc /usr/local/bin/musl-gcc
RUN printf "[target.${ARCH}-unknown-linux-musl]\nlinker = \"${ARCH}-linux-musl-gcc\"\n" > ${CARGO_HOME}/config
