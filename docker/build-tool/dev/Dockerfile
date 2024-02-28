ARG RUST_TOOLCHAIN

FROM datafuselabs/build-tool:debian-${RUST_TOOLCHAIN}

COPY rust-tools.txt /build/scripts/setup/rust-tools.txt
RUN /build/scripts/setup/dev_setup.sh -ycd && \
    rm -rf /opt/rust/cargo/git && \
    rm -rf /opt/rust/cargo/registry && \
    rm -rf /root/.cache/*
