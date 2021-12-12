FROM rustembedded/cross:aarch64-unknown-linux-gnu

RUN dpkg --add-architecture arm64 && \
    apt-get update && \
    apt-get install --assume-yes libssl-dev libssl-dev:arm64 zlib1g-dev zlib1g-dev:arm64

