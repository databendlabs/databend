FROM rust:1.48.0-buster AS builder
RUN rustup toolchain install nightly; \
rustup component add rustfmt; \
rustup component add rustfmt --toolchain nightly;

COPY ./ /app
WORKDIR /app
RUN make build

FROM ubuntu:20.04
COPY --from=builder /app/target/release/fuse-query /fuse-query
COPY --from=builder /app/tests /tests

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            brotli \
            expect \
            zstd \
            lsof \
            ncdu \
            netcat-openbsd \
            openssl \
            python3 \
            python3-lxml \
            python3-requests \
            python3-termcolor \
            python3-pip \
            qemu-user-static \
            sudo \
            telnet \
            tree \
            unixodbc \
            mysql-client \
            wget

ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV NUM_TRIES=1
ENV MAX_RUN_TIME=0

COPY docker/stateless/run.sh /run.sh
CMD ["/bin/bash", "/run.sh"]
