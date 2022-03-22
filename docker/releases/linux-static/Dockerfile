FROM alpine:3.15

# Make sure LANG is en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# Enable codegen-units=1 to make sure musl-gcc will not OOM
ENV RUSTFLAGS="-C codegen-units=1"
# Enable static openssl
ENV OPENSSL_STATIC=true
ENV OPENSSL_LIB_DIR=/usr/lib
ENV OPENSSL_INCLUDE_DIR=/usr/include

RUN apk update && apk add --no-cache binutils rustup gcc clang-dev musl-dev openssl-dev protoc make openssl-libs-static
# Setup rust env
RUN rustup-init -y
ENV PATH=/root/.cargo/bin:$PATH

VOLUME /code
WORKDIR /code

CMD ["cargo", "version"]
