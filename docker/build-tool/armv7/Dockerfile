FROM rustembedded/cross:armv7-unknown-linux-gnueabihf

WORKDIR /tmp
RUN apt-get update && \
    apt-get install -y wget
RUN wget https://www.openssl.org/source/openssl-1.0.1t.tar.gz
RUN tar xzf openssl-1.0.1t.tar.gz

RUN cd openssl-1.0.1t && ./config shared && MACHINE=armv7 ARCH=arm CC=arm-linux-gnueabihf-gcc make  && cd -

ENV OPENSSL_LIB_DIR=/tmp/openssl-1.0.1t/
ENV OPENSSL_INCLUDE_DIR=/tmp/openssl-1.0.1t/include