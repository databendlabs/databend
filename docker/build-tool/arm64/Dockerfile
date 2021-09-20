FROM rustembedded/cross:aarch64-unknown-linux-gnu

WORKDIR /tmp
RUN apt-get update && \
    apt-get install -y wget
COPY docker/build-tool/openssl.sh /
RUN bash /openssl.sh linux-aarch64 aarch64-linux-gnu-
ENV OPENSSL_DIR=/openssl \
    OPENSSL_INCLUDE_DIR=/openssl/include \
    OPENSSL_LIB_DIR=/openssl/lib
RUN apt-get install zlib1g-dev
