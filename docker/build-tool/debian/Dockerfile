FROM python:3.12-bullseye

ENV TERM=dumb
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -yq && \
    apt-get install -yq locales sudo unzip file && \
    printf 'en_US.UTF-8 UTF-8\n' > /etc/locale.gen && \
    locale-gen && \
    rm -rf /var/lib/apt/lists/*

ENV LANGUAGE en_US
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

COPY rust-toolchain.toml /build/rust-toolchain.toml
COPY dev_setup.sh /build/scripts/setup/dev_setup.sh
RUN chmod +x /build/scripts/setup/dev_setup.sh && \
    /build/scripts/setup/dev_setup.sh -yb && \
    rm -rf /root/.cargo/git && \
    rm -rf /root/.cargo/registry && \
    mkdir -p /opt/rust && \
    mv /root/.cargo /opt/rust/cargo && \
    mv /root/.rustup /opt/rust/rustup

RUN ln -s $(readlink -f `which javac` | sed "s:/bin/javac::") /opt/java

RUN curl -sSfLo /tmp/mold.tar.gz https://github.com/rui314/mold/releases/download/v2.35.1/mold-2.35.1-$(uname -m)-linux.tar.gz && \
    tar --strip-components=1 -xzf /tmp/mold.tar.gz -C /usr/local && rm -rf /tmp/mold.tar.gz

ENV LIBZ_SYS_STATIC=1
ENV LZMA_API_STATIC=1

ENV JAVA_HOME /opt/java
ENV LD_LIBRARY_PATH /opt/java/lib/server
ENV RUSTUP_HOME /opt/rust/rustup
ENV CARGO_HOME /opt/rust/cargo
ENV PATH /opt/rust/cargo/bin:/opt/java/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

VOLUME [ "/workspace", "/opt/rust/cargo/registry", "/opt/rust/cargo/git" ]
