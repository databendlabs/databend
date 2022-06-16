FROM debian:bullseye

ENV TERM=dumb
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -yq && \
    apt-get install -yq locales sudo && \
    printf 'en_US.UTF-8 UTF-8\n' > /etc/locale.gen && \
    locale-gen && \
    rm -rf /var/lib/apt/lists/*

ENV LANGUAGE en_US
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

RUN groupdel dialout && groupdel staff && \
    groupadd -g 20 staff && groupadd -g 121 docker && \
    useradd -u 1000 -g 100 builder && \
    useradd -u 1001 -g 121 runner && \
    useradd -u 501 -g 20 macos && \
    printf "builder,runner,macos ALL=(ALL:ALL) NOPASSWD:ALL\n" > /etc/sudoers.d/databend

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

RUN curl -qL https://github.com/rui314/mold/releases/download/v1.2.1/mold-1.2.1-$(uname -m)-linux.tar.gz -o /tmp/mold.tar.gz && \
    tar --strip-components=1 -xzf /tmp/mold.tar.gz -C /usr/local && \
    rm -rf /tmp/mold.tar.gz && \
    ln -sf /usr/local/bin/mold /usr/bin/$(uname -m)-linux-gnu-ld

ENV JAVA_HOME /opt/java
ENV RUSTUP_HOME /opt/rust/rustup
ENV CARGO_HOME /opt/rust/cargo
ENV PATH /opt/rust/cargo/bin:/opt/java/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
VOLUME [ "/workspace", "/opt/rust/cargo/registry", "/opt/rust/cargo/git" ]

