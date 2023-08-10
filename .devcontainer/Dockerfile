FROM datafuselabs/build-tool:dev

ENV TERM xterm-256color

RUN apt-get update && apt-get install -y git ripgrep && \
    curl -sSo /tmp/install.sh https://starship.rs/install.sh && \
    chmod +x /tmp/install.sh && \
    /tmp/install.sh --yes && \
    printf 'eval "$(starship init bash)"' >> /etc/bash.bashrc && \
    rm -rf /tmp/* /var/lib/apt/lists/*

RUN useradd -u 1000 builder
RUN mkdir /home/builder && chown -R builder /home/builder && \
    usermod --shell /bin/bash builder
RUN printf "builder ALL=(ALL:ALL) NOPASSWD:ALL\n" > /etc/sudoers.d/databend

USER builder
