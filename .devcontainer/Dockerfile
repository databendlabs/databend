FROM datafuselabs/build-tool:dev

ENV TERM xterm-256color

RUN apt-get update && apt-get install -y git ripgrep && \
    curl -sSo /tmp/install.sh https://starship.rs/install.sh && \
    chmod +x /tmp/install.sh && \
    /tmp/install.sh --yes && \
    printf 'eval "$(starship init bash)"' >> /etc/bash.bashrc && \
    curl -sSLo /tmp/lsd.deb https://github.com/Peltoche/lsd/releases/download/0.21.0/lsd_0.21.0_amd64.deb && \
    dpkg -i /tmp/lsd.deb && \
    rm -rf /tmp/* /var/lib/apt/lists/*

RUN mkdir /home/builder && chown -R builder /home/builder && \
    usermod --shell /bin/bash builder

USER builder
