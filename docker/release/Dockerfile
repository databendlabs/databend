FROM debian as installer
COPY ./scripts/installer/install-datafuse.sh /install.sh
RUN apt update \
    && apt install curl tar -y
# hack ssl issue in curl
RUN update-ca-certificates -f
RUN chmod +x /install.sh
RUN /install.sh
# double check whether binaries could run on host architecture
RUN timeout 3 /root/.datafuse/bin/fuse-query || true
RUN timeout 3 /root/.datafuse/bin/fuse-store || true
FROM debian:buster
COPY --from=installer /root/.datafuse/bin/fuse-query  /fuse-query
COPY --from=installer /root/.datafuse/bin/fuse-store /fuse-store
COPY ./docker/bootstrap.sh /bootstrap.sh
COPY ./docker/fusequery-docker.toml  /fuse-query.toml
ENTRYPOINT ["/bootstrap.sh"]