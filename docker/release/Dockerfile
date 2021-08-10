FROM debian as installer
ARG version
COPY ./scripts/installer/install-datafuse.sh /install.sh
RUN apt update \
    && apt install curl tar -y
# hack ssl issue in curl
RUN update-ca-certificates -f
RUN chmod +x /install.sh
RUN /install.sh $version
# double check whether binaries could run on host architecture
RUN timeout 3 /root/.datafuse/bin/fuse-query || true
RUN timeout 3 /root/.datafuse/bin/fuse-store || true
# CI environment build image under GLIBC 2.29 and debian:buster only run GLIBC 2.28
FROM debian:bullseye
COPY --from=installer /root/.datafuse/bin/fuse-query  /fuse-query
COPY --from=installer /root/.datafuse/bin/fuse-store /fuse-store
COPY ./docker/bootstrap.sh /bootstrap.sh
COPY ./docker/fusequery-docker.toml  /fuse-query.toml
ENTRYPOINT ["/bootstrap.sh"]