FROM debian as installer
COPY ./scripts/installer/install-datafuse.sh /install.sh
RUN apt update \
    && apt install curl tar -y
# hack ssl issue in curl
RUN update-ca-certificates -f
RUN chmod +x /install.sh
RUN /install.sh
# double check whether binaries could run on host architecture
RUN timeout 3 /root/.datafuse/bin/datafuse-query || true
RUN timeout 3 /root/.datafuse/bin/datafuse-store || true
# CI environment build image under GLIBC 2.29 and debian:buster only run GLIBC 2.28
FROM debian:bullseye
COPY --from=installer /root/.datafuse/bin/datafuse-query  /datafuse-query
COPY --from=installer /root/.datafuse/bin/datafuse-store /datafuse-store
COPY ./docker/bootstrap.sh /bootstrap.sh
COPY ./docker/datafuse-query-docker.toml  /datafuse-query.toml
ENTRYPOINT ["/bootstrap.sh"]