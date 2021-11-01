FROM debian as installer
ARG VERSION
ENV VERSION ${VERSION}
COPY ./scripts/installer/install-bendctl.sh /install.sh
RUN apt update \
    && apt install curl tar -y
# hack ssl issue in curl
RUN update-ca-certificates -f
RUN chmod +x /install.sh
RUN /install.sh -v ${VERSION}
# double check whether binaries could run on host architecture
RUN /root/.databend/bin/bendctl package fetch --databend_dir /root/.databend/ ${VERSION}
RUN timeout 3 /root/.databend/local/bin/${VERSION}/databend-query || true
RUN timeout 3 /root/.databend/local/bin/${VERSION}/databend-query || true
# CI environment build image under GLIBC 2.29 and debian:buster only run GLIBC 2.28
FROM debian:bullseye
ARG VERSION
ENV VERSION ${VERSION}
COPY --from=installer /root/.databend/local/bin/${VERSION}/databend-query  /databend-query
COPY --from=installer /root/.databend/local/bin/${VERSION}/databend-meta  /databend-meta
COPY ./docker/bootstrap.sh /bootstrap.sh
COPY ./docker/databend-query-docker.toml  /databend-query.toml
ENTRYPOINT ["/bootstrap.sh"]