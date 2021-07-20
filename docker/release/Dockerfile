FROM debian:buster as installer
COPY ./scripts/install.sh /install.sh
RUN apt-get update \
    && apt-get install curl tar jq -y
RUN chmod +x ./install.sh \
    && ./install.sh

FROM debian:buster
COPY --from=installer /root/.datafuse/bin/fuse-query  /fuse-query
COPY --from=installer /root/.datafuse/bin/fuse-store /fuse-store
COPY ./docker/bootstrap.sh /bootstrap.sh
COPY ./docker/fusequery-docker.toml  /fuse-query.toml
ENTRYPOINT ["/bootstrap.sh"]