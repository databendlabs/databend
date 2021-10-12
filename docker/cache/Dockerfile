FROM debian:bullseye
COPY ./distro/databend-query /databend-query
COPY ./distro/databend-meta /databend-meta
COPY ./distro/bootstrap.sh /bootstrap.sh
COPY ./distro/databend-query-docker.toml /databend-query.toml
ENTRYPOINT ["/bootstrap.sh"]