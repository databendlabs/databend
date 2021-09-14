FROM debian:bullseye
COPY ./distro/databend-query /databend-query
COPY ./distro/datafuse-store /datafuse-store
COPY ./distro/bootstrap.sh /bootstrap.sh
ENTRYPOINT ["/bootstrap.sh"]