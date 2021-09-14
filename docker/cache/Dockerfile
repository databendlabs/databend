FROM debian:bullseye
COPY ./distro/databend-query /databend-query
COPY ./distro/databend-store /databend-store
COPY ./distro/bootstrap.sh /bootstrap.sh
ENTRYPOINT ["/bootstrap.sh"]