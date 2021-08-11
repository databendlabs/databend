FROM debian:bullseye
COPY ./distro/datafuse-query /datafuse-query
COPY ./distro/datafuse-store /datafuse-store
COPY ./distro/bootstrap.sh /bootstrap.sh
ENTRYPOINT ["/bootstrap.sh"]