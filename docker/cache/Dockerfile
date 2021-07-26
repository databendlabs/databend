FROM debian:buster
COPY ./distro/fuse-query /fuse-query
COPY ./distro/fuse-store /fuse-store
COPY ./distro/bootstrap.sh /bootstrap.sh
ENTRYPOINT ["/bootstrap.sh"]
