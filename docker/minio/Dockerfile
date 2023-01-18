ARG VERSION
FROM datafuselabs/databend:${VERSION}

COPY --from=minio/minio:latest /opt/bin/minio /usr/bin/minio
COPY bootstrap.sh /bootstrap.sh
