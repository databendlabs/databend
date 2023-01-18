#!/bin/bash

MINIO_DATA_DIR=${MINIO_DATA_DIR:-"/var/lib/minio"}
MINIO_ROOT_USER=${MINIO_ROOT_USER:-"minio"}
MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-"miniostorage"}
MINIO_QUERY_BUCKET=${MINIO_QUERY_BUCKET:-"databend"}
MINIO_PRESIGNED_ENDPOINT=${MINIO_PRESIGNED_ENDPOINT:-"http://localhost:9000"}

mkdir -p "$MINIO_DATA_DIR"
# make sure the bucket exists
mkdir -p "$MINIO_DATA_DIR/$MINIO_QUERY_BUCKET"
minio server "$MINIO_DATA_DIR" &>"/tmp/std-minio.log" &
P1=$!

DATABEND_LOG_DIR=${QUERY_LOG_LEVEL:-"/var/log/databend"}
mkdir -p "$DATABEND_LOG_DIR"

databend-meta \
    --log-file-dir "$DATABEND_LOG_DIR" \
    --log-stderr-level WARN \
    --single &>"/tmp/std-meta.log" &
P2=$!

# TODO: add health check to remove the race condition issue during databend-query bootstrap
sleep 1

if [ -z "$QUERY_CONFIG_FILE" ]; then
    QUERY_CONFIG_FILE="/etc/databend/query.toml"

    cat <<EOF >>"$QUERY_CONFIG_FILE"
[log.file]
level = "INFO"
format = "text"
dir = "$DATABEND_LOG_DIR"
EOF

    if [ -n "$DATABEND_DEFAULT_USER" ] && [ -n "$DATABEND_DEFAULT_PASSWORD" ]; then
        DOUBLE_SHA1_PASSWORD=$(echo -n "$DATABEND_DEFAULT_PASSWORD" | sha1sum | cut -d' ' -f1 | xxd -r -p | sha1sum | cut -d' ' -f1)
        cat <<EOF >>"$QUERY_CONFIG_FILE"
[[query.users]]
name = "$DATABEND_DEFAULT_USER"
auth_type = "double_sha1_password"
auth_string = "$DOUBLE_SHA1_PASSWORD"
EOF
    fi

    cat <<EOF >>"$QUERY_CONFIG_FILE"
[storage]
type = "s3"
[storage.s3]
bucket = "$MINIO_QUERY_BUCKET"
endpoint_url = "http://localhost:9000"
access_key_id = "$MINIO_ROOT_USER"
secret_access_key = "$MINIO_ROOT_PASSWORD"
presign_endpoint_url = "$MINIO_PRESIGNED_ENDPOINT"
EOF

fi

databend-query -c "$QUERY_CONFIG_FILE" &>"/tmp/std-query.log" &
P3=$!

tail -f "/tmp/std-minio.log" &
P4=$!

tail -f "/tmp/std-meta.log" &
P5=$!

tail -f "/tmp/std-query.log" &
P6=$!

wait $P1 $P2 $P3 $P4 $P5 $P6
