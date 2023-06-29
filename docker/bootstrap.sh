#!/bin/bash

PROCESSES=()

mkdir -p /var/log/databend
mkdir -p /var/lib/databend/meta
mkdir -p /var/lib/databend/query

databend-meta \
    --log-file-dir /var/log/databend \
    --log-stderr-level WARN \
    --raft-dir /var/lib/databend/meta \
    --single &>/tmp/std-meta.log &
PROCESSES+=($!)
# wait for meta to be ready
sleep 1

function setup_query_default_user {
    if [ -n "$QUERY_DEFAULT_USER" ] && [ -n "$QUERY_DEFAULT_PASSWORD" ]; then
        DOUBLE_SHA1_PASSWORD=$(echo -n "$QUERY_DEFAULT_PASSWORD" | sha1sum | cut -d' ' -f1 | xxd -r -p | sha1sum | cut -d' ' -f1)
        cat <<EOF >>"$QUERY_CONFIG_FILE"
[[query.users]]
name = "$QUERY_DEFAULT_USER"
auth_type = "double_sha1_password"
auth_string = "$DOUBLE_SHA1_PASSWORD"
EOF
    else
        cat <<EOF >>"$QUERY_CONFIG_FILE"
[[query.users]]
name = "root"
auth_type = "no_password"
EOF
    fi
}

function setup_minio {
    echo "==> Downloading MinIO server..."
    arch=$(uname -m)
    case $arch in
    "x86_64")
        platform="amd64"
        ;;
    "aarch64")
        platform="arm64"
        ;;
    *)
        echo "==> Unsupported architecture: $arch"
        exit 1
        ;;
    esac
    curl -sSLo /usr/bin/minio "https://dl.min.io/server/minio/release/linux-${platform}/minio"
    echo "==> MinIO server downloaded."
    chmod +x /usr/bin/minio
    mkdir -p /var/lib/minio
    # make sure the bucket exists
    mkdir -p "/var/lib/minio/$AWS_S3_BUCKET"
    MINIO_ROOT_USER="$AWS_ACCESS_KEY_ID" MINIO_ROOT_PASSWORD="$AWS_SECRET_ACCESS_KEY" minio server /var/lib/minio &>/tmp/std-minio.log &
    PROCESSES+=($!)
    # wait for minio to be ready
    sleep 1
}

function setup_query_storage {
    QUERY_STORAGE_TYPE=${QUERY_STORAGE_TYPE:-"fs"}
    AWS_S3_BUCKET=${AWS_S3_BUCKET:-"databend"}
    if [[ -n $MINIO_ENABLED ]]; then
        # force to use s3 storage when minio is enabled
        QUERY_STORAGE_TYPE="s3"
        AWS_S3_ENDPOINT=${AWS_S3_ENDPOINT:-"http://0.0.0.0:9000"}
        AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-"minioadmin"}
        AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-"minioadmin"}
        setup_minio
    fi
    case $QUERY_STORAGE_TYPE in
    "fs")
        mkdir -p /var/lib/databend/query
        cat <<EOF >>"$QUERY_CONFIG_FILE"
[storage]
type = "fs"
[storage.fs]
data_path = "/var/lib/databend/query"
EOF
        ;;
    "s3")
        cat <<EOF >>"$QUERY_CONFIG_FILE"
[storage]
type = "s3"
[storage.s3]
bucket = "$AWS_S3_BUCKET"
endpoint_url = "$AWS_S3_ENDPOINT"
access_key_id = "$AWS_ACCESS_KEY_ID"
secret_access_key = "$AWS_SECRET_ACCESS_KEY"
EOF
        ;;

    *)
        echo "==> Unsupported storage type: $QUERY_STORAGE_TYPE"
        exit 1
        ;;
    esac
}

if [ -z "$QUERY_CONFIG_FILE" ]; then
    QUERY_CONFIG_FILE="/etc/databend/query.toml"
    echo "==> QUERY_CONFIG_FILE is not set, using default: $QUERY_CONFIG_FILE"
    cp /etc/databend/query_config_spec.toml "$QUERY_CONFIG_FILE"
    setup_query_default_user
    setup_query_storage
fi

databend-query -c "$QUERY_CONFIG_FILE" &>/tmp/std-query.log &
PROCESSES+=($!)
# wait for query to be ready
sleep 1

tail -n +1 -F /tmp/std-*.log &
PROCESSES+=($!)

wait "${PROCESSES[@]}"
