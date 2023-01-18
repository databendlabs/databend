#!/bin/bash

DATABEND_LOG_DIR=${QUERY_LOG_LEVEL:-"/var/log/databend"}
mkdir -p "$DATABEND_LOG_DIR"

QUERY_CONFIG_FILE=${QUERY_CONFIG_FILE:-"/etc/databend/query.toml"}
QUERY_STORAGE_DATA_DIR=${QUERY_STORAGE_DATA_DIR:-"/var/lib/databend"}

databend-meta \
    --log-file-dir "$DATABEND_LOG_DIR" \
    --log-stderr-level WARN \
    --single &>"${DATABEND_LOG_DIR}/std-meta.log" &
P1=$!

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

    mkdir -p "$QUERY_STORAGE_DATA_DIR"
    cat <<EOF >>"$QUERY_CONFIG_FILE"
[storage]
type = "fs"
[storage.fs]
data_path = "$QUERY_STORAGE_DATA_DIR"
EOF

fi

databend-query -c "$QUERY_CONFIG_FILE" &>"${DATABEND_LOG_DIR}/std-query.log" &
P2=$!

tail -f "${DATABEND_LOG_DIR}/std-query.log" &
P3=$!

tail -f "${DATABEND_LOG_DIR}/std-meta.log" &
P4=$!

wait $P1 $P2 $P3 $P4
