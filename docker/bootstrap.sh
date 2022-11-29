#!/bin/bash

# DATABEND_DEFAULT_USER="${DATABEND_DEFAULT_USER:-databend}"
# DATABEND_DEFAULT_PASSWORD="${DATABEND_DEFAULT_PASSWORD:-databend}"

/databend-meta --single &>/tmp/databend-meta.log &
P1=$!

# TODO: add health check to remove the race condition issue during databend-query bootstrap
sleep 1

if [ -n "$DATABEND_DEFAULT_USER" ] && [ -n "$DATABEND_DEFAULT_PASSWORD" ]; then
    DOUBLE_SHA1_PASSWORD=$(echo -n "$DATABEND_DEFAULT_PASSWORD" | sha1sum | cut -d' ' -f1 | xxd -r -p | sha1sum | cut -d' ' -f1)
    cat <<EOF >>databend-query.toml
[[query.users]]
name = "$DATABEND_DEFAULT_USER"
auth_type = "double_sha1_password"
auth_string = "$DOUBLE_SHA1_PASSWORD"
EOF
fi

/databend-query -c databend-query.toml &>/tmp/databend-query.log &
P2=$!

tail -f /tmp/databend-query.log &
P3=$!

tail -f /tmp/databend-meta.log &
P4=$!
wait $P1 $P2 $P3 $P4
