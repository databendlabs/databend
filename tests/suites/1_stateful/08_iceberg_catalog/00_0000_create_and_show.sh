#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "DROP CATALOG IF EXISTS iceberg_ctl" | $MYSQL_CLIENT_CONNECT

## Create iceberg catalog
cat <<EOF | $MYSQL_CLIENT_CONNECT
CREATE CATALOG iceberg_ctl
TYPE=ICEBERG
CONNECTION=(
    URI="s3a://testbucket/iceberg_ctl"
    AWS_KEY_ID=minioadmin
    AWS_SECRET_KEY=minioadmin
);
EOF

echo "SHOW DATABASES FROM iceberg_ctl;" | $MYSQL_CLIENT_CONNECT | awk '{print $2}'

echo "SHOW TABLES FROM iceberg_ctl.iceberg_db;" | $MYSQL_CLIENT_CONNECT | awk '{print $2}'
