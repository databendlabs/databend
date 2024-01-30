#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "DROP CATALOG IF EXISTS iceberg_ctl" | $BENDSQL_CLIENT_CONNECT

## Create iceberg catalog
cat <<EOF | $BENDSQL_CLIENT_CONNECT
CREATE CATALOG iceberg_ctl
TYPE=ICEBERG
CONNECTION=(
    URL='s3://testbucket/iceberg_ctl/'
    access_key_id ='minioadmin'
    secret_access_key ='minioadmin'
    ENDPOINT_URL='${STORAGE_S3_ENDPOINT_URL}'
);
EOF

echo "SHOW DATABASES IN iceberg_ctl;" | $BENDSQL_CLIENT_CONNECT

# FIXME: server warning: Failed to get database: iceberg_db, UnknownDatabase. Code: 1003, Text = Unknown database 'iceberg_db'.
# echo "SHOW TABLES IN iceberg_ctl.iceberg_db;" | $BENDSQL_CLIENT_CONNECT

echo "SHOW CREATE CATALOG iceberg_ctl;" | $BENDSQL_CLIENT_CONNECT
