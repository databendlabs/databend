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

echo "SELECT count(*) FROM iceberg_ctl.iceberg_db.iceberg_tbl;" | $BENDSQL_CLIENT_CONNECT

echo "SELECT * FROM iceberg_ctl.iceberg_db.iceberg_tbl WHERE id = 5;" | $BENDSQL_CLIENT_CONNECT

echo "SELECT data FROM iceberg_ctl.iceberg_db.iceberg_tbl WHERE id > 3 ORDER BY id;" | $BENDSQL_CLIENT_CONNECT

echo "SELECT data FROM iceberg_ctl.iceberg_db.iceberg_tbl WHERE id > 3 ORDER BY data;" | $BENDSQL_CLIENT_CONNECT
