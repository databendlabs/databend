#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "DROP CATALOG IF EXISTS iceberg_ctl" | $BENDSQL_CLIENT_CONNECT

## Create iceberg catalog
cat <<EOF | $BENDSQL_CLIENT_CONNECT
CREATE CATALOG iceberg_ctl
TYPE=ICEBERG
CONNECTION=(
    TYPE='rest'
    ADDRESS='http://127.0.0.1:8181'
    WAREHOUSE='s3://icebergdata/demo'
);
EOF

echo "SHOW CREATE CATALOG iceberg_ctl;" | $BENDSQL_CLIENT_CONNECT

echo "SHOW DATABASES IN iceberg_ctl;" | $BENDSQL_CLIENT_CONNECT

echo "SELECT CURRENT_CATALOG();USE CATALOG iceberg_ctl;SELECT CURRENT_CATALOG();" | $BENDSQL_CLIENT_CONNECT

#echo "SHOW TABLES IN iceberg_ctl.iceberg_db;" | $BENDSQL_CLIENT_CONNECT
