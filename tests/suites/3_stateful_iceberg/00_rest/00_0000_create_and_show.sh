#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "DROP CATALOG IF EXISTS iceberg_rest" | $BENDSQL_CLIENT_CONNECT
echo "DROP CATALOG IF EXISTS iceberg_hms" | $BENDSQL_CLIENT_CONNECT
echo "DROP CATALOG IF EXISTS iceberg_glue" | $BENDSQL_CLIENT_CONNECT


## hms
hms_ip=`docker inspect -f  {{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}} it-iceberg-catalogs-hive-metastore-1`

cat <<EOF |  $BENDSQL_CLIENT_CONNECT
CREATE CATALOG iceberg_rest TYPE = ICEBERG CONNECTION = (
    TYPE = 'rest' ADDRESS = 'http://localhost:8181' warehouse = 's3://warehouse/demo/' "s3.endpoint" = 'http://localhost:9000' "s3.access-key-id" = 'admin' "s3.secret-access-key" = 'password' "s3.region" = 'us-east-1'
);
EOF

cat <<EOF | $BENDSQL_CLIENT_CONNECT
CREATE CATALOG iceberg_hms TYPE = ICEBERG CONNECTION = (
    TYPE = 'hive' ADDRESS = '${hms_ip}:9083' warehouse = 's3a://warehouse/hive/' "s3.endpoint" = 'http://localhost:9000' "s3.access-key-id" = 'admin' "s3.secret-access-key" = 'password' "s3.region" = 'us-east-1'
);
EOF

cat <<EOF |  $BENDSQL_CLIENT_CONNECT
CREATE CATALOG iceberg_glue TYPE = ICEBERG CONNECTION = (
    TYPE = 'rest' ADDRESS = 'http://localhost:8181' warehouse = 's3://warehouse/glue/' "s3.endpoint" = 'http://localhost:9000' "s3.access-key-id" = 'admin' "s3.secret-access-key" = 'password' "s3.region" = 'us-east-1'
);
EOF


catalogs=(iceberg_rest iceberg_hms iceberg_glue)
database="db_${RANDOM}"
for catalog in "${catalogs[@]}";do
    echo "===== Testing ${catalog} ====="
    echo "SHOW CREATE CATALOG $catalog;" | $BENDSQL_CLIENT_CONNECT
    echo """
    SELECT CURRENT_CATALOG();
    USE CATALOG $catalog;

    drop database if exists ${database};

    create database ${database};
    create table ${database}.t(a int);
    show tables from ${database};
    drop table ${database}.t;
    drop database if exists ${database};

    create database ${database};
    create table ${database}.t(a int);
    show tables from ${database};
    drop table ${database}.t;
    drop database if exists ${database};

    SELECT CURRENT_CATALOG();
    """ | $BENDSQL_CLIENT_CONNECT

    echo  ""
    echo  ""
done
