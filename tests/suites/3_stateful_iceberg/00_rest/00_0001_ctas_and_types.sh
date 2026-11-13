#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

db="iceberg_ctas_types"

# Run one statement in the iceberg catalog and print the error it is expected to return.
error_of() {
	echo ">>>> $1"
	printf 'USE CATALOG iceberg_rest;\n%s\n' "$1" | bendsql_connect_root 2>&1
	echo "<<<<"
}

cat <<EOF | bendsql_connect_root
CREATE CATALOG IF NOT EXISTS iceberg_rest TYPE = ICEBERG CONNECTION = (
    TYPE = 'rest' ADDRESS = 'http://localhost:8181' warehouse = 's3://warehouse/demo/' "s3.endpoint" = 'http://localhost:9000' "s3.access-key-id" = 'admin' "s3.secret-access-key" = 'password' "s3.region" = 'us-east-1'
);
USE CATALOG iceberg_rest;
DROP TABLE IF EXISTS ${db}.ctas;
DROP TABLE IF EXISTS ${db}.ctas_types;
DROP TABLE IF EXISTS ${db}.types;
DROP TABLE IF EXISTS ${db}.too_wide;
DROP DATABASE IF EXISTS ${db};
CREATE DATABASE ${db};

-- CTAS: the catalog reply of an iceberg table has no meta table id sequence, and the CTAS path
-- used the meta-only staging protocol (as_dropped/commit_table_meta) that an external catalog
-- does not implement. The statement failed with an internal '
-- table_id_seq must have been set' error instead of creating the table.
CREATE TABLE ${db}.ctas AS SELECT number::bigint AS a, 'v' || number::string AS b FROM numbers(3);
SELECT * FROM ${db}.ctas ORDER BY a;
SELECT count(*) FROM ${db}.ctas;

-- On an existing table the statement is a no-op, the way Fuse behaves: it must not append
-- the rows of the select to the table that is already there.
CREATE TABLE IF NOT EXISTS ${db}.ctas AS SELECT 9::bigint AS a, 'z' AS b FROM numbers(1);
SELECT count(*) FROM ${db}.ctas;

-- The schema of a CTAS is converted like the schema of a CREATE TABLE.
CREATE TABLE ${db}.ctas_types AS
    SELECT number::bigint AS a, (number::decimal(10, 2)) AS d, [number, number + 1] AS li FROM numbers(2);
DESC ${db}.ctas_types;
SELECT * FROM ${db}.ctas_types ORDER BY a;

-- DECIMAL/TUPLE/ARRAY/MAP columns: databend hands DECIMAL to arrow as Decimal64/Decimal256 and
-- nested fields carry no field id, which the iceberg schema conversion used to reject.
CREATE TABLE ${db}.types (d decimal(10, 2), st tuple(k int, v string), li array(int), mp map(string, int));
DESC ${db}.types;
INSERT INTO ${db}.types VALUES (1.25, (1, 'x'), [1, 2], {'a': 1}), (2.50, (2, 'y'), [3], {'b': 2});
SELECT * FROM ${db}.types ORDER BY d;
SELECT count(*) FROM ${db}.types;

DROP TABLE ${db}.ctas;
DROP TABLE ${db}.ctas_types;
DROP TABLE ${db}.types;
EOF

# A decimal wider than iceberg's maximum precision is rejected instead of being written lossy.
error_of "CREATE TABLE ${db}.too_wide (d decimal(76, 2))"

cat <<EOF | bendsql_connect_root
USE CATALOG iceberg_rest;
DROP DATABASE ${db};
EOF
