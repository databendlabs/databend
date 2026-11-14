#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# Databend DDL only creates identity partition columns on iceberg tables, so the tables of the
# bucket/truncate/day transforms are created through the REST catalog API, the way a table
# created by another engine reaches databend.
rest="http://localhost:8181"
db="iceberg_partition"
tmp=$(mktemp -d)

rest_code() {
	curl -s -o /dev/null -w "%{http_code}" -H 'Content-Type: application/json' "$@"
}

# Drop whatever a previous run left behind, so the codes below do not depend on the run order.
for table in t_bucket t_truncate t_day; do
	curl -s -o /dev/null -X DELETE "$rest/v1/namespaces/${db}/tables/${table}?purgeRequested=true"
done
curl -s -o /dev/null -X DELETE "$rest/v1/namespaces/${db}"

echo "create namespace: $(rest_code -X POST "$rest/v1/namespaces" -d "{\"namespace\":[\"${db}\"]}")"

cat >"$tmp/t_bucket.json" <<'JSON'
{
  "name": "t_bucket",
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "fields": [
      {"id": 1, "name": "a", "required": false, "type": "long"},
      {"id": 2, "name": "b", "required": false, "type": "string"}
    ]
  },
  "partition-spec": {
    "spec-id": 0,
    "fields": [
      {"name": "a_bucket", "transform": "bucket[4]", "source-id": 1, "field-id": 1000}
    ]
  },
  "write-order": null,
  "properties": {}
}
JSON

cat >"$tmp/t_truncate.json" <<'JSON'
{
  "name": "t_truncate",
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "fields": [
      {"id": 1, "name": "s", "required": false, "type": "string"},
      {"id": 2, "name": "n", "required": false, "type": "long"}
    ]
  },
  "partition-spec": {
    "spec-id": 0,
    "fields": [
      {"name": "s_truncate", "transform": "truncate[2]", "source-id": 1, "field-id": 1000}
    ]
  },
  "write-order": null,
  "properties": {}
}
JSON

cat >"$tmp/t_day.json" <<'JSON'
{
  "name": "t_day",
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "fields": [
      {"id": 1, "name": "ts", "required": false, "type": "timestamp"},
      {"id": 2, "name": "n", "required": false, "type": "long"}
    ]
  },
  "partition-spec": {
    "spec-id": 0,
    "fields": [
      {"name": "ts_day", "transform": "day", "source-id": 1, "field-id": 1000}
    ]
  },
  "write-order": null,
  "properties": {}
}
JSON

for table in t_bucket t_truncate t_day; do
	echo "create ${table}: $(rest_code -X POST "$rest/v1/namespaces/${db}/tables" -d @"$tmp/${table}.json")"
done

cat <<EOF | bendsql_connect_root
CREATE CATALOG IF NOT EXISTS iceberg_rest TYPE = ICEBERG CONNECTION = (
    TYPE = 'rest' ADDRESS = 'http://localhost:8181' warehouse = 's3://warehouse/demo/' "s3.endpoint" = 'http://localhost:9000' "s3.access-key-id" = 'admin' "s3.secret-access-key" = 'password' "s3.region" = 'us-east-1'
);
USE CATALOG iceberg_rest;
SHOW TABLES FROM ${db};

-- An identity partition column is what databend DDL creates.
CREATE TABLE ${db}.t_identity (a bigint, b string) PARTITION BY (a);
INSERT INTO ${db}.t_identity VALUES (1, 'x'), (1, 'y'), (2, 'z');
SELECT * FROM ${db}.t_identity ORDER BY a, b;
SELECT partition_summaries FROM iceberg_manifest('${db}', 't_identity');

-- The stored partition value is the transformed source value. Writing only the identity value
-- of the source column put the raw column values in the metadata, and failed with an internal
-- error for the transforms whose result type differs from the source type.
INSERT INTO ${db}.t_bucket VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');
SELECT * FROM ${db}.t_bucket ORDER BY a;
SELECT partition_summaries FROM iceberg_manifest('${db}', 't_bucket');
-- The read side prunes with those partition values, so a wrong bucket value loses rows here.
SELECT count(*) FROM ${db}.t_bucket WHERE a = 3;

INSERT INTO ${db}.t_truncate VALUES ('alpha', 1), ('alpine', 2), ('beta', 3);
SELECT * FROM ${db}.t_truncate ORDER BY n;
SELECT partition_summaries FROM iceberg_manifest('${db}', 't_truncate');

INSERT INTO ${db}.t_day VALUES ('2024-01-01 10:00:00', 1), ('2024-01-02 11:00:00', 2);
SELECT * FROM ${db}.t_day ORDER BY n;
SELECT partition_summaries FROM iceberg_manifest('${db}', 't_day');
EOF

cat <<EOF | bendsql_connect_root
USE CATALOG iceberg_rest;
DROP TABLE ${db}.t_identity;
EOF

# The namespace can only be dropped once the tables created through the REST API are gone.
for table in t_bucket t_truncate t_day; do
	echo "drop ${table}: $(rest_code -X DELETE "$rest/v1/namespaces/${db}/tables/${table}?purgeRequested=true")"
done

# The namespace is dropped together with the database.
cat <<EOF | bendsql_connect_root
USE CATALOG iceberg_rest;
DROP DATABASE ${db};
EOF

rm -rf "$tmp"
