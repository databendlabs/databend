#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stmt "drop table if exists t1"
stmt "create table t1 (a string, b string, c string, d string not null)"

stmt "drop stage if exists s1"
stmt "create stage s1"

query "copy into @s1 from (select 'Null', 'NULL', '', '') file_format = (type = csv)"

curl -s -u root: -XPOST "http://localhost:8000/v1/query" --header 'Content-Type: application/json' -d '{"sql": "insert into t1 (a, b, c, d) values", "stage_attachment": {"location": "@s1/", "copy_options": {"purge": "true"},  "file_format_options":{"Type": "csv","Binary_Format":"hex", "null_display": "Null"}}, "pagination": { "wait_time_secs": 8}}' | jq -r '.state, .stats.scan_progress.bytes, .stats.write_progress.bytes, .error'

query "list @s1"

query "select a is null, b is null, c, d = '' from t1"

stmt "drop table if exists t1"
stmt "drop stage if exists s1"
