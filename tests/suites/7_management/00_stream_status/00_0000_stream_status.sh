#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stream_backlog() {
	local stream_name=$1
	local database=${2:-default}

	curl -X GET -s "http://localhost:8081/v1/tenants/system/stream_backlog?stream_name=${stream_name}&database=${database}" |
		jq -c '[.rows_added,.rows_removed,.estimated_rows,(.estimated_bytes > 0)]'
}

reset_stream() {
	local table_name=$1
	local stream_name=$2
	local append_only=$3
	local values=$4

	run_root_sql "create or replace table db_stream.${table_name}(a int);
insert into db_stream.${table_name} values ${values};
create or replace stream db_stream.${stream_name} on table db_stream.${table_name} append_only = ${append_only}"
}

run_root_sql "drop stream if exists default.s1"
run_root_sql "drop database if exists db_stream"

run_root_sql "CREATE DATABASE db_stream"
run_root_sql "create or replace table db_stream.t(a int)"
run_root_sql "create or replace stream default.s1 on table db_stream.t comment = 'test'"
run_root_sql "create or replace stream db_stream.s2 on table db_stream.t at(stream => default.s1)"

curl -X GET -s http://localhost:8081/v1/tenants/system/stream_status\?stream_name=s1 | jq .has_data
curl -X GET -s http://localhost:8081/v1/tenants/system/stream_status\?stream_name\=s2\&database\=db_stream | jq .has_data
stream_backlog s1

run_root_sql "insert into db_stream.t values (1), (2), (3)"
stream_backlog s1

reset_stream t_append s_append true "(1), (2)"
run_root_sql "delete from db_stream.t_append where a = 1"
run_root_sql "insert into db_stream.t_append values (3)"
stream_backlog s_append db_stream

reset_stream t_append s_append true "(1)"
run_root_sql "update db_stream.t_append set a = a + 10 where a = 1"
stream_backlog s_append db_stream

reset_stream t_append s_append true "(1), (2)"
run_root_sql "delete from db_stream.t_append where a = 1"
stream_backlog s_append db_stream

reset_stream t_standard s_standard false "(1)"
run_root_sql "update db_stream.t_standard set a = a + 10 where a = 1"
stream_backlog s_standard db_stream

reset_stream t_standard s_standard false "(1)"
run_root_sql "delete from db_stream.t_standard where a = 1"
stream_backlog s_standard db_stream

reset_stream t_standard s_standard false "(1), (2)"
run_root_sql "truncate table db_stream.t_standard"
stream_backlog s_standard db_stream

run_root_sql "create or replace table db_stream.t_standard(a int) row_per_block=1 block_per_segment=1 auto_compaction_imperfect_blocks_threshold=0"
run_root_sql "insert into db_stream.t_standard values (1)"
run_root_sql "insert into db_stream.t_standard values (2)"
run_root_sql "create or replace stream db_stream.s_standard on table db_stream.t_standard append_only = false"
run_root_sql "optimize table db_stream.t_standard compact"
stream_backlog s_standard db_stream

run_root_sql "drop stream if exists default.s1"
run_root_sql "drop stream if exists db_stream.s2"
run_root_sql "drop stream if exists db_stream.s_append"
run_root_sql "drop stream if exists db_stream.s_standard"
run_root_sql "drop table if exists db_stream.t all"
run_root_sql "drop table if exists db_stream.t_append all"
run_root_sql "drop table if exists db_stream.t_standard all"
run_root_sql "drop database if exists db_stream"
