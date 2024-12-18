#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# set up
cat <<EOF |  $BENDSQL_CLIENT_CONNECT
create or replace database i13947;
use i13947;
create or replace stage test_stage;
create or replace table tmp(id int);
insert into tmp values(1);
insert into tmp values(2);
copy into @test_stage from (select * from tmp);
EOF


# It is not convenient to extract the .stats.write_progress.rows from the output of bendsql,
# thus, curl is used. To prevent the immature result returned by curl(which will not poll the result until the query is finished),
# pagination.wait_time_secs is set to 6 seconds.

# since 2 rows will be copied into tmp table from stage
echo "expects .stats.write_progress.rows be 2"
echo "expects .error be null"
curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  --header 'Content-Type: application/json'              \
  -d '{"sql": "copy into i13947.tmp from (select * from @test_stage)", "pagination": { "wait_time_secs": 6}}'  \
  | jq -r '.stats.write_progress.rows, .error'

echo "DROP database i13947;" | $BENDSQL_CLIENT_CONNECT
