#!/usr/bin/env bash
# most features are tested in sqllogic tests with diff type of external stages
# this test is mainly to test internal stage and user stage (and paths) is parsed and used correctly, the file content is not important

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists iti" | bendsql_connect_root
echo "create table iti (a int, b string, c int)" | bendsql_connect_root

echo "drop stage if exists s1" | bendsql_connect_root
echo "CREATE STAGE s1;" | bendsql_connect_root

for STAGE in "s1" "~"
do
	echo "---- testing @$STAGE;"
	echo "remove @$STAGE;" | bendsql_connect_root
	echo "truncate table iti;" | bendsql_connect_root

	curl -s -u root: -XPUT -H "x-databend-stage-name:${STAGE}" -F "upload=@${TESTS_DATA_DIR}/csv/sample.csv" "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/upload_to_stage" | jq -r '.state'
	echo "list @$STAGE" | bendsql_connect_root | awk '{print $1}' | sort
	echo "-- testing copy;"
  echo "copy into iti from @$STAGE FILE_FORMAT = (type = CSV skip_header = 1) force=true;" | bendsql_connect_root
  echo "select count(*) from iti;" | bendsql_connect_root

	echo "-- testing copy with purge;"
	echo "truncate table iti;" | bendsql_connect_root
  echo "copy into iti from @$STAGE FILE_FORMAT = (type = CSV skip_header = 1) purge=true;" | bendsql_connect_root
  echo "select count(*) from iti;" | bendsql_connect_root
done

echo "---- check files are purged;"
## list stage has metacache, so we just we aws client to ensure the data are purged
aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 ls s3://testbucket/admin/stage/internal/s1/ | grep -o sample.csv  | wc -l | sed 's/ //g'
aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 ls s3://testbucket/admin/stage/user/root/ | grep -o sample.csv  | wc -l | sed 's/ //g'

echo "drop table if exists iti" | bendsql_connect_root
echo "drop stage if exists s1" | bendsql_connect_root
