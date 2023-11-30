#!/usr/bin/env bash
# most features are tested in sqllogic tests with diff type of external stages
# this test is mainly to test internal stage and user stage (and paths) is parsed and used correctly, the file content is not important

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists iti" | $BENDSQL_CLIENT_CONNECT
echo "create table iti (a int, b string, c int)" | $BENDSQL_CLIENT_CONNECT

echo "drop stage if exists s1" | $BENDSQL_CLIENT_CONNECT
echo "CREATE STAGE s1;" | $BENDSQL_CLIENT_CONNECT

for STAGE in "s1" "~"
do
	echo "---- testing @$STAGE;"
	echo "remove @$STAGE;" | $BENDSQL_CLIENT_CONNECT
	echo "truncate table iti;" | $BENDSQL_CLIENT_CONNECT

	curl -s -u root: -XPUT -H "x-databend-stage-name:${STAGE}" -F "upload=@${TESTS_DATA_DIR}/csv/sample.csv" "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/upload_to_stage" | jq -r '.state'
	echo "list @$STAGE" | $BENDSQL_CLIENT_CONNECT | awk '{print $1}' | sort
	echo "-- testing copy;"
  echo "copy into iti from @$STAGE FILE_FORMAT = (type = CSV skip_header = 1) force=true;" | $BENDSQL_CLIENT_CONNECT
  echo "select count(*) from iti;" | $BENDSQL_CLIENT_CONNECT

	echo "-- testing copy with purge;"
	echo "truncate table iti;" | $BENDSQL_CLIENT_CONNECT
  echo "copy into iti from @$STAGE FILE_FORMAT = (type = CSV skip_header = 1) purge=true;" | $BENDSQL_CLIENT_CONNECT
  echo "select count(*) from iti;" | $BENDSQL_CLIENT_CONNECT
done

echo "---- check files are purged;"
## list stage has metacache, so we just we aws client to ensure the data are purged
aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 ls s3://testbucket/admin/stage/internal/s1/ | grep -o sample.csv  | wc -l | sed 's/ //g'
aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 ls s3://testbucket/admin/stage/user/root/ | grep -o sample.csv  | wc -l | sed 's/ //g'

echo "drop table if exists iti" | $BENDSQL_CLIENT_CONNECT
echo "drop stage if exists s1" | $BENDSQL_CLIENT_CONNECT
