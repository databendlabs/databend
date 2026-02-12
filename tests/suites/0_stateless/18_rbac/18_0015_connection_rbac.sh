#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


export USER_A_CONNECT="bendsql -A --user=a --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
export USER_B_CONNECT="bendsql -A --user=b --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
export USER_C_CONNECT="bendsql -A --user=c --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

echo "=== OLD LOGIC: user has super privileges can operator all connections with enable_experimental_connection_privilege_check=0 ==="
echo "=== TEST USER A WITH SUPER PRIVILEGES ==="
run_root_sql "
set global enable_experimental_connection_privilege_check=0;
drop stage if exists c1;
drop stage if exists c2;
drop stage if exists c3;
drop role if exists role1;
drop role if exists role2;
drop user if exists a;
drop user if exists b;
drop user if exists c;
create user a identified by '123';
grant super on *.* to a;
drop connection if exists c1;
drop connection if exists c2;
drop connection if exists c3;
"

echo "CREATE CONNECTION c1 STORAGE_TYPE='azblob' ENDPOINT_URL='http://s3.amazonaws.com';" | $USER_A_CONNECT
echo "create CONNECTION c2 STORAGE_TYPE = 's3' access_key_id='minioadmin' secret_access_key='minioadmin' endpoint_url='http://127.0.0.1:9900/' region='auto';" | $USER_A_CONNECT
echo "create CONNECTION c3 STORAGE_TYPE = 's3' access_key_id='c3' secret_access_key='c3' endpoint_url='http://127.0.0.1:9900/' region='auto';" | $USER_A_CONNECT
echo "DESC CONNECTION c1;" | $USER_A_CONNECT
echo "DESC CONNECTION c2;" | $USER_A_CONNECT
echo "DESC CONNECTION c3;" | $USER_A_CONNECT
echo "show connections;" | $USER_A_CONNECT
echo "drop connection if exists c1;" | $USER_A_CONNECT
echo "drop connection if exists c2;" | $USER_A_CONNECT
echo "drop connection if exists c3;" | $USER_A_CONNECT


echo "=== NEW LOGIC: user has super privileges can operator all connections with enable_experimental_connection_privilege_check=1 ==="
echo "=== TEST USER A WITH SUPER PRIVILEGES ==="
echo "set global enable_experimental_connection_privilege_check=1;" | $USER_A_CONNECT
echo "--- CREATE 3 CONNECTIONS WILL SUCCESS ---"
echo "CREATE CONNECTION c1 STORAGE_TYPE='azblob' ENDPOINT_URL='http://s3.amazonaws.com';" | $USER_A_CONNECT
echo "create CONNECTION c2 STORAGE_TYPE = 's3' access_key_id='minioadmin' secret_access_key='minioadmin' endpoint_url='http://127.0.0.1:9900/' region='auto';" | $USER_A_CONNECT
echo "create CONNECTION c3 STORAGE_TYPE = 's3' access_key_id='c3' secret_access_key='c3' endpoint_url='http://127.0.0.1:9900/' region='auto';" | $USER_A_CONNECT
echo "DESC CONNECTION c1;" | $USER_A_CONNECT
echo "DESC CONNECTION c2;" | $USER_A_CONNECT
echo "DESC CONNECTION c3;" | $USER_A_CONNECT
echo "show connections;" | $USER_A_CONNECT
echo "drop connection if exists c1;" | $USER_A_CONNECT
echo "drop connection if exists c2;" | $USER_A_CONNECT
echo "drop connection if exists c3;" | $USER_A_CONNECT

echo "=== TEST USER B, C WITH OWNERSHIP OR CREATE/ACCESS PRIVILEGES PRIVILEGES ==="

run_root_sql "
drop role if exists role1;
drop role if exists role2;
drop role if exists role3;
create user b identified by '123';
create role role1;
create role role2;
create role role3;
grant create connection on *.* to role role1;
grant role role1 to b;
"
echo "--- USER b failed to create conn c1 because current role is public, can not create ---"
echo "CREATE CONNECTION c1 STORAGE_TYPE='azblob' ENDPOINT_URL='http://s3.amazonaws.com';" | $USER_B_CONNECT

echo "alter user b with default_role='role1';" | $BENDSQL_CLIENT_CONNECT

echo "--- success, c1,c2,c3 owner role is role1 ---";
echo "CREATE CONNECTION c1 STORAGE_TYPE='azblob' ENDPOINT_URL='http://s3.amazonaws.com';" | $USER_B_CONNECT
echo "create CONNECTION c2 STORAGE_TYPE = 's3' access_key_id='minioadmin' secret_access_key='minioadmin' endpoint_url='http://127.0.0.1:9900/' region='auto';" | $USER_B_CONNECT
echo "create CONNECTION c3 STORAGE_TYPE = 's3' access_key_id='c3' secret_access_key='c3' endpoint_url='http://127.0.0.1:9900/' region='auto';" | $USER_B_CONNECT
echo "DESC CONNECTION c1;" | $USER_B_CONNECT
echo "DESC CONNECTION c2;" | $USER_B_CONNECT
echo "DESC CONNECTION c3;" | $USER_B_CONNECT
echo "show connections;" | $USER_B_CONNECT

echo "--- transform c2'ownership from role1 to role2 ---"
echo "grant ownership on connection c2 to role role2;" | $BENDSQL_CLIENT_CONNECT
echo "--- USER failed to desc conn c2, c2 role is role2 ---"
echo "DESC CONNECTION c2;" | $USER_B_CONNECT
echo "show connections;" | $USER_B_CONNECT

echo "create user c identified by '123';" | $BENDSQL_CLIENT_CONNECT
echo "grant role role2 to c;" | $BENDSQL_CLIENT_CONNECT
echo "--- only return one row c2 ---"
echo "DESC CONNECTION c2;" | $USER_C_CONNECT
echo "show connections;" | $USER_C_CONNECT
echo "--- grant access connection c1 to role3 ---"
echo "grant access connection on connection c1 to role role3;" | $BENDSQL_CLIENT_CONNECT
echo "grant role role3 to c;" | $BENDSQL_CLIENT_CONNECT
echo "DESC CONNECTION c1;" | $USER_C_CONNECT
echo "--- grant access connection c3 to role3 ---"
echo "grant access connection on connection c3 to role role3;" | $BENDSQL_CLIENT_CONNECT
echo "DESC CONNECTION c3;" | $USER_C_CONNECT
echo "--- return three rows c1,2,3 ---"
echo "show connections;" | $USER_C_CONNECT

echo "--- user b can not drop connection c2 ---"
echo "drop connection if exists c2;" | $USER_B_CONNECT
curl -s -u "b:123" -XPOST "http://$QUERY_MYSQL_HANDLER_HOST:$QUERY_HTTP_HANDLER_PORT/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE STAGE my_s3_stage URL = 's3://databend-toronto' CONNECTION = (CONNECTION_NAME = 'c2')\"}" | jq -r '.error.message' |grep 'Permission denied: privilege AccessConnection' |wc -l
echo "show grants on connection c2;" | $USER_B_CONNECT

echo "--- revoke access connection from role3 , thne user c can not drop/use connection c1,3 ---"
echo "revoke access connection on connection c1 from role role3;" | $BENDSQL_CLIENT_CONNECT
echo "revoke access connection on connection c3 from role role3;" | $BENDSQL_CLIENT_CONNECT
curl -s -u "c:123" -XPOST "http://$QUERY_MYSQL_HANDLER_HOST:$QUERY_HTTP_HANDLER_PORT/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE STAGE my_s3_stage URL = 's3://databend-toronto' CONNECTION = (CONNECTION_NAME = 'c1');\"}" | jq -r '.error.message' |grep 'Permission denied: privilege AccessConnection' |wc -l
curl -s -u "c:123" -XPOST "http://$QUERY_MYSQL_HANDLER_HOST:$QUERY_HTTP_HANDLER_PORT/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE STAGE my_s3_stage URL = 's3://databend-toronto' CONNECTION = (CONNECTION_NAME = 'c3')\"}" | jq -r '.error.message' |grep 'Permission denied: privilege AccessConnection' |wc -l
echo "show grants on connection c1;" | $USER_C_CONNECT
echo "show grants on connection c3;" | $USER_C_CONNECT
echo "drop connection if exists c1;" | $USER_C_CONNECT
echo "drop connection if exists c3;" | $USER_C_CONNECT

echo "--- user b can drop/use connection c1,3 ---"
curl -s -u "b:123" -XPOST "http://$QUERY_MYSQL_HANDLER_HOST:$QUERY_HTTP_HANDLER_PORT/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE STAGE my_s3_stage URL = 's3://databend-toronto' CONNECTION = (CONNECTION_NAME = 'c1');\"}" | jq -r '.error.message'
curl -s -u "b:123" -XPOST "http://$QUERY_MYSQL_HANDLER_HOST:$QUERY_HTTP_HANDLER_PORT/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE STAGE my_s3_stage URL = 's3://databend-toronto' CONNECTION = (CONNECTION_NAME = 'c3')\"}" | jq -r '.error.message'
echo "show grants on connection c1;" | $USER_B_CONNECT
echo "show grants on connection c3;" | $USER_B_CONNECT
echo "drop connection if exists c1;" | $USER_B_CONNECT
echo "show grants for role role1;" | $USER_B_CONNECT
echo "drop connection if exists c3;" | $USER_B_CONNECT

echo "--- user c can drop/use connection c2 ---"
curl -s -u "c:123" -XPOST "http://$QUERY_MYSQL_HANDLER_HOST:$QUERY_HTTP_HANDLER_PORT/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE STAGE my_s3_stage URL = 's3://databend-toronto' CONNECTION = (CONNECTION_NAME = 'c2')\"}" | jq -r '.error.message'
echo "show grants for role role2;" | $USER_C_CONNECT
echo "show grants on connection c2;" | $USER_C_CONNECT
echo "drop connection if exists c2;" | $USER_C_CONNECT
echo "show grants for role role2;" | $USER_C_CONNECT

run_root_sql "
drop user if exists a;
drop user if exists b;
drop user if exists c;
drop stage if exists c1;
drop stage if exists c2;
drop stage if exists c3;
drop role if exists role1;
drop role if exists role2;
drop role if exists role3;
unset global enable_experimental_connection_privilege_check;
"
