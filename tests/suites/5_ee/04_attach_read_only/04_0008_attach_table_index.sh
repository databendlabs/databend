#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists phrases" | $BENDSQL_CLIENT_CONNECT
echo "create table phrases (id INT, text STRING, NGRAM INDEX idx_text (text)) 's3://testbucket/admin/data/' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}')" | $BENDSQL_CLIENT_CONNECT

echo "init table"
echo "INSERT INTO phrases VALUES(1, 'apple banana cherry'),(2, 'banana date fig'),(3, 'cherry elderberry fig'),(4, 'date grape kiwi')" | $BENDSQL_CLIENT_CONNECT

storage_prefix=$(mysql -uroot -h127.0.0.1 -P3307  -e "set global hide_options_in_show_create_table=0;show create table phrases" | grep -i snapshot_location | awk -F'SNAPSHOT_LOCATION='"'"'|_ss' '{print $2}')

# attach table
echo "drop table if exists att_phrases" | $BENDSQL_CLIENT_CONNECT
echo "attach table att_phrases 's3://testbucket/admin/data/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT

# check index
echo "check index"
echo "select name, type, database, table from system.indexes where database = 'default' and table = 'att_phrases';" | $BENDSQL_CLIENT_CONNECT

# refresh ngram index
stmt "REFRESH NGRAM INDEX idx_text ON phrases;"
echo "drop table if exists att_phrases" | $BENDSQL_CLIENT_CONNECT
echo "attach table att_phrases 's3://testbucket/admin/data/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT
echo "select name, type, database, table from system.indexes where database = 'default' and table = 'att_phrases';" | $BENDSQL_CLIENT_CONNECT

# drop ngram index
stmt "DROP NGRAM INDEX idx_text ON phrases;"
echo "drop table if exists att_phrases" | $BENDSQL_CLIENT_CONNECT
echo "attach table att_phrases 's3://testbucket/admin/data/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT
echo "select name, type, database, table from system.indexes where database = 'default' and table = 'att_phrases';" | $BENDSQL_CLIENT_CONNECT

echo "drop table if exists phrases" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists att_phrases" | $BENDSQL_CLIENT_CONNECT