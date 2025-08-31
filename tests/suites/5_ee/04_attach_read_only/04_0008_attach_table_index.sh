#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists phrases" | $BENDSQL_CLIENT_CONNECT
echo "create table phrases (id INT, text STRING,  text_0 STRING, embedding Vector(8), NGRAM INDEX idx_text (text), VECTOR INDEX idx_vector (embedding) m=10 ef_construct=40 distance='cosine') 's3://testbucket/data/' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}')" | $BENDSQL_CLIENT_CONNECT
echo "CREATE INVERTED INDEX idx_inverted ON phrases(text_0);" | $BENDSQL_CLIENT_CONNECT

echo "init table"
echo "INSERT INTO phrases VALUES(1, 'apple banana cherry', 'apple banana cherry', [0.61418788, 0.34545306, 0.14638622, 0.53249639, 0.09139293, 0.84940919, 0.105433, 0.4156201]),(2, 'banana date fig', 'banana date fig', [0.21828953, 0.87852734, 0.64221122, 0.24536394, 0.81689593, 0.86341877, 0.7218334, 0.45028494]),(3, 'cherry elderberry fig', 'cherry elderberry fig', [0.43279006, 0.45523681, 0.76060274, 0.66284758, 0.19131476, 0.13564463, 0.88712212, 0.93279565]),(4, 'date grape kiwi', 'date grape kiwi', [0.79671359, 0.86079789, 0.94477631, 0.5116732, 0.29733205, 0.33645561, 0.41380333, 0.75909903])" | $BENDSQL_CLIENT_CONNECT

storage_prefix=$(mysql -uroot -h127.0.0.1 -P3307  -e "set global hide_options_in_show_create_table=0;show create table phrases" | grep -i snapshot_location | awk -F'SNAPSHOT_LOCATION='"'"'|_ss' '{print $2}')

# attach table
echo "drop table if exists att_phrases" | $BENDSQL_CLIENT_CONNECT
echo "attach table att_phrases 's3://testbucket/data/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT

# check index
echo "check index"
echo "select name, type, database, table from system.indexes where database = 'default' and table = 'att_phrases';" | $BENDSQL_CLIENT_CONNECT

# refresh ngram index
stmt "REFRESH NGRAM INDEX idx_text ON phrases;"
echo "drop table if exists att_phrases" | $BENDSQL_CLIENT_CONNECT
echo "attach table att_phrases 's3://testbucket/data/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT
echo "select name, type, database, table from system.indexes where database = 'default' and table = 'att_phrases';" | $BENDSQL_CLIENT_CONNECT

# drop ngram index
stmt "DROP NGRAM INDEX idx_text ON phrases;"
echo "drop table if exists att_phrases" | $BENDSQL_CLIENT_CONNECT
echo "attach table att_phrases 's3://testbucket/data/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT
echo "select name, type, database, table from system.indexes where database = 'default' and table = 'att_phrases';" | $BENDSQL_CLIENT_CONNECT

# refresh inverted index
stmt "REFRESH INVERTED INDEX idx_inverted ON phrases;"
echo "drop table if exists att_phrases" | $BENDSQL_CLIENT_CONNECT
echo "attach table att_phrases 's3://testbucket/data/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT
echo "select name, type, database, table from system.indexes where database = 'default' and table = 'att_phrases';" | $BENDSQL_CLIENT_CONNECT

# drop inverted index
stmt "DROP INVERTED INDEX idx_inverted ON phrases;"
echo "drop table if exists att_phrases" | $BENDSQL_CLIENT_CONNECT
echo "attach table att_phrases 's3://testbucket/data/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT
echo "select name, type, database, table from system.indexes where database = 'default' and table = 'att_phrases';" | $BENDSQL_CLIENT_CONNECT

# drop vector index
stmt "DROP VECTOR INDEX idx_vector ON phrases;"
echo "drop table if exists att_phrases" | $BENDSQL_CLIENT_CONNECT
echo "attach table att_phrases 's3://testbucket/data/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT
echo "select name, type, database, table from system.indexes where database = 'default' and table = 'att_phrases';" | $BENDSQL_CLIENT_CONNECT

echo "drop table if exists phrases" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists att_phrases" | $BENDSQL_CLIENT_CONNECT