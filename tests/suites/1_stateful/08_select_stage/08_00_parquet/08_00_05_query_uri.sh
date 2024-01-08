#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

# TODO(andylokandy/everpcpc): skipping this test until https://github.com/datafuselabs/bendsql/pull/330 is released
# echo  "select * from 's3://testbucket/admin/data/parquet/tuple.parquet' (connection => (access_key_id  = 'minioadmin', secret_access_key  = 'minioadmin', endpoint_url = 'http://127.0.0.1:9900/'), file_format => 'parquet')"  | $BENDSQL_CLIENT_CONNECT

# expected result:
# 1	(1,'a')
# 2	(3,'b')
# 3	(3,'c')
