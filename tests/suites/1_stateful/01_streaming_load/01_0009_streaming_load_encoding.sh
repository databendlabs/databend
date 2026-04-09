#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

DATA="/tmp/streaming_load_09"
rm -rf $DATA
mkdir -p $DATA

python3 - <<'PY'
from pathlib import Path

data = Path("/tmp/streaming_load_09")
data.joinpath("gbk.csv").write_bytes("张三,1\n".encode("gbk"))
data.joinpath("gbk.text").write_bytes("张三\t1\n".encode("gbk"))
data.joinpath("bad_utf8.csv").write_bytes(b"ab\xffcd,1\n")
PY

stmt "drop table if exists streaming_load_09"
stmt "create or replace table streaming_load_09 (name string, id int)"

echo "--csv-gbk"
(set -x; curl -sS -H "x-databend-query-id:load-encoding-csv-gbk" -H "X-Databend-SQL:insert into streaming_load_09 from @_databend_load file_format = (type=csv encoding='gbk')" -F "upload=@$DATA/gbk.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load") | jq -r '.id, .stats.rows'
echo "<<<<"
query "select * from streaming_load_09;"
stmt "truncate table streaming_load_09"

echo "--text-gbk"
(set -x; curl -sS -H "x-databend-query-id:load-encoding-text-gbk" -H "X-Databend-SQL:insert into streaming_load_09 from @_databend_load file_format = (type=text encoding='gbk')" -F "upload=@$DATA/gbk.text" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load") | jq -r '.id, .stats.rows'
echo "<<<<"
query "select * from streaming_load_09;"
stmt "truncate table streaming_load_09"

echo "--csv-utf8-replace"
(set -x; curl -sS -H "x-databend-query-id:load-encoding-csv-replace" -H "X-Databend-SQL:insert into streaming_load_09 from @_databend_load file_format = (type=csv encoding='utf8' encoding_error_mode='replace')" -F "upload=@$DATA/bad_utf8.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load") | jq -r '.id, .stats.rows'
echo "<<<<"
query "select * from streaming_load_09;"

stmt "drop table if exists streaming_load_09"
