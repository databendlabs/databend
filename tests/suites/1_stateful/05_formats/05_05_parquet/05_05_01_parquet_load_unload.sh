# #!/usr/bin/env bash

# CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# . "$CURDIR"/../../../../shell_env.sh

# echo "drop table if exists test_load_unload" | $BENDSQL_CLIENT_CONNECT

# echo "CREATE TABLE test_load_unload
# (
#     a VARCHAR NULL,
#     b float,
#     c array(string not null),
#     d Variant,
#     e timestamp,
#     f decimal(4, 2),
#     h tuple(string not null,int not null)
# );" | $BENDSQL_CLIENT_CONNECT

# insert_data() {
# 	echo "insert into test_load_unload values
# 	('a\"b', 1, ['a\"b'], parse_json('{\"k\":\"v\"}'), '2044-05-06T03:25:02.868894-07:00', 010.011, ('a', 5)),
# 	(null, 2, ['a\'b'], parse_json('[1]'), '2044-05-06T03:25:02.868894-07:00', -010.011, ('b',10))
# 	" | $BENDSQL_CLIENT_CONNECT
# }

# test_format() {
# 	rm -rf /tmp/test_load_unload
# 	mkdir /tmp/test_load_unload
# 	echo "drop stage if exists s1" | $BENDSQL_CLIENT_CONNECT
# 	echo "create stage s1 url='fs:///tmp/test_load_unload/'" | $BENDSQL_CLIENT_CONNECT
# 	echo "truncate table test_load_unload" | $BENDSQL_CLIENT_CONNECT

# 	# insert data
# 	insert_data

# 	# unload1
# 	echo "copy into @s1/unload1/ from test_load_unload" | $BENDSQL_CLIENT_CONNECT
# 	mv `ls /tmp/test_load_unload/unload1/*` /tmp/test_load_unload/unload1.parquet

# 	echo "truncate table test_load_unload" | $BENDSQL_CLIENT_CONNECT

# 	# load streaming
# 	curl -sH "insert_sql:insert into test_load_unload file_format = (type = ${1})" \
# 	-F "upload=@/tmp/test_load_unload/unload1.parquet" \
# 	-u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c "SUCCESS"

# 	# unload2
# 	echo "copy into @s1/unload2/ from test_load_unload" | $BENDSQL_CLIENT_CONNECT
# 	mv `ls /tmp/test_load_unload/unload2/*` /tmp/test_load_unload/unload2.parquet

# 	echo "truncate table test_load_unload" | $BENDSQL_CLIENT_CONNECT

# 	# load with copy into table
# 	echo "copy into test_load_unload from @s1/unload1.parquet force=true;" | $BENDSQL_CLIENT_CONNECT

# 	# unload3
# 	echo "copy into @s1/unload3/ from test_load_unload" | $BENDSQL_CLIENT_CONNECT
# 	mv `ls /tmp/test_load_unload/unload3/*` /tmp/test_load_unload/unload3.parquet

# 	diff /tmp/test_load_unload/unload2.parquet /tmp/test_load_unload/unload1.parquet
# 	diff /tmp/test_load_unload/unload3.parquet /tmp/test_load_unload/unload1.parquet
# 	echo "truncate table test_load_unload" | $BENDSQL_CLIENT_CONNECT
# }

# test_format "PARQUET"

# echo "drop table if exists test_load_unload" | $BENDSQL_CLIENT_CONNECT
