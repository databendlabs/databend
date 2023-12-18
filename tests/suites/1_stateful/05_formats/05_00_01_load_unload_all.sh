#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists test_load_unload" | $BENDSQL_CLIENT_CONNECT

# todo(youngsofun): add more types
echo "CREATE TABLE test_load_unload
(
    a VARCHAR NULL,
    b float,
    c array(string),
    d Variant,
    e timestamp,
    f decimal(4, 2),
    g map(string,int),
    h tuple(string,int)
);" | $BENDSQL_CLIENT_CONNECT

insert_data() {
	echo "insert into test_load_unload values
	('a\"b', 1, ['a\"b'], parse_json('{\"k\":\"v\"}'), '2044-05-06T03:25:02.868894-07:00', 010.011, {'k1':10,'k2':20}, ('a', 5)),
	(null, 2, ['a\'b'], parse_json('[1]'), '2044-05-06T03:25:02.868894-07:00', -010.011, {}, ('b',10))
	" | $BENDSQL_CLIENT_CONNECT
}

test_format() {
	echo "---${1}"
	rm -rf /tmp/test_load_unload
	mkdir /tmp/test_load_unload
	echo "drop stage if exists s1" | $BENDSQL_CLIENT_CONNECT
	echo "create stage s1 url='fs:///tmp/test_load_unload/'" | $BENDSQL_CLIENT_CONNECT
	echo "truncate table test_load_unload" | $BENDSQL_CLIENT_CONNECT

	# insert
	insert_data

	# unload1
	echo "copy into @s1/unload1/ from test_load_unload file_format=(type=${1})" | $BENDSQL_CLIENT_CONNECT
	mv `ls /tmp/test_load_unload/unload1/*` /tmp/test_load_unload/unload1.txt
	cat /tmp/test_load_unload/unload1.txt

	# load unload1
	echo "truncate table test_load_unload" | $BENDSQL_CLIENT_CONNECT
	echo "copy into test_load_unload from @s1/unload1.txt file_format=(type=${1}) force=true" | $BENDSQL_CLIENT_CONNECT

	# unload2
	echo "copy into @s1/unload2/ from test_load_unload file_format=(type=${1})" | $BENDSQL_CLIENT_CONNECT
  mv `ls /tmp/test_load_unload/unload2/*` /tmp/test_load_unload/unload2.txt
  diff /tmp/test_load_unload/unload1.txt /tmp/test_load_unload/unload2.txt
}

test_format "CSV"

test_format "TSV"

test_format "NDJSON"

echo "drop table if exists test_load_unload" | $BENDSQL_CLIENT_CONNECT
