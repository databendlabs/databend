#!/usr/bin/env bash

 CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
 . "$CURDIR"/../../../../shell_env.sh

 stmt "drop table if exists test_load_unload"

 stmt "CREATE TABLE test_load_unload
 (
     a VARCHAR NULL,
     b float,
     c array(string not null),
     d Variant,
     e timestamp,
     f decimal(4, 2),
     g tuple(string not null,int not null),
     h array(variant),
     i array(tuple(string, int))
 );"

 insert_data() {
 	stmt "insert into test_load_unload values
 	('a\"b', 1, ['a\"b'], '{\"k\":\"v\"}', '2044-05-06T03:25:02.868894-07:00', 010.011, ('a', 5),
 		['{\"k\":\"v\"}'],
 		[('a',5)]
 	),
 	(null, 2, ['a\'b'], '[1]', '2044-05-06T03:25:02.868894-07:00', -010.011, ('b',10),
 		['[1]'],
 		[('b',10)]
 	);"
 }

 test_format() {
 	rm -rf /tmp/test_load_unload
 	mkdir /tmp/test_load_unload
 	stmt "drop stage if exists s1"
 	stmt "create stage s1 url='fs:///tmp/test_load_unload/'"
 	stmt "truncate table test_load_unload"

 	# insert data
 	insert_data

 	# unload1
 	query "select * from test_load_unload"
 	query "select * from test_load_unload" > /tmp/test_load_unload/select1.txt
 	stmt "copy into @s1/unload1/ from test_load_unload"
 	mv `ls /tmp/test_load_unload/unload1/*` /tmp/test_load_unload/unload1.parquet

 	# reload with copy into table
 	stmt "truncate table test_load_unload"
 	stmt "copy into test_load_unload from @s1/unload1.parquet force=true;"

 	# unload2
 	query "select * from test_load_unload" > /tmp/test_load_unload/select2.txt

	echo "begin diff select"
 	diff /tmp/test_load_unload/select1.txt /tmp/test_load_unload/select2.txt
	echo "end diff"

 	stmt "copy into @s1/unload2/ from test_load_unload"
 	mv `ls /tmp/test_load_unload/unload2/*` /tmp/test_load_unload/unload2.parquet


	echo "begin diff parquet"
 	diff /tmp/test_load_unload/unload2.parquet /tmp/test_load_unload/unload1.parquet
	echo "end diff"
 	stmt "truncate table test_load_unload"
 }

 test_format "PARQUET"

 echo "drop table if exists test_load_unload" | $BENDSQL_CLIENT_CONNECT
