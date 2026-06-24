#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


echo "drop table if exists s_distinct;" | bendsql_connect_root
echo "create table s_distinct (a String not null);" | bendsql_connect_root

for i in `seq 1 20`;do
	echo "insert into s_distinct values ('$i'), ('$[i+1]'), ('$[i+2]')" | bendsql_connect_root
done

echo "select count(distinct a) from s_distinct" |  bendsql_connect_root

echo "drop table s_distinct;" | bendsql_connect_root
