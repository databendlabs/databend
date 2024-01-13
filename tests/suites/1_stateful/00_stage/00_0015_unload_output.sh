#!/usr/bin/env bash
# most features are tested in sqllogic tests with diff type of external stages
# this test is mainly to test internal stage and user stage (and paths) is parsed and used correctly, the file content is not important

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export MYSQL="mysql -uroot --host 127.0.0.1 --port 3307  -s"
export RM_UUID="sed -E ""s/[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}/UUID/g"""

stmt "drop table if exists t1"
stmt "create table t1 (a int)"

stmt "drop stage if exists s1"
stmt "create stage s1"

for i in `seq 0 9`;do
		echo "insert into t1 values($i)" | $MYSQL
done

echo ">>>> unload csv with detailed_output=true"
echo "copy into @s1/a/bc from (select * from t1) file_format = (type=csv) max_file_size=1 detailed_output=true" | $MYSQL | $RM_UUID | sort

echo ">>>> unload csv with detailed_output=false"
echo "copy into @s1/a/bc from (select * from t1) file_format = (type=csv) max_file_size=1 detailed_output=false" | $MYSQL

echo ">>>> unload parquet with detailed_output=true"
echo "copy into @s1/a/bc from (select * from t1)  max_file_size=1 detailed_output=true" | $MYSQL | $RM_UUID | sort

echo ">>>> unload parquet with detailed_output=false"
echo "copy into @s1/a/bc from (select * from t1)  max_file_size=1 detailed_output=false" | $MYSQL

stmt "drop stage if exists s1"
stmt "drop table if exists t1"
