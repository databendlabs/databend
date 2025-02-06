#!/usr/bin/env bash
# most features are tested in sqllogic tests with diff type of external stages
# this test is mainly to test internal stage and user stage (and paths) is parsed and used correctly, the file content is not important

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export RM_UUID="sed -E ""s/[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}/UUID/g"""

stmt "drop table if exists t1"
stmt "create table t1 (a int)"

stmt "drop stage if exists s1"
stmt "create stage s1"

for i in `seq 0 9`;do
		stmt "insert into t1 values($i)"
done

echo "copy1"
query "copy /*+ set_var(max_threads=1) */ into @s1/a/bc from (select * from t1) file_format = (type=csv) max_file_size=1 detailed_output=true" | $RM_UUID | tail -n +2 | sort

echo "copy2"
query "copy into @s1/a/bc from (select * from t1) file_format = (type=csv) max_file_size=1 detailed_output=false"

echo "copy3"
query "copy /*+ set_var(max_threads=1) */ into @s1/a/bc from (select * from t1)  max_file_size=1 detailed_output=true" | $RM_UUID | tail -n +2 | sort

query "copy into @s1/a/bc from (select * from t1)  max_file_size=1 detailed_output=false" | $MYSQL

echo ">>>> unload path"
query "copy /*+ set_var(max_threads=1) */ into @s1 from (select 1)  detailed_output=true" | $RM_UUID
query "copy /*+ set_var(max_threads=1) */ into @s1/ from (select 1)  detailed_output=true" | $RM_UUID
query "copy /*+ set_var(max_threads=1) */ into @s1/a from (select 1)  detailed_output=true" | $RM_UUID
query "copy /*+ set_var(max_threads=1) */ into @s1/a/ from (select 1)  detailed_output=true" | $RM_UUID
query "copy /*+ set_var(max_threads=1) */ into @s1/a/bc from (select 1)  detailed_output=true" | $RM_UUID
query "copy /*+ set_var(max_threads=1) */ into @s1/a/data_ from (select 1)  detailed_output=true" | $RM_UUID

stmt "drop stage if exists s1"
stmt "drop table if exists t1"
