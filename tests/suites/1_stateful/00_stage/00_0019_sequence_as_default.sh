#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

name="sequence_as_default"

path="/tmp/$name/"
# rm -r $path/*

stmt "create or replace stage ${name} url='fs://$path'"
stmt "create or replace table src1(a int)"
stmt "insert into src1 values (1), (2)"
stmt "create or replace table src2(seq int, a int)"
stmt "insert into src2 values (-1, 3), (-2, 4)"

for fmt in "csv" "ndjson" "parquet" "tsv"
do
	 echo "---- ${fmt}"
	 stmt "remove @${name}"
	 stmt "create or replace sequence seq"
   stmt "create or replace table dest(seq int default nextval(seq), a int)"
	 stmt "copy INTO @${name}/src1/ from src1 file_format=(type=${fmt});"
	 stmt "copy INTO @${name}/src2/ from src2 file_format=(type=${fmt});"
	 stmt "copy INTO dest(a) from @${name}/src1 file_format=(type=${fmt}) return_failed_only=true;"
	 stmt "copy INTO dest from @${name}/src2 file_format=(type=${fmt}) return_failed_only=true;"
	 query "select * from dest order by seq"
done

