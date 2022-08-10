#!/bin/bash

for t in customer lineitem nation orders partsupp part region supplier
do
    echo "$t"
    curl -XPUT 'http://root:@127.0.0.1:8000/v1/streaming_load' -H 'insert_sql: insert into tpch.'$t' format CSV' -H 'skip_header: 0' -H 'field_delimiter:|' -H 'record_delimiter: \n' -F 'upload=@"data/'$t'.tbl"'
done
