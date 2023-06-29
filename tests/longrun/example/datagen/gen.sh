#!/bin/bash


DSN=$WAREHOUSE_DSN

bendsql --dsn $DSN -q 'create table if not exists example_db_gen_table(id int, message string, time timestamp) engine=Random'
for ((i=1; i<=10; i++)); do
    output_file="$(dirname "${BASH_SOURCE[0]}")/_datagen${i}.csv"
    bendsql --dsn $DSN -q "SELECT * FROM example_db_gen_table WHERE time > to_timestamp('2000-01-01') order by time ASC LIMIT 10000 " -o csv >> "$output_file"
    echo "Generated $output_file"
done