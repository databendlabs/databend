#!/bin/bash

rm -f benchmark/clickbench/tpch/queries.sql

for line in tests/suites/0_stateless/13_tpch/13_*.sql; do
    tr '\n' ' ' <"$line" | sed -e 's/ \{2,\}/ /g' | sed -e 's/[\ ]*$/\n/g' | sed -e 's/^ //g' >>benchmark/clickbench/tpch/queries.sql
done
