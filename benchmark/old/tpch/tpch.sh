#!/usr/bin/env bash


echo """
INSTALL tpch;
LOAD tpch;
SELECT * FROM dbgen(sf=1); -- sf can be other values, such as 0.1, 1, 10, ...
EXPORT DATABASE '/tmp/tpch_1/' (FORMAT CSV, DELIMITER '|');
""" | duckdb

mv /tmp/tpch_1/ "$(pwd)/data/"

# `parquet` is the only supported storage format; an empty option string lets
# the table fall back to it.
sh ./load_data.sh ""

