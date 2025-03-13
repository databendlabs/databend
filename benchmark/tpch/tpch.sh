#!/usr/bin/env bash


echo """
INSTALL tpch;
LOAD tpch;
SELECT * FROM dbgen(sf=1); -- sf can be other values, such as 0.1, 1, 10, ...
EXPORT DATABASE '/tmp/tpch_1/' (FORMAT CSV, DELIMITER '|');
""" | duckdb

mv /tmp/tpch_1/ "$(pwd)/data/"

if [[ $2 == native ]]; then
  echo "native"
  sh ./load_data.sh "storage_format = 'native' compression = 'lz4'"
else
  echo "fuse"
  sh ./load_data.sh ""
fi

