#!/usr/bin/env bash

# generate tpch data
sh ./gen_data.sh $1

if [[ $2 == native ]]; then
  echo "native"
  sh ./create_table_native.sh
else
  echo "parquet"
  sh ./create_table.sh
fi

