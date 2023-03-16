#!/usr/bin/env bash

# generate tpch data
sh ./gen_data.sh $1

if [[ $2 == native ]]; then
  echo "native"
  sh ./prepare_table.sh "storage_format = 'native' compression = 'lz4'"
else
  echo "fuse"
  sh ./prepare_table.sh ""
fi

