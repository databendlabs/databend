#!/usr/bin/env bash

# generate tpch data
sh ./gen_data.sh $1

if [[ $2 == native ]]; then
  echo "native"
  sh ./prepare_native_table.sh
else
  echo "fuse"
  sh ./prepare_fuse_table.sh
fi

