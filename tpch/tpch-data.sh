#!/bin/bash

# Construct a docker imagine to generate tpch-data
docker build  -f tpchdata.dockerfile -t databend:latest .

# Generate data into the ./data directory if it does not already exist
FILE=./data/customer.tbl
if test -f "$FILE"; then
    echo "$FILE exists."
else
  mkdir data 2>/dev/null
  docker run -v `pwd`/data:/data --rm databend:latest
  ls -l data
fi
