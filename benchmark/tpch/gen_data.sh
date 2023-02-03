#!/usr/bin/env bash

rm -rf ./data/*
docker pull ghcr.io/databloom-ai/tpch-docker:main
docker run -it -v "$(pwd)/data":/data ghcr.io/databloom-ai/tpch-docker:main dbgen -vf -s $1