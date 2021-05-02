#!/bin/bash

killall fuse-query
sleep 1

echo 'Start...'
nohup target/debug/fuse-query -c scripts/ci/config/fusequery-cluster-1.toml &

echo "Waiting on 10 seconds..."
timeout 10 sh -c 'until nc -z $0 $1; do sleep 1; done' 0.0.0.0 3307
