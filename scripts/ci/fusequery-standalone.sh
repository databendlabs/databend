#!/bin/bash

killall fuse-query
sleep 1

echo 'Start...'
nohup target/debug/fuse-query -c scripts/ci/config/fusequery-cluster-1.toml &

echo "Waiting on..."
while ! nc -z -w 5 0.0.0.0 3307; do
  sleep 1
done
