#!/bin/bash

killall fuse-query
sleep 1

echo 'Start...'
nohup target/debug/fuse-query --rpc-api-address=0.0.0.0:9091 --http-api-address=0.0.0.0:8081 --mysql-handler-port=3307 --metric-api-address=0.0.0.0:7071 --log-level ERROR &

echo "Waiting on..."
while ! nc -z -w 5 0.0.0.0 3307; do
  sleep 1
done
