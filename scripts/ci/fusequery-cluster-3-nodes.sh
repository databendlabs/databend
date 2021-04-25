#!/bin/bash

killall fuse-query
sleep 1

echo 'start cluster-1'
nohup ../../target/release/fuse-query --rpc-api-address=0.0.0.0:9091 --http-api-address=0.0.0.0:8081 --mysql-handler-port=3307 --metric-api-address=0.0.0.0:7071 &

echo "Waiting on cluster-1..."
while ! nc -z localhost 9091; do
  sleep 0.1
done

while ! nc -z localhost 8081; do
  sleep 0.1
done

echo 'start cluster-2'
nohup ../../target/release/fuse-query --rpc-api-address=0.0.0.0:9092 --http-api-address=0.0.0.0:8082 --mysql-handler-port=3308 --metric-api-address=0.0.0.0:7072 &

echo "Waiting on cluster-2..."
while ! nc -z localhost 9092; do
  sleep 0.1
done

echo 'start cluster-3'
nohup ../../target/release/fuse-query --rpc-api-address=0.0.0.0:9093 --http-api-address=0.0.0.0:8083 --mysql-handler-port=3309 --metric-api-address=0.0.0.0:7073 &

echo "Waiting on cluster-3..."
while ! nc -z localhost 9093; do
  sleep 0.1
done

curl http://127.0.0.1:8081/v1/cluster/add -X POST -H "Content-Type: application/json" -d '{"name":"cluster1","address":"0.0.0.0:9091", "priority":3, "cpus":8}'
curl http://127.0.0.1:8081/v1/cluster/add -X POST -H "Content-Type: application/json" -d '{"name":"cluster2","address":"0.0.0.0:9092", "priority":3, "cpus":8}'
curl http://127.0.0.1:8081/v1/cluster/add -X POST -H "Content-Type: application/json" -d '{"name":"cluster3","address":"0.0.0.0:9093", "priority":1, "cpus":8}'

echo "All done..."

