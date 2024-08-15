#!/bin/sh

set -o errexit

BUILD_PROFILE="${BUILD_PROFILE:-debug}"
DATABEND_META="./target/${BUILD_PROFILE}/databend-meta"

echo " === start a single node databend-meta"
chmod +x ${DATABEND_META}
${DATABEND_META} --single &
METASRV_PID=$!
echo $METASRV_PID

echo " === test kvapi::upsert"
${DATABEND_META} --grpc-api-address "127.0.0.1:9191" --cmd kvapi::upsert --key 1:key1 --value value1
${DATABEND_META} --grpc-api-address "127.0.0.1:9191" --cmd kvapi::upsert --key 1:key2 --value value2
${DATABEND_META} --grpc-api-address "127.0.0.1:9191" --cmd kvapi::upsert --key 1:key3 --value value3
${DATABEND_META} --grpc-api-address "127.0.0.1:9191" --cmd kvapi::upsert --key 2:key1 --value value1
${DATABEND_META} --grpc-api-address "127.0.0.1:9191" --cmd kvapi::upsert --key 2:key2 --value value2

echo " === test kvapi::get"
${DATABEND_META} --grpc-api-address "127.0.0.1:9191" --cmd kvapi::get --key 1:key1
${DATABEND_META} --grpc-api-address "127.0.0.1:9191" --cmd kvapi::get --key 2:key2

echo " === test kvapi::mget"
${DATABEND_META} --grpc-api-address "127.0.0.1:9191" --cmd kvapi::mget --key 1:key1 2:key2

echo " === test kvapi::list"
${DATABEND_META} --grpc-api-address "127.0.0.1:9191" --cmd kvapi::list --prefix 1:
${DATABEND_META} --grpc-api-address "127.0.0.1:9191" --cmd kvapi::list --prefix 2:

kill $METASRV_PID
