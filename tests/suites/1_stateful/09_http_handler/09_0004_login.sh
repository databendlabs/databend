#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stmt "drop table if exists t1;"
stmt "drop table if exists t2;"
stmt "create table t1 (a int);"

echo "# auth fail"
curl -s --header 'Content-Type: application/json'  --request POST '127.0.0.1:8000/v1/login/'  --data-raw '{}' -u user1: | jq -c ".error"
echo "# empty body"
curl -s --header 'Content-Type: application/json'  --request POST '127.0.0.1:8000/v1/login/'  -u root: | jq -c ".error"
echo "# db"
curl -s --header 'Content-Type: application/json'  --request POST '127.0.0.1:8000/v1/login/'  --data-raw '{"database":"t1"}' -u root: | jq -c ".error"
echo "# db not exists"
curl -s --header 'Content-Type: application/json'  --request POST '127.0.0.1:8000/v1/login/'  --data-raw '{"database":"t2"}' -u root: | jq -c ".error"
echo "# allow unknown key"
curl -s --header 'Content-Type: application/json'  --request POST '127.0.0.1:8000/v1/login/'  --data-raw '{"unknown": null}' -u root:xx | jq -c ".error"

stmt "drop table if exists t1;"
