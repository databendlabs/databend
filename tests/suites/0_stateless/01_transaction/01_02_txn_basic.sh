#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

post() {
	echo ">>>> curl $1: $2 | jq $3"
	curl -s -u root: -XPOST "http://localhost:8000/v1/query" --header 'Content-Type: application/json' --header x-databend-query-id:"$1" -d "$2" | jq "$3"
	echo "<<<<"
}

post 'Q1' '{"sql": "begin", "pagination": { "wait_time_secs": 2}}' ".session.txn_state"

post 'Q2' '{"sql": "commit", "pagination": { "wait_time_secs": 2}}' ".session.txn_state"

post 'Q3' '{"sql": "select 1", "pagination": { "wait_time_secs": 2}}' ".session.txn_state"

post 'Q4' '{"sql": "select 1", "pagination": { "wait_time_secs": 2}, "session": {"txn_state": "Fail"}}' ".session.txn_state, .error.code"
post 'Q5' '{"sql": "select 1", "pagination": { "wait_time_secs": 2}, "session": {"txn_state": "Active"}}' ".session.txn_state, .error.code"
