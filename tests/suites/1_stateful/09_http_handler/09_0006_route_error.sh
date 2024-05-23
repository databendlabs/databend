#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

QID="my_query_for_route_${RANDOM}"
NODE=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H "x-databend-query-id:${QID}"  -H 'Content-Type: application/json' -d '{"sql": "select 1;"}' | jq -r ".session.last_server_info.id")
echo "## kill"
curl -s -u root: -XGET -w "\n"  "http://localhost:8000/v1/query/${QID}/XXX/kill" | sed "s/${QID}/QID/g" | sed "s/${NODE}/NODE/g" | sed 's/at.*secs/.../'
echo "## page"
curl -s -u root: -XGET -w "\n" "http://localhost:8000/v1/query/${QID}/XXX/page/0" | sed "s/${QID}/QID/g" | sed "s/${NODE}/NODE/g" | sed 's/at.*secs/.../'
echo "## final"
curl -s -u root: -XGET -w "\n" "http://localhost:8000/v1/query/${QID}/XXX/final" | sed "s/${QID}/QID/g" | sed "s/${NODE}/NODE/g" | sed 's/at.*secs/.../'
