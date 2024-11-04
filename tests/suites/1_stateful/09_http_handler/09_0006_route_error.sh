#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

QID="my_query_for_route_${RANDOM}"
NODE=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H "x-databend-query-id:${QID}"  -H 'Content-Type: application/json' -d '{"sql": "select 1;"}' | jq -r ".session.last_server_info.id")
echo "# error"
echo "## page"
curl -s -u root: -XGET -H "x-databend-node-id:XXX"  -w "\n" "http://localhost:8000/v1/query/${QID}/page/0" | sed "s/${QID}/QID/g" | sed "s/${NODE}/NODE/g" | sed 's/at.*secs/.../'
echo "## kill"
curl -s -u root: -XGET -H "x-databend-node-id:XXX" -w "\n"  "http://localhost:8000/v1/query/${QID}/kill" | sed "s/${QID}/QID/g" | sed "s/${NODE}/NODE/g" | sed 's/at.*secs/.../'
echo "## final"
curl -s -u root: -XGET -H "x-databend-node-id:XXX" -w "\n" "http://localhost:8000/v1/query/${QID}/final" | sed "s/${QID}/QID/g" | sed "s/${NODE}/NODE/g" | sed 's/at.*secs/.../'

echo ""

echo "# ok"
echo "## page"
curl -s -u root: -XGET -H "x-databend-node-id:${NODE}"  -w "\n" "http://localhost:8000/v1/query/${QID}/page/0" | jq -c ".data"
echo "## kill"
curl -s -u root: -XGET -H "x-databend-node-id:${NODE}" -w "%{http_code}\n"  "http://localhost:8000/v1/query/${QID}/kill"
echo "## final"
curl -s -u root: -XGET -H "x-databend-node-id:${NODE}" -w "\n" "http://localhost:8000/v1/query/${QID}/final" | jq ".next_uri"
