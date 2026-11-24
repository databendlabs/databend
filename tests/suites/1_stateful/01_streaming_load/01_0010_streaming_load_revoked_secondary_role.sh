#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

DATA="/tmp/streaming_load_10"
rm -rf $DATA
mkdir $DATA
echo '1' >$DATA/data.csv

stmt "DROP USER IF EXISTS streaming_load_10_user;"
stmt "DROP ROLE IF EXISTS streaming_load_10_role;"
stmt "CREATE OR REPLACE TABLE streaming_load_10 (c1 int);"
stmt "CREATE USER streaming_load_10_user IDENTIFIED BY 'password';"
stmt "CREATE ROLE streaming_load_10_role;"
stmt "GRANT INSERT ON default.streaming_load_10 TO streaming_load_10_user;"
stmt "GRANT ROLE streaming_load_10_role TO streaming_load_10_user;"
stmt "REVOKE ROLE streaming_load_10_role FROM streaming_load_10_user;"

# Send the session as the client holds it, then keep the session from the response
# header, as clients do even when the load fails.
load() {
	curl -sS -o "$DATA/body" -D "$DATA/headers" -w '%{http_code}\n' \
		-H "X-Databend-Query-Context:$1" \
		-H "X-Databend-SQL:insert into streaming_load_10 from @_databend_load file_format = (type=csv)" \
		-F "upload=@$DATA/data.csv" -u streaming_load_10_user:password \
		-XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load"
	grep -i '^x-databend-query-context:' "$DATA/headers" | cut -d ':' -f 2- | tr -d ' \r' | python3 -c '
import base64, json, sys
value = sys.stdin.read().strip()
if not value.startswith("{"):
    value = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)).decode()
print(value)' >"$DATA/session"
	python3 -c 'import json, sys; print("secondary_roles:", json.dumps(json.load(open(sys.argv[1])).get("secondary_roles")))' "$DATA/session"
}

echo "-- a session that still names the revoked role fails once and gets NONE back"
load '{"secondary_roles":["streaming_load_10_role"]}'
grep -c "Invalid secondary role streaming_load_10_role" "$DATA/body"

echo "-- replaying the returned session succeeds"
load "$(cat "$DATA/session")"

stmt "SELECT count(*) FROM streaming_load_10;"
stmt "DROP USER streaming_load_10_user;"
stmt "DROP ROLE streaming_load_10_role;"
stmt "DROP TABLE streaming_load_10;"
