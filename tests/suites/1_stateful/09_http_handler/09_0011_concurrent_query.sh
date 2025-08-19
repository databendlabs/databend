session_id="065142ee-68e8-470b-a4d6-2ed67f90e2ae"
curl -s -u root: -XPOST "http://127.0.0.1:8000/v1/query"  \
 -H 'x-databend-query-id: qq1' \
 -H 'Content-Type: application/json' \
 -d '{"sql": "select * from numbers(10);","pagination": {"max_rows_per_page": 2}}' \
 -b "cookie_enabled=true;session_id=${session_id}" | jq "(.data | length), .error,.session.need_sticky"

echo

curl -s -u root: -XPOST "http://127.0.0.1:8000/v1/query"  \
 -H 'x-databend-query-id: qq2' \
 -H 'Content-Type: application/json' \
 -d '{"sql": "select * from numbers(10);", "session": {"internal": "{\"variables\": [], \"last_query_result_cache_key\":\"x\"}", "last_query_ids":["qq1"]}}' \
 -b "cookie_enabled=true;session_id=${session_id}" | jq  "(.data| length), .error, .session.need_sticky"

 echo

# session field should be empty
curl -s -u root: -XGET  "http://127.0.0.1:8000/v1/query/qq1/page/1"  \
  -b "cookie_enabled=true;session_id=${session_id}"| jq  "(.data| length), .error,.session"
