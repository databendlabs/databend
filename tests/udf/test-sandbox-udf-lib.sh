#!/bin/bash

check_response_error() {
	local response="$1"
	local error_msg
	error_msg=$(echo "$response" | jq -r 'if .state == "Failed" then .error.message else empty end')

	if [ -n "$error_msg" ]; then
		echo "[Test Error] $error_msg" >&2
		exit 1
	fi
}

execute_query() {
	local sql="$1"
	local settings_json="${2:-}"
	local payload
	if [ -n "$settings_json" ]; then
		payload=$(jq -n --arg sql "$sql" --argjson settings "$settings_json" \
			'{sql: $sql, settings: $settings}')
	else
		payload=$(jq -n --arg sql "$sql" '{sql: $sql}')
	fi
	local response
	response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
		-H 'Content-Type: application/json' \
		-d "$payload")
	check_response_error "$response"
	echo "$response"
}

collect_query_data() {
	local response="$1"
	local data next_uri page
	local start_ts elapsed
	data=$(echo "$response" | jq -c '.data // []')
	next_uri=$(echo "$response" | jq -r '.next_uri // empty')
	start_ts=$(date +%s)

	while [ -n "$next_uri" ]; do
		elapsed=$(($(date +%s) - start_ts))
		if [ "$elapsed" -ge "$UDF_QUERY_TIMEOUT_SECS" ]; then
			echo "[Test Error] query timed out after ${UDF_QUERY_TIMEOUT_SECS}s" >&2
			exit 1
		fi

		response=$(curl -s -u root: "http://localhost:8000${next_uri}")
		check_response_error "$response"
		page=$(echo "$response" | jq -c '.data // []')
		data=$(jq -c --argjson acc "$data" --argjson page "$page" -n '$acc + $page')
		next_uri=$(echo "$response" | jq -r '.next_uri // empty')
		if [ -n "$next_uri" ] && [ "$page" = "[]" ]; then
			sleep 0.2
		fi
	done

	echo "$data"
}

check_result() {
	local sql="$1"
	local expected="$2"
	local settings_json="${3:-}"
	local response
	response=$(execute_query "$sql" "$settings_json")
	local actual
	actual=$(collect_query_data "$response")
	if [ "$actual" = "$expected" ]; then
		echo "✅ Query result matches expected"
	else
		echo "❌ Mismatch"
		echo "Expected: $expected"
		echo "Actual  : $actual"
		exit 1
	fi
}

execute_query_expect_error() {
	local sql="$1"
	local expected="$2"
	local settings_json="${3:-}"
	local payload
	if [ -n "$settings_json" ]; then
		payload=$(jq -n --arg sql "$sql" --argjson settings "$settings_json" \
			'{sql: $sql, settings: $settings}')
	else
		payload=$(jq -n --arg sql "$sql" '{sql: $sql}')
	fi
	local response
	response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
		-H 'Content-Type: application/json' \
		-d "$payload")
	local error_msg
	error_msg=$(echo "$response" | jq -r '.error.message // empty')
	if [ -z "$error_msg" ]; then
		echo "[Test Error] expected query failure but got: $response" >&2
		exit 1
	fi
	if [ -n "$expected" ] && ! echo "$error_msg" | grep -q "$expected"; then
		echo "[Test Error] expected error containing '$expected' but got: $error_msg" >&2
		exit 1
	fi
}

upload_stage_file() {
	local stage_name="$1"
	local relative_path="$2"
	local file_path="$3"
	local upload_response
	upload_response=$(curl -s -u root: -XPUT "http://localhost:8000/v1/upload_to_stage" \
		-H "x-databend-stage-name: ${stage_name}" \
		-H "x-databend-relative-path: ${relative_path}" \
		-F "file=@${file_path}")
	local upload_state
	upload_state=$(echo "$upload_response" | jq -r '.state')
	if [ "$upload_state" != "SUCCESS" ]; then
		echo "[Test Error] upload to stage failed: $upload_response" >&2
		exit 1
	fi
}

find_status_url() {
	local status_url=""
	for _ in $(seq 1 30); do
		if command -v rg >/dev/null 2>&1; then
			status_url=$(rg -o "http://[^ ]+/health" ./.databend/cloud-control.out | tail -n 1 || true)
		else
			status_url=$(grep -oE "http://[^ ]+/health" ./.databend/cloud-control.out | tail -n 1 || true)
		fi
		if [ -n "$status_url" ]; then
			echo "$status_url"
			return
		fi
		sleep 1
	done
	echo ""
}

check_health() {
	local health_url="$1"
	local code
	code=$(curl -s -o /dev/null -w "%{http_code}" "$health_url")
	if [ "$code" != "200" ]; then
		echo "[Test Error] health endpoint status code: $code" >&2
		exit 1
	fi
	local body
	body=$(curl -s "$health_url")
	if [ "$body" != "1" ]; then
		echo "[Test Error] health endpoint returned unexpected body: $body" >&2
		exit 1
	fi
}

check_stats() {
	local stats_url="$1"
	local response
	response=$(curl -s "$stats_url")
	echo "Stats response: $response"
	if ! echo "$response" | jq -e \
		'has("request_count") and has("first_request_time") and has("last_request_time") and (.request_count >= 1)' >/dev/null; then
		echo "[Test Error] stats endpoint returned invalid payload: $response" >&2
		exit 1
	fi
}
