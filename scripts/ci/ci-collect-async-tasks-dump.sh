#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -u

OUTPUT_PATH=${1:-"./.databend/async_tasks_dump.log"}
INTERVAL_SECONDS=${2:-1}
ENDPOINTS=${3:-"http://127.0.0.1:8080/debug/async_tasks/dump,http://127.0.0.1:8082/debug/async_tasks/dump,http://127.0.0.1:8083/debug/async_tasks/dump"}

mkdir -p "$(dirname "${OUTPUT_PATH}")"
touch "${OUTPUT_PATH}"

IFS=',' read -r -a ENDPOINT_LIST <<< "${ENDPOINTS}"

{
    echo "=== async_tasks_dump collector started at $(date -u +"%Y-%m-%dT%H:%M:%SZ") ==="
    echo "interval_seconds=${INTERVAL_SECONDS}"
    echo "endpoints=${ENDPOINTS}"
} >> "${OUTPUT_PATH}"

while true; do
    TIMESTAMP="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    for endpoint in "${ENDPOINT_LIST[@]}"; do
        echo "" >> "${OUTPUT_PATH}"
        echo "===== ${TIMESTAMP} ${endpoint} =====" >> "${OUTPUT_PATH}"

        curl --silent --show-error --max-time 3 "${endpoint}" >> "${OUTPUT_PATH}" 2>&1
        CURL_EXIT_CODE=$?
        if [ "${CURL_EXIT_CODE}" -ne 0 ]; then
            echo "[collector] curl exit code: ${CURL_EXIT_CODE}" >> "${OUTPUT_PATH}"
        fi
    done

    sleep "${INTERVAL_SECONDS}"
done
