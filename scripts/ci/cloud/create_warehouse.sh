#!/bin/bash

set -e

CLOUD_USER=${CLOUD_USER:-}
CLOUD_PASSWORD=${CLOUD_PASSWORD:-}
CLOUD_GATEWAY=${CLOUD_GATEWAY:-}

WAREHOUSE_SIZE=${WAREHOUSE_SIZE:-XSmall}
WAREHOUSE_VERSION=${WAREHOUSE_VERSION:-}
WAREHOUSE_NAME=${WAREHOUSE_NAME:-}

for var in CLOUD_USER CLOUD_PASSWORD CLOUD_GATEWAY WAREHOUSE_SIZE WAREHOUSE_VERSION WAREHOUSE_NAME; do
	if [[ -z "${!var}" ]]; then
		echo "Please set ${var} to create warehouse."
		exit 1
	fi
done

echo "#######################################################"
echo "Creating warehouse on Databend Cloud..."

export BENDSQL_DSN="databend://${CLOUD_USER}:${CLOUD_PASSWORD}@${CLOUD_GATEWAY}:443?login=disable&warehouse=default"

echo "Creating warehouse: ${WAREHOUSE_NAME}"
echo "DROP WAREHOUSE IF EXISTS '${WAREHOUSE_NAME}';" | bendsql
echo "CREATE WAREHOUSE '${WAREHOUSE_NAME}' WITH version='${WAREHOUSE_VERSION}' warehouse_size='${WAREHOUSE_SIZE}';" | bendsql
echo "SHOW WAREHOUSES;" | bendsql --output table

echo "Waiting for warehouse to be ready..."
max_retry=20
counter=0
until bendsql --query="SHOW WAREHOUSES LIKE '${WAREHOUSE_NAME}'" | grep -q "Running"; do
	if [[ $counter -gt $max_retry ]]; then
		echo "Failed to start warehouse ${WAREHOUSE_NAME} in time."
		exit 1
	fi
	echo "Waiting for warehouse to be ready... (attempt $((counter + 1))/$((max_retry + 1)))"
	counter=$((counter + 1))
	sleep 10
done

echo "Warehouse ${WAREHOUSE_NAME} is now running!"
