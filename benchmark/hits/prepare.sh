#!/bin/bash
# Download ClickBench's hits.parquet to local disk and expose it as a stage,
# so the measured load time covers only the COPY from local disk, as in ClickBench.

set -e

dir="$(pwd)/data/hits_source"
mkdir -p "${dir}"
curl -fSL --retry 3 -o "${dir}/hits.parquet" https://datasets.clickhouse.com/hits_compatible/hits.parquet
sync

echo "CREATE STAGE IF NOT EXISTS hits_local URL = 'fs://${dir}/';" | bendsql
