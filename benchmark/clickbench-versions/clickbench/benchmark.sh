#!/bin/bash
export BENCH_DOWNLOAD_SCRIPT="download-hits-tsv"
# databend's first cold start (meta + query coming up together, opening
# every object-store dir) regularly runs past the lib's 300s default;
# the original benchmark used a 600s wait, restore that.
export BENCH_CHECK_TIMEOUT=600
exec ../lib/benchmark-common.sh
