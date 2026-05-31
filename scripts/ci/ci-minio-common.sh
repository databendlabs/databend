#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

setup_minio_storage_env() {
	echo "*************************************"
	echo "* Setting STORAGE_TYPE to S3.       *"
	echo "*                                   *"
	echo "* Please make sure that S3 backend  *"
	echo "* is ready, and configured properly.*"
	echo "*************************************"
	export STORAGE_TYPE=s3
	export STORAGE_S3_BUCKET=testbucket
	export STORAGE_S3_ROOT=admin
	export STORAGE_S3_ENDPOINT_URL=http://127.0.0.1:9900
	export STORAGE_S3_ACCESS_KEY_ID=minioadmin
	export STORAGE_S3_SECRET_ACCESS_KEY=minioadmin
	export STORAGE_ALLOW_INSECURE=true
}

append_minio_spill_config() {
	export SPILL_SPILL_LOCAL_DISK_PATH=''

	for query_config in "$@"; do
		cat >>"$query_config" <<'EOF'

[spill.storage]
type = "s3"

[spill.storage.s3]
bucket = "spillbucket"
root = "admin"
endpoint_url = "http://127.0.0.1:9900"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
allow_insecure = true
EOF
	done
}

has_minio_sqllogic_suite() {
	case " $* " in
	*" tpch "* | *" tpch,"* | *",tpch "* | *",tpch,"*) return 0 ;;
	*" tpcds "* | *" tpcds,"* | *",tpcds "* | *",tpcds,"*) return 0 ;;
	*) return 1 ;;
	esac
}
