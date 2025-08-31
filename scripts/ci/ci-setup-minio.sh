 #!/bin/bash
 # Copyright 2020-2021 The Databend Authors.
 # SPDX-License-Identifier: Apache-2.0.

set -ex

docker run -d --network host --name minio \
				-e "MINIO_ACCESS_KEY=minioadmin" \
				-e "MINIO_SECRET_KEY=minioadmin" \
				-e "MINIO_ADDRESS=:9900" \
				-v /tmp/data:/data \
				-v /tmp/config:/root/.minio \
				minio/minio server /data

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_EC2_METADATA_DISABLED=true

aws --endpoint-url http://127.0.0.1:9900/ s3 mb s3://testbucket
aws --endpoint-url http://127.0.0.1:9900/ s3 mb s3://spillbucket
aws --endpoint-url http://127.0.0.1:9900/ s3 mb s3://backupbucket

if [ "$TEST_MINIO_ENABLE_VERSION:-false" = "true" ]; then
	aws --endpoint-url http://127.0.0.1:9900/ s3api put-bucket-versioning --bucket testbucket --versioning-configuration Status=Enabled
fi

if [ "$TEST_MINIO_CP_DATA:-false" = "true" ]; then
	aws --endpoint-url http://127.0.0.1:9900/ s3 cp tests/data s3://testbucket/data  --recursive --no-progress
fi
