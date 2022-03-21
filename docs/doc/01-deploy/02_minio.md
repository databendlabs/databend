---
title: Deploy Databend With MinIO
sidebar_label: With MinIO
description:
  How to deploy Databend with MinIO
---

:::tip

Expected deployment time: ** 5 minutes ⏱ **

:::

This guideline will deploy Databend(standalone) with MinIO step by step.

<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/deploy-minio-standalone.png" width="300"/>
</p>


## 1. Deploy MinIO

Run a standalone MinIO server via Docker:
```shell
docker run -d -p 9900:9000 --name minio \
  -e "MINIO_ACCESS_KEY=minioadmin" \
  -e "MINIO_SECRET_KEY=minioadmin" \
  -v /tmp/data:/data \
  -v /tmp/config:/root/.minio \
  minio/minio server /data
```

We recommend using [aws cli](https://aws.amazon.com/cli/) to create a new MinIO bucket:

```shell
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_EC2_METADATA_DISABLED=true
aws --endpoint-url http://127.0.0.1:9900/ s3 mb s3://databend
```

## 2. Download

You can find the latest binaries on the [github release](https://github.com/datafuselabs/databend/releases) page.

```shell
mkdir databend && cd databend
```

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="operating-systems">
<TabItem value="linux" label="Ubuntu">

```shell
curl -LJO https://github.com/datafuselabs/databend/releases/download/v0.6.96-nightly/databend-v0.6.96-nightly-x86_64-unknown-linux-gnu.tar.gz
```

</TabItem>
<TabItem value="mac" label="MacOS">

```shell
curl -LJO https://github.com/datafuselabs/databend/releases/download/v0.6.96-nightly/databend-v0.6.96-nightly-aarch64-apple-darwin.tar.gz
```

</TabItem>

<TabItem value="arm" label="Arm">

```shell
curl -LJO https://github.com/datafuselabs/databend/releases/download/v0.6.96-nightly/databend-v0.6.96-nightly-aarch64-unknown-linux-gnu.tar.gz
```

</TabItem>
</Tabs>

<Tabs groupId="operating-systems">
<TabItem value="linux" label="Ubuntu">

```shell
tar xzvf databend-v0.6.96-nightly-x86_64-unknown-linux-gnu.tar.gz
```

</TabItem>
<TabItem value="mac" label="MacOS">

```shell
tar xzvf databend-v0.6.96-nightly-aarch64-apple-darwin.tar.gz
```

</TabItem>

<TabItem value="arm" label="Arm">

```shell
tar xzvf databend-v0.6.96-nightly-aarch64-unknown-linux-gnu.tar.gz
```

</TabItem>
</Tabs>

## 3. Deploy databend-meta (Standalone)

databend-meta is a global service for the meta data(such as user, table schema etc.).

### 3.1 Create databend-meta.toml

```shell title="databend-meta.toml"
log_dir = "metadata/_logs"
admin_api_address = "127.0.0.1:8101"
grpc_api_address = "127.0.0.1:9101"

[raft_config]
id = 1
single = true
raft_dir = "metadata/datas"
```

### 3.2 Start the databend-meta

```shell
./databend-meta -c ./databend-meta.toml > meta.log 2>&1 &
```

### 3.3 Check databend-meta

```shell
curl -I  http://127.0.0.1:8101/v1/health
```

Check the response is `HTTP/1.1 200 OK`.


## 4. Deploy databend-query (Standalone)

### 4.1 Create databend-query.toml

```shell title="databend-query.toml"
[log]
log_level = "INFO"
log_dir = "benddata/_logs"

[query]
# For admin RESET API.
admin_api_address = "127.0.0.1:8001"

# Metrics.
metric_api_address = "127.0.0.1:7071"

# Cluster flight RPC.
flight_api_address = "127.0.0.1:9091"

# Query MySQL Handler.
mysql_handler_host = "127.0.0.1"
mysql_handler_port = 3307

# Query ClickHouse Handler.
clickhouse_handler_host = "127.0.0.1"
clickhouse_handler_port = 9001

# Query HTTP Handler.
http_handler_host = "127.0.0.1"
http_handler_port = 8081

tenant_id = "tenant1"
cluster_id = "cluster1"

[meta]
# databend-meta grpc api address. 
meta_address = "127.0.0.1:9101"
meta_username = "root"
meta_password = "root"


[storage]
# disk|s3
storage_type = "s3"

[storage.disk]

[storage.s3]
bucket = "databend"
// highlight-next-line
endpoint_url = "http://127.0.0.1:9900"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
```

### 4.2 Start databend-query

```shell
./databend-query -c ./databend-query.toml > query.log 2>&1 &
```

### 4.3 Check databend-query 

```shell
curl -I  http://127.0.0.1:8001/v1/health
```

Check the response is `HTTP/1.1 200 OK`.

## 5. Play

```shell
mysql -h127.0.0.1 -uroot -P3307 
```

```shell title="mysql>"
create table t1(a int);
```

```shell title="mysql>"
insert into t1 values(1), (2);
```

```shell title="mysql>"
select * from t1
```

```shell"
+------+
| a    |
+------+
|    1 |
|    2 |
+------+
```
