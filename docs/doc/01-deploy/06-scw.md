---
title: Deploy Databend With Scaleway OS
sidebar_label: With Scaleway OS
description:
  How to deploy Databend with Scaleway OS
---

:::tip

Expected deployment time: ** 5 minutes ⏱ **

:::

This guideline will deploy Databend(standalone) with Scaleway OS step by step.

<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/deploy-scaleway-standalone.png" width="300"/>
</p>


### Before you begin

* **SCW:** Scaleway OS is a S3-like object storage.
  * [Object Storage FAQ](https://www.scaleway.com/en/docs/faq/objectstorage/)

## 1. Download

You can find the latest binaries on the [github release](https://github.com/datafuselabs/databend/releases) page or [build from source](../06-contributing/01-building-from-source.md).

```shell
mkdir databend && cd databend
```
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="operating-systems">
<TabItem value="linux" label="Linux">

```shell
curl -LJO https://github.com/datafuselabs/databend/releases/download/v0.7.4-nightly/databend-v0.7.4-nightly-x86_64-unknown-linux-musl.tar.gz
```

</TabItem>
</Tabs>

<Tabs groupId="operating-systems">
<TabItem value="linux" label="Linux">

```shell
tar xzvf databend-v0.7.4-nightly-x86_64-unknown-linux-musl.tar.gz
```

</TabItem>
</Tabs>

## 2. Deploy databend-meta (Standalone)

databend-meta is a global service for the meta data(such as user, table schema etc.).

### 2.1 Create databend-meta.toml

```shell title="databend-meta.toml"
log_dir = "metadata/_logs"
admin_api_address = "127.0.0.1:8101"
grpc_api_address = "127.0.0.1:9101"

[raft_config]
id = 1
single = true
raft_dir = "metadata/datas"
```

### 2.2 Start the databend-meta 

```shell
./databend-meta -c ./databend-meta.toml > meta.log 2>&1 &
```

### 2.3 Check databend-meta 

```shell
curl -I  http://127.0.0.1:8101/v1/health
```

Check the response is `HTTP/1.1 200 OK`.


## 3. Deploy databend-query (Standalone)

### 3.1 Create databend-query.toml

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
meta_address = "127.0.0.1:9101"
meta_username = "root"
meta_password = "root"

[storage]
# disk|s3
storage_type = "s3"

[storage.disk]

[storage.s3]
# How to create a bucket:
# https://www.scaleway.com/en/docs/faq/objectstorage/
// highlight-next-line
bucket = "<name of the bucket>"
# Known regions: nl-ams, fr-par, pl-waw
// highlight-next-line
region = "nl-ams"

# Change the url below to match your s3 region
// highlight-next-line
endpoint_url = "https://s3.nl-ams.scw.cloud"

# How to get access_key_id and secret_access_key:
# https://www.scaleway.com/en/docs/faq/objectstorage/
// highlight-next-line
access_key_id = "<your-access-key>"
// highlight-next-line
secret_access_key = "<your-secret-key>"

[storage.azure_storage_blob]
```

:::tip
In this example SCW region is `nl-ams`.
:::

### 3.2 Start databend-query

```shell
./databend-query -c ./databend-query.toml > query.log 2>&1 &
```

### 3.3 Check databend-query

```shell
curl -I  http://127.0.0.1:8001/v1/health
```

Check the response is `HTTP/1.1 200 OK`.

## 4. Play

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
