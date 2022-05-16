---
title: Deploy Databend With QingCloud QingStore
sidebar_label: With QingCloud QingStore
description: How to deploy Databend with QingCloud(青云) QingStore.
---
import GetLatest from '@site/src/components/GetLatest';

:::tip

Expected deployment time: ** 5 minutes ⏱ **

:::

This guideline will deploy Databend(standalone) with QingCloud(青云) QingStore step by step.

<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/deploy/deploy-qingcloud-standalone.png" width="300"/>
</p>


### Before you begin

* **QingStore:** QingCloud QingStore is a S3-like object storage.
  * [How to Create QingStore Bucket](https://docsv3.qingcloud.com/storage/object-storage/manual/console/bucket_manage/basic_opt/)
  * [How to Get QingStore access_key_id and secret_access_key](https://docs.qingcloud.com/product/api/common/overview.html)

## 1. Download

You can find the latest binaries on the [github release](https://github.com/datafuselabs/databend/releases) page or [build from source](../60-contributing/00-building-from-source.md).

```shell
mkdir databend && cd databend
```
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="operating-systems">
<TabItem value="linux" label="Linux">

```shell
curl -LJO https://github.com/datafuselabs/databend/releases/download/${version}/databend-${version}-x86_64-unknown-linux-musl.tar.gz
```

</TabItem>
</Tabs>

<Tabs groupId="operating-systems">
<TabItem value="linux" label="Linux">

```shell
tar xzvf databend-${version}-x86_64-unknown-linux-musl.tar.gz
```

</TabItem>
</Tabs>

## 2. Deploy databend-meta (Standalone)

databend-meta is a global service for the meta data(such as user, table schema etc.).

### 2.1 Create databend-meta.toml

```shell title="databend-meta.toml"
dir = "metadata/_logs"
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
level = "INFO"
dir = "benddata/_logs"

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
address = "127.0.0.1:9101"
username = "root"
password = "root"

[storage]
# s3
type = "s3"

[storage.s3]
bucket = "databend"

# You can get the URL from the bucket detail page.
# https://docsv3.qingcloud.com/storage/object-storage/intro/object-storage/#zone
endpoint_url = "https://pek3b.qingstor.com"

# How to get access_key_id and secret_access_key:
# https://docs.qingcloud.com/product/api/common/overview.html
access_key_id = "<your-key-id>"
secret_access_key = "<your-access-key>"
```

:::tip
In this example QingStore region is `pek3b`.
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

```sql
CREATE TABLE t1(a INT);
```

```sql
INSERT INTO t1 VALUES(1), (2);
```

```sql
SELECT * FROM T1
```
```text
  +------+
  | a    |
  +------+
  |    1 |
  |    2 |
  +------+
```

<GetLatest/>