---
title: Deploy Databend With Azure Blob Storage
sidebar_label: With Azure Blob Storage
description: How to deploy Databend with Azure Blob Storage.
---
import GetLatest from '@site/src/components/GetLatest';

:::tip

Expected deployment time: ** 5 minutes ⏱ **

:::

This guideline will deploy Databend(standalone) with Azure Blob Storage container step by step.

<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/deploy/deploy-azblob-standalone.png" width="300"/>
</p>


### Before you begin

  * [Azure Account](https://azure.microsoft.com/en-us/)
  * [Create a Container in Azure Storage](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal#create-a-container)
  * [Azure Account Access Keys](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal#view-account-access-keys)

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
# azblob
type = "azblob"

[storage.azblob]
endpoint_url = "https://<your-storage-account-name>.blob.core.windows.net"
# https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal#create-a-container
container = "<your-azure-storage-container-name>"
account_name = "<your-storage-account-name>"
# https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal#view-account-access-keys
account_key = "<your-account-key>"
```

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
SELECT * FROM T1;
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