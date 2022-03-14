---
title: Deploy Databend With Local Disk
sidebar_label: With Local Disk
description:
  How to deploy Databend with Local Disk
---

:::tip

Expected deployment time: ** 5 minutes ⏱ **

:::

This guideline will deploy databend(standalone) with local disk step by step.

<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/deploy-local-standalone.png" width="300"/>
</p>

## 1. Download

You can find the latest binaries on the [github release](https://github.com/datafuselabs/databend/releases) page.

```shell
mkdir databend && cd databend
```

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="operating-systems">
<TabItem value="linux" label="Linux">

```shell
curl -LJO https://github.com/datafuselabs/databend/releases/download/v0.6.90-nightly/databend-v0.6.90-nightly-x86_64-unknown-linux-gnu.tar.gz
```

</TabItem>
<TabItem value="mac" label="MacOS">

```shell
curl -LJO https://github.com/datafuselabs/databend/releases/download/v0.6.90-nightly/databend-v0.6.90-nightly-aarch64-apple-darwin.tar.gz
```

</TabItem>

<TabItem value="arm" label="Arm">

```shell
curl -LJO https://github.com/datafuselabs/databend/releases/download/v0.6.90-nightly/databend-v0.6.90-nightly-aarch64-unknown-linux-gnu.tar.gz
```

</TabItem>
</Tabs>

<Tabs groupId="operating-systems">
<TabItem value="linux" label="Linux">

```shell
tar xzvf databend-v0.6.90-nightly-x86_64-unknown-linux-gnu.tar.gz
```

</TabItem>
<TabItem value="mac" label="MacOS">

```shell
tar xzvf databend-v0.6.90-nightly-aarch64-apple-darwin.tar.gz
```

</TabItem>

<TabItem value="arm" label="Arm">

```shell
tar xzvf databend-v0.6.90-nightly-aarch64-unknown-linux-gnu.tar.gz
```

</TabItem>
</Tabs>

## 2. Deploy databend-meta (standalone)

databend-meta is a global service for the meta data(such as user, table schema etc.).

### 2.1 Create databend-meta.toml

```shell title="databend-meta.toml"
log_dir = "metadata/_logs"
admin_api_address = "0.0.0.0:8101"
grpc_api_address = "0.0.0.0:9101"

[raft_config]
id = 1
single = true
raft_dir = "metadata/datas"
```

### 2.2 Start the databend-meta service

```shell
./databend-meta -c ./databend-meta.toml 2>&1 > meta.log&
```

### 2.3 Check databend-meta status

```shell
curl -I  http://127.0.0.1:8101/v1/health
```

Check the response is `HTTP/1.1 200 OK`.


## 3. Deploy databend-query (standalone)

### 3.1 Create databend-query.toml

```shell title="databend-query.toml"
[log]
log_level = "INFO"
log_dir = "benddata/_logs"

[query]
# For admin RESET API.
admin_api_address = "0.0.0.0:8001"

# Query MySQL Handler.
mysql_handler_host = "0.0.0.0"
mysql_handler_port = 3307

# Query ClickHouse Handler.
clickhouse_handler_host = "0.0.0.0"
clickhouse_handler_port = 9001

# Query HTTP Handler.
http_handler_host = "0.0.0.0"
http_handler_port = 8081

tenant_id = "tenant1"
cluster_id = "cluster1"

[meta]
# databend-meta grpc api address. 
meta_address = "0.0.0.0:9101"
meta_username = "root"
meta_password = "root"

[storage]
# disk|s3
storage_type = "disk"

[storage.disk]
data_path = "bendata/datas"

[storage.s3]

[storage.azure_storage_blob]
```

### 3.2 Start databend-query

```shell
./databend-query -c ./databend-query.toml 2>&1 > query.log&
```

### 3.3 Check databend-query status

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
