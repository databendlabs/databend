---
title: Deploy with local disk
---

This guideline will deploy databend(standalone) with local disk step by step.

:::tip

This deploy only takes **5 minutes ⏱**!

:::

## 1. Download

You can find the latest binaries on the [github release](https://github.com/datafuselabs/databend/releases) page.

```shell
mkdir databend & cd databend
```
```shell
curl -LJO https://github.com/datafuselabs/databend/releases/download/v0.6.87-nightly/databend-v0.6.87-nightly-x86_64-unknown-linux-gnu.tar.gz
```
```shell
tar xzvf databend-v0.6.87-nightly-x86_64-unknown-linux-gnu.tar.gz
```

## 2. Deploy databend-meta (standalone)

databend-meta is a global service for the meta data(such as user, table schema etc.).

### 2.1 Create databend-meta.toml

```shell
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

```shell
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
mysql -uroot -P3307 -h127.0.0.1
```

```shell
mysql -uroot -P3307 -h127.0.0.1
Server version: 8.0.26-0.1.0-7bbafdc-simd(1.61.0-nightly-2022-03-09T16:28:17.590103545+00:00) 0

mysql> create table t1(a int);

mysql> insert into t1 values(1), (2);

mysql> select * from t1;
+------+
| a    |
+------+
|    1 |
|    2 |
+------+
```
