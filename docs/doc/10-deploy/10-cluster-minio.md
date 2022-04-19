---
title: Start a Local Query Cluster
sidebar_label: Local Cluster
description:
  How to Deploy a Local Query Cluster.
---

:::tip

Expected deployment time: ** 5 minutes ⏱ **

:::

Once you've [installed Databend](04-minio.md), it's very simple to run a databend-query cluster locally.

The new databend-query node only needs to register itself to the databend-meta with the same `cluster_id`, they will autodiscovery and formed into a cluster.

<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/deploy-minio-cluster.png" width="300"/>
</p>

## Before you begin

* Make sure you have already [installed Databend](04-minio.md).
* Databend Cluster mode only works on shared storage(AWS S3 or MinIO s3-like storage).
* Note that running multiple nodes on a single host is useful for testing Databend, but it's not suitable for production.


## Step 1. Start standalone

Install Databend with standalone mode, please see [Install Databend with MinIO](04-minio.md).

## Step 2. Scale new databend-query Node to the Cluster

### 2.1 Create databend-query-node2.toml

```shell title="databend-query-node2.toml"
[log]
log_level = "INFO"
log_dir = "benddata/_logs"

[query]
# For admin RESET API.
admin_api_address = "127.0.0.1:8002"

# Metrics.
metric_api_address = "127.0.0.1:7072"

# Cluster flight RPC.
flight_api_address = "127.0.0.1:9092"

# Query MySQL Handler.
mysql_handler_host = "127.0.0.1"
mysql_handler_port = 3308

# Query ClickHouse Handler.
clickhouse_handler_host = "127.0.0.1"
clickhouse_handler_port = 9002

# Query HTTP Handler.
http_handler_host = "127.0.0.1"
http_handler_port = 8082

// highlight-next-line
tenant_id = "tenant1"
// highlight-next-line
cluster_id = "cluster1"

[meta]
// highlight-next-line
meta_address = "127.0.0.1:9101"
meta_username = "root"
meta_password = "root"


[storage]
# fs|s3
storage_type = "s3"

[storage.fs]

# The storage should be same for the cluster.
[storage.s3]
bucket = "databend"
endpoint_url = "http://127.0.0.1:9900"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
```

### 2.2 Start new databend-query Node

```shell
./databend-query -c ./databend-query-node2.toml 2>&1 > query.node2.log&
```

### 2.3 Check new databend-query Node Status

```shell
curl -I  http://127.0.0.1:8002/v1/health
```

Check the response is `HTTP/1.1 200 OK`.

### 2.4 Check the Cluster Information

```shell
mysql -h127.0.0.1 -uroot -P3308
```

```sql
SELECT * FROM system.clusters
```
```
+------------------------+-----------+------+
| name                   | host      | port |
+------------------------+-----------+------+
| QXyxUbieMYMV6OGrjoDKL6 | 127.0.0.1 | 9092 |
| Y1lJiseTjCLwpVRYItQ2f3 | 127.0.0.1 | 9091 |
+------------------------+-----------+------+
```

## Step 3. Distributed query

```text
EXPLAIN SELECT max(number), sum(number) FROM numbers_mt(10000000000) GROUP BY number % 3, number % 4, number % 5 LIMIT 10;
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| explain                                                                                                                                                                                                           |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Limit: 10                                                                                                                                                                                                         |
|   RedistributeStage[expr: 0]                                                                                                                                                                                      |
|     Projection: max(number):UInt64, sum(number):UInt64                                                                                                                                                            |
|       AggregatorFinal: groupBy=[[(number % 3), (number % 4), (number % 5)]], aggr=[[max(number), sum(number)]]                                                                                                    |
// highlight-next-line
|         RedistributeStage[expr: sipHash(_group_by_key)]                                                                                                                                                           |
|           AggregatorPartial: groupBy=[[(number % 3), (number % 4), (number % 5)]], aggr=[[max(number), sum(number)]]                                                                                              |
|             Expression: (number % 3):UInt8, (number % 4):UInt8, (number % 5):UInt8, number:UInt64 (Before GroupBy)                                                                                                |
|               ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10000000000, read_bytes: 80000000000, partitions_scanned: 1000001, partitions_total: 1000001], push_downs: [projections: [0]] |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

The distributed query works, the cluster will efficient transfer data through `flight_api_address`.
