---
title: Using Databend as a Sink for Vector
sidebar_label: Vector
description:
  Using Databend as a sink for Vector.
---

<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/integration/integration-databend-vector.png" width="550"/>
</p>

## What is [Vector](https://vector.dev/)?

* A lightweight, ultra-fast tool for building observability pipelines.
* Allows you to Gather, Transform, and Route all log and metric data with one simple tool.
* Made up of three components (sources, transforms, sinks) of two types (logs, metrics).

Databend supports ClickHouse REST API, so it's easy to integration with Vector to stream, aggregate, and gain insights.

## Create a Databend User

Connect to Databend server with MySQL client:
```shell
mysql -h127.0.0.1 -uroot -P3307 
```

Create a user:
```
CREATE USER user1 IDENTIFIED BY 'abc123';
```

Grant insert privileges for the user:
```sql
GRANT INSERT ON nginx.* TO user1;
```

See also [How To Create User](../../30-reference/30-sql/00-ddl/30-user/01-user-create-user.md).

## Configure Vector

To use Databend with Vector you will need to configure for [Clickhouse Sink](https://vector.dev/docs/reference/configuration/sinks/clickhouse/#example-configurations):

```shell
[sinks.databend_sink]
type = "clickhouse"
inputs = [ "my-source-or-transform-id" ] # input source
// highlight-next-line
database = "mydatabase" #Your database
// highlight-next-line
table = "mytable" #Your table.
// highlight-next-line
endpoint = "http://localhost:8000/clickhouse" #Databend ClickHouse REST API: http://{http_handler_host}:{http_handler_port}/clickhouse
compression = "gzip"
```

```shell
[sinks.databend_sink.auth]
strategy = "basic"
// highlight-next-line
user = "user1" #Databend username
// highlight-next-line
password = "abc123" #Databend password
```

## Tutorial

[How to Analyze Nginx Access Logs With Databend](../../90-learn/02-analyze-nginx-logs-with-databend-and-vector.md)
