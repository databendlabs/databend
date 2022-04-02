---
title: Using Databend as a Sink for Vector
sidebar_label: Vector
description:
  Using Databend as a Sink for Vector
---

<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/integration-databend-vector.png" width="550"/>
</p>

## What is [Vector](https://vector.dev/)?

* A lightweight, ultra-fast tool for building observability pipelines.
* Allows you to Gather, Transform, and Route all log and metric data with one simple tool.
* Made up of three components (sources, transforms, sinks) of two types (logs, metrics).

Databend supports ClickHouse REST API, so it's easy to integration with Vector to stream, aggregate, and gain insights.

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
user = "vector" #Databend username
// highlight-next-line
password = "vector123" #Databend password
```

## Create a User for Vector

Connect to Databend server with MySQL client:
```shell
mysql -h127.0.0.1 -uroot -P3307 
```

```shell title='mysql>'
create user 'vector' identified by 'vector123';
```
Please replace `vector`, `vector123` to your own username and password.

```shell title='mysql>'
grant insert on nginx.* TO 'vector'@'%';
```

## Tutorial

[How to Ingest Nginx Access Logs into Databend with Vector](../09-learn/03-analyze-nginx-logs-with-databend-and-vector.md)


