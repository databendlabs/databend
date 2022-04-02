---
title: Using Databend as a Sink for Vector
sidebar_label: Sink for Vector
description:
  Using Databend as a Sink for Vector
---

<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/integration-databend-vector.png" width="550"/>
</p>

What is [Vector](https://vector.dev/)?

* A lightweight, ultra-fast tool for building observability pipelines.
* Allows you to Gather, Transform, and Route all log and metric data with one simple tool.
* Made up of three components (sources, transforms, sinks) of two types (logs, metrics).

Databend supports ClickHouse REST API, so it's easy to integration with Vector to stream, aggregate, and gain insights.

Configuration:

```shell
[sinks.my_sink_id]
type = "clickhouse" #Required
inputs = [ "my-source-or-transform-id" ] #Your input source
database = "mydatabase" #Your database
table = "mytable" #Your table.
endpoint = "http://localhost:8000/clickhouse" #Required
compression = "gzip"
```

More configuration please see: https://vector.dev/docs/reference/configuration/sinks/clickhouse/