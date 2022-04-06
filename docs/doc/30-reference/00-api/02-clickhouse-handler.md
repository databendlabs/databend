---
title: ClickHouse Handler
sidebar_label: ClickHouse Handler
description:
  Databend is ClickHouse wire protocol-compatible.
---

<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/api/api-handler-clickhouse.png" width="200"/>
</p>

## Overview

Databend is ClickHouse wire protocol-compatible, allow you to connect to Databend server with Clickhouse client, make it easier for users/developers to use Databend.

## ClickHouse Protocol(TCP)

Databend supports ClickHouse client to connect(Default port is 9000), it is same as you connect to a ClickHouse server.

```shell
clickhouse-client --host 127.0.0.1 --port 9000
```

## ClickHouse REST API

:::tip
Databend ClickHouse HTTP handler is a simplified version of the implementation, it only providers:
* Heath check
* Insert with JSONEachRow format file
:::

### Health check

```sql title='query=select 1'
curl '127.0.0.1:8000/clickhouse/?query=select%201'
```

```sql title='Response'
1
```

### Insert with JSONEachRow(ndjson) Format File

:::note
** Databend ClickHouse Handler only supports put ndjson(JSONEachRow in ClickHouse) format files**.
:::

ndjson is a newline delimited JSON format:
* Line Separator is '\n' 
* Each Line is a Valid JSON Value

For example, we have a table:
```sql title='table t1'
CREATE TABLE t1(a UInt8);
```

and a ndjson file:
````json title='t1.json'
{"a":1}
{"a":2}
````

Insert into `t1`:
```sql title='insert into t1 format JSONEachRow'
curl -i 'http://localhost:8000/clickhouse/?query=INSERT%20INTO%20default.t1%20FORMAT%20JSONEachRow' --data-binary @t1.json
```