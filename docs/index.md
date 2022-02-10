---
title: Get Started
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This document describes how to build and run [DatabendQuery](https://github.com/datafuselabs/databend/tree/main/query) as a distributed query engine.

## 1. Deploy

<Tabs>
  <TabItem value="binary" label="Release binary(Recommended)" default>

```shell
$ curl -fsS https://repo.databend.rs/databend/install-bendctl.sh | bash
$ export PATH="${HOME}/.databend/bin:${PATH}"
$ bendctl cluster create
```

  </TabItem>
  <TabItem value="docker" label="Run with Docker">

```shell
$ docker pull datafuselabs/databend
$ docker run --init --rm -p 3307:3307 datafuselabs/databend
```
  </TabItem>
  <TabItem value="source" label="From source">

```shell
$ git clone https://github.com/datafuselabs/databend.git
$ cd databend && make setup
$ make run
```
  </TabItem>
</Tabs>


## 2. Client

:::note
numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

<Tabs>
<TabItem value="bendctl" label="bendctl" default>

```
[local] [admin]> \sql
Mode switched to SQL query mode
[local] [sql]> SELECT avg(number) FROM numbers(1000000000);
+-------------+
| avg(number) |
+-------------+
| 499999999.5 |
+-------------+
[ok] read rows: 1,000,000,000, read bytes: 8.00 GB, rows/sec: 1,259,445,843 (rows/sec), bytes/sec: 10.07556675 (GB/sec)
[local] [sql]>
```
</TabItem>
<TabItem value="mysql" label="MySQL Client">

```
mysql -h127.0.0.1 -uroot -P3307
```
```markdown
mysql> SELECT avg(number) FROM numbers(1000000000);
+-------------+
| avg(number) |
+-------------+
| 499999999.5 |
+-------------+
1 row in set (0.05 sec)
```
</TabItem>
<TabItem value="clickhouse" label="ClickHouse Client">

```
clickhouse client --host 0.0.0.0 --port 9001
```

```
databend :) SELECT avg(number) FROM numbers(1000000000);

SELECT avg(number)
  FROM numbers(1000000000)

Query id: 89e06fba-1d57-464d-bfb0-238df85a2e66

┌─avg(number)─┐
│ 499999999.5 │
└─────────────┘

1 rows in set. Elapsed: 0.062 sec. Processed 1.00 billion rows, 8.01 GB (16.16 billion rows/s., 129.38 GB/s.)
```
</TabItem>
<TabItem value="http" label="HTTP Client">

```
curl --location --request POST '127.0.0.1:8001/v1/statement/' --header 'Content-Type: text/plain' --data-raw 'SELECT avg(number) FROM numbers(1000000000)'
```

```
{"id":"efca332b-13f4-4b8c-982d-cebf91626214","nextUri":null,"data":[[499999999.5]],"columns":{"fields":[{"name":"avg(number)","data_type":"Float64","nullable":false}],"metadata":{}},"error":null,"stats":{"read_rows":1000000000,"read_bytes":8000000000,"total_rows_to_read":0}}
```
</TabItem>
</Tabs>

## Learn more

- [Deploy Databend on AWS EC2 and S3](/learn/lessons/analyze-ontime-with-databend-on-ec2-and-s3)