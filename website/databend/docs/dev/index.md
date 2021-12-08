---
title: Get Started
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This document describes how to build and run [DatabendQuery](https://github.com/datafuselabs/databend/tree/main/query) as a distributed query engine.

## 1. Build and run

```shell
$ git clone https://github.com/datafuselabs/databend.git
$ cd databend && make setup
$ make run
```

## 2. Client

:::note
numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

<Tabs>
  <TabItem value="mysql" label="MySQL Client" default>

```shell
mysql -h127.0.0.1 -uroot -P3307
```

```text
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

```shell
clickhouse client --host 0.0.0.0 --port 9001
```

```text
datafuse :) SELECT avg(number) FROM numbers(1000000000);

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

```shell
$ curl --location --request POST 'localhost:8001/v1/statement/' \
--header 'Content-Type: text/plain' \
--data-raw 'SELECT avg(number) FROM numbers(1000000000)'
```

```json
{
    "id": "93114794-a532-4706-84c9-61a137398fb8",
    "next_uri": null,
    "data": [
        [
            499999999.5
        ]
    ],
    "schema": {
        "fields": [
            {
                "name": "avg(number)",
                "data_type": "Float64",
                "nullable": false
            }
        ],
        "metadata": {}
    },
    "error": null,
    "stats": {
        "progress": {
            "read_rows": 1000000000,
            "read_bytes": 8000000000,
            "total_rows_to_read": 0
        },
        "wall_time_ms": 2
    }
}
```

  </TabItem>
</Tabs>
