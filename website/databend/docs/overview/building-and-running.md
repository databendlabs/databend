---
id: building-and-running
title: Deploy
---

This document describes how to build and run [DatabendQuery](https://github.com/datafuselabs/databend/tree/main/query) as a distributed query engine.


## 1. Deploy


=== "Release binary"

    ```markdown
    curl -fsS https://repo.databend.rs/databend/install-bendctl.sh | bash
    ```

    ```markdown
    $ export PATH="${HOME}/.databend/bin:${PATH}"
    ```

    ```markdown
    $ bendctl cluster create
    ```

=== "Run with Docker(Recommended)"

    ```markdown
    docker pull datafuselabs/databend
    ```

    ```markdown
    docker run --init --rm -p 3307:3307 datafuselabs/databend
    ```

=== "From source"

    ```markdown
    git clone https://github.com/datafuselabs/databend.git
    ```

    ```markdown
    cd databend && make setup
    ```

    ```markdown
    make run
    ```


## 2. Client

=== "MySQL Client"

    !!! note
        numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.

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

=== "ClickHouse Client"

    !!! note
        numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.

    ```
    clickhouse client --host 0.0.0.0 --port 9001
    ```

    ```
    datafuse :) SELECT avg(number) FROM numbers(1000000000);

    SELECT avg(number)
      FROM numbers(1000000000)

    Query id: 89e06fba-1d57-464d-bfb0-238df85a2e66

    ┌─avg(number)─┐
    │ 499999999.5 │
    └─────────────┘

    1 rows in set. Elapsed: 0.062 sec. Processed 1.00 billion rows, 8.01 GB (16.16 billion rows/s., 129.38 GB/s.)
    ```
=== "HTTP Client"

    !!! note
        numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.

    ```
    $ curl --location --request POST 'localhost:8001/v1/statement/' \
    --header 'Content-Type: text/plain' \
    --data-raw 'SELECT avg(number) FROM numbers(1000000000)'
    ```

    ```
    {
        "id": "93114794-a532-4706-84c9-61a137398fb8",
        "nextUri": null,
        "data": [
            [
                499999999.5
            ]
        ],
        "columns": {
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
            "read_rows": 1000000000,
            "read_bytes": 8000000000,
            "total_rows_to_read": 0
        }
    }
    ```
