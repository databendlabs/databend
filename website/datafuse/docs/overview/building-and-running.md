---
id: building-and-running
title: Deploy
---

This document describes how to build and run [DatafuseQuery](https://github.com/datafuselabs/datafuse/tree/master/query) as a distributed query engine.


## 1. Deploy

=== "Run with Docker(Recommended)"

    ```markdown
    $ docker pull datafuselabs/datafuse
    $ docker run --init --rm -p 3307:3307 datafuselabs/datafuse
    ```

=== "Release binary"

    ```markdown
    $ curl -fsS https://raw.githubusercontent.com/datafuselabs/datafuse/master/scripts/installer/install-datafuse.sh | bash
    ```

=== "From source"

    ```markdown
    $ git clone https://github.com/datafuselabs/datafuse.git
    $ cd datafuse
    $ make setup
    $ make run
    ```


## 2. Client

=== "MySQL Client"

    !!! note
        numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.

    ```
    $ mysql -h127.0.0.1 -P3307
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
    $ clickhouse client --host 0.0.0.0 --port 9001
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
