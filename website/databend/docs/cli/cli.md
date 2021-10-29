---
id: cli
title: Databend CLI Reference
---

`bendctl` is a command-line tool for creating, listing, logging,
deleting databend running instances on local or
on cloud.
It also supports to port forward webUI, monitoring dashboard on local
and run SQL queries from command line.


## 1. Install

```markdown
curl -fsS https://repo.databend.rs/databend/install-bendctl.sh | bash
```

```markdown
export PATH="${HOME}/.databend/bin:${PATH}"
```

## 2. Setting up a cluster

```markdown
$ bendctl
[local] [sql]> \admin
Mode switched to admin mode
[local] [admin]> cluster create
[ok] databend cluster precheck passed!
[ok] 👏 successfully started meta service with rpc endpoint 127.0.0.1:9191
[ok] local data would be stored in /home/ubuntu/.databend/local/data
[ok] 👏 successfully started query service.
[ok] ✅  To run queries through RESTful api, run: bendctl query 'SQL statement'
[ok] ✅  For example: bendctl query 'SELECT sum(number), avg(number) FROM numbers(100);'
[ok] ✅ To process mysql queries, run: mysql -h 127.0.0.1 -P 3307 -uroot
[ok] ✅ To process clickhouse queries, run: clickhouse client --host 127.0.0.1 --port 9000 --user root
[local] [admin]>
```

## 3. Query Example

=== "bendctl"

    !!! note
        numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.

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

=== "MySQL Client"

    !!! note
        numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.

    ```
    $ mysql -h127.0.0.1 -uroot -P3307
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
    databend :) SELECT avg(number) FROM numbers(1000000000);

    SELECT avg(number)
      FROM numbers(1000000000)

    Query id: 89e06fba-1d57-464d-bfb0-238df85a2e66

    ┌─avg(number)─┐
    │ 499999999.5 │
    └─────────────┘

    1 rows in set. Elapsed: 0.062 sec. Processed 1.00 billion rows, 8.01 GB (16.16 billion rows/s., 129.38 GB/s.)
    ```
