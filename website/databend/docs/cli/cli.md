---
id: cli
title: Databend CLI Reference
---

`bendctl` is a command-line tool for creating, listing, logging,
deleting databend running instances on local or on cloud.
It also supports to port forward webUI, monitoring dashboard on local and run SQL queries from command line.


## 1. Install

```markdown
curl -fsS https://repo.databend.rs/databend/install-bendctl.sh | bash
```

```markdown
export PATH="${HOME}/.databend/bin:${PATH}"
```

## 2. Help

```markdown
$ bendctl
[local] [sql]> help
[ok] ✅ Mode switch commands:
\sql                 Switch to query mode, you could run query directly under this mode
\admin               Switch to cluster administration mode, you could profile/view/update databend cluster
[ok] ✅ Admin commands:
+---------+-------------------------------------------+
| Name    | About                                     |
+---------+-------------------------------------------+
| version | Databend CLI version                      |
| package | Package command                           |
| cluster | Cluster life cycle management             |
| up      | Bootstrap a single cluster with dashboard |
+---------+-------------------------------------------+
```

## 3. Setting up a cluster

```markdown
[local] [sql]> \admin
Mode switched to admin mode
[local] [admin]> cluster create
[ok] ✅ Databend cluster pre-check passed!
[ok] ✅ Successfully started meta service with rpc endpoint 127.0.0.1:9191
[ok] ✅ Local data would be stored in /home/bohu/.databend/local/data
[ok] ✅ Successfully started query service.
[ok] ✅ To run queries through bendctl, run: bendctl query 'your SQL'
[ok] ✅ For example: bendctl query 'SELECT sum(number), avg(number) FROM numbers(100)'
[ok] ✅ To process mysql queries, run: mysql -h127.0.0.1 -P3307 -uroot
[ok] ✅ To process clickhouse queries, run: clickhouse client --host 127.0.0.1 --port 9000 --user root
[ok] ✅ To process HTTP REST queries, run: curl --location --request POST '127.0.0.1:24974/v1/statement/' --header 'Content-Type: text/plain' --data-raw 'your SQL'
[local] [admin]> 
```

## 4. Query Example

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
    databend :) SELECT avg(number) FROM numbers(1000000000);

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
    curl --location --request POST '127.0.0.1:8001/v1/statement/' --header 'Content-Type: text/plain' --data-raw 'SELECT avg(number) FROM numbers(1000000000)'
    ```

    ```
    {"id":"efca332b-13f4-4b8c-982d-cebf91626214","nextUri":null,"data":[[499999999.5]],"columns":{"fields":[{"name":"avg(number)","data_type":"Float64","nullable":false}],"metadata":{}},"error":null,"stats":{"read_rows":1000000000,"read_bytes":8000000000,"total_rows_to_read":0}}
    ```

## 5. Stop a cluster

```markdown
[local] [admin]> cluster stop
[ok] ⚠ Start to clean up local services
[ok] ⚠ Stopped query service with config in /home/bohu/.databend/local/configs/local/query_config_0.yaml
[ok] ⚠ Stopped meta service with config in /home/bohu/.databend/local/configs/local/meta_config_0.yaml
[ok] 🚀 Stopped all services
[local] [admin]> 
```

## 6. Demo

<figure>
  <img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/bendctl-how-to-use.gif"/>
  <figcaption>bendctl on AWS arm64-server</figcaption>
</figure>

