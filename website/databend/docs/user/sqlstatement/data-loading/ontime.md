---
title: OnTime
---

Load OnTime dataset into Databend and do some queries.

## 1. Download OnTime raw data

Download raw data to `/tmp/dataset`:

```
wget -P /tmp/ https://repo.databend.rs/dataset/stateful/ontime.csv
```

## 2. Install bendctl


```shell
curl -fsS https://repo.databend.rs/databend/install-bendctl.sh | bash
```

```shell
export PATH="${HOME}/.databend/bin:${PATH}"
```

## 3. Setting up Databend server

```shell
benctl
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

## 4. Create the ontime table

```
bendctl --databend_dir ~/.databend --group local query ./tests/suites/0_stateful/ddl/create_table.sql
```

## 5. Load raw data into ontime table
```
bendctl --databend_dir ~/.databend --group local load /tmp/ontime.csv --table ontime
```

## 6. Queries

```
bendctl --databend_dir ~/.databend --group local query "SELECT avg(c1)
> FROM
> (
>     SELECT Year, Month, count(*) AS c1
>     FROM ontime
>     GROUP BY Year, Month
> )"
[ok] ✅ Query precheck passed!
+----------+
| avg(c1)  |
+----------+
| 371357.0 |
+----------+
[ok] ✅ read rows: 371,357, read bytes: 1.11 MB, rows/sec: 4,951,426 (rows/sec), bytes/sec: 14.85428 (MB/sec), time: 0.075 sec
```

