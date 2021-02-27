---
id: building-and-running
title: Building and Running
---

This document describes how to build and run [FuseQuery](https://github.com/datafusedev/fuse-query) as a distributed query engine. 

## 1. Run with Docker (Recommended)

```text
docker pull datafusedev/fuse-query
docker run --init --rm -p 3307:3307 datafusedev/fuse-query
...
05:12:36 [ INFO] Options { log_level: "debug", num_cpus: 8, mysql_handler_port: 3307 }
05:12:36 [ INFO] Fuse-Query Cloud Compute Starts...
05:12:36 [ INFO] Usage: mysql -h127.0.0.1 -P3307
```

Or 

## 2. Download the release binary

https://github.com/datafusedev/fuse-query/releases

Or

## 3. Run from Source

### 3.1 Dependencies

1. Clone:

```text
git clone https://github.com/datafusedev/fuse-query
```

2. Setup development toolchain:

```text
$ cd fuse-query
$ make setup
```

3. Running:

```text
$ make run
```

## 4. Connect

 Connect FuseQuery with MySQL client

```text
mysql -h127.0.0.1 -P3307
```

### Avg Demo

```text
mysql> SELECT avg(number) FROM system.numbers(10000);
+-------------+
| Avg(number) |
+-------------+
|      4999.5 |
+-------------+
1 row in set (0.00 sec)

```


### 10 Billion Performance

```text
mysql> SELECT avg(number) FROM system.numbers(10000000000);
+-------------------+
| Avg(number)       |
+-------------------+
| 4999999999.494631 |
+-------------------+
1 row in set (2.02 sec)
```

### Explain  Plan

```text
mysql> explain select (number+1) as c1, number/2 as c2 from system.numbers_mt(10000000) where (c1+c2+1) < 100 limit 3;
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| explain                                                                                                                                                                                                                          |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Limit: 3
  Projection: (number + 1) as c1:UInt64, (number / 2) as c2:UInt64
    Filter: (((c1 + c2) + 1) < 100)
      ReadDataSource: scan parts [8](Read from system.numbers_mt table, Read Rows:10000000, Read Bytes:80000000) |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)
```

### Explain Pipeline
```text
mysql> explain pipeline select (number+1) as c1, number/2 as c2 from system.numbers_mt(10000000) where (c1+c2+1) < 100 limit 3;
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| explain                                                                                                                                                                                                                                                                                                               |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 
  └─ LimitTransform × 1 processor
    └─ Merge (LimitTransform × 8 processors) to (MergeProcessor × 1)
      └─ LimitTransform × 8 processors
        └─ ProjectionTransform × 8 processors
          └─ FilterTransform × 8 processors
            └─ SourceTransform × 8 processors                                |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```

### Select

```shell
mysql> select (number+1) as c1, number/2 as c2 from system.numbers_mt(10000000) where (c1+c2+1) < 100 limit 3;
+------+------+
| c1   | c2   |
+------+------+
|    1 |    0 |
|    2 |    0 |
|    3 |    1 |
+------+------+
3 rows in set (0.06 sec)
```
