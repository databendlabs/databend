---
id: building-and-running
title: Building and Running
---

This document describes how to build and run [FuseQuery](https://github.com/datafusedev/fuse-query) as a distributed query engine. 

## 1. Run with Docker (Recommended)
    docker pull datafusedev/fuse-query
    docker run --init --rm -p 3307:3307 datafusedev/fuse-query
    ...
    05:12:36 [ INFO] Options { log_level: "debug", num_cpus: 8, mysql_handler_port: 3307 }
    05:12:36 [ INFO] Fuse-Query Cloud Compute Starts...
    05:12:36 [ INFO] Usage: mysql -h127.0.0.1 -P3307

Or 

## 2. Run from Source

### 2.1 Dependencies

FuseQuery is a Rust project. Clang, Rust are supported. 


To install dependencies on Ubuntu:

    apt install git clang

On Arch Linux:

    pacman -S git clang

On Mac via Homebrew:

    brew install git clang


To install Rust(nightly):

    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    rustup toolchain install nightly

### 2.2 Running on Linux and macOS


Clone:

    git clone https://github.com/datafusedev/fuse-query

Running:

    cd fuse-query
    make run


## 3. Connect

 Connect FuseQuery with MySQL client

    mysql -h127.0.0.1 -P3307

### Avg Demo
```
mysql> SELECT avg(number) FROM system.numbers_mt(10000);
+-------------+
| Avg(number) |
+-------------+
|      4999.5 |
+-------------+
1 row in set (0.00 sec)

```

### Explain Demo
```
mysql> explain SELECT avg(number) FROM system.numbers_mt(10000);
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| explain                                                                                                                                                                                                                                      |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| └─ Aggregate: avg([number]):UInt64
  └─ ReadDataSource: scan parts [8](Read from system.numbers_mt table)                                                                                                                                    |
| 
  └─ AggregateFinalTransform × 1 processor
    └─ Merge (AggregatePartialTransform × 8 processors) to (MergeProcessor × 1)
      └─ AggregatePartialTransform × 8 processors
        └─ SourceTransform × 8 processors                      |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
2 rows in set (0.00 sec)

```


### 10 Billion Performance
```
mysql> SELECT avg(number) FROM system.numbers_mt(10000000000);
+-------------------+
| Avg(number)       |
+-------------------+
| 4999999999.494631 |
+-------------------+
1 row in set (2.02 sec)

```