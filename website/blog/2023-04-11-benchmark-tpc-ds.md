---
title: Benchmarking TPC-DS with Databend
description: Benchmarking Databend using TPC-DS
date: 2023-04-11
tags: [databend, benchmark, TPC-DS]
cover_url: databend-using-TPC-DS.png
authors:
- name: xudong
  url: https://github.com/xudong963
  image_url: https://github.com/xudong963.png
---
The TPC-DS benchmark is widely used for measuring the performance of decision support and analytical systems. 
Databend is a data warehouse that supports TPC-DS SQLs. 
In this blog, we will walk you through the process of benchmarking TPC-DS with Databend, covering key aspects such as generating TPC-DS data, preparing create tables for Databend, and executing benchmark queries.

## What's TPC-DS?
TPC-DS is a decision support benchmark that models several generally applicable aspects of a decision support system, 
including queries and data maintenance. 
The benchmark provides a representative evaluation of performance as a general purpose decision support system.

It includes 7 fact tables, 17 dimension tables, with an average of 18 columns per table and 99 test queries.

You can find more information about TPC-DS at https://www.tpc.org/tpcds/.

## Running TPC-DS Benchmark on Databend

This section describes the steps to run the TPC-DS benchmark on Databend and provides the related scripts. You can find more detail information at: https://github.com/datafuselabs/databend/tree/main/benchmark/tpcds.

### Step 1: Generate TPC-DS test data

Leverage duckdb to generate TPC-DS data:

```sql
INSTALL tpcds;
LOAD tpcds;
SELECT * FROM dsdgen(sf=1);
EXPORT DATABASE 'TARGET_DIR' (FORMAT CSV, DELIMITER '|');
```

### Step 2: Load TPC-DS data into Databend
```shell
./load_data.sh
```

### Step3: Run TPC-DS queries
```shell
databend-sqllogictests --handlers mysql --database tpcds --run_dir tpcds --bench 
```


