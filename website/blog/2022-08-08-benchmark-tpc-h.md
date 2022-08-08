---
title: Benchmarking Databend using TPC-H
description: Benchmarking Databend using TPC-H
slug: benchmark
date: 2022-08-08
tags: [databend, benchmark, TPC-H]
authors:
- name: BohuTANG
  url: https://github.com/BohuTANG
  image_url: https://github.com/BohuTANG.png
---

This post gives you a general idea about the TPC-H benchmark and explains how to run a TPC-H benchmark on Databend.

## What's TPC-H?

TPC-H is a decision support benchmark. It consists of a suite of business-oriented ad hoc queries and concurrent data modifications. The queries and the data populating the database have been chosen to have broad industry-wide relevance. This benchmark illustrates decision support systems that examine large volumes of data, execute queries with a high degree of complexity, and give answers to critical business questions.

The TPC-H benchmark simulates a system for online sales of parts and components and defines eight tables in total. The structure, data volume, and mutual relationship of each table are shown in the figure below:

![](../static/img/blog/tables.jpeg)

The benchmark workload consists of twenty-two decision support queries that must be executed as part of the TPC-H benchmark. Each TPC-H query asks a business question and includes the corresponding query to answer the question. More information about TPC-H can be found at https://www.tpc.org/tpch/.

## Running TPC-H Benchmark on Databend

This section describes the steps to run the TPC-H benchmark on Databend and provides the related scripts.

### Step 1: Generate test data with TPC-H Docker

The following code pulls a docker image and allocates the data in the path where you are running the TPC-H benchmark.

```shell
docker pull ghcr.io/databloom-ai/tpch-docker:main
docker run -it -v "$(pwd)":/data ghcr.io/databloom-ai/tpch-docker:main dbgen -vf -s 1
```

### Step 2: Create database and tables

```sql
CREATE DATABASE IF NOT EXISTS tpch;
USE tpch;
CREATE TABLE IF NOT EXISTS nation  ( N_NATIONKEY  INTEGER NOT NULL,
                            N_NAME       VARCHAR NOT NULL,
                            N_REGIONKEY  INT NOT NULL,
                            N_COMMENT    VARCHAR);

CREATE TABLE IF NOT EXISTS region  ( R_REGIONKEY  INT NOT NULL,
       	               R_NAME       VARCHAR NOT NULL,
                       R_COMMENT    VARCHAR);

CREATE TABLE IF NOT EXISTS part  ( P_PARTKEY     INT NOT NULL,
                          P_NAME        VARCHAR NOT NULL,
                          P_MFGR        VARCHAR NOT NULL,
                          P_BRAND       VARCHAR NOT NULL,
                          P_TYPE        VARCHAR NOT NULL,
                          P_SIZE        INT NOT NULL,
                          P_CONTAINER   VARCHAR NOT NULL,
                          P_RETAILPRICE FLOAT NOT NULL,
                          P_COMMENT     VARCHAR NOT NULL);

CREATE TABLE IF NOT EXISTS supplier  ( S_SUPPKEY     INT NOT NULL,
                             S_NAME        VARCHAR NOT NULL,
                             S_ADDRESS     VARCHAR NOT NULL,
                             S_NATIONKEY   INT NOT NULL,
                             S_PHONE       VARCHAR NOT NULL,
                             S_ACCTBAL     FLOAT NOT NULL,
                             S_COMMENT     VARCHAR NOT NULL);

CREATE TABLE IF NOT EXISTS partsupp ( PS_PARTKEY     INT NOT NULL,
                             PS_SUPPKEY     INT NOT NULL,
                             PS_AVAILQTY    INT NOT NULL,
                             PS_SUPPLYCOST  FLOAT NOT NULL,
                             PS_COMMENT     VARCHAR NOT NULL);

CREATE TABLE IF NOT EXISTS customer  ( C_CUSTKEY     INT NOT NULL,
                             C_NAME        VARCHAR NOT NULL,
                             C_ADDRESS     VARCHAR NOT NULL,
                             C_NATIONKEY   INT NOT NULL,
                             C_PHONE       VARCHAR NOT NULL,
                             C_ACCTBAL     FLOAT   NOT NULL,
                             C_MKTSEGMENT  VARCHAR NOT NULL,
                             C_COMMENT     VARCHAR NOT NULL);

CREATE TABLE IF NOT EXISTS orders  ( O_ORDERKEY       INT NOT NULL,
                           O_CUSTKEY        INT NOT NULL,
                           O_ORDERSTATUS    VARCHAR NOT NULL,
                           O_TOTALPRICE     FLOAT NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  VARCHAR NOT NULL,  
                           O_CLERK          VARCHAR NOT NULL, 
                           O_SHIPPRIORITY   INT NOT NULL,
                           O_COMMENT        VARCHAR NOT NULL);

CREATE TABLE IF NOT EXISTS lineitem ( L_ORDERKEY    INT NOT NULL,
                             L_PARTKEY     INT NOT NULL,
                             L_SUPPKEY     INT NOT NULL,
                             L_LINENUMBER  INT NOT NULL,
                             L_QUANTITY    FLOAT NOT NULL,
                             L_EXTENDEDPRICE  FLOAT NOT NULL,
                             L_DISCOUNT    FLOAT NOT NULL,
                             L_TAX         FLOAT NOT NULL,
                             L_RETURNFLAG  VARCHAR NOT NULL,
                             L_LINESTATUS  VARCHAR NOT NULL,
                             L_SHIPDATE    DATE NOT NULL,
                             L_COMMITDATE  DATE NOT NULL,
                             L_RECEIPTDATE DATE NOT NULL,
                             L_SHIPINSTRUCT VARCHAR NOT NULL,
                             L_SHIPMODE     VARCHAR NOT NULL,
                             L_COMMENT      VARCHAR NOT NULL);
```

### Step 3: Load test data to Databend

```shell
#!/bin/bash

for t in customer lineitem nation orders partsupp part region supplier
do
    echo "$t"
    curl -XPUT 'http://root:@127.0.0.1:8000/v1/streaming_load' -H 'insert_sql: insert into tpch.'$t' format CSV' -H 'skip_header: 0' -H 'field_delimiter:|' -H 'record_delimiter: \n' -F 'upload=@"./'$t'.tbl"'
done
```

### Step 4: Run TPC-H queries

All the definitions of the TPC-H queries can be found at https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v3.0.1.pdf. You can simply run them by copying and pasting the scripts to Databend.

The Databend team ran the TPC-H benchmark around two months ago and uploaded their queries and results to GitHub. You can find them at https://github.com/datafuselabs/databend/tree/main/tests/suites/0_stateless/13_tpch. Please note that Databend now uses the new planner by default, so you DO NOT need to enable it before running the queries.