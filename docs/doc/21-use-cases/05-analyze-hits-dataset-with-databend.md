---
title: Analyzing Hits Dataset with Databend
sidebar_label: Analyzing Hits Dataset
description: Analyzing Hits Dataset with Databend
---

This usecase shows how to analyze the [Hits](https://clickhouse.com/docs/en/getting-started/example-datasets/metrica) dataset with Databend.

## Step 1. Deploy Databend

Make sure you have installed Databend, if not please see:

* [How to Deploy Databend](../01-guides/index.md#deployment)

## Step 2. Load hits Datasets

### 2.1 Create a Databend User

Connect to Databend server with MySQL client:
```shell
mysql -h127.0.0.1 -uroot -P3307 
```

Create a user:
```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
```

Grant privileges for the user:
```sql
GRANT ALL ON *.* TO user1;
```

See also [How To Create User](../14-sql-commands/00-ddl/30-user/01-user-create-user.md).

### 2.2 Create hits Table
 
[Create SQL](https://github.com/datafuselabs/databend/blob/main/tests/suites/1_stateful/ddl/hits.sql)

### 2.3 Load Data Into hits Table

```shell
mysql -h127.0.0.1 -P3307 -uroot
```

`COPY` data into `hits` table:
```shell title='Load CSV files into Databend'
COPY INTO hits FROM 'https://repo.databend.rs/hits/hits_1m.tsv.gz' FILE_FORMAT=(type=TSV compression=AUTO);
```

## Step 3. Queries

Execute Queries:

```shell
mysql -h127.0.0.1 -P3307 -uroot
```

```sql
SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;
```

Example Queries:

| Number      | Query |
| ----------- | ----------- |
| Q1 | `SELECT COUNT(*) FROM hits;` |
| Q2 | `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;` |
| Q3 | `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;` |
| Q4 | `SELECT AVG(UserID) FROM hits;` |
| Q5 | `SELECT COUNT(DISTINCT UserID) FROM hits;` |
| Q6 | `SELECT COUNT(DISTINCT SearchPhrase) FROM hits;` |
| Q7 | `SELECT MIN(EventDate), MAX(EventDate) FROM hits;` |
| Q8 | `SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;` |
| Q9 | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;` |
| Q10 | `SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;` |
