---
title: Analyzing OnTime Dataset with Databend
sidebar_label: Analyzing OnTime Dataset
description: Analyzing OnTime Dataset with Databend
---

This usecase shows how to analyze the [OnTime dataset](https://github.com/datafuselabs/databend/blob/main/tests/data/ontime_200.csv) with Databend. Please note that the dataset used in this example is a subset of the OnTime dataset and does not represent the entire dataset. The use of this smaller-sized dataset is for demonstration purposes and ease of illustration.

## Step 1. Deploy Databend

Follow the [Docker and Local Deployments](../10-deploy/05-deploying-local.md) guide to deploy a local Databend.

## Step 2. Load OnTime Dataset

1. Create a table using the SQL statement provided in the file: [ontime.sql](https://github.com/datafuselabs/databend/blob/main/tests/data/ddl/ontime.sql).

2. Download the [OnTime dataset](https://github.com/datafuselabs/databend/blob/main/tests/data/ontime_200.csv).

3. Load data into Databend using [BendSQL](../13-sql-clients/01-bendsql.md):

```shell
eric@macdeMacBook-Pro Documents % bendsql --query='INSERT INTO default.ontime VALUES;' --format-opt='skip_header=1' --data=@ontime_200.csv
```

## Step 3. Run Queries

```shell
mysql -h127.0.0.1 -P3307 -uroot
```
```sql
SELECT Year, count(*) FROM ontime GROUP BY Year;
```

All Queries:

| Number      | Query |
| ----------- | ----------- |
| Q1   |SELECT DayOfWeek, count(*) AS c FROM ontime WHERE Year >= 2000 AND Year <= 2008 GROUP BY DayOfWeek ORDER BY c DESC;       |
| Q2   |SELECT DayOfWeek, count(*) AS c FROM ontime WHERE DepDelay>10 AND Year >= 2000 AND Year <= 2008 GROUP BY DayOfWeek ORDER BY c DESC;    |
| Q3   |SELECT Origin, count(*) AS c FROM ontime WHERE DepDelay>10 AND Year >= 2000 AND Year <= 2008 GROUP BY Origin ORDER BY c DESC LIMIT 10;   |
| Q4   |SELECT IATA_CODE_Reporting_Airline AS Carrier, count(*) FROM ontime WHERE DepDelay>10 AND Year = 2007 GROUP BY Carrier ORDER BY count(*) DESC;      |
| Q5   |SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(cast(DepDelay>10 as Int8))*1000 AS c3 FROM ontime WHERE Year=2007 GROUP BY Carrier ORDER BY c3 DESC;|
| Q6   |SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(cast(DepDelay>10 as Int8))*1000 AS c3 FROM ontime WHERE Year>=2000 AND Year <=2008 GROUP BY Carrier ORDER BY c3 DESC;|
| Q7   |SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(DepDelay) * 1000 AS c3 FROM ontime WHERE Year >= 2000 AND Year <= 2008 GROUP BY Carrier; |
| Q8   |SELECT Year, avg(DepDelay) FROM ontime GROUP BY Year;      |
| Q9   |SELECT Year, count(*) as c1 FROM ontime GROUP BY Year;      |
| Q10  |SELECT avg(cnt) FROM (SELECT Year,Month,count(*) AS cnt FROM ontime WHERE DepDel15=1 GROUP BY Year,Month) a;      |
| Q11  |SELECT avg(c1) FROM (SELECT Year,Month,count(*) AS c1 FROM ontime GROUP BY Year,Month) a;      |
| Q12  |SELECT OriginCityName, DestCityName, count(*) AS c FROM ontime GROUP BY OriginCityName, DestCityName ORDER BY c DESC LIMIT 10;     |
| Q13  |SELECT OriginCityName, count(*) AS c FROM ontime GROUP BY OriginCityName ORDER BY c DESC LIMIT 10;      |
| Q14  |SELECT count(*) FROM ontime;     |
