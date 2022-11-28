---
title: Continuous Benchmarking
sidebar_label: Benchmarking
description: Continuous Benchmarking ---
---

One of Databend design goals is to **keep top performance**, to guarantee it Databend runs Continuous Benchmarking on every [nightly release](https://github.com/datafuselabs/databend/releases) to detect performance regressions and visualizes it on the website: [perf.databend.rs](https://perf.databend.rs).

The benchmark runner and results which run daily are defined in the repository [datafuselabs/databend-perf](https://github.com/datafuselabs/databend-perf).

## Vectorized Execution Benchmarking

This benchmarking mainly for Databend vectorized execution, it will tell us how fast the vectorized execution in the memory is, we run these queries to measure it:

| Number | Query                                                                                                              |
| ------ | ------------------------------------------------------------------------------------------------------------------ |
| Q1     | SELECT avg(number) FROM numbers_mt(10000000000);                                                                   |
| Q2     | SELECT sum(number) FROM numbers_mt(10000000000);                                                                   |
| Q3     | SELECT min(number) FROM numbers_mt(10000000000);                                                                   |
| Q4     | SELECT max(number) FROM numbers_mt(10000000000);                                                                   |
| Q5     | SELECT count(number) FROM numbers_mt(10000000000);                                                                 |
| Q6     | SELECT sum(number+number+number) FROM numbers_mt(10000000000);                                                     |
| Q7     | SELECT sum(number) / count(number) FROM numbers_mt(10000000000);                                                   |
| Q8     | SELECT sum(number) / count(number), max(number), min(number) FROM numbers_mt(10000000000);                         |
| Q9     | SELECT number FROM numbers_mt(10000000000) ORDER BY number DESC LIMIT 10;                                          |
| Q10    | SELECT max(number), sum(number) FROM numbers_mt(10000000000) GROUP BY number % 3, number % 4, number % 5 LIMIT 10; |


<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/contributing/vector-perf.png" width="800"/>
</p>

## Ontime Benchmarking


This benchmarking will tell us what the performance is when Databend works with Ontime dataset which on the AWS S3, we measure it by these queries:

| Number | Query                                                                                                                                                                              |
| ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Q1     | SELECT DayOfWeek, count(*) AS c FROM default.ontime WHERE Year >= 2000 AND Year <= 2008 GROUP BY DayOfWeek ORDER BY c DESC;                                                        |
| Q2     | SELECT DayOfWeek, count(*) AS c FROM default.ontime WHERE DepDelay>10 AND Year >= 2000 AND Year <= 2008 GROUP BY DayOfWeek ORDER BY c DESC;                                        |
| Q3     | SELECT Origin, count(*) AS c FROM default.ontime WHERE DepDelay>10 AND Year >= 2000 AND Year <= 2008 GROUP BY Origin ORDER BY c DESC LIMIT 10;                                     |
| Q4     | SELECT IATA_CODE_Reporting_Airline AS Carrier, count() FROM default.ontime WHERE DepDelay>10 AND Year = 2007 GROUP BY Carrier ORDER BY count() DESC;                             |
| Q5     | SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(cast(DepDelay>10 as Int8))*1000 AS c3 FROM default.ontime WHERE Year=2007 GROUP BY Carrier ORDER BY c3 DESC;                  |
| Q6     | SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(cast(DepDelay>10 as Int8))*1000 AS c3 FROM default.ontime WHERE Year>=2000 AND Year <=2008 GROUP BY Carrier ORDER BY c3 DESC; |
| Q7     | SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(DepDelay) * 1000 AS c3 FROM default.ontime WHERE Year >= 2000 AND Year <= 2008 GROUP BY Carrier;                              |
| Q8     | SELECT Year, avg(DepDelay) FROM default.ontime GROUP BY Year;                                                                                                                      |
| Q9     | SELECT Year, count(*) as c1 FROM default.ontime GROUP BY Year;                                                                                                                     |
| Q10    | SELECT avg(cnt) FROM (SELECT Year,Month,count(*) AS cnt FROM default.ontime WHERE DepDel15=1 GROUP BY Year,Month) a;                                                               |
| Q11    | SELECT avg(c1) FROM (SELECT Year,Month,count(*) AS c1 FROM default.ontime GROUP BY Year,Month) a;                                                                                  |
| Q12    | SELECT OriginCityName, DestCityName, count(*) AS c FROM default.ontime GROUP BY OriginCityName, DestCityName ORDER BY c DESC LIMIT 10;                                             |
| Q13    | SELECT OriginCityName, count(*) AS c FROM default.ontime GROUP BY OriginCityName ORDER BY c DESC LIMIT 10;                                                                         |
| Q14    | SELECT count(*) FROM default.ontime;                                                                                                                                               |

<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/contributing/ontime-perf.png" width="800"/>
</p>
