---
title: Databend On Scaleway OS Performance
sidebar_label: On Scaleway OS Performance
description:
  Sub-second analytics on Scaleway + OS experience.
---

:::tip
* Hardware: General(GP1-XL, nl-ams), 48 vCPUs
* Storage: Scaleway Object Storage (nl-ams)
* Dataset: [ontime](https://transtats.bts.gov/PREZIP/), 60.8 GB Raw CSV Data, 195662214 records
* Databend: [v0.6.100-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.6.98-nightly)
* No Local Caching(Data Caching/Query Result Caching)
* Databend is automatic performance optimization with **no manual tuning** required.
* [Analyzing OnTime Datasets with Databend on Scaleway and OS](../01-deploy/07-scw.md)
:::

## Q1 (1.420 sec., 137.82 million rows/sec., 413.46 MB/sec.)

```sql title='mysql>'
SELECT
    DayOfWeek,
    count(*) AS c
FROM ontime
WHERE (Year >= 2000) AND (Year <= 2008)
GROUP BY DayOfWeek
ORDER BY c DESC
```

```sql title='result'
+-----------+---------+
| DayOfWeek | c       |
+-----------+---------+
|         5 | 8518339 |
|         1 | 8499468 |
|         4 | 8478252 |
|         3 | 8439882 |
|         2 | 8395393 |
|         7 | 8055509 |
|         6 | 7318926 |
+-----------+---------+
7 rows in set (2.23 sec)
Read 195662214 rows, 586.99 MB in 1.420 sec., 137.82 million rows/sec., 413.46 MB/sec.
```

```sql title='explain'
┌─explain────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: DayOfWeek:UInt8, count() as c:UInt64                                                                                                                                                                                                           │
│   Sort: count():UInt64                                                                                                                                                                                                                                     │
│     AggregatorFinal: groupBy=[[DayOfWeek]], aggr=[[count()]]                                                                                                                                                                                               │
│       AggregatorPartial: groupBy=[[DayOfWeek]], aggr=[[count()]]                                                                                                                                                                                           │
│         Filter: ((Year >= 2000) and (Year <= 2008))                                                                                                                                                                                                        │
│           ReadDataSource: scan schema: [Year:UInt16, DayOfWeek:UInt8], statistics: [read_rows: 195662214, read_bytes: 586986642, partitions_scanned: 196, partitions_total: 196], push_downs: [projections: [0, 4], filters: [((Year >= 2000) AND (Year <= 2008))]] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q2 (2.730 sec., 71.68 million rows/sec., 501.74 MB/sec.)

```sql title='mysql>'
SELECT
    DayOfWeek,
    count(*) AS c
FROM ontime
WHERE (DepDelay > 10) AND (Year >= 2000) AND (Year <= 2008)
GROUP BY DayOfWeek
ORDER BY c DESC
```

```sql title='result'
+-----------+---------+
| DayOfWeek | c       |
+-----------+---------+
|         5 | 2042282 |
|         4 | 1874544 |
|         1 | 1753732 |
|         7 | 1744528 |
|         3 | 1602002 |
|         2 | 1503090 |
|         6 | 1362701 |
+-----------+---------+
7 rows in set (3.62 sec)
Read 195662214 rows, 1.37 GB in 2.730 sec., 71.68 million rows/sec., 501.74 MB/sec.
```

```sql title='explain'
┌─explain────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: DayOfWeek:UInt8, count() as c:UInt64                                                                                                                                                                                                           │
│   Sort: count():UInt64                                                                                                                                                                                                                                     │
│     AggregatorFinal: groupBy=[[DayOfWeek]], aggr=[[count()]]                                                                                                                                                                                               │
│       AggregatorPartial: groupBy=[[DayOfWeek]], aggr=[[count()]]                                                                                                                                                                                           │
│         Filter: (((DepDelay > 10) and (Year >= 2000)) and (Year <= 2008))                                                                                                                                                                                  │
│           ReadDataSource: scan schema: [Year:UInt16, DayOfWeek:UInt8, DepDelay:Int32], statistics: [read_rows: 195662214, read_bytes: 1369635498, partitions_scanned: 196, partitions_total: 196], push_downs: [projections: [0, 4, 31], filters: [(((DepDelay > 10) AND (Year >= 2000)) AND (Year <= 2008))]] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q3 (2.739 sec., 71.43 million rows/sec., 1.5 GB/sec.)

```sql title='mysql>'
SELECT
    Origin,
    count(*) AS c
FROM ontime
WHERE (DepDelay > 10) AND (Year >= 2000) AND (Year <= 2008)
GROUP BY Origin
ORDER BY c DESC
LIMIT 10
```

```sql title='result'
+---------+--------+
| Origin  | c      |
+---------+--------+
| ORD\0\0 | 832507 |
| ATL\0\0 | 800277 |
| DFW\0\0 | 594161 |
| LAX\0\0 | 384148 |
| PHX\0\0 | 383200 |
| LAS\0\0 | 343654 |
| DEN\0\0 | 339157 |
| EWR\0\0 | 284785 |
| DTW\0\0 | 282756 |
| IAH\0\0 | 277686 |
+---------+--------+
10 rows in set (3.64 sec)
Read 195662214 rows, 4.11 GB in 2.739 sec., 71.43 million rows/sec., 1.5 GB/sec.
```

```sql title='explain'
┌─explain────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Limit: 10                                                                                                                                                                                                                                                  │
│   Projection: Origin:String, count() as c:UInt64                                                                                                                                                                                                           │
│     Sort: count():UInt64                                                                                                                                                                                                                                   │
│       AggregatorFinal: groupBy=[[Origin]], aggr=[[count()]]                                                                                                                                                                                                │
│         AggregatorPartial: groupBy=[[Origin]], aggr=[[count()]]                                                                                                                                                                                            │
│           Filter: (((DepDelay > 10) and (Year >= 2000)) and (Year <= 2008))                                                                                                                                                                                │
│             ReadDataSource: scan schema: [Year:UInt16, Origin:String, DepDelay:Int32], statistics: [read_rows: 195662214, read_bytes: 4108908062, partitions_scanned: 196, partitions_total: 196], push_downs: [projections: [0, 14, 31], filters: [(((DepDelay > 10) AND (Year >= 2000)) AND (Year <= 2008))]] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q4 (2.005 sec., 97.61 million rows/sec., 1.56 GB/sec.)

```sql title='mysql>'
SELECT
    IATA_CODE_Reporting_Airline AS Carrier,
    count()
FROM ontime
WHERE (DepDelay > 10) AND (Year = 2007)
GROUP BY Carrier
ORDER BY count() DESC
```

```sql title='result'
+---------+---------+
| Carrier | count() |
+---------+---------+
| WN      |  296293 |
| AA      |  176203 |
| MQ      |  145630 |
| US      |  135987 |
| UA      |  128174 |
| OO      |  127426 |
| EV      |  101796 |
| XE      |   99915 |
| DL      |   93675 |
| NW      |   90429 |
| CO      |   76662 |
| YV      |   67905 |
| FL      |   59460 |
| OH      |   59034 |
| B6      |   50740 |
| 9E      |   46948 |
| AS      |   42830 |
| F9      |   23035 |
| AQ      |    4299 |
| HA      |    2746 |
+---------+---------+
20 rows in set (2.66 sec)
Read 195662214 rows, 3.13 GB in 2.005 sec., 97.61 million rows/sec., 1.56 GB/sec.
```

```sql title='explain'
┌─explain────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: IATA_CODE_Reporting_Airline as Carrier:String, count():UInt64                                                                                                                                                                                  │
│   Sort: count():UInt64                                                                                                                                                                                                                                     │
│     AggregatorFinal: groupBy=[[IATA_CODE_Reporting_Airline]], aggr=[[count()]]                                                                                                                                                                             │
│       AggregatorPartial: groupBy=[[IATA_CODE_Reporting_Airline]], aggr=[[count()]]                                                                                                                                                                         │
│         Filter: ((DepDelay > 10) and (Year = 2007))                                                                                                                                                                                                        │
│           ReadDataSource: scan schema: [Year:UInt16, IATA_CODE_Reporting_Airline:String, DepDelay:Int32], statistics: [read_rows: 195662214, read_bytes: 3130596992, partitions_scanned: 196, partitions_total: 196], push_downs: [projections: [0, 8, 31], filters: [((DepDelay > 10) AND (Year = 2007))]] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q5 (1.589 sec., 123.11 million rows/sec., 1.97 GB/sec.)

```sql title='mysql>'
SELECT
    IATA_CODE_Reporting_Airline AS Carrier,
    avg(CAST(DepDelay > 10, Int8)) * 1000 AS c3
FROM ontime
WHERE Year = 2007
GROUP BY Carrier
ORDER BY c3 DESC
```

```sql title='result'
+---------+--------------------+
| Carrier | c3                 |
+---------+--------------------+
| EV      |  355.6390924907593 |
| US      |  280.1273877477871 |
| AA      | 277.98541311368336 |
| MQ      | 269.43869867195565 |
| AS      |  267.3783437899928 |
| B6      |  265.0300339514233 |
| UA      |  261.5785241692891 |
| WN      | 253.48648396615198 |
| OH      | 250.11015455531455 |
| CO      | 237.23274877688758 |
| F9      | 235.62806873977087 |
| YV      | 230.68534661403305 |
| XE      |  229.8095787916913 |
| FL      | 225.94705102238572 |
| NW      | 218.15036933750838 |
| OO      |  213.1297250284338 |
| DL      |  196.8421207466447 |
| 9E      |  181.3707499681284 |
| AQ      |  92.73080241587576 |
| HA      |  48.88295505117935 |
+---------+--------------------+
20 rows in set (1.94 sec)
Read 195662214 rows, 3.13 GB in 1.589 sec., 123.11 million rows/sec., 1.97 GB/sec.
```

```sql title='explain'
┌─explain────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: IATA_CODE_Reporting_Airline as Carrier:String, (avg(cast((DepDelay > 10) as Int8)) * 1000) as c3:Float64                                                                                                                                       │
│   Sort: (avg(cast((DepDelay > 10) as Int8)) * 1000):Float64                                                                                                                                                                                                │
│     Expression: IATA_CODE_Reporting_Airline:String, (avg(cast((DepDelay > 10) as Int8)) * 1000):Float64 (Before OrderBy)                                                                                                                                   │
│       AggregatorFinal: groupBy=[[IATA_CODE_Reporting_Airline]], aggr=[[avg(cast((DepDelay > 10) as Int8))]]                                                                                                                                                │
│         AggregatorPartial: groupBy=[[IATA_CODE_Reporting_Airline]], aggr=[[avg(cast((DepDelay > 10) as Int8))]]                                                                                                                                            │
│           Expression: IATA_CODE_Reporting_Airline:String, cast((DepDelay > 10) as Int8):Int8 (Before GroupBy)                                                                                                                                              │
│             Filter: (Year = 2007)                                                                                                                                                                                                                          │
│               ReadDataSource: scan schema: [Year:UInt16, IATA_CODE_Reporting_Airline:String, DepDelay:Int32], statistics: [read_rows: 195662214, read_bytes: 3130596992, partitions_scanned: 196, partitions_total: 196], push_downs: [projections: [0, 8, 31], filters: [(Year = 2007)]] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q6 (2.162 sec., 90.49 million rows/sec., 1.45 GB/sec.)

```sql title='mysql>'
SELECT
    IATA_CODE_Reporting_Airline AS Carrier,
    avg(CAST(DepDelay > 10, Int8)) * 1000 AS c3
FROM ontime
WHERE (Year >= 2000) AND (Year <= 2008)
GROUP BY Carrier
ORDER BY c3 DESC
```

```sql title='result'
+---------+--------------------+
| Carrier | c3                 |
+---------+--------------------+
| EV      | 273.75673908484157 |
| AS      |  249.3750261156234 |
| B6      |  242.9765484752507 |
| FL      | 235.78579837350685 |
| WN      | 234.25767821665684 |
| YV      | 232.75640004870877 |
| XE      | 225.37125427720642 |
| MQ      | 223.90123340736127 |
| UA      |   216.795406879701 |
| F9      | 215.44602190134705 |
| DH      | 215.11592809811052 |
| OH      | 207.80180638232238 |
| HP      |  205.4787666304912 |
| AA      | 202.27635104259056 |
| US      | 194.92934952964256 |
| TW      |  185.2254167220212 |
| OO      | 181.59144617288132 |
| CO      | 179.07975376134564 |
| DL      | 178.57936839556356 |
| 9E      | 171.55638804818648 |
| NW      |  169.9350396739337 |
| RU      | 165.93486875604194 |
| TZ      | 159.30095696228443 |
| AQ      | 111.66529559984713 |
| HA      |  57.99147625931697 |
+---------+--------------------+
25 rows in set (2.61 sec)
Read 195662214 rows, 3.13 GB in 2.162 sec., 90.49 million rows/sec., 1.45 GB/sec.
```

```sql title='explain'
┌─explain────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: IATA_CODE_Reporting_Airline as Carrier:String, (avg(cast((DepDelay > 10) as Int8)) * 1000) as c3:Float64                                                                                                                                       │
│   Sort: (avg(cast((DepDelay > 10) as Int8)) * 1000):Float64                                                                                                                                                                                                │
│     Expression: IATA_CODE_Reporting_Airline:String, (avg(cast((DepDelay > 10) as Int8)) * 1000):Float64 (Before OrderBy)                                                                                                                                   │
│       AggregatorFinal: groupBy=[[IATA_CODE_Reporting_Airline]], aggr=[[avg(cast((DepDelay > 10) as Int8))]]                                                                                                                                                │
│         AggregatorPartial: groupBy=[[IATA_CODE_Reporting_Airline]], aggr=[[avg(cast((DepDelay > 10) as Int8))]]                                                                                                                                            │
│           Expression: IATA_CODE_Reporting_Airline:String, cast((DepDelay > 10) as Int8):Int8 (Before GroupBy)                                                                                                                                              │
│             Filter: ((Year >= 2000) and (Year <= 2008))                                                                                                                                                                                                    │
│               ReadDataSource: scan schema: [Year:UInt16, IATA_CODE_Reporting_Airline:String, DepDelay:Int32], statistics: [read_rows: 195662214, read_bytes: 3130596992, partitions_scanned: 196, partitions_total: 196], push_downs: [projections: [0, 8, 31], filters: [((Year >= 2000) AND (Year <= 2008))]] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q7 (1.984 sec., 98.63 million rows/sec., 1.58 GB/sec.)

```sql title='mysql>'
SELECT
    IATA_CODE_Reporting_Airline AS Carrier,
    avg(DepDelay) * 1000 AS c3
FROM ontime
WHERE (Year >= 2000) AND (Year <= 2008)
GROUP BY Carrier
```

```sql title='result'
+---------+--------------------+
| Carrier | c3                 |
+---------+--------------------+
| HA      | -538.8768939038356 |
| OO      |  7156.105931260668 |
| EV      | 13248.640462884143 |
| B6      | 11177.923627230142 |
| WN      |  9800.984032216535 |
| FL      |   10091.3806677191 |
| OH      |  9174.990431086686 |
| 9E      |  7700.845777541507 |
| UA      | 10474.691240783097 |
| RU      |   6137.49191332561 |
| F9      |  6078.962351003004 |
| US      |  7003.805255113001 |
| AA      |  9107.644700861949 |
| AQ      | 1569.9276465368148 |
| CO      |  7723.676547969114 |
| DL      |  7316.332086536578 |
| HP      |  8606.658580393145 |
| XE      | 11066.582047557225 |
| NW      |  5953.887737746666 |
| MQ      |   9004.73935505603 |
| AS      |  9446.867494009617 |
| TW      |  7715.900299305268 |
| YV      | 12465.722388227468 |
| DH      |  9562.764009429655 |
| TZ      |  5604.692697405455 |
+---------+--------------------+
25 rows in set (2.67 sec)
Read 195662214 rows, 3.13 GB in 1.984 sec., 98.63 million rows/sec., 1.58 GB/sec.
```

```sql title='explain'
┌─explain────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: IATA_CODE_Reporting_Airline as Carrier:String, (avg(DepDelay) * 1000) as c3:Float64                                                                                                                                                            │
│   Expression: IATA_CODE_Reporting_Airline:String, (avg(DepDelay) * 1000):Float64 (Before Projection)                                                                                                                                                       │
│     AggregatorFinal: groupBy=[[IATA_CODE_Reporting_Airline]], aggr=[[avg(DepDelay)]]                                                                                                                                                                       │
│       AggregatorPartial: groupBy=[[IATA_CODE_Reporting_Airline]], aggr=[[avg(DepDelay)]]                                                                                                                                                                   │
│         Filter: ((Year >= 2000) and (Year <= 2008))                                                                                                                                                                                                        │
│           ReadDataSource: scan schema: [Year:UInt16, IATA_CODE_Reporting_Airline:String, DepDelay:Int32], statistics: [read_rows: 195662214, read_bytes: 3130596992, partitions_scanned: 196, partitions_total: 196], push_downs: [projections: [0, 8, 31], filters: [((Year >= 2000) AND (Year <= 2008))]] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q8 (1.371 sec., 142.75 million rows/sec., 856.47 MB/sec.)

```sql title='mysql>'
SELECT
    Year,
    avg(DepDelay)
FROM ontime
GROUP BY Year
```

```sql title='result'
+------+--------------------+
| Year | avg(DepDelay)      |
+------+--------------------+
| 1987 |  7.942636447211749 |
| 1988 |  6.642095416924255 |
| 1989 |  8.082143933983971 |
| 1990 |  6.840675574328676 |
| 1991 | 5.7044772180010535 |
| 1992 |   5.62925082631977 |
| 1993 | 6.0503186963181745 |
| 1994 |  6.574114757237771 |
| 1995 |  8.135946473302818 |
| 1996 |   9.75112383578199 |
| 1997 |  8.086793537802187 |
| 1998 |  8.782369968657616 |
| 1999 |  9.076455475549054 |
| 2000 |  10.90851967263336 |
| 2001 |  7.838910616678229 |
| 2002 |  5.463908832617927 |
| 2003 |  5.343154272904457 |
| 2004 |  7.750883470537657 |
| 2005 |  8.653783323640981 |
| 2006 |  9.921313478360586 |
| 2007 | 11.154102001513522 |
| 2008 |  9.778738997786789 |
| 2009 |  7.875202103472947 |
| 2010 |  8.122003058239098 |
| 2011 |   8.30051069128936 |
| 2012 |  7.697317526910186 |
| 2013 |   9.60551109179679 |
| 2014 | 10.417677824932802 |
| 2015 |  9.231430609551786 |
| 2016 |  8.837048463968436 |
| 2017 |   9.58803416122416 |
| 2018 |  9.807826800117446 |
| 2019 | 10.731779968221662 |
| 2020 | 1.9366636990295527 |
+------+--------------------+
34 rows in set (2.00 sec)
Read 195662214 rows, 1.17 GB in 1.371 sec., 142.75 million rows/sec., 856.47 MB/sec.
```

```sql title='explain'
┌─explain──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: Year:UInt16, avg(DepDelay):Float64                                                                                                                                                                   │
│   AggregatorFinal: groupBy=[[Year]], aggr=[[avg(DepDelay)]]                                                                                                                                                      │
│     AggregatorPartial: groupBy=[[Year]], aggr=[[avg(DepDelay)]]                                                                                                                                                  │
│       ReadDataSource: scan schema: [Year:UInt16, DepDelay:Int32], statistics: [read_rows: 195662214, read_bytes: 1173973284, partitions_scanned: 196, partitions_total: 196], push_downs: [projections: [0, 31]] │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q9 (1.525 sec., 128.28 million rows/sec., 256.55 MB/sec.)
```sql title='mysql>'
SELECT
    Year,
    count(*) AS c1
FROM ontime
GROUP BY Year
```

```sql title='result'
+------+---------+
| Year | c1      |
+------+---------+
| 1987 | 1311826 |
| 1988 | 5202096 |
| 1989 | 5041200 |
| 1990 | 5270893 |
| 1991 | 5076925 |
| 1992 | 5092157 |
| 1993 | 5070501 |
| 1994 | 5180048 |
| 1995 | 5327435 |
| 1996 | 5351983 |
| 1997 | 5411843 |
| 1998 | 5384721 |
| 1999 | 5527884 |
| 2000 | 5683047 |
| 2001 | 5967780 |
| 2002 | 5271359 |
| 2003 | 6013240 |
| 2004 | 7129270 |
| 2005 | 6033967 |
| 2006 | 7141922 |
| 2007 | 7455458 |
| 2008 | 7009726 |
| 2009 | 6450285 |
| 2010 | 6450117 |
| 2011 | 6085281 |
| 2012 | 6096762 |
| 2013 | 6369482 |
| 2014 | 5819811 |
| 2015 | 5819079 |
| 2016 | 5617658 |
| 2017 | 5674621 |
| 2018 | 7213446 |
| 2019 | 7422037 |
| 2020 | 4688354 |
+------+---------+
34 rows in set (2.23 sec)
Read 195662214 rows, 391.32 MB in 1.525 sec., 128.28 million rows/sec., 256.55 MB/sec.
```

```sql title='explain'
┌─explain─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: Year:UInt16, count() as c1:UInt64                                                                                                                                               │
│   AggregatorFinal: groupBy=[[Year]], aggr=[[count()]]                                                                                                                                       │
│     AggregatorPartial: groupBy=[[Year]], aggr=[[count()]]                                                                                                                                   │
│       ReadDataSource: scan schema: [Year:UInt16], statistics: [read_rows: 195662214, read_bytes: 391324428, partitions_scanned: 196, partitions_total: 196], push_downs: [projections: [0]] │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q10 (1.370 sec., 142.84 million rows/sec., 999.85 MB/sec.)

```sql title='mysql>'
SELECT avg(cnt)
FROM
(
    SELECT
        Year,
        Month,
        count(*) AS cnt
    FROM ontime
    WHERE DepDel15 = 1
    GROUP BY
        Year,
        Month
) AS a
```

```sql title='result'
+-------------------+
| avg(cnt)          |
+-------------------+
| 80743.73182957394 |
+-------------------+
1 row in set (2.02 sec)
Read 195662214 rows, 1.37 GB in 1.370 sec., 142.84 million rows/sec., 999.85 MB/sec.
```

```sql title='explain'
┌─explain────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: avg(cnt):Float64                                                                                                                                                                                                                               │
│   AggregatorFinal: groupBy=[[]], aggr=[[avg(cnt)]]                                                                                                                                                                                                         │
│     AggregatorPartial: groupBy=[[]], aggr=[[avg(cnt)]]                                                                                                                                                                                                     │
│       Projection: Year:UInt16, Month:UInt8, count() as cnt:UInt64                                                                                                                                                                                          │
│         AggregatorFinal: groupBy=[[Year, Month]], aggr=[[count()]]                                                                                                                                                                                         │
│           AggregatorPartial: groupBy=[[Year, Month]], aggr=[[count()]]                                                                                                                                                                                     │
│             Filter: (DepDel15 = 1)                                                                                                                                                                                                                         │
│               ReadDataSource: scan schema: [Year:UInt16, Month:UInt8, DepDel15:Int32], statistics: [read_rows: 195662214, read_bytes: 1369635498, partitions_scanned: 196, partitions_total: 196], push_downs: [projections: [0, 2, 33], filters: [(DepDel15 = 1)]] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q11 (1.394 sec., 140.32 million rows/sec., 420.95 MB/sec.)

```sql title='mysql>'
SELECT avg(c1)
FROM
(
    SELECT
        Year,
        Month,
        count(*) AS c1
    FROM ontime
    GROUP BY
        Year,
        Month
) AS a
```

```sql title='result'
+-------------------+
| avg(c1)           |
+-------------------+
| 490381.4887218045 |
+-------------------+
1 row in set (1.96 sec)
Read 195662214 rows, 586.99 MB in 1.394 sec., 140.32 million rows/sec., 420.95 MB/sec.
```

```sql title='explain'
┌─explain───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: avg(c1):Float64                                                                                                                                                                                       │
│   AggregatorFinal: groupBy=[[]], aggr=[[avg(c1)]]                                                                                                                                                                 │
│     AggregatorPartial: groupBy=[[]], aggr=[[avg(c1)]]                                                                                                                                                             │
│       Projection: Year:UInt16, Month:UInt8, count() as c1:UInt64                                                                                                                                                  │
│         AggregatorFinal: groupBy=[[Year, Month]], aggr=[[count()]]                                                                                                                                                │
│           AggregatorPartial: groupBy=[[Year, Month]], aggr=[[count()]]                                                                                                                                            │
│             ReadDataSource: scan schema: [Year:UInt16, Month:UInt8], statistics: [read_rows: 195662214, read_bytes: 586986642, partitions_scanned: 196, partitions_total: 196], push_downs: [projections: [0, 2]] │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q12 (4.691 sec., 41.71 million rows/sec., 1.77 GB/sec.)

```sql title='mysql>'
SELECT
    OriginCityName,
    DestCityName,
    count(*) AS c
FROM ontime
GROUP BY
    OriginCityName,
    DestCityName
ORDER BY c DESC
LIMIT 10
```

```sql title='result'
+-------------------+-------------------+--------+
| OriginCityName    | DestCityName      | c      |
+-------------------+-------------------+--------+
| San Francisco, CA | Los Angeles, CA   | 508490 |
| Los Angeles, CA   | San Francisco, CA | 505651 |
| New York, NY      | Chicago, IL       | 447403 |
| Chicago, IL       | New York, NY      | 440075 |
| Chicago, IL       | Minneapolis, MN   | 429892 |
| Minneapolis, MN   | Chicago, IL       | 425768 |
| Los Angeles, CA   | Las Vegas, NV     | 420528 |
| Las Vegas, NV     | Los Angeles, CA   | 414502 |
| New York, NY      | Boston, MA        | 412145 |
| Boston, MA        | New York, NY      | 408873 |
+-------------------+-------------------+--------+
10 rows in set (5.48 sec)
Read 195662214 rows, 8.28 GB in 4.691 sec., 41.71 million rows/sec., 1.77 GB/sec.
```

```sql title='explain'
┌─explain──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Limit: 10                                                                                                                                                                                                                            │
│   Projection: OriginCityName:String, DestCityName:String, count() as c:UInt64                                                                                                                                                        │
│     Sort: count():UInt64                                                                                                                                                                                                             │
│       AggregatorFinal: groupBy=[[OriginCityName, DestCityName]], aggr=[[count()]]                                                                                                                                                    │
│         AggregatorPartial: groupBy=[[OriginCityName, DestCityName]], aggr=[[count()]]                                                                                                                                                │
│           ReadDataSource: scan schema: [OriginCityName:String, DestCityName:String], statistics: [read_rows: 195662214, read_bytes: 8280594103, partitions_scanned: 196, partitions_total: 196], push_downs: [projections: [15, 24]] │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

```

## Q13 (2.326 sec., 84.12 million rows/sec., 1.78 GB/sec.)

```sql title='mysql>'
SELECT
    OriginCityName,
    count(*) AS c
FROM ontime
GROUP BY OriginCityName
ORDER BY c DESC
LIMIT 10
```
```sql title='result'
+-----------------------+----------+
| OriginCityName        | c        |
+-----------------------+----------+
| Chicago, IL           | 12226995 |
| Atlanta, GA           | 10556206 |
| Dallas/Fort Worth, TX |  8746526 |
| Houston, TX           |  6642902 |
| Los Angeles, CA       |  6517688 |
| New York, NY          |  6144673 |
| Denver, CO            |  6034004 |
| Phoenix, AZ           |  5484188 |
| Washington, DC        |  4855803 |
| San Francisco, CA     |  4579841 |
+-----------------------+----------+
10 rows in set (3.12 sec)
Read 195662214 rows, 4.14 GB in 2.326 sec., 84.12 million rows/sec., 1.78 GB/sec.
```

```sql title='explain'
┌─explain─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Limit: 10                                                                                                                                                                                                   │
│   Projection: OriginCityName:String, count() as c:UInt64                                                                                                                                                    │
│     Sort: count():UInt64                                                                                                                                                                                    │
│       AggregatorFinal: groupBy=[[OriginCityName]], aggr=[[count()]]                                                                                                                                         │
│         AggregatorPartial: groupBy=[[OriginCityName]], aggr=[[count()]]                                                                                                                                     │
│           ReadDataSource: scan schema: [OriginCityName:String], statistics: [read_rows: 195662214, read_bytes: 4140328419, partitions_scanned: 196, partitions_total: 196], push_downs: [projections: [15]] │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q14

```sql title='mysql>'
SELECT
       count(*)
FROM ontime;
```

```sql title='result'
+-----------+
| count()   |
+-----------+
| 195662214 |
+-----------+
1 row in set (0.35 sec)
```

```sql title='explain'
┌─explain─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: count():UInt64                                                                                                              │
│   Projection: 195662214 as count():UInt64                                                                                               │
│     Expression: 195662214:UInt64 (Exact Statistics)                                                                                     │
│       ReadDataSource: scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1, partitions_scanned: 1, partitions_total: 1] │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```
