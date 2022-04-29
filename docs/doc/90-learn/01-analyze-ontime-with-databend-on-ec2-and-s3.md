---
title: Analyzing OnTime with Databend on Object Storage
sidebar_label: Analyzing OnTime Dataset
---

Analyzing `OnTime` datasets on S3 with Databend step by step.

## Step 1. Deploy Databend

Make sure you have installed Databend, if not please see:

* [How to Deploy Databend with Amazon S3](../10-deploy/01-s3.md)
* [How to Deploy Databend with Tencent COS](../10-deploy/02-cos.md)
* [How to Deploy Databend with Alibaba OSS](../10-deploy/03-oss.md)
* [How to Deploy Databend with Wasabi](../10-deploy/05-wasabi.md)
* [How to Deploy Databend with Scaleway OS](../10-deploy/06-scw.md)
* [How to Deploy Databend with QingCloud QingStore](../10-deploy/07-qingstore.md)

## Step 2. Load OnTime Datasets

### 2.1 Create OnTime Table

```sql
CREATE TABLE ontime
(
    Year                            SMALLINT UNSIGNED,
    Quarter                         TINYINT UNSIGNED,
    Month                           TINYINT UNSIGNED,
    DayofMonth                      TINYINT UNSIGNED,
    DayOfWeek                       TINYINT UNSIGNED,
    FlightDate                      DATE,
    Reporting_Airline               VARCHAR,
    DOT_ID_Reporting_Airline        INT,
    IATA_CODE_Reporting_Airline     VARCHAR,
    Tail_Number                     VARCHAR,
    Flight_Number_Reporting_Airline VARCHAR,
    OriginAirportID                 INT,
    OriginAirportSeqID              INT,
    OriginCityMarketID              INT,
    Origin                          VARCHAR,
    OriginCityName                  VARCHAR,
    OriginState                     VARCHAR,
    OriginStateFips                 VARCHAR,
    OriginStateName                 VARCHAR,
    OriginWac                       INT,
    DestAirportID                   INT,
    DestAirportSeqID                INT,
    DestCityMarketID                INT,
    Dest                            VARCHAR,
    DestCityName                    VARCHAR,
    DestState                       VARCHAR,
    DestStateFips                   VARCHAR,
    DestStateName                   VARCHAR,
    DestWac                         INT,
    CRSDepTime                      INT,
    DepTime                         INT,
    DepDelay                        INT,
    DepDelayMinutes                 INT,
    DepDel15                        INT,
    DepartureDelayGroups            VARCHAR,
    DepTimeBlk                      VARCHAR,
    TaxiOut                         INT,
    WheelsOff                       INT,
    WheelsOn                        INT,
    TaxiIn                          INT,
    CRSArrTime                      INT,
    ArrTime                         INT,
    ArrDelay                        INT,
    ArrDelayMinutes                 INT,
    ArrDel15                        INT,
    ArrivalDelayGroups              INT,
    ArrTimeBlk                      VARCHAR,
    Cancelled                       TINYINT UNSIGNED,
    CancellationCode                VARCHAR,
    Diverted                        TINYINT UNSIGNED,
    CRSElapsedTime                  INT,
    ActualElapsedTime               INT,
    AirTime                         INT,
    Flights                         INT,
    Distance                        INT,
    DistanceGroup                   TINYINT UNSIGNED,
    CarrierDelay                    INT,
    WeatherDelay                    INT,
    NASDelay                        INT,
    SecurityDelay                   INT,
    LateAircraftDelay               INT,
    FirstDepTime                    VARCHAR,
    TotalAddGTime                   VARCHAR,
    LongestAddGTime                 VARCHAR,
    DivAirportLandings              VARCHAR,
    DivReachedDest                  VARCHAR,
    DivActualElapsedTime            VARCHAR,
    DivArrDelay                     VARCHAR,
    DivDistance                     VARCHAR,
    Div1Airport                     VARCHAR,
    Div1AirportID                   INT,
    Div1AirportSeqID                INT,
    Div1WheelsOn                    VARCHAR,
    Div1TotalGTime                  VARCHAR,
    Div1LongestGTime                VARCHAR,
    Div1WheelsOff                   VARCHAR,
    Div1TailNum                     VARCHAR,
    Div2Airport                     VARCHAR,
    Div2AirportID                   INT,
    Div2AirportSeqID                INT,
    Div2WheelsOn                    VARCHAR,
    Div2TotalGTime                  VARCHAR,
    Div2LongestGTime                VARCHAR,
    Div2WheelsOff                   VARCHAR,
    Div2TailNum                     VARCHAR,
    Div3Airport                     VARCHAR,
    Div3AirportID                   INT,
    Div3AirportSeqID                INT,
    Div3WheelsOn                    VARCHAR,
    Div3TotalGTime                  VARCHAR,
    Div3LongestGTime                VARCHAR,
    Div3WheelsOff                   VARCHAR,
    Div3TailNum                     VARCHAR,
    Div4Airport                     VARCHAR,
    Div4AirportID                   INT,
    Div4AirportSeqID                INT,
    Div4WheelsOn                    VARCHAR,
    Div4TotalGTime                  VARCHAR,
    Div4LongestGTime                VARCHAR,
    Div4WheelsOff                   VARCHAR,
    Div4TailNum                     VARCHAR,
    Div5Airport                     VARCHAR,
    Div5AirportID                   INT,
    Div5AirportSeqID                INT,
    Div5WheelsOn                    VARCHAR,
    Div5TotalGTime                  VARCHAR,
    Div5LongestGTime                VARCHAR,
    Div5WheelsOff                   VARCHAR,
    Div5TailNum                     VARCHAR
);
```

### 2.2 Load Data Into OnTime Table

```shell title='t_ontime.csv.zip'
wget --no-check-certificate https://repo.databend.rs/t_ontime/t_ontime.csv.zip
```

```shell title='Unzip'
unzip t_ontime.csv.zip
```

```shell title='Load CSV files into Databend'
ls *.csv|xargs -I{} echo  curl -H \"insert_sql:insert into ontime format CSV\" -H \"skip_header:0\" -H \"field_delimiter:\t\"  -F  \"upload=@{}\"  -XPUT http://127.0.0.1:8081/v1/streaming_load |bash
```

:::tip

* http://127.0.0.1:8081/v1/streaming_load
    * `127.0.0.1` is `http_handler_host` value in your *databend-query.toml*
    * `8081` is `http_handler_port` value in your *databend-query.toml*
:::

## Step 3. Queries

Execute Queries:

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


## Benchmark Report

* [Amazon S3: Databend Ontime Datasets Benchmark Report](../70-performance/01-ec2-s3-performance.md)
* [Tencent COS: Databend Ontime Datasets Benchmark Report](../70-performance/02-cvm-cos-performance.md)
* [Alibaba OSS: Databend Ontime Datasets Benchmark Report](../70-performance/03-ecs-oss-performance.md)
* [Wasabi: Databend Ontime Datasets Benchmark Report](../70-performance/04-ec2-wasabi-performance.md)
