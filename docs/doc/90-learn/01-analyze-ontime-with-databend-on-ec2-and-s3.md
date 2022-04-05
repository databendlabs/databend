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

## Step 2. Load OnTime Datasets

### 2.1 Create OnTime Table

```sql
CREATE TABLE ontime
(
    Year                            UInt16 NOT NULL,
    Quarter                         UInt8 NOT NULL,
    Month                           UInt8 NOT NULL,
    DayofMonth                      UInt8 NOT NULL,
    DayOfWeek                       UInt8 NOT NULL,
    FlightDate                      Date NOT NULL,
    Reporting_Airline               String NOT NULL,
    DOT_ID_Reporting_Airline        Int32 NOT NULL,
    IATA_CODE_Reporting_Airline     String NOT NULL,
    Tail_Number                     String NOT NULL,
    Flight_Number_Reporting_Airline String NOT NULL,
    OriginAirportID                 Int32 NOT NULL,
    OriginAirportSeqID              Int32 NOT NULL,
    OriginCityMarketID              Int32 NOT NULL,
    Origin                          String NOT NULL,
    OriginCityName                  String NOT NULL,
    OriginState                     String NOT NULL,
    OriginStateFips                 String NOT NULL,
    OriginStateName                 String NOT NULL,
    OriginWac                       Int32 NOT NULL,
    DestAirportID                   Int32 NOT NULL,
    DestAirportSeqID                Int32 NOT NULL,
    DestCityMarketID                Int32 NOT NULL,
    Dest                            String NOT NULL,
    DestCityName                    String NOT NULL,
    DestState                       String NOT NULL,
    DestStateFips                   String NOT NULL,
    DestStateName                   String NOT NULL,
    DestWac                         Int32 NOT NULL,
    CRSDepTime                      Int32 NOT NULL,
    DepTime                         Int32 NOT NULL,
    DepDelay                        Int32 NOT NULL,
    DepDelayMinutes                 Int32 NOT NULL,
    DepDel15                        Int32 NOT NULL,
    DepartureDelayGroups            String NOT NULL,
    DepTimeBlk                      String NOT NULL,
    TaxiOut                         Int32 NOT NULL,
    WheelsOff                       Int32 NOT NULL,
    WheelsOn                        Int32 NOT NULL,
    TaxiIn                          Int32 NOT NULL,
    CRSArrTime                      Int32 NOT NULL,
    ArrTime                         Int32 NOT NULL,
    ArrDelay                        Int32 NOT NULL,
    ArrDelayMinutes                 Int32 NOT NULL,
    ArrDel15                        Int32 NOT NULL,
    ArrivalDelayGroups              Int32 NOT NULL,
    ArrTimeBlk                      String NOT NULL,
    Cancelled                       UInt8 NOT NULL,
    CancellationCode                String NOT NULL,
    Diverted                        UInt8 NOT NULL,
    CRSElapsedTime                  Int32 NOT NULL,
    ActualElapsedTime               Int32 NOT NULL,
    AirTime                         Int32 NOT NULL,
    Flights                         Int32 NOT NULL,
    Distance                        Int32 NOT NULL,
    DistanceGroup                   UInt8 NOT NULL,
    CarrierDelay                    Int32 NOT NULL,
    WeatherDelay                    Int32 NOT NULL,
    NASDelay                        Int32 NOT NULL,
    SecurityDelay                   Int32 NOT NULL,
    LateAircraftDelay               Int32 NOT NULL,
    FirstDepTime                    String NOT NULL,
    TotalAddGTime                   String NOT NULL,
    LongestAddGTime                 String NOT NULL,
    DivAirportLandings              String NOT NULL,
    DivReachedDest                  String NOT NULL,
    DivActualElapsedTime            String NOT NULL,
    DivArrDelay                     String NOT NULL,
    DivDistance                     String NOT NULL,
    Div1Airport                     String NOT NULL,
    Div1AirportID                   Int32 NOT NULL,
    Div1AirportSeqID                Int32 NOT NULL,
    Div1WheelsOn                    String NOT NULL,
    Div1TotalGTime                  String NOT NULL,
    Div1LongestGTime                String NOT NULL,
    Div1WheelsOff                   String NOT NULL,
    Div1TailNum                     String NOT NULL,
    Div2Airport                     String NOT NULL,
    Div2AirportID                   Int32 NOT NULL,
    Div2AirportSeqID                Int32 NOT NULL,
    Div2WheelsOn                    String NOT NULL,
    Div2TotalGTime                  String NOT NULL,
    Div2LongestGTime                String NOT NULL,
    Div2WheelsOff                   String NOT NULL,
    Div2TailNum                     String NOT NULL,
    Div3Airport                     String NOT NULL,
    Div3AirportID                   Int32 NOT NULL,
    Div3AirportSeqID                Int32 NOT NULL,
    Div3WheelsOn                    String NOT NULL,
    Div3TotalGTime                  String NOT NULL,
    Div3LongestGTime                String NOT NULL,
    Div3WheelsOff                   String NOT NULL,
    Div3TailNum                     String NOT NULL,
    Div4Airport                     String NOT NULL,
    Div4AirportID                   Int32 NOT NULL,
    Div4AirportSeqID                Int32 NOT NULL,
    Div4WheelsOn                    String NOT NULL,
    Div4TotalGTime                  String NOT NULL,
    Div4LongestGTime                String NOT NULL,
    Div4WheelsOff                   String NOT NULL,
    Div4TailNum                     String NOT NULL,
    Div5Airport                     String NOT NULL,
    Div5AirportID                   Int32 NOT NULL,
    Div5AirportSeqID                Int32 NOT NULL,
    Div5WheelsOn                    String NOT NULL,
    Div5TotalGTime                  String NOT NULL,
    Div5LongestGTime                String NOT NULL,
    Div5WheelsOff                   String NOT NULL,
    Div5TailNum                     String NOT NULL
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

```shell title='mysql'
mysql -h127.0.0.1 -P3307 -uroot
```
```shell
select Year, count(*) from ontime group by Year;
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
