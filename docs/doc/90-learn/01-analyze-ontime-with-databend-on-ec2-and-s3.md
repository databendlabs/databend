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

```text
CREATE TABLE ontime
(
    Year                            UInt16,
    Quarter                         UInt8,
    Month                           UInt8,
    DayofMonth                      UInt8,
    DayOfWeek                       UInt8,
    FlightDate                      Date,
    Reporting_Airline               String,
    DOT_ID_Reporting_Airline        Int32,
    IATA_CODE_Reporting_Airline     String,
    Tail_Number                     String,
    Flight_Number_Reporting_Airline String,
    OriginAirportID                 Int32,
    OriginAirportSeqID              Int32,
    OriginCityMarketID              Int32,
    Origin                          String,
    OriginCityName                  String,
    OriginState                     String,
    OriginStateFips                 String,
    OriginStateName                 String,
    OriginWac                       Int32,
    DestAirportID                   Int32,
    DestAirportSeqID                Int32,
    DestCityMarketID                Int32,
    Dest                            String,
    DestCityName                    String,
    DestState                       String,
    DestStateFips                   String,
    DestStateName                   String,
    DestWac                         Int32,
    CRSDepTime                      Int32,
    DepTime                         Int32,
    DepDelay                        Int32,
    DepDelayMinutes                 Int32,
    DepDel15                        Int32,
    DepartureDelayGroups            String,
    DepTimeBlk                      String,
    TaxiOut                         Int32,
    WheelsOff                       Int32,
    WheelsOn                        Int32,
    TaxiIn                          Int32,
    CRSArrTime                      Int32,
    ArrTime                         Int32,
    ArrDelay                        Int32,
    ArrDelayMinutes                 Int32,
    ArrDel15                        Int32,
    ArrivalDelayGroups              Int32,
    ArrTimeBlk                      String,
    Cancelled                       UInt8,
    CancellationCode                String,
    Diverted                        UInt8,
    CRSElapsedTime                  Int32,
    ActualElapsedTime               Int32,
    AirTime                         Int32,
    Flights                         Int32,
    Distance                        Int32,
    DistanceGroup                   UInt8,
    CarrierDelay                    Int32,
    WeatherDelay                    Int32,
    NASDelay                        Int32,
    SecurityDelay                   Int32,
    LateAircraftDelay               Int32,
    FirstDepTime                    String,
    TotalAddGTime                   String,
    LongestAddGTime                 String,
    DivAirportLandings              String,
    DivReachedDest                  String,
    DivActualElapsedTime            String,
    DivArrDelay                     String,
    DivDistance                     String,
    Div1Airport                     String,
    Div1AirportID                   Int32,
    Div1AirportSeqID                Int32,
    Div1WheelsOn                    String,
    Div1TotalGTime                  String,
    Div1LongestGTime                String,
    Div1WheelsOff                   String,
    Div1TailNum                     String,
    Div2Airport                     String,
    Div2AirportID                   Int32,
    Div2AirportSeqID                Int32,
    Div2WheelsOn                    String,
    Div2TotalGTime                  String,
    Div2LongestGTime                String,
    Div2WheelsOff                   String,
    Div2TailNum                     String,
    Div3Airport                     String,
    Div3AirportID                   Int32,
    Div3AirportSeqID                Int32,
    Div3WheelsOn                    String,
    Div3TotalGTime                  String,
    Div3LongestGTime                String,
    Div3WheelsOff                   String,
    Div3TailNum                     String,
    Div4Airport                     String,
    Div4AirportID                   Int32,
    Div4AirportSeqID                Int32,
    Div4WheelsOn                    String,
    Div4TotalGTime                  String,
    Div4LongestGTime                String,
    Div4WheelsOff                   String,
    Div4TailNum                     String,
    Div5Airport                     String,
    Div5AirportID                   Int32,
    Div5AirportSeqID                Int32,
    Div5WheelsOn                    String,
    Div5TotalGTime                  String,
    Div5LongestGTime                String,
    Div5WheelsOff                   String,
    Div5TailNum                     String
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
