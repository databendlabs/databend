---
title: Deploy Databend on EC2 use S3
draft: true
---

Complie Databend Deploy on EC2 use S3

## 1. Deploy environment
- EC2 size : c5a.4xlarge
  >EC2 region: <your S3 bucket region\> 
  >
  >local disk 300G, local disk only used for ontime save and databend complie. 

- Os Type: ubuntu 20 x64

prepare install package:

```shell
$sudo apt-get install unzip make mysql-client-core-8.0
```

## 2. Deploy Databend
### 2.1 Complie Databend

```shell
$git clone https://github.com/datafuselabs/databend.git

$cd databend

$make setup

$export PATH=$PATH:~/.cargo/bin

$make build-native
```

Finally, the databend-related binary file at ./target/release/{databend-meta, databend-query}

### 2.2 Start Databend

```shell
# Please replace the s3 env config with your own. 
export STORAGE_TYPE=s3
export S3_STORAGE_BUCKET=<your-s3-bucket>
export S3_STORAGE_REGION=<your-s3-region>
export S3_STORAGE_ENDPOINT_URL=<your-bucket>.s3.amazonaws.com
export S3_STORAGE_ACCESS_KEY_ID=<your-s3-key-id>
export S3_STORAGE_SECRET_ACCESS_KEY=<your-s3-access-key>

echo "Starting standalone DatabendQuery(release)"
./scripts/ci/deploy/databend-query-standalone.sh release
```

### 2.3 Test databend

```shell
mysql -h 127.0.0.1 -P3307 -uroot
```
Check connect is ok .

## 3. Load Ontime
### 3.1 Create ontime table

```shell
wget --no-check-certificate https://repo.databend.rs/t_ontime/create_table.sql

cat create_table.sql |mysql -h 127.0.0.1 -P3307 -uroot
```

3.2 Load Data

```shell
wget --no-check-certificate https://repo.databend.rs/t_ontime/t_ontime.csv.zip

unzip t_ontime.csv.zip

ls *.csv|xargs -I{} echo  curl -H \"insert_sql:insert into ontime format CSV\" -H \"csv_header:0\" -H \"field_delimitor:'\t'\"  -F  \"upload=@{}\"  -XPUT http://localhost:8001/v1/streaming_load |bash
```

## 4. Queries
Execute Query
```shell
mysql -h 127.0.0.1 -P3307 -uroot 
mysql>set parallel_read_threads=4;
mysql>select count(*) from ontime;
mysql>select Year, count(*) from ontime group by Year;
```

Benchmark Queries gist

| Number      | Query | 
| ----------- | ----------- |
| Q1   | SELECT DayOfWeek, count(*) AS c FROM ontime WHERE Year >= 2000 AND Year <= 2008 GROUP BY DayOfWeek ORDER BY c DESC;       |
| Q2   | SELECT DayOfWeek, count(*) AS c FROM ontime WHERE DepDelay>10 AND Year >= 2000 AND Year <= 2008 GROUP BY DayOfWeek ORDER BY c DESC;    |
| Q3   |SELECT Origin, count(*) AS c FROM ontime WHERE DepDelay>10 AND Year >= 2000 AND Year <= 2008 GROUP BY Origin ORDER BY c DESC LIMIT 10;   | 
| Q4   |SELECT IATA_CODE_Reporting_Airline AS Carrier, count(*) FROM ontime WHERE DepDelay>10 AND Year = 2007 GROUP BY Carrier ORDER BY count(*) DESC;      | 
| Q5   |SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(cast(DepDelay>10 as Int8))*1000 AS c3 FROM ontime WHERE Year=2007 GROUP BY Carrier ORDER BY c3 DESC;| 
| Q6   |SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(cast(DepDelay>10 as Int8))*1000 AS c3 FROM ontime WHERE Year>=2000 AND Year <=2008 GROUP BY Carrier ORDER BY c3 DESC;| 
| Q7   |SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(DepDelay) * 1000 AS c3 FROM ontime WHERE Year >= 2000 AND Year <= 2008 GROUP BY Carrier| 
| Q8   |SELECT Year, avg(DepDelay) FROM ontime GROUP BY Year;      |
| Q9   |select Year, count(*) as c1 from ontime group by Year;      | 
| Q10  |SELECT avg(cnt) FROM (SELECT Year,Month,count(*) AS cnt FROM ontime WHERE DepDel15=1 GROUP BY Year,Month) a;      |
| Q11  |select avg(c1) from (select Year,Month,count(*) as c1 from ontime group by Year,Month) a;      |
| Q12  | SELECT OriginCityName, DestCityName, count(*) AS c FROM ontime GROUP BY OriginCityName, DestCityName ORDER BY c DESC LIMIT 10;     |
| Q13  |SELECT OriginCityName, count(*) AS c FROM ontime GROUP BY OriginCityName ORDER BY c DESC LIMIT 10;      |
| Q14  | select count(*) from ontime     |

## Next steps

- [Streaming Load](/user/data-loading/http-streaming-load)

## Learn more

- [Databend CLI](/user/cli/)
- [Deploy Databend](/user/self-hosted/get-started/deploy-databend)