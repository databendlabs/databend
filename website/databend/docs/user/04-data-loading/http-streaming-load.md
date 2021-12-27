---
title: Streaming Load
---

Load data into a table using HTTP streaming API.

## HTTP Streaming Load
### Syntax

```
curl -u user:passwd -H <options>  -F  "upload=@<files_location>"  -XPUT http://localhost:8001/v1/streaming_load

```
### Parameters

  * `options`: key value options, supported options: `insert_sql`, `field_delimitor`, `record_delimitor`, `csv_header`
  * `insert_sql`: must be specified in options, eg: `insert into table_name (a,b,c) format CSV`
  * `files_location`: local file path, eg: `/tmp/data.csv`

:::note Notes
Currently, only csv format is supported for streaming load.
:::

### Response
```
{
	"id": "af101056-116a-4c3a-b42b-0c56ade3885d",  // unique id for this load
	"state": "SUCCESS",   // load state
	"stats": {   // progress stats
		"read_rows": 371357,
		"read_bytes": 271086978,
		"total_rows_to_read": 0
	},
	"error": null  // error msg
}

```

## Examples

### 1. Download OnTime raw data

Download raw data to `/tmp/dataset`:

```
wget -P /tmp/ https://repo.databend.rs/dataset/stateful/ontime.csv
```


### 2. Build databend-query

```shell
make build
```

### 3. Setting up Databend server
```
./target/release/databend-query 
2021-12-22T08:49:15.182736Z  INFO databend_query: Config { config_file: "", query: QueryConfig { tenant_id: "", cluster_id: "", num_cpus: 16, mysql_handler_host: "127.0.0.1", mysql_handler_port: 3307, max_active_sessions: 256, clickhouse_handler_host: "127.0.0.1", clickhouse_handler_port: 9000, http_handler_host: "127.0.0.1", http_handler_port: 8000, flight_api_address: "127.0.0.1:9090", http_api_address: "127.0.0.1:8080", metric_api_address: "127.0.0.1:7070", http_handler_tls_server_cert: "", http_handler_tls_server_key: "", http_handler_tls_server_root_ca_cert: "", api_tls_server_cert: "", api_tls_server_key: "", api_tls_server_root_ca_cert: "", rpc_tls_server_cert: "", rpc_tls_server_key: "", rpc_tls_query_server_root_ca_cert: "", rpc_tls_query_service_domain_name: "localhost", table_engine_csv_enabled: false, table_engine_parquet_enabled: false, table_engine_memory_enabled: true, database_engine_github_enabled: true, wait_timeout_mills: 5000, max_query_log_size: 10000, table_cache_enabled: false, table_memory_cache_mb_size: 256, table_disk_cache_root: "_cache", table_disk_cache_mb_size: 1024 }, log: LogConfig { log_level: "INFO", log_dir: "./_logs" }, meta: {meta_address: "", meta_user: "", meta_password: "******"}, storage: StorageConfig { storage_type: "disk", disk: DiskStorageConfig { data_path: "_data", temp_data_path: "" }, s3: {s3.storage.region: "", s3.storage.endpoint_url: "", s3.storage.bucket: "", }, azure_storage_blob: {Azure.storage.container: "", } } }
2021-12-22T08:49:15.182815Z  INFO databend_query: DatabendQuery v-0.1.0-edbe0d4-simd(1.59.0-nightly-2021-12-22T03:01:13.951230282+00:00)
2021-12-22T08:49:15.182914Z  INFO databend_query::catalogs::impls::mutable_catalog: use embedded meta
2021-12-22T08:49:15.185035Z  INFO databend_query: MySQL handler listening on 127.0.0.1:3307, Usage: mysql -h127.0.0.1 -P3307
2021-12-22T08:49:15.185756Z  INFO databend_query: ClickHouse handler listening on 127.0.0.1:9000, Usage: clickhouse-client --host 127.0.0.1 --port 9000
2021-12-22T08:49:15.186376Z  INFO databend_query: Http handler listening on 127.0.0.1:8000  examples:
curl --request POST '127.0.0.1:8000/v1/statement/' --header 'Content-Type: text/plain' --data-raw 'SELECT avg(number) FROM numbers(100000000)'
curl --request POST '127.0.0.1:8000/v1/query/' --header 'Content-Type: application/json' --data-raw '{"sql": "SELECT avg(number) FROM numbers(100000000)"}'
2021-12-22T08:49:15.186534Z  INFO databend_query: Metric API server listening on 127.0.0.1:7070
2021-12-22T08:49:15.186518Z  INFO poem::server: listening addr=socket://127.0.0.1:8000
2021-12-22T08:49:15.186675Z  WARN databend_query::api::http_service: Http API TLS not set
2021-12-22T08:49:15.186693Z  INFO poem::server: listening addr=socket://127.0.0.1:7070
2021-12-22T08:49:15.186658Z  INFO poem::server: server started
2021-12-22T08:49:15.186770Z  INFO poem::server: server started
2021-12-22T08:49:15.187045Z  INFO databend_query: HTTP API server listening on 127.0.0.1:8080
2021-12-22T08:49:15.187105Z  INFO databend_query: RPC API server listening on 127.0.0.1:9090
2021-12-22T08:49:15.187115Z  INFO poem::server: listening addr=socket://127.0.0.1:8080
2021-12-22T08:49:15.187146Z  INFO poem::server: server started
2021-12-22T08:49:15.188753Z  INFO databend_query: Databend query has been registered:"" to metasrv:[""].
2021-12-22T08:49:15.188805Z  INFO databend_query: Ready for connections.

```

### 4. Create the ontime table

```
mysql -h127.0.0.1 -uroot -P3307
```
```
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
) ENGINE = FUSE;
```

### 5. Load raw data into ontime table
```
curl -H "insert_sql:insert into ontime format CSV" -H "csv_header:1" -F  "upload=@/tmp/ontime.csv"  -XPUT http://localhost:8000/v1/streaming_load
```

### 6. Queries

:::note Notes
Example query result is based on ontime 1987-2021 datasets(60GB+), not databend small dataset.
:::

#### Q0
```
mysql> SELECT DayOfWeek, count(*) AS c FROM ontime WHERE Year >= 2000 AND Year <= 2008 GROUP BY DayOfWeek ORDER BY c DESC;
+-----------+---------+
| DayOfWeek | c       |
+-----------+---------+
|         5 | 8732422 |
|         1 | 8730614 |
|         4 | 8710843 |
|         3 | 8685626 |
|         2 | 8639632 |
|         7 | 8274367 |
|         6 | 7514194 |
+-----------+---------+
```

#### Q1
```
mysql> SELECT DayOfWeek, count(*) AS c FROM ontime WHERE DepDelay>10 AND Year >= 2000 AND Year <= 2008 GROUP BY DayOfWeek ORDER BY c DESC;
+-----------+---------+
| DayOfWeek | c       |
+-----------+---------+
|         5 | 2175733 |
|         4 | 2012848 |
|         1 | 1898879 |
|         7 | 1880896 |
|         3 | 1757508 |
|         2 | 1665303 |
|         6 | 1510894 |
+-----------+---------+
```

#### Q2
```
mysql> SELECT Origin, count(*) AS c FROM ontime WHERE DepDelay>10 AND Year >= 2000 AND Year <= 2008 GROUP BY Origin ORDER BY c DESC LIMIT 10;
+--------+--------+
| Origin | c      |
+--------+--------+
| ORD    | 860911 |
| ATL    | 831822 |
| DFW    | 614403 |
| LAX    | 402671 |
| PHX    | 400475 |
| LAS    | 362026 |
| DEN    | 352893 |
| EWR    | 302267 |
| DTW    | 296832 |
| IAH    | 290729 |
+--------+--------+
```

#### Q3
```
mysql> SELECT IATA_CODE_Reporting_Airline AS Carrier, count() FROM ontime WHERE DepDelay>10 AND Year = 2007 GROUP BY Carrier ORDER BY count() DESC;
+---------+---------+
| Carrier | count() |
+---------+---------+
| WN      |  296451 |
| AA      |  179769 |
| MQ      |  152293 |
| OO      |  147019 |
| US      |  140199 |
| UA      |  135061 |
| XE      |  108571 |
| EV      |  104055 |
| NW      |  102206 |
| DL      |   98427 |
| CO      |   81039 |
| YV      |   79553 |
| FL      |   64583 |
| OH      |   60532 |
| AS      |   54326 |
| B6      |   53716 |
| 9E      |   48578 |
| F9      |   24100 |
| AQ      |    6764 |
| HA      |    4059 |
+---------+---------+
```

#### Q4
```
mysql> SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(cast(DepDelay>10, Int))*1000 AS c3 FROM ontime WHERE Year=2007 GROUP BY Carrier ORDER BY c3 DESC;
+---------+--------------------+
| Carrier | c3                 |
+---------+--------------------+
| EV      | 375.22447478309783 |
| AS      |  344.6600093895522 |
| US      | 294.22356533509617 |
| MQ      | 294.17116410599147 |
| AA      |  291.8645372142749 |
| B6      | 286.11910088420154 |
| UA      |  282.5112482821661 |
| YV      | 281.03152874679853 |
| OH      | 266.51990137372314 |
| XE      | 256.07998622553794 |
| WN      | 255.80863559408323 |
| CO      | 253.08869456589633 |
| OO      |  251.8768395769016 |
| NW      | 251.29821593658414 |
| FL      |  247.8651504277375 |
| F9      |  247.5298371027711 |
| DL      |  209.7011300301682 |
| 9E      | 193.60572631041958 |
| AQ      |  147.1330375010876 |
| HA      |  72.56377710638755 |
+---------+--------------------+
```

#### Q5
```
mysql> SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(cast(DepDelay>10, Int))*1000 AS c3 FROM ontime WHERE Year>=2000 AND Year <=2008 GROUP BY Carrier ORDER BY c3 DESC;
+---------+--------------------+
| Carrier | c3                 |
+---------+--------------------+
| AS      | 300.16054129040174 |
| EV      | 290.38981458284934 |
| YV      |  280.2119596805801 |
| B6      | 260.37899104450076 |
| MQ      | 255.53357961252766 |
| XE      |  252.9195476037731 |
| FL      |  251.8442154957222 |
| WN      | 235.84250684474472 |
| DH      |  234.6173854586053 |
| UA      |  230.5225780592861 |
| F9      | 227.28181321845082 |
| OH      | 222.69791588865388 |
| AA      |  217.1668568387609 |
| US      | 211.46089212168485 |
| HP      | 209.72498201847765 |
| OO      |  206.7869938260996 |
| NW      | 195.14773751954678 |
| TW      | 192.83579347949788 |
| DL      |  192.0257041477902 |
| CO      | 189.97455773567307 |
| 9E      | 186.99556442451123 |
| RU      | 185.12495614348302 |
| TZ      | 178.87275426586388 |
| AQ      | 148.38594731563109 |
| HA      |  79.77123428701232 |
+---------+--------------------+
```

#### Q6
```
mysql> SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(DepDelay) * 1000 AS c3 FROM ontime WHERE Year >= 2000 AND Year <= 2008 GROUP BY Carrier;
+---------+--------------------+
| Carrier | c3                 |
+---------+--------------------+
| OH      | 13076.047727826812 |
| OO      | 11850.612432410993 |
| AQ      |  7460.374544686692 |
| TW      | 11080.878345056713 |
| AS      | 15092.753681433545 |
| AA      | 13839.899526869556 |
| B6      |   16983.6611696882 |
| CO      | 12824.261901022188 |
| DH      | 15818.094685863602 |
| US      | 12147.763005018298 |
| HP      | 11875.281344265648 |
| NW      | 11925.914608626837 |
| 9E      | 13477.094055933694 |
| HA      |  6884.741502804615 |
| F9      |  11292.37461327096 |
| RU      | 12809.675911982687 |
| WN      | 10595.506511109697 |
| FL      | 15348.271252597235 |
| DL      | 11187.216291143079 |
| TZ      | 12760.000582204906 |
| MQ      |  14710.31183749049 |
| XE      | 17531.281432904856 |
| UA      |  14990.54869123628 |
| YV      | 18624.374635216514 |
| EV      | 16857.624838267257 |
+---------+--------------------+
```

#### Q7
```
mysql> SELECT Year, avg(DepDelay) FROM ontime GROUP BY Year;
+------+--------------------+
| Year | avg(DepDelay)      |
+------+--------------------+
| 1989 |  8.950126987226787 |
| 1997 | 10.101709232830519 |
| 2016 | 14.811187637756063 |
| 1990 |  8.046787398904078 |
| 2003 |  9.933812071292147 |
| 2006 |  14.48459327850703 |
| 1996 | 11.418927577900188 |
| 2012 | 13.321426252180064 |
| 1993 |  7.293806838865011 |
| 1988 |  7.404806128694523 |
| 2001 | 11.334588087470902 |
| 2019 |  17.28629620126477 |
| 1995 |  9.492405926429607 |
| 2002 | 10.103424252854664 |
| 2005 | 12.842189075686619 |
| 2008 | 14.945066707092563 |
| 2010 | 13.429455604020601 |
| 1987 |  8.731817193324877 |
| 2007 | 15.771928299771517 |
| 2011 | 13.748676536097841 |
| 2000 | 13.916001963040324 |
| 2014 |  15.84793619315476 |
| 1994 |  7.860020949256333 |
| 2018 |  16.42888727112437 |
| 2013 |  15.11882775513273 |
| 2004 | 12.154611296158416 |
| 1991 |  7.000398734856221 |
| 1992 |  6.757480025582812 |
| 1998 | 11.184470780953138 |
| 2021 | 15.705075632651738 |
| 2020 |  11.29906259676009 |
| 2009 |   13.3450648740797 |
| 2017 | 15.927763332462206 |
| 1999 |  11.89956719672367 |
| 2015 | 14.858317724666252 |
+------+--------------------+
```

#### Q8
```
mysql> SELECT Year, COUNT(*) AS c1 FROM ontime GROUP BY Year;
+------+---------+
| Year | c1      |
+------+---------+
| 2019 | 7422037 |
| 2016 | 5617658 |
| 1999 | 5527884 |
| 2012 | 6096762 |
| 2013 | 6369482 |
| 1988 | 4760978 |
| 1997 | 5411843 |
| 1991 | 5076925 |
| 1994 | 5180048 |
| 2007 | 7455458 |
| 1995 | 5327435 |
| 2021 | 4331165 |
| 2009 | 6450285 |
| 2018 | 7213446 |
| 2003 | 6488540 |
| 2004 | 7129270 |
| 1998 | 5384721 |
| 2002 | 5271359 |
| 2006 | 7141922 |
| 1990 | 5270893 |
| 2001 | 5967780 |
| 2014 | 5819811 |
| 2015 | 5819079 |
| 1987 | 1311826 |
| 2017 | 5674621 |
| 2011 | 6085281 |
| 1996 | 5351983 |
| 1989 | 5041200 |
| 2008 | 7009726 |
| 1993 | 5070501 |
| 2020 | 4688354 |
| 2000 | 5683047 |
| 1992 | 5092157 |
| 2005 | 7140596 |
| 2010 | 6450117 |
+------+---------+
```

#### Q9
```
mysql> SELECT avg(cnt) FROM (SELECT Year,Month,count(*) AS cnt FROM ontime WHERE DepDel15=1 GROUP BY Year,Month);
+-------------------+
| avg(cnt)          |
+-------------------+
| 81330.11302211302 |
+-------------------+
```

#### Q10
```
mysql> **SELECT avg(c1) FROM (SELECT Year, Month, count(*) AS c1 FROM ontime GROUP BY Year, Month)**;
+------------------+
| avg(c1)          |
+------------------+
| 494187.199017199 |
+------------------+
```

#### Q11
```
mysql> SELECT OriginCityName, DestCityName, count(*) AS c FROM ontime GROUP BY OriginCityName, DestCityName ORDER BY c DESC LIMIT 10;
+-----------------------------------+----------------------------------+--------+
| OriginCityName                    | DestCityName                     | c      |
+-----------------------------------+----------------------------------+--------+
| San Francisco, CALos Angeles, CA | an Francisco, CALos Angeles, CA | 515142 |
| Los Angeles, CASan Francisco, CA | os Angeles, CASan Francisco, CA | 512226 |
| New York, NY
              Chicago, IL          | ew York, NY
                                                Chicago, IL          | 454454 |
| Chicago, ILNew York, NY          | hicago, ILNew York, NY          | 447130 |
| Chicago, ILMinneapolis, MN       | hicago, ILMinneapolis, MN       | 437056 |
| Minneapolis, MN
                 Chicago, IL       | inneapolis, MN
                                                   Chicago, IL       | 432832 |
Las Vegas, NV     | 428568 |es, CA
| Las Vegas, NVLos Angeles, CA     | as Vegas, NVLos Angeles, CA     | 422558 |
| New York, NY
Boston, MA           | ew York, NY
Boston, MA           | 418789 |
| Boston, MANew York, NY           | oston, MANew York, NY           | 415559 |
+-----------------------------------+----------------------------------+--------+
```

#### Q12
```
mysql> SELECT OriginCityName, count(*) AS c FROM ontime GROUP BY OriginCityName ORDER BY c DESC LIMIT 10;
+-----------------------+----------+
| OriginCityName        | c        |
+-----------------------+----------+
| Chicago, IL           | 12508636 |
| Atlanta, GA           | 10868334 |
| Dallas/Fort Worth, TX |  8978534 |
| Houston, TX           |  6823982 |
| Los Angeles, CA       |  6678142 |
| New York, NY          |  6282096 |
| Denver, CO            |  6252576 |
| Phoenix, AZ           |  5638100 |
| Washington, DC        |  4980743 |
| San Francisco, CA     |  4665304 |
+-----------------------+----------+
```
