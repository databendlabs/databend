# Ontime dataset
## Introduction
`Reporting Carrier On-Time Performance (1987-present)`  contains on-time arrival and departure data for non-stop domestic flights by month and year, by carrier and by origin and destination airport. Includes scheduled and actual departure and arrival times, canceled and diverted flights, taxi-out and taxi-in times, causes of delay and cancellation, air time, and non-stop distance.

## Prerapre ontime dataset
You can prepare dataset in 2 ways
1. run `bendctl generate --dataset ontime` directly.
2. download dataset on load by your own, see document below.

### Prepare dataset
Download dataset
```bash
mkdir -p ./raw_datas
cd ./raw_datas
echo https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{2019..2021}_{1..12}.zip | xargs -P10 wget --no-check-certificate --continue
cd ../
```

Download csv tools
```bash
cargo install xsv
```

Unzip data
```bash
mkdir -p ./raw_csvs
cd raw_csvs
unzip "../raw_datas/*.zip"
cd ../
```

Merge data(nearly 70G)
```bash
xsv cat rows ./raw_csvs/*.csv > merged.csv
```

### Create table
```sql
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
) ENGINE = FUSE
```

### Load data

See [http streaming loading doc](https://databend.rs/user/data-loading/http-streaming-load)

### queries
Q0:
```sql
SELECT avg(c1)
FROM
(
    SELECT Year, Month, count(*) AS c1
    FROM ontime
    GROUP BY Year, Month
);
```
Q1: The number of flights per day from the year 2019 to 2021
```sql
SELECT DayOfWeek, count(*) AS c
FROM ontime
WHERE Year>=2019 AND Year<=2021
GROUP BY DayOfWeek
ORDER BY c DESC;
```
Q2: The number of flights delayed by more than 10 minutes, grouped by the day of the week, for 2019-2021
```sql
SELECT DayOfWeek, count(*) AS c
FROM ontime
WHERE DepDelay>10 AND Year>=2019 AND Year<=2021
GROUP BY DayOfWeek
ORDER BY c DESC;
```
Q3: The number of delays by the airport for 2019-2021
```sql
SELECT Origin, count(*) AS c
FROM ontime
WHERE DepDelay>10 AND Year>=2019 AND Year<=2021
GROUP BY Origin
ORDER BY c DESC
LIMIT 10;
```