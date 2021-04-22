
# Benchmarks

## NYC Taxi Benchmark

These benchmarks are based on the [New York Taxi and Limousine Commission][3] data set.

### How to run NYC Taxi Benchmark

1. Download the trip data from [TLC Trip Record Data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page), such as [2021-01-Yellow Taxi Trip Records (CSV,593.6MB)](https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-01.csv)
   
2. Run benchmark

```bash
cargo run --release --bin nyctaxi -- --path /datas/yellow_tripdata_2020-01.csv
```

Example output:

```bash
Running benchmarks with the following options: Opt { iterations: 3, threads: 0, path: "/datas/yellow_tripdata_2020-01.csv" }, max_threads [16], block_size[10000]
Executing 'fare_amt_by_passenger'
Query 'SELECT passenger_count, MIN(fare_amount), MAX(fare_amount), SUM(fare_amount) FROM nyctaxi  GROUP BY passenger_count'
Query 'fare_amt_by_passenger' iteration 0 took 3707 ms
Query 'fare_amt_by_passenger' iteration 1 took 3635 ms
Query 'fare_amt_by_passenger' iteration 2 took 3733 ms

```