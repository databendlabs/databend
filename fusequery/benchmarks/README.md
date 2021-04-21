
# Benchmarks

## NYC Taxi Benchmark

These benchmarks are based on the [New York Taxi and Limousine Commission][3] data set.

```bash
cargo run --release --bin nyctaxi -- --iterations 3 --path /mnt/nyctaxi/csv 
```

Example output:

```bash
Running benchmarks with the following options: Opt { iterations: 3, path: "/mnt/nyctaxi/csv"}
Executing 'fare_amt_by_passenger'
Query 'fare_amt_by_passenger' iteration 0 took 7138 ms
Query 'fare_amt_by_passenger' iteration 1 took 7599 ms
Query 'fare_amt_by_passenger' iteration 2 took 7969 ms
```