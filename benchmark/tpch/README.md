## Databend TPCH-Benchmark

### TPCH Benchmark

Generate tpch dataset and create tpch tables, then insert data to it

The first argument means scale_factor: scale of the database population. scale 1.0 represents ~1 GB of data.

The second argument means storage format, default is parquet.
```shell
./tpch.sh 0.1 native
```

Generate `databend-sqllogictests`, then run the following command to benchmark
```shell
databend-sqllogictests --handlers mysql --run_dir tpch --tpch
```