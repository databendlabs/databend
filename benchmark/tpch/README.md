## Databend TPCH-Benchmark

### TPCH Benchmark

#### On local fs

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

#### On S3
Because it involves importing data on s3 and configuring s3, there is no direct script, but you can refer to `create_table_s3.sh`

After completing the above steps in `create_table_s3.sh`, generate `databend-sqllogictests`, then run the following command to benchmark
```shell
databend-sqllogictests --handlers mysql --run_dir tpch --tpch
```
