## Databend TPCH-Benchmark

### TPCH DataSet
Run the following command to generate tpch dataset:
```shell
# scale_factor: scale of the database population. scale 1.0 represents ~1 GB of data
./gen_data.sh <scale_factor>
```

### TPCH Benchmark

Create tpch tables and insert data to it

```shell
./prepare.sh
```

Generate `databend-sqllogictests`, then run the following command to benchmark
```shell
databend-sqllogictests --handlers mysql --run_dir tpch --tpch
```