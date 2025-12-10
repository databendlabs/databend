# Databend TPCDS-Benchmark

## Preparing the Table and Data

We use [DuckDB](https://duckdb.org/docs/installation/) to generate TPC-DS data.

After installing DuckDB, you can use these commands to generate the data ([more information](https://github.com/duckdb/duckdb/tree/master/extension/tpcds)):

```shell
./load_data.sh 0.1
```

## Benchmark

To run the TPC-DS Benchmark, first build `databend-sqllogictests` binary.

Then, execute the following command in your shell:

```shell
databend-sqllogictests --handlers mysql --database tpcds --run_dir tpcds --bench --run_file queries.test
```