# Databend TPCDS-Benchmark

## Preparing the Table and Data

We use [DuckDB](https://github.com/duckdb/duckdb) to generate TPC-DS data.

After installing DuckDB, you can use these commands to generate the data ([more information](https://github.com/duckdb/duckdb/tree/master/extension/tpcds)):

```sql
INSTALL tpcds;
LOAD tpcds;
SELECT * FROM dsdgen(sf=0.01) -- sf can be other values, such as 0.1, 1, 10, ...
EXPORT DATABASE '/tmp/tpcds_0_01/' (FORMAT CSV, DELIMITER '|');
```

Then, move the data to current directory:

```shell
mv /tmp/tpcds_0_01/ "$(pwd)/data/"
```

After that, you can load data to Databend:

```shell
./load_data.sh
```

## Benchmark

To run the TPC-DS Benchmark, first build `databend-sqllogictests` binary.

Then, execute the following command in your shell:

```shell
databend-sqllogictests --handlers mysql --database tpcds --run_dir tpcds --bench 
```