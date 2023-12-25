# Databend TPCH-Benchmark


## Preparing the Table and Data

To prepare the table and data for the TPC-H Benchmark, run the following command in your shell:

```shell
./tpch.sh 1
```
The scale factor is set to 1, which consists of the base row size (several million elements).

More information about the scale factor:

| SF (Gigabytes) | Size                                                                    |
|----------------|-------------------------------------------------------------------------|
| 1              | Consists of the base row size (several million elements).               |
| 10             | Consists of the base row size x 10.                                     |
| 100            | Consists of the base row size x 100 (several hundred million elements). |
| 1000           | Consists of the base row size x 1000 (several billion elements).        |



## Benchmark

To run the TPC-H Benchmark, first build `databend-sqllogictests` binary.

Then, execute the following command in your shell:

```shell
databend-sqllogictests --handlers mysql --database tpch --run_dir tpch --bench
```

## More

[Benchmarking Databend using TPC-H](https://www.databend.com/blog/2022/08/08/benchmark-tpc-h)