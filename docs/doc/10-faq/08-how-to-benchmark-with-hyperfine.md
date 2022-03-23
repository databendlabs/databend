---
title: How to Benchmark with Hyperfine
---

Hyperfine is a cross-platform command-line benchmarking tool, that supports warm-up and parameterized benchmarks.

Databend recommends using hyperfine to perform benchmarking via the ClickHouse/MySQL client. In this article, we will use the ClickHouse client to introduce it.

### Before you begin

* Make sure you have already [How to installed Databend](/doc/category/deploy).
* Install the ClickHouse client, refer to [ClickHouse - Quick Start](https://clickhouse.com/#quick-start).
* Check out [hyperfine - Installation](https://github.com/sharkdp/hyperfine#installation) to install hyperfine according to your distribution.

## Design SQL for benchmark

Design benchmarks based on your dataset and key SQLs, write them to a file.

Some SQLs for stateless computing benchmarks are listed below. Save them to a file called `bench.sql`.

```sql
SELECT avg(number) FROM numbers_mt(100000000000)
SELECT sum(number) FROM numbers_mt(100000000000)
SELECT min(number) FROM numbers_mt(100000000000)
SELECT max(number) FROM numbers_mt(100000000000)
SELECT count(number) FROM numbers_mt(100000000000)
SELECT sum(number+number+number) FROM numbers_mt(100000000000)
SELECT sum(number) / count(number) FROM numbers_mt(100000000000)
SELECT sum(number) / count(number), max(number), min(number) FROM numbers_mt(100000000000)
SELECT number FROM numbers_mt(10000000000) ORDER BY number DESC LIMIT 10
SELECT max(number), sum(number) FROM numbers_mt(1000000000) GROUP BY number % 3, number % 4, number % 5 LIMIT 10
```

## Write an easy-to-use script

Open a file called `benchmark.sh` and write the following.

```shell
#!/bin/bash

WARMUP=3
RUN=10

export script="hyperfine -w $WARMUP -r $RUN"

script=""
function run() {
        port=$1
        sql=$2
        result=$3
        script="hyperfine -w $WARMUP -r $RUN"
        while read SQL; do
                s="clickhouse-client --host 127.0.0.1 --port $port --query=\"$SQL\" "
                script="$script '$s'"
        done <<< $(cat $sql)

        script="$script  --export-markdown $result"
        echo $script | bash -x
}


run "$1" "$2" "$3"
```

In this script:

- Use the `-w/--warmup` & `WARMUP` to perform 3 program executions before the actual benchmarking.
- And use `-r/--runs` & `RUN` to execute 10 benchmarking runs.
- Allows to specify ClickHouse compatible service ports for Databend.
- Need to Specify the input SQL file and the output Markdown file.

The usage is shown below. For executable, run `chmod a+x ./benchmark.sh` first.

```shell
./benchmark.sh <port> <sql> <result>
```

### Execute and review benchmark results

In this example, the ClickHouse compatible port is `9001`, benchmark SQLs file is `bench.sql`, and expected output is `databend-hyperfine.md`.

Run `./benchmark.sh 9001 bench.sql databend-hyperfine.md`. Of course, if you deploy in your own configuration, you can adjust it to suit.

:::Note
The following results were benchmarked with AMD Ryzen 9 5900HS and 16GB of RAM, for example only.
:::

The output in the terminal is shown in the following example.

```text
Benchmark 1: clickhouse-client --host 127.0.0.1 --port 9001 --query="SELECT avg(number) FROM numbers_mt(100000000000)"
  Time (mean ± σ):      3.504 s ±  0.021 s    [User: 0.029 s, System: 0.013 s]
  Range (min … max):    3.479 s …  3.534 s    10 runs
```

The final result in databend-hyperfine.md is as follows.

| Command | Mean [s] | Min [s] | Max [s] | Relative |
|:---|---:|---:|---:|---:|
| `clickhouse-client --host 127.0.0.1 --port 9001 --query="SELECT avg(number) FROM numbers_mt(100000000000)" ` | 3.504 ± 0.021 | 3.479 | 3.534 | 2.93 ± 0.04 |
| `clickhouse-client --host 127.0.0.1 --port 9001 --query="SELECT sum(number) FROM numbers_mt(100000000000)" ` | 3.519 ± 0.018 | 3.481 | 3.537 | 2.94 ± 0.04 |
| `clickhouse-client --host 127.0.0.1 --port 9001 --query="SELECT min(number) FROM numbers_mt(100000000000)" ` | 6.153 ± 0.248 | 5.935 | 6.538 | 5.14 ± 0.22 |
| `clickhouse-client --host 127.0.0.1 --port 9001 --query="SELECT max(number) FROM numbers_mt(100000000000)" ` | 6.129 ± 0.055 | 6.047 | 6.227 | 5.12 ± 0.08 |
| `clickhouse-client --host 127.0.0.1 --port 9001 --query="SELECT count(number) FROM numbers_mt(100000000000)" ` | 2.350 ± 0.020 | 2.308 | 2.367 | 1.97 ± 0.03 |
| `clickhouse-client --host 127.0.0.1 --port 9001 --query="SELECT sum(number+number+number) FROM numbers_mt(100000000000)" ` | 16.178 ± 0.384 | 15.646 | 16.796 | 13.53 ± 0.36 |
| `clickhouse-client --host 127.0.0.1 --port 9001 --query="SELECT sum(number) / count(number) FROM numbers_mt(100000000000)" ` | 3.564 ± 0.046 | 3.508 | 3.656 | 2.98 ± 0.05 |
| `clickhouse-client --host 127.0.0.1 --port 9001 --query="SELECT sum(number) / count(number), max(number), min(number) FROM numbers_mt(100000000000)" ` | 10.398 ± 0.057 | 10.308 | 10.470 | 8.69 ± 0.12 |
| `clickhouse-client --host 127.0.0.1 --port 9001 --query="SELECT number FROM numbers_mt(10000000000) ORDER BY number DESC LIMIT 10" ` | 2.182 ± 0.017 | 2.158 | 2.220 | 1.82 ± 0.03 |
| `clickhouse-client --host 127.0.0.1 --port 9001 --query="SELECT max(number), sum(number) FROM numbers_mt(1000000000) GROUP BY number % 3, number % 4, number % 5 LIMIT 10" ` | 1.196 ± 0.015 | 1.165 | 1.224 | 1.00 |

## Follow up

- Try to use MySQL to complete the benchmark.
- Refer [Analyzing OnTime Datasets with Databend on AWS EC2 and S3](../09-lessons/02-analyze-ontime-with-databend-on-ec2-and-s3.md) to run benchmarks for the ontime dataset or your own dataset.
