---
title: How to Benchmark With Hyperfine
---

Hyperfine is a cross-platform command-line benchmarking tool, that supports warm-up and parameterized benchmarks.

Databend recommends using hyperfine to perform benchmarking via the ClickHouse/MySQL client. In this article, we will use the MySQL client to introduce it.

### Before you begin

* Make sure you have already [installed Databend](/doc/deploy).
* Install the MySQl client.
* Check out [hyperfine - Installation](https://github.com/sharkdp/hyperfine#installation) to install hyperfine according to your distribution.

## Design SQL for benchmark

Design benchmarks based on your dataset and key SQLs, write them to a file.

Some SQLs for stateless computing benchmarks are listed below. Save them to a file called `bench.sql`:

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

Open a file called `benchmark.sh` and write the following:

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
                n="-n \"$SQL\" "
                s="echo \"$SQL\" | mysql -h127.0.0.1 -P$port -uroot -s"
                script="$script '$n' '$s'"
        done <<< $(cat $sql)

        script="$script  --export-markdown $result"
        echo $script | bash -x
}


run "$1" "$2" "$3"
```

In this script:

- Use the `-w/--warmup` & `WARMUP` to perform 3 program executions before the actual benchmarking.
- And use `-r/--runs` & `RUN` to execute 10 benchmarking runs.
- Allows to specify MySQL compatible service ports for Databend.
- Need to Specify the input SQL file and the output Markdown file.

The usage is shown below. For executable, run `chmod a+x ./benchmark.sh` first.

```shell
./benchmark.sh <port> <sql> <result>
```

## Execute and review benchmark results

In this example, the MySQL compatible port is `3307`, benchmark SQLs file is `bench.sql`, and expected output is `databend-hyperfine.md`.

Run `./benchmark.sh 3307 bench.sql databend-hyperfine.md`. Of course, if you deploy in your own configuration, you can adjust it to suit.

:::Note
The following results were benchmarked with AMD Ryzen 9 5900HS and 16GB of RAM, for example only.
:::

The output in the terminal is shown in the following example.

```text
Benchmark 1:  "SELECT avg(number) FROM numbers_mt(100000000000)"
  Time (mean ± σ):      3.486 s ±  0.016 s    [User: 0.003 s, System: 0.002 s]
  Range (min … max):    3.459 s …  3.506 s    10 runs
```

The final result in databend-hyperfine.md is as follows.

| Command | Mean [s] | Min [s] | Max [s] | Relative |
|:---|---:|---:|---:|---:|
| ` "SELECT avg(number) FROM numbers_mt(100000000000)" ` | 3.524 ± 0.025 | 3.497 | 3.567 | 2.94 ± 0.06 |
| ` "SELECT sum(number) FROM numbers_mt(100000000000)" ` | 3.531 ± 0.024 | 3.494 | 3.574 | 2.94 ± 0.06 |
| ` "SELECT min(number) FROM numbers_mt(100000000000)" ` | 5.970 ± 0.043 | 5.925 | 6.083 | 4.98 ± 0.09 |
| ` "SELECT max(number) FROM numbers_mt(100000000000)" ` | 6.201 ± 0.137 | 6.025 | 6.535 | 5.17 ± 0.15 |
| ` "SELECT count(number) FROM numbers_mt(100000000000)" ` | 2.368 ± 0.050 | 2.334 | 2.499 | 1.97 ± 0.05 |
| ` "SELECT sum(number+number+number) FROM numbers_mt(100000000000)" ` | 17.406 ± 0.830 | 16.375 | 18.474 | 14.51 ± 0.74 |
| ` "SELECT sum(number) / count(number) FROM numbers_mt(100000000000)" ` | 3.580 ± 0.018 | 3.556 | 3.621 | 2.98 ± 0.05 |
| ` "SELECT sum(number) / count(number), max(number), min(number) FROM numbers_mt(100000000000)" ` | 10.391 ± 0.113 | 10.167 | 10.527 | 8.66 ± 0.18 |
| ` "SELECT number FROM numbers_mt(10000000000) ORDER BY number DESC LIMIT 10" ` | 2.175 ± 0.022 | 2.155 | 2.216 | 1.81 ± 0.04 |
| ` "SELECT max(number), sum(number) FROM numbers_mt(1000000000) GROUP BY number % 3, number % 4, number % 5 LIMIT 10" ` | 1.199 ± 0.021 | 1.164 | 1.247 | 1.00 |

## Follow up

- Refer [Analyzing OnTime Datasets with Databend on AWS EC2 and S3](../learn/analyze-ontime-with-databend-on-ec2-and-s3) to run benchmarks for the ontime dataset or your own dataset.
