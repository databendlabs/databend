<div align="center">
<h1>Datafuse</h1>
<strong>
Modern Real-Time Data Processing & Analytics DBMS with Cloud-Native Architecture
</strong>

<br>
<br>

<div>
<a href="https://join.slack.com/t/datafusecloud/shared_invite/zt-nojrc9up-50IRla1Y1h56rqwCTkkDJA">
<img src="https://badgen.net/badge/Slack/Join%20Datafuse/0abd59?icon=slack" alt="slack" />
</a>

<a href="https://github.com/datafuselabs/datafuse/actions">
<img src="https://github.com/datafuselabs/datafuse/actions/workflows/unit-tests.yml/badge.svg" alt="CI Status" />
</a>

<a href="https://codecov.io/gh/datafuselabs/datafuse">
<img src="https://codecov.io/gh/datafuselabs/datafuse/branch/master/graph/badge.svg" alt="codecov" />
</a>

<a href="https://deps.rs/repo/github/datafuselabs/datafuse">
<img src="https://deps.rs/repo/github/datafuselabs/datafuse/status.svg" alt="dependency status" />
</a>

<img src="https://img.shields.io/badge/Platform-Linux,%20ARM,%20OS%20X,%20Windows-green.svg?style=flat" alt="patform" />

<a href="https://opensource.org/licenses/Apache-2.0">
<img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="license" />
</a>

</div>
</div>

Datafuse is a Real-Time Data Processing & Analytics DBMS with Cloud-Native Architecture written
in Rust, inspired by [ClickHouse](https://github.com/ClickHouse/ClickHouse) and powered by [arrow-rs](https://github.com/apache/arrow-rs), built to make it easy to power the Data Cloud.

## Principles

* **Fearless**
  - No data races, No unsafe, Minimize unhandled errors

* **High Performance**
  - Everything is Parallelism

* **High Scalability**
  - Everything is Distributed

* **High Reliability**
  - Datafuse primary design goal is reliability

## Architecture

![Datafuse Architecture](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/datafuse-v1.svg)

## Performance

* **Memory SIMD-Vector processing performance only**
* Dataset: 100,000,000,000 (100 Billion)
* Hardware: AMD Ryzen 7 PRO 4750U, 8 CPU Cores, 16 Threads
* Rust: rustc 1.53.0-nightly (673d0db5e 2021-03-23)
* Build with Link-time Optimization and Using CPU Specific Instructions
* ClickHouse server version 21.4.6 revision 54447


| Query                                                        | FuseQuery (v0.4.40-nightly)                                  | ClickHouse (v21.4.6)                                         |
| ------------------------------------------------------------ | --------------------------------------------------- | ------------------------------------------------------------ |
| SELECT avg(number) FROM numbers_mt(100000000000)             | 4.35 s.<br /> (22.97 billion rows/s., 183.91 GB/s.) | **×1.4 slow, (6.04 s.)** <br /> (16.57 billion rows/s., 132.52 GB/s.) |
| SELECT sum(number) FROM numbers_mt(100000000000)             | 4.20 s.<br />(23.79 billion rows/s., 190.50 GB/s.)  | **×1.4 slow, (5.90 s.)** <br />(16.95 billion rows/s., 135.62 GB/s.) |
| SELECT min(number) FROM numbers_mt(100000000000)             | 4.92 s.<br />(20.31 billion rows/s., 162.64 GB/s.)  | **×2.7 slow, (13.05 s.)** <br /> (7.66 billion rows/s., 61.26 GB/s.) |
| SELECT max(number) FROM numbers_mt(100000000000)             | 4.77 s.<br />(20.95 billion rows/s., 167.78 GB/s.)  | **×3.0 slow, (14.07 s.)** <br /> (7.11 billion rows/s., 56.86 GB/s.) |
| SELECT count(number) FROM numbers_mt(100000000000)           | 2.91 s.<br />(34.33 billion rows/s., 274.90 GB/s.)  | **×1.3 slow, (3.71 s.)** <br /> (26.93 billion rows/s., 215.43 GB/s.) |
| SELECT sum(number+number+number) FROM numbers_mt(100000000000) | 19.83 s.<br />(5.04 billion rows/s., 40.37 GB/s.)   | **×12.1 slow, (233.71 s.)** <br /> (427.87 million rows/s., 3.42 GB/s.) |
| SELECT sum(number) / count(number) FROM numbers_mt(100000000000) | 3.90 s.<br />(25.62 billion rows/s., 205.13 GB/s.)  | **×2.5 slow, (9.70 s.)** <br /> (10.31 billion rows/s., 82.52 GB/s.) |
| SELECT sum(number) / count(number), max(number), min(number) FROM numbers_mt(100000000000) | 8.28 s.<br />(12.07 billion rows/s., 96.66 GB/s.)   | **×4.0 slow, (32.87 s.)** <br /> (3.04 billion rows/s., 24.34 GB/s.) |
| SELECT number FROM numbers_mt(10000000000) ORDER BY number DESC LIMIT 100 | 4.80 s.<br />(2.08 billion rows/s., 16.67 GB/s.)    | **×2.9 slow, (13.95 s.)** <br /> (716.62 million rows/s., 5.73 GB/s.) |
| SELECT max(number), sum(number) FROM numbers_mt(1000000000) GROUP BY sipHash64(number % 3), sipHash64(number % 4), sipHash64(number % 5) | 37.07 s.<br />(31.18 million rows/s., 249.68 MB/s.) | **×4 fast, (9.17 s.)** <br /> (109.02 million rows/s., 872.20 MB/s.) |

Note:

* ClickHouse system.numbers_mt is <b>16-way</b> parallelism processing, [gist](https://gist.github.com/BohuTANG/bba7ec2c23da8017eced7118b59fc7d5)
* FuseQuery system.numbers_mt is <b>16-way</b> parallelism processing, [gist](https://gist.github.com/BohuTANG/8c37f5390e129cfc9d648ff930d9ef03)

## Status

#### General

- [x] SQL Parser
- [x] Query Planner
- [x] Query Optimizer
- [x] Predicate Push Down
- [x] Limit Push Down
- [x] Projection Push Down
- [x] Type coercion
- [x] Parallel Query Execution
- [x] Distributed Query Execution
- [x] Shuffle Hash GroupBy
- [x] Merge-Sort OrderBy
- [ ] Joins (WIP)

#### SQL Support

- [x] Projection
- [x] Filter (WHERE)
- [x] Limit
- [x] Aggregate Functions
- [x] Scalar Functions
- [x] UDF Functions
- [x] SubQueries
- [x] Sorting
- [ ] Joins (WIP)
- [ ] Window (TODO)

## Getting Started

* [Quick Start](https://datafuse.rs/overview/architecture/)
* [Architecture](https://datafuse.rs/overview/architecture/)
* [Performance](https://datafuse.rs/overview/performance/)

## Roadmap

Datafuse is currently in **Alpha** and is not ready to be used in production, [Roadmap 2021](https://github.com/datafuselabs/datafuse/issues/746)

## Contributing

* [Contribution Guide](https://datafuse.rs/development/contributing/)
* [Coding Guidelines](https://datafuse.rs/development/coding-guidelines/)


## License

Datafuse is licensed under [Apache 2.0](LICENSE).
