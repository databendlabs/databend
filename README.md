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

| Query                                                        | FuseQuery (v0.4.1)                                  | ClickHouse (v21.4.6)                                         |
| ------------------------------------------------------------ | --------------------------------------------------- | ------------------------------------------------------------ |
| SELECT avg(number) FROM numbers_mt(100000000000)             | 3.87 s.<br /> (25.83 billion rows/s., 206.79 GB/s.) | **×1.6 slow, (6.04 s.)** <br /> (16.57 billion rows/s., 132.52 GB/s.) |
| SELECT sum(number) FROM numbers_mt(100000000000)             | 4.86 s.<br />(20.57 billion rows/s., 164.70 GB/s.)  | **×1.2 slow, (5.90 s.)** <br />(16.95 billion rows/s., 135.62 GB/s.) |
| SELECT min(number) FROM numbers_mt(100000000000)             | 5.61 s.<br />(17.82 billion rows/s., 142.65 GB/s.)  | **×2.3 slow, (13.05 s.)** <br /> (7.66 billion rows/s., 61.26 GB/s.) |
| SELECT max(number) FROM numbers_mt(100000000000)             | 5.61 s.<br />(17.82 billion rows/s., 142.67 GB/s.)  | **×2.5 slow, (14.07 s.)** <br /> (7.11 billion rows/s., 56.86 GB/s.) |
| SELECT count(number) FROM numbers_mt(100000000000)           | 3.12 s.<br />(32.03 billion rows/s., 256.48 GB/s.)  | **×1.2 slow, (3.71 s.)** <br /> (26.93 billion rows/s., 215.43 GB/s.) |
| SELECT sum(number+number+number) FROM numbers_mt(100000000000) | 17.85 s.<br />(5.60 billion rows/s., 44.85 GB/s.)   | **×16.9 slow, (233.71 s.)** <br /> (427.87 million rows/s., 3.42 GB/s.) |
| SELECT sum(number) / count(number) FROM numbers_mt(100000000000) | 4.02 s.<br />(24.86 billion rows/s., 199.10 GB/s.)  | **×2.4 slow, (9.70 s.)** <br /> (10.31 billion rows/s., 82.52 GB/s.) |
| SELECT sum(number) / count(number), max(number), min(number) FROM numbers_mt(100000000000) | 9.60 s.<br />(10.41 billion rows/s., 83.38 GB/s.)   | **×3.4 slow, (32.87 s.)** <br /> (3.04 billion rows/s., 24.34 GB/s.) |
| SELECT number FROM numbers_mt(10000000000) ORDER BY number DESC LIMIT 1000 | 5.34 s.<br />(1.87 billion rows/s., 14.99 GB/s.)    | **×2.6 slow, (13.95 s.)** <br /> (716.62 million rows/s., 5.73 GB/s.) |
| SELECT max(number),sum(number) FROM numbers_mt(1000000000) GROUP BY number % 3, number % 4, number % 5 | 9.03 s.<br />(110.71 million rows/s., 886.50 MB/s.) | **×3.5 fast, (2.60 s.)** <br /> (385.28 million rows/s., 3.08 GB/s.) |


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
<<<<<<< HEAD
- [x] Hash GroupBy(Re-shuffle)
=======
- [x] Shuffle Hash GroupBy
>>>>>>> master
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

## Current roadmap

Datafuse is currently in **Alpha** and is not ready to be used in production.

| Feature                                         | Status    | Release Target | Progress        |  Comments     |
| ----------------------------------------------- | --------- | -------------- | --------------- | ------------- |
| Compute&Storage integration | PROGRESS  |  v0.5 | [Track](https://github.com/datafuselabs/datafuse/issues/745) |  |

## Roadmap

- [x] 0.1 Support aggregation select (2021.02)
- [x] 0.2 Support distributed query (2021.03)
- [x] 0.3 Support group by (2021.04)
- [x] 0.4 Support order by (2021.04)
- [ ] 0.5 Support join
- [ ] 1.0 Support TPC-H benchmark

## Contributing

* [Contribution Guide](https://datafuse.rs/development/contributing/)
* [Coding Guidelines](https://datafuse.rs/development/coding-guidelines/)


## License

Datafuse is licensed under [Apache 2.0](LICENSE).
