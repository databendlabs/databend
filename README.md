# FuseQuery
[![Github Actions Status](https://github.com/datafusedev/fuse-query/workflows/FuseQuery%20Lint/badge.svg)](https://github.com/datafusedev/fuse-query/actions?query=workflow%3A%22FuseQuery+Lint%22)
[![Github Actions Status](https://github.com/datafusedev/fuse-query/workflows/FuseQuery%20Test/badge.svg)](https://github.com/datafusedev/fuse-query/actions?query=workflow%3A%22FuseQuery+Test%22)
[![Github Actions Status](https://github.com/datafusedev/fuse-query/workflows/FuseQuery%20Docker%20build/badge.svg)](https://github.com/datafusedev/fuse-query/actions?query=workflow%3A%22FuseQuery+Docker+build%22)
[![codecov.io](https://codecov.io/gh/datafusedev/fuse-query/graphs/badge.svg)](https://codecov.io/gh/datafusedev/fuse-query/branch/master)
![Platform](https://img.shields.io/badge/Platform-Linux,%20ARM,%20OS%20X,%20Windows-green.svg?style=flat)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


FuseQuery is a Cloud Distributed SQL Query Engine at scale.

Cloud-Native and Distributed ClickHouse from scratch in Rust.

Give thanks to [ClickHouse](https://github.com/ClickHouse/ClickHouse) and [Arrow](https://github.com/apache/arrow).

## Features

* **High Performance** 
  - Everything is Parallelism
  
* **High Scalability**
  - Everything is Distributed
  
* **High Reliability**
  - True Separation of Storage and Compute

## Architecture

![DataFuse Architecture](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/datafuse.svg)

## Crates

| Crate     | Description |  Status |
|-----------|-------------|-------------|
| [optimizers](src/optimizers) | Optimizer for Distributed&Local plan | WIP |
| [datablocks](src/datablocks) | Vectorized data processing unit | WIP |
| [datastreams](src/datastreams) | Async streaming iterators | WIP |
| [datasources](src/datasources) | Interface to the datasource([system.numbers for performance](src/datasources/system)/Fuse-Store) | WIP|
| [interpreters](src/interpreters) | Executor for the DML&DDL query plan | WIP |
| [functions](src/functions) | Scalar and Aggregation Functions | WIP |
| [processors](src/processors) | Dataflow Streaming Processor| WIP |
| [planners](src/planners) | Distributed&Local planners for building processor pipelines| WIP |
| [servers](src/servers) | Server handler([MySQL](src/servers/mysql)/HTTP) | MySQL |
| [transforms](src/transforms) | Data Stream Transform([Source](src/transforms/transform_source.rs)/[Filter](src/transforms/transform_filter.rs)/[Projection](src/transforms/transform_projection.rs)/[AggregatorPartial](src/transforms/transform_aggregator_partial.rs)/[AggregatorFinal](src/transforms/transform_aggregator_final.rs)/[Limit](src/transforms/transform_limit.rs)) | WIP |
| [executors](src/executors) | Distributed&Local planners scheduler and executor | WIP |

## Performance

* **Memory SIMD-Vector processing performance only**
* Dataset: 100,000,000,000 (100 Billion)
* Hardware: AMD Ryzen 7 PRO 4750U, 8 CPU Cores, 16 Threads
* Rust: rustc 1.49.0 (e1884a8e3 2020-12-29)
* Build with Link-time Optimization and Using CPU Specific Instructions
* ClickHouse server version 21.2.1 revision 54447

|Query |FuseQuery (v0.1)| ClickHouse (v21.2.1)|
|-------------------------------|---------------| ----|
|SELECT avg(number) FROM system.numbers_mt | (3.11 s.)| **×3.14 slow, (9.77 s.)** <br /> 10.24 billion rows/s., 81.92 GB/s.|
|SELECT sum(number) FROM system.numbers_mt | (2.96 s.)| **×2.02 slow, (5.97 s.)** <br /> 16.75 billion rows/s., 133.97 GB/s.|
|SELECT min(number) FROM system.numbers_mt | (3.57 s.)| **×3.90 slow, (13.93 s.)** <br /> 7.18 billion rows/s., 57.44 GB/s.|
|SELECT max(number) FROM system.numbers_mt | (3.59 s.)| **×4.09 slow, (14.70 s.)** <br /> 6.80 billion rows/s., 54.44 GB/s.|
|SELECT count(number) FROM system.numbers_mt | (1.76 s.)| **×2.22 slow, (3.91 s.)** <br /> 25.58 billion rows/s., 204.65 GB/s.|
|SELECT sum(number+number+number) FROM numbers_mt | (23.14 s.)|**×5.47 slow, (126.67 s.)** <br /> 789.47 million rows/s., 6.32 GB/s.|
|SELECT sum(number) / count(number) FROM system.numbers_mt | (3.09 s.) | **×1.96 slow, (6.07 s.)** <br /> 16.48 billion rows/s., 131.88 GB/s.|
|SELECT sum(number) / count(number), max(number), min(number) FROM system.numbers_mt |(6.73 s.)| **×4.01 slow, (27.59 s.)** <br /> 3.62 billion rows/s., 28.99 GB/s.|

Note:
* ClickHouse system.numbers_mt is <b>16-way</b> parallelism processing
* FuseQuery system.numbers_mt is <b>16-way</b> parallelism processing

## Status

- [x] Projection
- [x] Filter
- [x] Limit
- [x] Aggregate
- [x] Functions
- [x] Filter Push-Down
- [ ] Projection Push-Down (TODO)
- [ ] Work-Stealing Distributed Query Engine (WIP)
- [ ] Sorting (TODO)
- [ ] SubQueries (TODO)
- [ ] Joins (TODO)

## Roadmap

- [x] 0.1 support aggregation select
- [ ] 0.2 support distributed query (WIP)
- [ ] 0.3 support order by
- [ ] 0.5 support group by
- [ ] 0.6 support sub queries
- [ ] 0.7 support join
- [ ] 0.8 support TPC-H benchmark

## Contributing

You can learn more about contributing to the FuseQuery project by reading our [Contribution Guide](docs/development/contributing.md) and by viewing our [Code of Conduct](docs/policies/code-of-conduct.md).

## License

FuseQuery is licensed under [Apache 2.0](LICENSE).
