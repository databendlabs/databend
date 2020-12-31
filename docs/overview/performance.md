---
id: performance
title: Performance
---


* **Memory SIMD-Vector processing performance only**
* Dataset: 10,000,000,000 (10 Billion)
* Hardware: 8vCPUx16G Cloud Instance
* Rust: rustc 1.50.0-nightly (f76ecd066 2020-12-15)

|Query |FuseQuery Cost| ClickHouse Cost|
|-------------------------------|---------------| ----|
|SELECT avg(number) FROM system.numbers_mt | [2.02s] | [1.70s], 5.90 billion rows/s., 47.16 GB/s|
|SELECT sum(number) FROM system.numbers_mt | [1.77s] | [1.34s], 7.48 billion rows/s., 59.80 GB/s|
|SELECT max(number) FROM system.numbers_mt | [2.83s] | [2.33s], 4.34 billion rows/s., 34.74 GB/s|
|SELECT max(number+1) FROM system.numbers_mt | [6.13s] | [3.29s], 3.04 billion rows/s., 24.31 GB/s|
|SELECT count(number) FROM system.numbers_mt | [1.55s] | [0.67s], 15.00 billion rows/s., 119.99 GB/s|
|SELECT sum(number) / count(number) FROM system.numbers_mt | [2.04s] | [1.28s], 7.84 billion rows/s., 62.73 GB/s|
|SELECT sum(number) / count(number), max(number), min(number) FROM system.numbers_mt | [6.40s] | [4.30s], 2.33 billion rows/s., 18.61 GB/s|

Note:
* ClickHouse system.numbers_mt is <b>8-way</b> parallelism processing
* FuseQuery system.numbers_mt is <b>8-way</b> parallelism processing


