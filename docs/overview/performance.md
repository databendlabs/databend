---
id: performance
title: Performance
---


* **Memory SIMD-Vector processing performance only**
* Dataset: 10,000,000,000 (10 Billion)
* Hardware: 8vCPUx16G Cloud Instance
* Rust: rustc 1.50.0-nightly (f76ecd066 2020-12-15)
* Build with Link-time Optimization and Using CPU Specific Instructions

|Query |FuseQuery (v0.1)| ClickHouse (v19.17.6)|
|-------------------------------|---------------| ----|
|SELECT avg(number) FROM system.numbers_mt | (0.60 s.)| **×2.27 slow, (1.70 s.)** <br /> 5.90 billion rows/s., 47.16 GB/s|
|SELECT sum(number) FROM system.numbers_mt | (0.75 s.)| **×1.79 slow, (1.34 s.)** <br /> 7.48 billion rows/s., 59.80 GB/s|
|SELECT min(number) FROM system.numbers_mt | (0.67 s.)| **×1.75 slow, (1.57 s.)** <br /> 6.36 billion rows/s., 50.89 GB/s|
|SELECT max(number) FROM system.numbers_mt | (0.70 s.)| **×2.53 slow, (2.33 s.)** <br />  4.34 billion rows/s., 34.74 GB/s|
|SELECT max(number+1) FROM system.numbers_mt | (2.92 s.)| **×1.13 slow, (3.29 s.)** <br />  3.04 billion rows/s., 24.31 GB/s|
|SELECT count(number) FROM system.numbers_mt | (0.44 s.)| **×1.52 slow, (0.67 s.)** <br />  15.00 billion rows/s., 119.99 GB/s|
|SELECT sum(number+number+number) FROM numbers_mt | (3.21 s.)|**×1.22 slow, (4.95 s.)** <br /> 2.02 billion rows/s., 16.17 GB/s|
|SELECT sum(number) / count(number) FROM system.numbers_mt | (0.67 s.) | **×1.54 slow, (1.28 s.)** <br /> 7.84 billion rows/s., 62.73 GB/s|
|SELECT sum(number) / count(number), max(number), min(number) FROM system.numbers_mt |(1.14 s.)| **×3.54 slow, (4.03 s.)** <br /> 2.33 billion rows/s., 18.61 GB/s|

Note:
* ClickHouse system.numbers_mt is <b>8-way</b> parallelism processing
* FuseQuery system.numbers_mt is <b>8-way</b> parallelism processing

Experience 10 billion performance on your laptop, [talk is cheap just bench it](building-and-running.md)
