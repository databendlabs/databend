---
title: Databend Vectorized Engine Performance
sidebar_label: Vectorized Engine Performance
description:
  Ultra-fast analytics experience.
---

:::tip
* **Memory SIMD-Vector processing performance only**
* Dataset: 100,000,000,000 (100 Billion)
* Hardware: AMD Ryzen 9 5950X 16-Core Processor, 32 CPUs
* Rust: rustc 1.61.0-nightly (8769f4ef2 2022-03-02)
:::

| Query                                                                                                            | DatabendQuery (v0.6.87-nightly)                      |
|------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|
| SELECT avg(number) FROM numbers_mt(100000000000)                                                                  | 1.682 s.<br /> (59.47 billion rows/s., 475.76 GB/s.) |
| SELECT sum(number) FROM numbers_mt(100000000000)                                                                  | 1.621 s.<br /> (61.67 billion rows/s., 493.37 GB/s.) |
| SELECT min(number) FROM numbers_mt(100000000000)                                                                  | 3.962 s.<br /> (25.24 billion rows/s., 201.93 GB/s.) |
| SELECT max(number) FROM numbers_mt(100000000000)                                                                  | 2.792 s.<br /> (35.82 billion rows/s., 286.54 GB/s.) |
| SELECT count(number) FROM numbers_mt(100000000000)                                                                | 1.172 s.<br /> (85.31 billion rows/s., 682.46 GB/s.) |
| SELECT sum(number+number+number) FROM numbers_mt(100000000000)                                                    | 6.032 s.<br /> (16.58 billion rows/s., 132.63 GB/s.)  |
| SELECT sum(number) / count(number) FROM numbers_mt(100000000000)                                                  | 1.652 s.<br /> (60.52 billion rows/s., 484.16 GB/s.) |
| SELECT sum(number) / count(number), max(number), min(number) FROM numbers_mt(100000000000)                        | 6.212 s.<br /> (16.10 billion rows/s., 128.78 GB/s.)  |
| SELECT number FROM numbers_mt(10000000000) ORDER BY number DESC LIMIT 10                                          | 1.414 s.<br /> (8.76 billion rows/s., 70.09 GB/s.)   |
| SELECT max(number), sum(number) FROM numbers_mt(10000000000) GROUP BY number % 3, number % 4, number % 5 LIMIT 10 | 5.791 s.<br /> (1.73 billion rows/s., 13.81 GB/s.)  |


<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/databend-v1.61-groupby-10b.gif" width="1100"/>

Experience 100 billion performance on your laptop, [talk is cheap just bench it](../01-deploy/00_local.md)
