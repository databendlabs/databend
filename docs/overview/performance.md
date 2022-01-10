---
title: Performance
---

:::note
* **Memory SIMD-Vector processing performance only**
* Dataset: 100,000,000,000 (100 Billion)
* Hardware: AMD Ryzen 7 PRO 4750U, 8 CPU Cores, 16 Threads
* Rust: rustc 1.56.0-nightly (e3b1c12be 2021-08-02)
* Build with Link-time Optimization and Using CPU Specific Instructions
:::

| Query                                                                                                            | DatabendQuery (v0.4.76-nightly)                      |
|------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|
| SELECT avg(number) FROM numbers_mt(100000000000)                                                                 | 3.712 s.<br /> (26.94 billion rows/s., 215.52 GB/s.) |
| SELECT sum(number) FROM numbers_mt(100000000000)                                                                 | 3.669 s.<br /> (27.26 billion rows/s., 218.07 GB/s.) |
| SELECT min(number) FROM numbers_mt(100000000000)                                                                 | 4.498 s.<br /> (22.23 billion rows/s., 177.85 GB/s.) |
| SELECT max(number) FROM numbers_mt(100000000000)                                                                 | 4.438 s.<br /> (22.53 billion rows/s., 180.25 GB/s.) |
| SELECT count(number) FROM numbers_mt(100000000000)                                                               | 2.125 s.<br /> (47.07 billion rows/s., 376.53 GB/s.) |
| SELECT sum(number+number+number) FROM numbers_mt(100000000000)                                                   | 17.169 s.<br /> (5.82 billion rows/s., 46.60 GB/s.)  |
| SELECT sum(number) / count(number) FROM numbers_mt(100000000000)                                                 | 3.696 s.<br /> (27.06 billion rows/s., 216.45 GB/s.) |
| SELECT sum(number) / count(number), max(number), min(number) FROM numbers_mt(100000000000)                       | 8.348 s.<br /> (11.98 billion rows/s., 95.83 GB/s.)  |
| SELECT number FROM numbers_mt(10000000000) ORDER BY number DESC LIMIT 10                                         | 3.164 s.<br /> (3.16 billion rows/s., 25.28 GB/s.)   |
| SELECT max(number), sum(number) FROM numbers_mt(1000000000) GROUP BY number % 3, number % 4, number % 5 LIMIT 10 | 1.657 s.<br /> (603.62 million rows/s., 4.83 GB/s.)  |

:::note Notes
DatabendQuery system.numbers_mt is **16-way** parallelism processing, [gist](https://gist.github.com/BohuTANG/ab211a47c1493b9ecd01bf99a7731037)
:::

<figure>
  <img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/datafuse-avg-100b.gif"/>
  <figcaption>100,000,000,000 records on laptop show</figcaption>
</figure>

Experience 100 billion performance on your laptop, [talk is cheap just bench it](/user)
