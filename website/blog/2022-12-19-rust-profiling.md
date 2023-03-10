---
title:  CPU and Memory Profiling for Rust
description: CPU and Memory Profiling
slug: profiling-rust
date: 2022-12-19
tags: [databend, CPU, Memory, Profiling]
cover_url: cpu-and-memory.png
authors:
- name: BohuTANG
  url: https://github.com/BohuTANG
  image_url: https://github.com/BohuTANG.png
---

Profiling CPU and memory for Go applications is easy and can be of great help in performance troubleshooting, for example, with [flamegraphs](http://www.brendangregg.com/flamegraphs.html). For Rust applications, however, the profiling requires extra work. This post explains how to use flamegraphs to visualize performance data of your CPU and memory for Databend.

To support CPU and memory profiling, some APIs need to be included in the application. For example, Databend includes the following in the code:
- [cpu/pprof.rs](https://github.com/datafuselabs/databend/blob/589068f2ae4bfeeaaf1dff955cc6f6bfc4c38920/src/common/http/src/debug/pprof.rs)
- [mem/jeprof.rs](https://github.com/datafuselabs/databend/blob/589068f2ae4bfeeaaf1dff955cc6f6bfc4c38920/src/common/http/src/debug/jeprof.rs)

## CPU Profiling

To do a CPU profiling, simply run the following command on the Databend server:

```bash
go tool pprof -http="0.0.0.0:8081" http://localhost:8080/debug/pprof/profile?seconds=30
```
- `localhost:8080`: Databend management address.
- `0.0.0.0:8081`: *pprof* server address.
- `seconds=30`: Profiling lasts for 30 seconds.

Then open the URL `<your-ip>:8081/ui/flamegraph` in your browser to view the flamegraph:

![Alt text](/img/blog/databend-cpu-flamegraph.png)

## Memory Profiling

Compared to CPU profiling, memory profiling is a bit more involved, and can be done in the following steps:

### 1. Enable Memory Profiling

```bash
cargo build --bin databend-query --release --features memory-profiling
```

### 2. Start with MALLOC_CONF

```bash
MALLOC_CONF=prof:true,lg_prof_interval:30 ./target/release/databend-query
```
- [`lg_prof_interval:30`](https://jemalloc.net/jemalloc.3.html#opt.lg_prof_interval): Profiles are dumped into a file for each allocation of 1 GiB (2^30 bytes).

### 3. Replace add2line with a Faster One

This will rocket your `jeprof` from 30 minutes to 3 seconds.

```bash
git clone https://github.com/gimli-rs/addr2line
cd addr2line
cargo b --examples -r
cp ./target/release/examples/addr2line <your-addr2line-find-with-whereis-addr2line>
```

### 4. Upgrade jeprof to the Latest Version

`jeprof` needs an upgrade because the old version doesn't support some parameters for creating flamegraphs. `jeporf` is a perl script, so the way to upgrade it is a little bit rough-and-ready.

First, find out the path of your local `jeprof` file:

```bash
whereis jeprof
```

Open and copy the [latest version of `jeprof`](https://raw.githubusercontent.com/jemalloc/jemalloc/dev/bin/jeprof.in), then overwrite your local copy with the copied script EXCEPT for the following two parameters:

```bash
my $JEPROF_VERSION = "5.2.1-0-gea6b3e973b477b8061e0076bb257dbd7f3faa756";
my $PPROF_VERSION = "2.0";
```

### 5. Create a Flamegraph

```bash
jeprof ./databend-query-main ./jeprof.206330.563.i563.heap --collapse | flamegraph.pl --reverse --invert --minwidth 3 > heap.svg
```
- `flamegraph.pl`: [Download](https://github.com/brendangregg/FlameGraph/blob/master/flamegraph.pl) from GitHub.
- `databend-query-main`: Path to your executable.
- `jeprof.206330.563.i563.heap`: Selects a heap file.

![Alt text](/img/blog/mem-profiling.png)

## References

- [FlameGraph](https://github.com/brendangregg/FlameGraph)
-  https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in
- Databend, Cloud Lakehouse: https://github.com/datafuselabs/databend
