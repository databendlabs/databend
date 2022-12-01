---
title: How to Profile Databend
---

## CPU profiling

```shell
go tool pprof -svg http://localhost:8080/debug/pprof/profile?seconds=30 > cpu.svg
```

<img src="https://user-images.githubusercontent.com/172204/204954988-4ed58738-4b43-47b9-9bb2-ef2f9d5e6f84.png" width="600"/>

## Memory profiling

`databend-query` and `databend-meta` can be built optionally with `jemalloc`,
which provides various memory profiling features.

Currently, it does not work on Mac, with either intel or ARM.

### Bring up with memory profiling enabled

1. Build `databend-query` and `databend-meta` with memory-profiling feature enabled:
  `cargo build --bin databend-query --release --features memory-profiling`

2. Fire up `databend`, using environment variable `MALLOC_CONF` to enable memory profiling:
  
  `MALLOC_CONF=prof:true,lg_prof_interval:30 ./target/debug/databend-query`

### Memory usage

Generate a call graph in `pdf` illustrating memory allocation during this interval:

```shell
jeprof --pdf ./target/release/databend-query heap.prof > heap.pdf
```

<img src="https://user-images.githubusercontent.com/44069/174307263-a2c9bbe6-e417-48b7-bf4d-cbbbaad03a6e.png" width="600"/>
    
