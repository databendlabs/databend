---
title: How to Profile Databend
---

## CPU profiling

```
go tool pprof -http="0.0.0.0:8081" http://localhost:8080/debug/pprof/profile?seconds=30
```

Open `<your-ip>:8081` and select `Flame Graph` from the VIEW menus in the site header:
<img src="https://user-images.githubusercontent.com/172204/208336392-5b64bb9b-cce8-4562-9e05-c3d538e9d8a6.png"/>

## Memory profiling

`databend-query` and `databend-meta` can be built optionally with `jemalloc`,
which provides various memory profiling features.

Currently, it does not work on Mac, with either intel or ARM.

### Enable memory profiling

1. Build `databend-query` with `memory-profiling` feature enabled:
  ```
  cargo build --bin databend-query --release --features memory-profiling
  ```

2. Fire up `databend`, using environment variable `MALLOC_CONF` to enable memory profiling:
  
  ```
  MALLOC_CONF=prof:true,lg_prof_interval:30 ./target/release/databend-query
  ```

### Generate heap profile

Generate a call graph in `pdf` illustrating memory allocation during this interval:

```
jeprof --pdf ./target/release/databend-query heap.prof > heap.pdf
```

<img src="https://user-images.githubusercontent.com/172204/204963954-f6eacf10-d8bd-4469-9c8d-7d30955f1a78.png" width="600"/>

### Fast jeprof
jeprof is very slow for large heap analysis, the bottleneck is `addr2line`, if you want to speed up from **30 minutes to 3s**, please use :
```
git clone https://github.com/gimli-rs/addr2line
cd addr2line
cargo b --examples -r
cp ./target/release/examples/addr2line <your-addr2line-find-with-whereis-addr2line>
```
    
