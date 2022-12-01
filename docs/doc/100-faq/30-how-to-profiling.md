---
title: How to Profile Databend
---

## CPU profiling

`go tool pprof http://localhost:8080/debug/pprof/profile?seconds=20`

```shell
Fetching profile over HTTP from http://localhost:8080/debug/pprof/profile?seconds=20
Saved profile in /home/bohu/pprof/pprof.cpu.007.pb.gz
Type: cpu
Entering interactive mode (type "help" for commands, "o" for options)
(pprof) top
Showing nodes accounting for 5011, 100% of 5011 total
Dropped 248 nodes (cum <= 25)
Showing top 10 nodes out of 204
      flat  flat%   sum%        cum   cum%
      5011   100%   100%       5011   100%  backtrace::backtrace::libunwind::trace
         0     0%   100%        162  3.23%  <&alloc::vec::Vec<T,A> as core::iter::traits::collect::IntoIterator>::into_iter
         0     0%   100%         45   0.9%  <&mut I as core::iter::traits::iterator::Iterator>::next
         0     0%   100%         77  1.54%  <[A] as core::slice::cmp::SlicePartialEq<B>>::equal
         0     0%   100%         35   0.7%  <[u8; 8] as ahash::convert::Convert<u64>>::convert
         0     0%   100%        199  3.97%  <[u8] as ahash::convert::ReadFromSlice>::read_last_u64
         0     0%   100%         73  1.46%  <[u8] as ahash::convert::ReadFromSlice>::read_last_u64::as_array
         0     0%   100%        220  4.39%  <[u8] as ahash::convert::ReadFromSlice>::read_u64
         0     0%   100%        701 13.99%  <ahash::fallback_hash::AHasher as core::hash::Hasher>::write
         0     0%   100%         26  0.52%  <ahash::random_state::RandomState as core::hash::BuildHasher>::build_hash
```

Or
```shell
go tool pprof -http=0.0.0.0:8080 $HOME/pprof/pprof.cpu.007.pb.gz
```

## Memory profiling

`databend-query` and `databend-meta` can be built optionally with `jemalloc`,
which provides various memory profiling features.

Currently, it does not work on Mac, with either intel or ARM.

### Bring up with memory profiling enabled

- Build `databend-query` and `databend-meta` with memory-profiling feature enabled. For example:
  `cargo build --bin databend-query --release --features memory-profiling`

- Fire up `databend`, using environment variable `MALLOC_CONF` to enable memory profiling.
  
  `MALLOC_CONF=prof:true,lg_prof_interval:30 ./target/debug/databend-query`

### Examine memory usage

Generate a call graph in `pdf` illustrating memory allocation during this interval:

```shell
jeprof \
    --pdf \
    ./target/release/databend-query \
    heap.prof \
    > heap.pdf
```

<img src="https://user-images.githubusercontent.com/44069/174307263-a2c9bbe6-e417-48b7-bf4d-cbbbaad03a6e.png" width="600"/>
    
