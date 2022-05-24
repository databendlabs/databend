---
title: How to Profile Databend
---

## go pprof tool

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
go tool pprof -http=0.0.0.0:8081 $HOME/pprof/pprof.cpu.007.pb.gz
```

## memory profiling

- Build `databend-query` with memory-profiling feature enabled

  under the project root path:

  `~/workspace/fuse-query$ cargo build --features memory-profiling`

- Fire up `databend`, with proper `MALLOC_CONF` setting

  for example `MALLOC_CONF=prof:true ./target/debug/databend-query`

- Dump memory prof

  NOTE: currently, periodical heap prof dump is NOT supported. A "snaphost" of the heap prof is dump instead.

  - by using `jeprof`

  ```shell
      ~/workspace/fuse-query$ jeprof ./target/debug/databend-query http://localhost:8080/debug/mem
      Using local file ./target/debug/databend-query.
      Gathering CPU profile from http://localhost:8080/debug/mem/pprof/profile?seconds=30 for 30 seconds to ~/jeprof/databend-query.1650949265.localhost
      Be patient...
      Wrote profile to /home/zhaobr/jeprof/databend-query.1650949265.localhost
      Welcome to jeprof!  For help, type 'help'.
      (jeprof) top
      Total: 16.2 MB
          10.2  62.7%  62.7%     10.2  62.7% ::alloc
           6.0  37.3% 100.0%      6.0  37.3% ::alloc_zeroed
           0.0   0.0% 100.0%     10.2  62.7% ::allocate
           0.0   0.0% 100.0%      0.5   3.3% ::call
           0.0   0.0% 100.0%      4.0  24.7% ::default
           0.0   0.0% 100.0%      1.2   7.2% ::deref
           0.0   0.0% 100.0%      1.2   7.2% ::deref::__stability (inline)
           0.0   0.0% 100.0%      1.2   7.2% ::deref::__static_ref_initialize (inline)
           0.0   0.0% 100.0%      0.5   3.1% ::from
           0.0   0.0% 100.0%      9.2  56.6% ::from_iter
      (jeprof)
  ```
  - or curl
   ```shell
    ~/workspace/fuse-query$ curl -O  -J  http://localhost:8080/debug/mem/pprof/profile
        % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                       Dload  Upload   Total   Spent    Left  Speed
      100 22733  100 22733    0     0  21.6M      0 --:--:-- --:--:-- --:--:-- 21.6M
      curl: Saved to filename 'heap_dump_NIKcRe.prof'
   ```


