# Databend Common

- [`arrow`](./arrow/), a simple wrapper for reading and writing parquet.
- [`base`](./base/) contains runtime, pool, allocator and rangemap.
- [`building`](./building/) sets up the environment for building components and internal use.
- [`cache`](./cache/) contains cache traits designed for memory and disk, and provides a basic LRU implementation.
- [`contexts`](./contexts/) is the context of the data access layer.
- [`exception`](./exception/), error handling and backtracking.
- [`grpc`](./grpc/) wraps some of the utility code snippets for grpc.
- [`hashtable`](./hashtable/), a linear probe hashtable, mainly used in scenarios such as `group by` aggregation functions and `join`.
- [`http`](./http/) is a common http handler that includes health check, cpu/memory profile and graceful shutdown.
- [`io`](./io/) focus on binary serialisation and deserialisation.
- [`macros`](./macros/) are some of the procedural macros used with `common_base::base::Runtime`
- [`metrics`](./metrics/) takes over the initialization of the `PrometheusRecorder` and owns the `PrometheusHandle`. 
- [`storage`](./storage/) provides storage related types and functions.
- [`tracing`](./tracing/) handles logging and tracing.
