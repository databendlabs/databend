# Databend Common

- [`base`](./base/) contains runtime, pool, allocator and rangemap.
- [`building`](./building/) sets up the environment for building components and internal use.
- [`cache`](./cache/) contains cache traits designed for memory and disk, and provides a basic LRU implementation.
- [`exception`](./exception/), error handling and backtracking.
- [`grpc`](./grpc/) wraps some of the utility code snippets for grpc.
- [`hashtable`](./hashtable/), a linear probe hashtable, mainly used in scenarios such as `group by` aggregation functions and `join`.
- [`http`](./http/) is a common http handler that includes health check, cpu/memory profile and graceful shutdown.
- [`io`](./io/) focus on binary serialisation and deserialisation.
- [`metrics`](./metrics/) takes over the initialization of the `PrometheusRecorder` and owns the `PrometheusHandle`. 
- [`storage`](./storage/) provides storage related types and functions.
- [`tracing`](./tracing/) handles logging and tracing.
