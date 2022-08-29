# Databend Common

- `arrow`, a simple wrapper for reading and writing parquet.
- `base` contains runtime, pool, allocator and rangemap.
- `building` sets up the environment for building components and internal use.
- `cache` contains cache traits designed for memory and disk, and provides a basic LRU implementation.
- `contexts` is the context of the data access layer.
- `exception`, error handling and backtracking.
- `grpc` wraps some of the utility code snippets for grpc.
- `hashtable`, a linear probe hashtable, mainly used in scenarios such as `group by` aggregation functions and `join`.
- `http` is a common http handler that includes health check, cpu/memory profile and graceful shutdown.
- `io` focus on binary serialisation and deserialisation.
- `macros` are some of the procedural macros used with `common_base::base::Runtime`
- `metrics` takes over the initialization of the `PrometheusRecorder` and owns the `PrometheusHandle`. 
- `storage` provides storage related types and functions.
- `tracing` handles logging and tracing.
