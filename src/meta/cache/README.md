# Databend Common Meta Cache

A distributed cache implementation based on meta-service, providing reliable resource management and data synchronization across distributed systems.


## Features

- **Automatic Synchronization**: Background watcher task keeps local cache in sync with meta-service
- **Concurrency Control**: Two-level concurrency control mechanism for safe access
- **Event-based Updates**: Real-time updates through meta-service watch API
- **Safe Reconnection**: Automatic recovery from connection failures with state consistency

## Key Components

### Cache Structure

```text
<prefix>/foo
<prefix>/..
<prefix>/..
```

- `<prefix>`: User-defined string to identify a cache instance

### Main Types

- `Cache`: The main entry point for cache operations
  - Provides safe access to cached data
- `CacheData`: Internal data structure holding the cached values
- `EventWatcher`: Background task that watches for changes in meta-service
  - Handles synchronization with meta-service

## Usage

```rust
let client = MetaGrpcClient::try_create(/*..*/);
let cache = Cache::new(
        client,
        "your/cache/key/space/in/meta/service",
        "your-app-name-for-logging",
).await;

// Access cached data
cache.try_access(|c: &CacheData| {
    println!("last-seq:{}", c.last_seq);
    println!("all data: {:?}", c.data);
}).await?;

// Get a specific value
let value = cache.try_get("key").await?;

// List all entries under a prefix
let entries = cache.try_list_dir("prefix").await?;
```

## Concurrency Control

The cache employs a two-level concurrency control mechanism:

1. **Internal Lock (Mutex)**: Protects concurrent access between user operations and the background cache updater. This lock is held briefly during each operation.

2. **External Lock (Method Design)**: Public methods require `&mut self` even for read-only operations. This prevents concurrent access to the cache instance from multiple call sites. External synchronization should be implemented by the caller if needed.

This design intentionally separates concerns:
- The internal lock handles short-term, fine-grained synchronization with the updater
- The external lock requirement (`&mut self`) enables longer-duration access patterns without blocking the background updater unnecessarily

Note that despite requiring `&mut self`, all operations are logically read-only with respect to the cache's public API.

## Initialization Process

When a `Cache` is created, it goes through the following steps:

1. Creates a new instance with specified prefix and context
2. Spawns a background task to watch for key-value changes
3. Establishes a watch stream to meta-service
4. Fetches and processes initial data
5. Waits for the cache to be fully initialized before returning
6. Maintains continuous synchronization

The initialization is complete only when the cache has received a full copy of the data from meta-service, ensuring users see a consistent view of the data.

## Error Handling

The cache implements robust error handling:

- Connection failures are automatically retried in the background
- Background watcher task automatically recovers from errors
- Users are shielded from transient errors through the abstraction
- The cache ensures data consistency by tracking sequence numbers

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.
