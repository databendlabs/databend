# Databend Common Meta Cache

A distributed cache implementation based on meta-service, providing reliable resource management and data synchronization across distributed systems.

## Features

- **Automatic Synchronization**: Background watcher task keeps local cache in sync with meta-service
- **Concurrency Control**: Two-level concurrency control mechanism for safe access
- **Event-based Updates**: Real-time updates through meta-service watch API
- **Safe Reconnection**: Automatic recovery from connection failures with state consistency


## Key Components

See https://github.com/databendlabs/sub-cache/ for details about Usage, Internal types and Concurrency Control.


### Cache Structure

```text
<prefix>/foo
<prefix>/..
<prefix>/..
```

- `<prefix>`: User-defined string to identify a cache instance

## Usage

```rust,ignore
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

## Initialization Process

1. Creates instance with prefix and context
2. Spawns background watcher task; Establishes watch stream to meta-service
3. Fetches initial data; Waits for full initialization
4. Maintains continuous sync

Initialization completes when cache receives full data copy from meta-service, ensuring consistent view.

## Error Handling

The cache implements robust error handling:

- Connection failures are automatically retried in the background
- Background watcher task automatically recovers from errors
- Users are shielded from transient errors through the abstraction
- The cache ensures data consistency by tracking sequence numbers

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.
