---
title: Async Insert Mode
description: RFC for async insert mode
---

- RFC PR: [datafuselabs/databend#5567](https://github.com/datafuselabs/databend/pull/5567)
- Tracking Issue: [datafuselabs/databend#4577](https://github.com/datafuselabs/databend/issues/4577)

# Summary

Async insert mode aims to improve the throughput of high rate of small inserts.

# Motivation

When thousands of clients concurrently insert a small batch of data, each insert will be executed as follows:

`Parser` -> `Planner` -> `Interpreter` -> `Pipeline`

It's inefficient because of I/O depth and cache locality.

To solve the problem, we want to buffer small inserts into batches in server which sacrifices tiny latency for better insert throughput, smaller block count and larger `DataBlock` in storage.

After doing this, inserts into the same table will be parsed and planned individual. The insert data will be convert to `DataBlock` and buffered. When some conditions are triggered, the buffered `DataBlock` will be interpreted once which also is beneficial for pipelines.

# Design

## Buffer Structure
In order to buffer data of different tables. We need a `HashMap` to save datablocks of different insert plan. The settings of inserts also should be considered. But the settings of `QueryContext` take up big space which is unnecessary. Therefore, we save the changed settings. It's the settings which are different from default values of settings.

**The key of HashMap:**

```rust
#[derive(Clone)]
pub struct InsertKey {
    plan: Arc<InsertPlan>,
    // settings different with default settings
    changed_settings: Settings,
}
```

The plan is the first plan arrived at the bucket of hashmap. For example there comes two inserts:

```rust
insert into test values(1);
insert into test select 1;
```

They are same plan because they have same schema and share one table. So the datablock of these two inserts will be put the same bucket. The plan of InsertKey is the plan `insert into test values(1);` which is insignificant. Because we only need the plan to construct `Interpreter`.

**The Value of HashMap:**

```rust
pub struct InsertData {
    entries: Vec<EntryPtr>,
    data_size: u64,
    first_update: Instant,
    last_update: Instant,
}
```

The `entries` is array of `Entry`:

```rust
pub struct Entry {
    block: DataBlock,
    finished: Arc<RwLock<bool>>,
    notify: Arc<Notify>,
}
```

Each insert corresponds to each `block`. The `finished` and `notify` is for query response. When server completed a batch of inserts, the entry will be finished and notified.

The `data_size` is the memory size of `entries`. The `first_update` is the timestamp of the first insert into the bucket. The `last_update` is the timestamp of the last insert into the bucket. They all are used to trigger the batch inserts.

## Execution

Let's look at the `AsyncInsertQueue`:

```rust
type Queue = HashMap<InsertKey, InsertData>;
type QueryIdToEntry = HashMap<String, EntryPtr>;

pub struct AsyncInsertQueue {
    pub session_mgr: Arc<RwLock<Option<Arc<SessionManager>>>>,
    runtime: Arc<Runtime>,
    max_data_size: u64,
    busy_timeout: Duration,
    stale_timeout: Duration,
    queue: Arc<RwLock<Queue>>,
    current_processing_insert: Arc<RwLock<QueryIdToEntry>>,
}

impl AsyncInsertQueue {
    pub fn try_create() -> Self {}
    pub async fn start(self: Arc<Self>) {}
    pub async fn push(self: Arc<Self>, plan_node: Arc<InsertPlan>, ctx: Arc<QueryContext>) -> Result<()> {}
    pub async fn wait_for_processing_insert(self: Arc<Self>, query_id: String, time_out: Duration,) -> Result<()> {}
    fn schedule(self: Arc<Self>, key: InsertKey, data: InsertData) {}
    async fn process(self: Arc<Self>, key: InsertKey, data: InsertData) {}
    fn busy_check(self: Arc<Self>) -> Duration {}
    fn stale_check(self: Arc<Self>) {}
}
```

- `session_mgr`: needed because of `AsyncInsertQueue` needs a `QueryContext` when it process a batch of data.
- `runtime`: a global storage runtime to execute the insert task.
- `max_data_size`: the maximum memory size of the buffered data collected per insert before being inserted.
- `busy_timeout`: the maximum timeout in milliseconds since the first insert before inserting collected data.
- `stale_timeout`: the maximum timeout in milliseconds since the last insert before inserting collected data.
- `queue`: the buffer to cache batch inserts which is a `HashMap` because we need to distinguish different inserts.
- `current_processing_insert`: inserts which are processing and clients wait for the processing result. We use `QueryId` as the key to distinguish different clients.

When `databend-query` starts, `AsyncInsertQueue` starts. The queue will call `start` method. Every insert will call `push` method with `InsertPlan` and `QueryContext` as inputs and call `wait_for_processing_insert` to waiting. The queue receives many inserts and two ticker task `busy_check` and `stale_check` runs in the background. When specific condition is triggered, the queue will call `schedule` to schedule the task of batch inserts and the data will be `process`ed by the runtime executor.

# Some Details

`AsyncInsertQueue` is a global instance, so we better put it in the `SessionManager`. But the queue also needs `SessionManager` which is a circular dependency. Therefore, the `session_mgr` of `AsyncInsertQueue` must be `Arc<RwLock<Option<Arc<SessionManager>>>>`, not `Arc<SessionManager>`. We need to modify the `AsyncInsertQueue` after `SessionManager` has been initialized. Maybe we should use `Weak` better here.

The method arguments of `AsyncInsertQueue` is `Arc<Self>` because tokio runtime needs a static lifetime of task.

# Configs

- `async_insert_max_data_size`: The maximum memory size of the buffered data collected per insert before being inserted.
- `async_insert_busy_timeout`: The maximum timeout in milliseconds since the first insert before inserting collected data.
- `async_insert_stale_timeout`: The maximum timeout in milliseconds since the last insert before inserting collected data.

# Settings

- `enable_async_insert`: Client open async insert mode.
- `wait_for_async_insert`: Enable waiting for processing of async insert.
- `wait_for_async_insert_timeout`: The timeout in seconds for waiting for processing of async insert.

# Unresolved questions

None

# Future possibilities

None