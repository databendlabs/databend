# Databend Meta Control Lua API Documentation

This document describes the Lua runtime API available in `databend-metactl lua` command.

## Overview

The Lua runtime provides access to meta service operations through the `metactl` namespace. All functions and utilities are available in the global `metactl` table.

## Core Functions

### metactl.new_grpc_client(address)

Creates a new gRPC client for communicating with the meta service.

**Parameters:**
- `address` (string): The gRPC address of the meta service (e.g., "127.0.0.1:9191")

**Returns:**
- A gRPC client object with methods for meta operations

**Example:**
```lua
local client = metactl.new_grpc_client("127.0.0.1:9191")
```

### metactl.spawn(function)

Spawns an asynchronous task that runs concurrently.

**Parameters:**
- `function`: A Lua function to execute asynchronously

**Returns:**
- A task handle that can be awaited with `join()`

**Example:**
```lua
local task = metactl.spawn(function()
    metactl.sleep(1.0)
    print("Task completed")
end)
task:join()
```

### metactl.sleep(seconds)

Asynchronously sleeps for the specified duration.

**Parameters:**
- `seconds` (number): Finite, non-negative duration in seconds (supports fractional values)

**Example:**
```lua
metactl.sleep(0.5)  -- Sleep for 500ms
metactl.sleep(2.0)  -- Sleep for 2 seconds
```

### metactl.now_ms()

Returns a monotonic clock reading in milliseconds, for timing and benchmarks.

**Returns:**
- A number of milliseconds (fractional). The value never decreases between calls; only the difference between two readings is meaningful (the origin is when the Lua environment was created).

**Example:**
```lua
local t0 = metactl.now_ms()
client:upsert("my_key", "my_value")
local elapsed_ms = metactl.now_ms() - t0
print(string.format("upsert took %.3f ms", elapsed_ms))
```

### metactl.to_string(value)

Converts any Lua value to a human-readable string representation.

**Parameters:**
- `value`: Any Lua value (nil, boolean, number, string, table, etc.)

**Returns:**
- String representation of the value

**Features:**
- Handles nested tables recursively
- Detects and converts byte vectors to escaped strings
- Sorts table keys for consistent output
- Handles NULL values from meta service
- Escapes quotes, backslashes, control bytes, and non-ASCII bytes in strings
- Single-line output format

**Example:**
```lua
print(metactl.to_string({key="value", data={count=3}}))
-- Output: {"data"={"count"=3},"key"="value"}

print(metactl.to_string(metactl.NULL))
-- Output: NULL
```

## Constants

### metactl.NULL

A special constant representing NULL values returned from the meta service.

**Example:**
```lua
local result, err = client:get("nonexistent_key") 
if result == metactl.NULL then
    print("Key not found")
end
```

## gRPC Client Methods

The client object returned by `metactl.new_grpc_client()` provides these methods:

### client:get(key)

Retrieves a value from the meta service.

**Parameters:**
- `key` (string): The key to retrieve

**Returns:**
- `result`: The retrieved value or `metactl.NULL` if not found
- `error`: Error message string if operation failed, nil otherwise

**Example:**
```lua
local client = metactl.new_grpc_client("127.0.0.1:9191")
local result, err = client:get("my_key")
if err then
    print("Error:", err)
else
    print("Value:", metactl.to_string(result))
end
```

### client:upsert(key, value)

Inserts or updates a key-value pair in the meta service.

**Parameters:**
- `key` (string): The key to upsert
- `value` (string): The binary-safe Lua string to store

**Returns:**
- `result`: Operation result containing sequence number and other metadata
- `error`: Error message string if operation failed, nil otherwise

**Example:**
```lua
local client = metactl.new_grpc_client("127.0.0.1:9191")
local result, err = client:upsert("my_key", "my_value")
if err then
    print("Error:", err)
else
    print("Upsert result:", metactl.to_string(result))
end
```

### client:transaction(transaction)

Executes a transaction atomically.

**Parameters:**
- `transaction` (table): Conditions and operations in the format described below

**Returns:**
- `result`: Transaction reply with `success`, `execution_path`, and `responses` fields
- `error`: Validation or gRPC error message, nil otherwise

A false condition is not an error. In that case, the client executes the `else` operations and returns a reply with `success = false`.

**Example:**
```lua
local client = metactl.new_grpc_client("127.0.0.1:9191")
local transaction = {
    conditions = {
        {key = "my_key", op = "eq_seq", value = 0}
    },
    ["then"] = {
        {op = "put", key = "my_key", value = "my_value", ttl = 60},
        {op = "get", key = "my_key"}
    },
    ["else"] = {
        {op = "get", key = "my_key"}
    }
}

local result, err = client:transaction(transaction)
if err then
    print("Transaction failed:", err)
else
    print("Transaction result:", metactl.to_string(result))
end
```

## Transaction Tables

A transaction table accepts these optional fields:

- `conditions`: A dense array of conditions. All conditions must match to run the `then` operations.
- `["then"]`: A dense array of operations to run when all conditions match.
- `["else"]`: A dense array of operations to run when a condition does not match.

Each condition has `key`, `op`, and `value` fields. Supported condition operations are:

- `eq_seq`: The key's sequence must equal the non-negative integer `value`. Sequence 0 means the key must not exist.
- `ge_seq`: The key's sequence must be greater than or equal to the non-negative integer `value`.
- `eq_value`: The key's bytes must equal the binary-safe Lua string `value`.

Each operation has `op` and `key` fields. Supported operations are:

- `put`: Stores the binary-safe Lua string `value`. The optional numeric `ttl` sets the lifetime in seconds.
- `get`: Returns the current value.
- `delete`: Deletes the key.

The `metactl` namespace provides helpers that create the same tables:

- `metactl.eq_seq(key, seq)`
- `metactl.ge_seq(key, seq)`
- `metactl.eq_value(key, value)`
- `metactl.put_op(key, value, ttl)`
- `metactl.get_op(key)`
- `metactl.delete_op(key)`
- `metactl.new_txn(conditions, then_ops, else_ops)`

```lua
local transaction = metactl.new_txn(
    {metactl.eq_seq("my_key", 0)},
    {
        metactl.put_op("my_key", "my_value", 60),
        metactl.get_op("my_key")
    },
    {metactl.get_op("my_key")}
)
```

## Task Handling

### task:join()

Waits for a spawned task to complete and returns its result.

**Returns:**
- The return value of the spawned function

**Example:**
```lua
local task = metactl.spawn(function()
    return "task result"
end)

local result = task:join()
print(result)  -- Output: task result
```

## Load Generation

### metactl.ZipfGenerator:new(num_keys, alpha)

Creates a Zipf-distribution load generator. A Zipf distribution models realistic key-access skew, where a small set of keys receives most of the traffic (see [Zipf's law](https://en.wikipedia.org/wiki/Zipf%27s_law)).

**Parameters:**
- `num_keys` (integer): Number of distinct keys in the dataset. Defaults to `1000`.
- `alpha` (number): Zipf exponent; higher values skew the distribution more toward low indices. Defaults to `1.0`.

**Returns:**
- A generator object exposing the `generate_key_index` method, plus the readable fields `num_keys` and `alpha`.

**Example:**
```lua
local zipf = metactl.ZipfGenerator:new(1000000, 1.2)
```

### zipf:generate_key_index(x)

Maps a uniform sample to a Zipf-distributed key index using an O(1) transformation (no lookup table).

**Parameters:**
- `x` (number): A uniform sample in `[0, 1)`, typically from `math.random()`.

**Returns:**
- An integer key index, biased toward low values. `x = 0` returns `1`, and the index increases monotonically with `x`.

For `x` close to `1` the result can exceed `num_keys`. Clamp with `math.min` when a strict upper bound is required.

**Example:**
```lua
local zipf = metactl.ZipfGenerator:new(1000000, 1.2)

-- Generate a Zipf-distributed access sequence
for i = 1, 10 do
    local index = math.min(zipf.num_keys, zipf:generate_key_index(math.random()))
    print(index)
end
```

## Usage Patterns

### Basic Key-Value Operations

```lua
local client = metactl.new_grpc_client("127.0.0.1:9191")

-- Store a value
local upsert_result, err = client:upsert("config/timeout", "30")
if not err then
    print("Stored successfully:", metactl.to_string(upsert_result))
end

-- Retrieve a value
local get_result, err = client:get("config/timeout")
if not err then
    if get_result == metactl.NULL then
        print("Key not found")
    else
        print("Retrieved:", metactl.to_string(get_result))
    end
end
```

### Concurrent Operations

```lua
local client = metactl.new_grpc_client("127.0.0.1:9191")

local task1 = metactl.spawn(function()
    client:upsert("key1", "value1")
    print("Task 1 completed")
end)

local task2 = metactl.spawn(function()
    metactl.sleep(0.1)
    local result, _ = client:get("key1")
    print("Task 2 got:", metactl.to_string(result))
end)

task1:join()
task2:join()
```

### Error Handling

```lua
local client = metactl.new_grpc_client("127.0.0.1:9191")

local result, err = client:get("some_key")
if err then
    print("Operation failed:", err)
    return
end

if result == metactl.NULL then
    print("Key does not exist")
else
    print("Key value:", metactl.to_string(result))
end
```

## Data Types

The meta service returns structured data that can be processed using `metactl.to_string()`:

```lua
-- Typical get response structure:
{
    "data" = "actual_value",  -- The stored value as byte array
    "seq" = 1                 -- Sequence number
}

-- Typical upsert response structure:
{
    "result" = {
        "data" = "stored_value",
        "seq" = 1
    }
}
```

## Best Practices

1. **Always check for errors**: `get`, `upsert`, and `transaction` operations can fail
2. **Handle NULL values**: Use `metactl.NULL` comparison for missing keys
3. **Use concurrent operations**: Leverage `metactl.spawn()` for parallel processing
4. **Format output**: Use `metactl.to_string()` for readable data display
5. **Clean resource usage**: Ensure tasks are properly joined

## Examples

See the Lua test scripts in `src/meta/control/tests/it/lua/` for comprehensive
usage examples:
- `test_grpc_kv.lua` - Basic gRPC operations
- `test_transaction.lua` - Atomic conditional transactions
- `test_spawn.lua` - Task spawning and concurrent gRPC operations
- `test_sleep.lua` - Async sleep
- `test_to_string.lua` - `metactl.to_string` rendering rules

The CLI surface of `databend-metactl lua` (file and stdin input) is covered by
`tests/metactl/subcommands/cmd_lua.py`.
