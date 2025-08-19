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
- `seconds` (number): Duration to sleep in seconds (supports fractional values)

**Example:**
```lua
metactl.sleep(0.5)  -- Sleep for 500ms
metactl.sleep(2.0)  -- Sleep for 2 seconds
```

### metactl.to_string(value)

Converts any Lua value to a human-readable string representation.

**Parameters:**
- `value`: Any Lua value (nil, boolean, number, string, table, etc.)

**Returns:**
- String representation of the value

**Features:**
- Handles nested tables recursively
- Detects and converts byte vectors to strings
- Sorts table keys for consistent output
- Handles NULL values from meta service
- Escapes quotes in strings
- Single-line output format

**Example:**
```lua
print(metactl.to_string({key="value", data={1,2,3}}))
-- Output: {"data"={1,2,3},"key"="value"}

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
- `value` (string): The value to store

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

1. **Always check for errors**: Both `get` and `upsert` operations can fail
2. **Handle NULL values**: Use `metactl.NULL` comparison for missing keys
3. **Use concurrent operations**: Leverage `metactl.spawn()` for parallel processing
4. **Format output**: Use `metactl.to_string()` for readable data display
5. **Clean resource usage**: Ensure tasks are properly joined

## Examples

See the test files in `tests/metactl/subcommands/` for comprehensive usage examples:
- `cmd_lua_grpc.py` - Basic gRPC operations
- `cmd_lua_spawn_grpc.py` - Concurrent gRPC operations
- `cmd_lua_spawn_concurrent.py` - Task spawning patterns