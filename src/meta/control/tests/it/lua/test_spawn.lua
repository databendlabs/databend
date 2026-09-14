-- `metactl.spawn` task semantics: concurrent execution, `join` returning the
-- task function's value, and visibility of gRPC writes across tasks.
--
-- Runs under the metactl Lua test harness (tests/it/lua.rs); keys are
-- namespaced under `lua_spawn/`. Failures are raised via `assert`.

print("spawn: task interleaving, join return values, cross-task gRPC visibility")

-- Two tasks run concurrently: both start before either finishes, and the
-- shorter sleep finishes first.
local events = {}

local task1 = metactl.spawn(function()
    table.insert(events, "task1 start")
    metactl.sleep(0.1)
    table.insert(events, "task1 done")
    return "one"
end)

local task2 = metactl.spawn(function()
    table.insert(events, "task2 start")
    metactl.sleep(0.3)
    table.insert(events, "task2 done")
    return "two"
end)

assert(task1:join() == "one", "join should return the task function's value")
assert(task2:join() == "two", "join should return the task function's value")

local order = table.concat(events, ", ")
assert(order == "task1 start, task2 start, task1 done, task2 done",
    "unexpected task interleaving: " .. order)
print("spawn interleaving OK: " .. order)

-- Writes made by one task are visible to another through separate clients.
local function bytes_to_string(value)
    return string.char(table.unpack(value))
end

local writer1 = metactl.spawn(function()
    local client = metactl.new_grpc_client(TEST_GRPC_ADDRESS)

    local _, err = client:upsert("lua_spawn/k1", "v1")
    assert(err == nil, "task 1 upsert failed: " .. tostring(err))

    metactl.sleep(0.5)

    local result, err = client:get("lua_spawn/k2")
    assert(err == nil, "task 1 get failed: " .. tostring(err))
    return result
end)

local writer2 = metactl.spawn(function()
    local client = metactl.new_grpc_client(TEST_GRPC_ADDRESS)

    -- Let task 1 write first.
    metactl.sleep(0.2)

    local _, err = client:upsert("lua_spawn/k2", "v2")
    assert(err == nil, "task 2 upsert failed: " .. tostring(err))

    metactl.sleep(0.5)

    local result, err = client:get("lua_spawn/k1")
    assert(err == nil, "task 2 get failed: " .. tostring(err))
    return result
end)

local k2_seen_by_1 = writer1:join()
local k1_seen_by_2 = writer2:join()

assert(k2_seen_by_1 ~= metactl.NULL, "task 1 should see task 2's write")
assert(bytes_to_string(k2_seen_by_1.data) == "v2",
    "unexpected k2 value: " .. metactl.to_string(k2_seen_by_1))
assert(k1_seen_by_2 ~= metactl.NULL, "task 2 should see task 1's write")
assert(bytes_to_string(k1_seen_by_2.data) == "v1",
    "unexpected k1 value: " .. metactl.to_string(k1_seen_by_2))
print("spawn cross-task gRPC OK")
