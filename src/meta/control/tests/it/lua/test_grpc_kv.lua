-- Basic gRPC round-trip against the in-process meta-service.
--
-- `TEST_GRPC_ADDRESS` is injected as a global by the Rust test runner
-- (tests/it/lua.rs). This exercises the meta-store brought up for the test and
-- serves as the template for gRPC-based Lua tests. Failures are raised via
-- `assert`.

local client = metactl.new_grpc_client(TEST_GRPC_ADDRESS)

-- The single-node cluster may need a moment to elect itself leader; retry the
-- first write until the service accepts it.
local ready = false
for _ = 1, 25 do
    local _, err = client:upsert("lua_it/ready", "1")
    if err == nil then
        ready = true
        break
    end
    metactl.sleep(0.2)
end
assert(ready, "meta-service did not become ready for gRPC writes")

-- Write, then read the value back.
local _, err = client:upsert("lua_it/greeting", "hello")
assert(err == nil, "upsert failed: " .. tostring(err))

local result, err = client:get("lua_it/greeting")
assert(err == nil, "get failed: " .. tostring(err))
assert(result ~= metactl.NULL, "key should exist after upsert")

-- The stored bytes should round-trip to the original value exactly.
local value = string.char(table.unpack(result.data))
assert(value == "hello", "unexpected value: " .. metactl.to_string(result))

print("gRPC round-trip OK: " .. metactl.to_string(result))
