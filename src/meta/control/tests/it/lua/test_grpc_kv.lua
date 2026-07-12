-- Basic gRPC round-trip against the in-process meta-service.
--
-- `TEST_GRPC_ADDRESS` is injected as a global by the Rust test runner
-- (tests/it/lua.rs), which also waits for the service to accept writes before
-- running scripts. This exercises the meta-store brought up for the test and
-- serves as the template for gRPC-based Lua tests. Failures are raised via
-- `assert`.

print("kv: upsert/get round-trip, upsert reply value, missing key is NULL")

local client = metactl.new_grpc_client(TEST_GRPC_ADDRESS)

-- Write; the upsert reply carries the stored value.
local reply, err = client:upsert("lua_it/greeting", "hello")
assert(err == nil, "upsert failed: " .. tostring(err))
local stored = string.char(table.unpack(reply.result.data))
assert(stored == "hello", "unexpected upsert result: " .. metactl.to_string(reply))

-- Read the value back; the stored bytes round-trip to the original exactly.
local result, err = client:get("lua_it/greeting")
assert(err == nil, "get failed: " .. tostring(err))
assert(result ~= metactl.NULL, "key should exist after upsert")
local value = string.char(table.unpack(result.data))
assert(value == "hello", "unexpected value: " .. metactl.to_string(result))
assert(result.seq == reply.result.seq,
    "get seq " .. result.seq .. " != upsert seq " .. reply.result.seq)

-- A key that was never written reads back as NULL.
local missing, err = client:get("lua_it/nonexistent")
assert(err == nil, "get nonexistent failed: " .. tostring(err))
assert(missing == metactl.NULL,
    "missing key should be NULL, got: " .. metactl.to_string(missing))

print("gRPC round-trip OK: " .. metactl.to_string(result))
