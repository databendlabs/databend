-- Transaction API coverage against the in-process meta-service: table-form
-- and helper-form transactions, conditions, atomicity under contention,
-- unconditional/empty transactions, binary values, TTL, and validation errors.
--
-- Runs under the metactl Lua test harness (tests/it/lua.rs). Sequence-number
-- assertions are relative to this script's own writes, which is safe because
-- scripts run one at a time against the shared meta-service. Keys are
-- namespaced under `lua-txn/`. Failures are raised via `error`.

print("transaction: table/helper txns, conditions, atomicity, binary, TTL, validation")

local client = metactl.new_grpc_client(TEST_GRPC_ADDRESS)

local function assert_equal(actual, expected, label)
    if actual ~= expected then
        error(label .. ": expected " .. metactl.to_string(expected) ..
            ", got " .. metactl.to_string(actual))
    end
end

local function assert_fields(value, expected, label)
    local fields = {}
    for field in pairs(value) do
        table.insert(fields, field)
    end
    table.sort(fields)
    assert_equal(table.concat(fields, ","), expected, label)
end

local function bytes_to_string(value)
    if value == nil or value == metactl.NULL then
        return nil
    end
    return string.char(table.unpack(value))
end

local function assert_seqv(value, seq, data, label)
    assert_fields(value, "data,meta,seq", label .. " fields")
    assert_equal(value.seq, seq, label .. " seq")
    assert_equal(bytes_to_string(value.data), data, label .. " data")
    assert_equal(type(value.meta), "table", label .. " meta")
    assert_equal(type(value.meta.proposed_at_ms), "number", label .. " proposed_at_ms")
end

local function assert_reply(reply, success, path, response_count, label)
    assert_fields(reply, "execution_path,responses,success", label .. " fields")
    assert_equal(reply.success, success, label .. " success")
    assert_equal(reply.execution_path, path, label .. " path")
    assert_equal(#reply.responses, response_count, label .. " response count")
end

local function response(reply, index, kind, fields, label)
    local wrapper = reply.responses[index]
    assert_fields(wrapper, "response", label .. " wrapper")
    assert_fields(wrapper.response, kind, label .. " variant")
    local value = wrapper.response[kind]
    assert_fields(value, fields, label .. " fields")
    return value
end

local function transact(txn, label)
    local reply, err = client:transaction(txn)
    assert_equal(err, nil, label .. " error")
    if reply == nil then
        error(label .. ": missing reply")
    end
    return reply
end

local function get(key, label)
    local value, err = client:get(key)
    assert_equal(err, nil, label .. " error")
    return value
end

local function upsert(key, value, label)
    local reply, err = client:upsert(key, value)
    assert_equal(err, nil, label .. " error")
    if reply == nil then
        error(label .. ": missing reply")
    end
    return reply
end

local seed = upsert("lua-txn/delete", "old", "seed")
local seed_seq = seed.result.seq
assert_seqv(seed.result, seed_seq, "old", "seed result")

local table_txn = {
    conditions = {{key = "lua-txn/created", op = "eq_seq", value = 0}},
    ["then"] = {
        {op = "put", key = "lua-txn/created", value = "alpha"},
        {op = "get", key = "lua-txn/created"},
        {op = "delete", key = "lua-txn/delete"}
    },
    ["else"] = {{op = "put", key = "lua-txn/unexpected", value = "bad"}}
}
local table_reply = transact(table_txn, "table transaction")
assert_reply(table_reply, true, "then", 3, "table transaction")

local put = response(
    table_reply, 1, "Put", "current,key,prev_value", "table put")
assert_equal(put.key, "lua-txn/created", "table put key")
assert_equal(put.prev_value, metactl.NULL, "table put previous")
local created_seq = put.current.seq
assert_equal(created_seq, seed_seq + 1, "table put sequence")
assert_seqv(put.current, created_seq, "alpha", "table put current")

local read = response(table_reply, 2, "Get", "key,value", "table get")
assert_equal(read.key, "lua-txn/created", "table get key")
assert_seqv(read.value, created_seq, "alpha", "table get value")

local deleted = response(
    table_reply, 3, "Delete", "key,prev_value,success", "table delete")
assert_equal(deleted.key, "lua-txn/delete", "table delete key")
assert_equal(deleted.success, true, "table delete success")
assert_seqv(deleted.prev_value, seed_seq, "old", "table delete previous")
assert_seqv(get("lua-txn/created", "created state"), created_seq, "alpha", "created state")
assert_equal(get("lua-txn/delete", "deleted state"), metactl.NULL, "deleted state")
assert_equal(get("lua-txn/unexpected", "unexpected state"), metactl.NULL, "unexpected state")
print("table transaction: ok")

local else_txn = metactl.new_txn(
    {metactl.eq_seq("lua-txn/created", 0)},
    {metactl.put_op("lua-txn/unexpected", "bad")},
    {
        metactl.get_op("lua-txn/created"),
        metactl.put_op("lua-txn/else", "selected")
    })
local else_reply = transact(else_txn, "helper else")
assert_reply(else_reply, false, "else", 2, "helper else")
local else_get = response(else_reply, 1, "Get", "key,value", "else get")
assert_equal(else_get.key, "lua-txn/created", "else get key")
assert_seqv(else_get.value, created_seq, "alpha", "else get value")
local else_put = response(
    else_reply, 2, "Put", "current,key,prev_value", "else put")
assert_equal(else_put.key, "lua-txn/else", "else put key")
assert_equal(else_put.prev_value, metactl.NULL, "else put previous")
local else_seq = else_put.current.seq
assert_seqv(else_put.current, else_seq, "selected", "else put current")
assert_equal(get("lua-txn/unexpected", "else unexpected"), metactl.NULL, "else unexpected")

local no_else_reply = transact(
    metactl.new_txn(
        {metactl.eq_seq("lua-txn/created", 0)},
        {metactl.put_op("lua-txn/unexpected", "bad")}),
    "omitted else")
assert_reply(no_else_reply, false, "else", 0, "omitted else")
assert_equal(get("lua-txn/unexpected", "omitted else state"), metactl.NULL, "omitted else state")
print("helper else: ok")

local and_txn = metactl.new_txn(
    {
        metactl.eq_seq("lua-txn/created", created_seq),
        metactl.ge_seq("lua-txn/created", created_seq),
        metactl.eq_value("lua-txn/created", "alpha")
    },
    {
        metactl.put_op("lua-txn/and", "matched"),
        metactl.get_op("lua-txn/and")
    },
    {metactl.put_op("lua-txn/and-failed", "bad")})
local and_reply = transact(and_txn, "AND conditions")
assert_reply(and_reply, true, "then", 2, "AND conditions")
local and_put = response(and_reply, 1, "Put", "current,key,prev_value", "AND put")
local and_seq = and_put.current.seq
assert_equal(and_seq, else_seq + 1, "AND put sequence")
assert_seqv(and_put.current, and_seq, "matched", "AND put current")
local and_get = response(and_reply, 2, "Get", "key,value", "AND get")
assert_seqv(and_get.value, and_seq, "matched", "AND get value")
assert_equal(get("lua-txn/and-failed", "AND else state"), metactl.NULL, "AND else state")

local failed_and = metactl.new_txn(
    {
        metactl.eq_seq("lua-txn/created", created_seq),
        metactl.eq_value("lua-txn/created", "wrong")
    },
    {metactl.put_op("lua-txn/and-unexpected", "bad")},
    {metactl.put_op("lua-txn/and-failed", "selected")})
local failed_reply = transact(failed_and, "failed AND")
assert_reply(failed_reply, false, "else", 1, "failed AND")
assert_equal(get("lua-txn/and-unexpected", "failed AND then"), metactl.NULL, "failed AND then")
local failed_value = get("lua-txn/and-failed", "failed AND else")
assert_seqv(failed_value, failed_value.seq, "selected", "failed AND else")
print("conditions: ok")

local contenders_ready = 0

local function contend(value)
    local worker = metactl.new_grpc_client(TEST_GRPC_ADDRESS)
    contenders_ready = contenders_ready + 1
    while contenders_ready < 2 do
        metactl.sleep(0.001)
    end
    local reply, err = worker:transaction(metactl.new_txn(
        {metactl.eq_seq("lua-txn/race", 0)},
        {
            metactl.put_op("lua-txn/race", value),
            metactl.put_op("lua-txn/race-side/" .. value, value)
        },
        {metactl.get_op("lua-txn/race")}))
    assert_equal(err, nil, value .. " contender error")
    return reply
end

local left_task = metactl.spawn(function() return contend("left") end)
local right_task = metactl.spawn(function() return contend("right") end)
local left_reply = left_task:join()
local right_reply = right_task:join()
local winner_count = (left_reply.success and 1 or 0) + (right_reply.success and 1 or 0)
assert_equal(winner_count, 1, "race winner count")

local winner = left_reply.success and "left" or "right"
local loser = left_reply.success and "right" or "left"
local winner_reply = left_reply.success and left_reply or right_reply
local loser_reply = left_reply.success and right_reply or left_reply
assert_reply(winner_reply, true, "then", 2, "race winner")
assert_reply(loser_reply, false, "else", 1, "race loser")
local race_put = response(winner_reply, 1, "Put", "current,key,prev_value", "race put")
assert_equal(race_put.prev_value, metactl.NULL, "race put previous")
local race_seq = race_put.current.seq
assert_seqv(race_put.current, race_seq, winner, "race put current")
local race_side_put = response(
    winner_reply, 2, "Put", "current,key,prev_value", "race side put")
local race_side_seq = race_side_put.current.seq
assert_equal(race_side_put.key, "lua-txn/race-side/" .. winner, "race side put key")
assert_equal(race_side_put.prev_value, metactl.NULL, "race side put previous")
assert_seqv(race_side_put.current, race_side_seq, winner, "race side put current")
local race_get = response(loser_reply, 1, "Get", "key,value", "race loser get")
assert_seqv(race_get.value, race_seq, winner, "race loser value")
assert_seqv(get("lua-txn/race", "race state"), race_seq, winner, "race state")
local winner_side = get("lua-txn/race-side/" .. winner, "race winner side")
assert_seqv(winner_side, race_side_seq, winner, "race winner side")
assert_equal(get("lua-txn/race-side/" .. loser, "race loser side"), metactl.NULL,
    "race loser side")
print("atomic: ok")

local unconditional = metactl.new_txn(nil, {
    metactl.put_op("lua-txn/unconditional", "written"),
    metactl.get_op("lua-txn/unconditional")
})
local unconditional_reply = transact(unconditional, "unconditional")
assert_reply(unconditional_reply, true, "then", 2, "unconditional")
local unconditional_put = response(
    unconditional_reply, 1, "Put", "current,key,prev_value", "unconditional put")
local unconditional_seq = unconditional_put.current.seq
assert_seqv(
    unconditional_put.current, unconditional_seq, "written", "unconditional put current")
local unconditional_get = response(
    unconditional_reply, 2, "Get", "key,value", "unconditional get")
assert_seqv(
    unconditional_get.value, unconditional_seq, "written", "unconditional get value")
local empty_reply = transact({}, "empty transaction")
assert_reply(empty_reply, true, "then", 0, "empty transaction")
print("unconditional: ok")

local binary = string.char(0, 1, 127, 128, 255)
local binary_upsert = upsert("lua-txn/upsert-binary", binary, "binary upsert")
local binary_upsert_seq = binary_upsert.result.seq
assert_seqv(
    binary_upsert.result, binary_upsert_seq, binary, "binary upsert result")
assert_seqv(
    get("lua-txn/upsert-binary", "binary upsert state"),
    binary_upsert_seq,
    binary,
    "binary upsert state")
local binary_reply = transact(
    metactl.new_txn(nil, {
        metactl.put_op("lua-txn/binary", binary),
        metactl.get_op("lua-txn/binary")
    }),
    "binary")
assert_reply(binary_reply, true, "then", 2, "binary")
local binary_put = response(
    binary_reply, 1, "Put", "current,key,prev_value", "binary put")
local binary_seq = binary_put.current.seq
assert_seqv(binary_put.current, binary_seq, binary, "binary put current")
local binary_get = response(binary_reply, 2, "Get", "key,value", "binary get")
assert_seqv(binary_get.value, binary_seq, binary, "binary get value")
assert_seqv(get("lua-txn/binary", "binary state"), binary_seq, binary, "binary state")
print("binary: ok")

local ttl_reply = transact(
    metactl.new_txn(nil, {metactl.put_op("lua-txn/ttl", "temporary", 2)}),
    "TTL")
assert_reply(ttl_reply, true, "then", 1, "TTL")
local ttl_put = response(ttl_reply, 1, "Put", "current,key,prev_value", "TTL put")
local ttl_seq = ttl_put.current.seq
assert_seqv(ttl_put.current, ttl_seq, "temporary", "TTL current")
assert_seqv(get("lua-txn/ttl", "TTL immediate"), ttl_seq, "temporary", "TTL immediate")
metactl.sleep(2.5)
assert_equal(get("lua-txn/ttl", "TTL expired"), metactl.NULL, "TTL expired")
print("ttl: ok")

local function expect_invalid(txn, expected, label)
    local result, err = client:transaction(txn)
    assert_equal(result, nil, label .. " result")
    assert_equal(err, "Invalid transaction: " .. expected, label .. " error")
end

expect_invalid(
    {unknown = {}},
    "transaction: unknown field `unknown`",
    "unknown transaction field")
expect_invalid(
    {conditions = "invalid"},
    "transaction.conditions: must be an array, got string",
    "conditions type")
expect_invalid(
    {conditions = {{key = "key", op = "unknown", value = 0}}},
    "transaction.conditions[1]: unknown condition `unknown`",
    "unknown condition")
expect_invalid(
    {["then"] = {{op = "unknown", key = "lua-txn/invalid"}}},
    "transaction[\"then\"][1]: unknown operation `unknown`",
    "unknown operation")
expect_invalid(
    {["then"] = {{op = "put", key = "lua-txn/invalid"}}},
    "transaction[\"then\"][1]: missing field `value`",
    "missing put value")
expect_invalid(
    {["then"] = {[1] = metactl.put_op("lua-txn/invalid", "bad"),
        [3] = metactl.get_op("lua-txn/invalid")}},
    "transaction[\"then\"]: must be a dense array",
    "sparse operations")
expect_invalid(
    {["then"] = {{op = "get", key = "lua-txn/invalid", ttl = 1}}},
    "transaction[\"then\"][1]: unknown field `ttl`",
    "get TTL")
expect_invalid(
    metactl.new_txn(nil, {metactl.put_op("lua-txn/invalid", "bad", "soon")}),
    "transaction[\"then\"][1]: field `ttl` must be a number, got string",
    "TTL type")

for label, ttl in pairs({negative = -1, infinite = math.huge, nan = 0 / 0}) do
    expect_invalid(
        metactl.new_txn(nil, {metactl.put_op("lua-txn/invalid", "bad", ttl)}),
        "transaction[\"then\"][1].ttl: duration must be finite, non-negative, and representable",
        label .. " TTL")
end
assert_equal(get("lua-txn/invalid", "invalid state"), metactl.NULL, "invalid state")
print("invalid: ok")
