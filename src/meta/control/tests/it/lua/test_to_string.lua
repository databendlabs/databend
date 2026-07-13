-- `metactl.to_string` rendering: scalars, string escaping, table key sorting,
-- bytes-vector detection, and NULL/nil handling.
--
-- Runs under the metactl Lua test harness (tests/it/lua.rs); pure Lua, no
-- meta-service access. Failures are raised via `error`.

print("to_string: scalars, escaping, key sorting, bytes vectors, NULL")

local function assert_eq(actual, expected, label)
    if actual ~= expected then
        error(label .. ": expected " .. expected .. ", got " .. actual)
    end
end

-- Scalars.
assert_eq(metactl.to_string(nil), "nil", "nil")
assert_eq(metactl.to_string(metactl.NULL), "NULL", "NULL")
assert_eq(metactl.to_string(true), "true", "true")
assert_eq(metactl.to_string(false), "false", "false")
assert_eq(metactl.to_string(42), "42", "integer")
assert_eq(metactl.to_string(1.5), "1.5", "float")

-- Strings are quoted; control bytes, quotes, and non-ASCII are escaped.
assert_eq(metactl.to_string("hello"), '"hello"', "plain string")
assert_eq(metactl.to_string('He said "Hello"'), '"He said \\"Hello\\""', "embedded quotes")
assert_eq(metactl.to_string("tab\tnewline\n"), '"tab\\tnewline\\n"', "control escapes")
assert_eq(metactl.to_string(string.char(97, 1, 98)), '"a\\x01b"', "hex escape")

-- Tables: string keys are quoted and sorted; number keys are bracketed and
-- sort before string keys.
assert_eq(metactl.to_string({}), "{}", "empty table")
assert_eq(metactl.to_string({b = "hello", a = 1, c = true}),
    '{"a"=1,"b"="hello","c"=true}', "sorted string keys")
assert_eq(metactl.to_string({name = "mixed", [1] = "first"}),
    '{[1]="first","name"="mixed"}', "numbers before strings")
assert_eq(metactl.to_string({address = {city = "Anytown"}, age = 30}),
    '{"address"={"city"="Anytown"},"age"=30}', "nested table")

-- A dense 1-based array of u8 numbers is rendered as a byte string; anything
-- else falls back to regular table rendering.
assert_eq(metactl.to_string({104, 101, 108, 108, 111}), '"hello"', "bytes vector")
assert_eq(metactl.to_string({104, 256}), "{[1]=104,[2]=256}", "not u8: regular table")
assert_eq(metactl.to_string({[2] = 104}), "{[2]=104}", "not dense: regular table")
assert_eq(metactl.to_string({"apple", "cherry"}),
    '{[1]="apple",[2]="cherry"}', "string array is not a bytes vector")

-- nil and NULL values are omitted from tables.
assert_eq(metactl.to_string({a = 1, b = metactl.NULL}), '{"a"=1}', "NULL value skipped")

-- Non-sortable keys keep insertion-order semantics; a single entry stays
-- deterministic. Functions render as a type placeholder.
assert_eq(metactl.to_string({[true] = "yes"}), '{[true]="yes"}', "boolean key")
assert_eq(metactl.to_string(print), "<function>", "function value")
local table_key = metactl.to_string({[{}] = "v"})
assert(table_key:match('^{%[table: 0x') ~= nil,
    "table key should render with its address, got " .. table_key)

print("to_string OK")
