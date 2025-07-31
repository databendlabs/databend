-- Copyright 2021 Datafuse Labs
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Function to check if a value is NULL (mlua NULL constant)
local function is_null(value)
    return value == metactl.NULL
end

-- Function to normalize NULL values to nil for easier handling
local function normalize_value(value)
    if is_null(value) then
        return nil
    end
    return value
end

local string_escapes = {
    [8] = "\\b",
    [9] = "\\t",
    [10] = "\\n",
    [12] = "\\f",
    [13] = "\\r",
    [34] = '\\"',
    [92] = "\\\\"
}

local function quote_string(value)
    local result = {}
    for i = 1, #value do
        local byte = string.byte(value, i)
        result[i] = string_escapes[byte]
            or (byte >= 32 and byte <= 126 and string.char(byte))
            or string.format("\\x%02X", byte)
    end
    return '"' .. table.concat(result) .. '"'
end

-- Function to check if a table represents a bytes vector and convert it to string
local function bytes_vector_to_string(value)
    -- Check if table represents a bytes vector (integer indices starting from 1, u8 values)
    local max_index = 0
    local min_index = math.huge

    -- First pass: check if all keys are integers and find range
    for k, v in pairs(value) do
        if type(k) ~= "number" or k ~= math.floor(k) or k < 1 then
            return nil -- Not a bytes vector
        end
        if type(v) ~= "number" or v ~= math.floor(v) or v < 0 or v > 255 then
            return nil -- Not u8 values
        end
        max_index = math.max(max_index, k)
        min_index = math.min(min_index, k)
    end

    -- Check if indices are consecutive starting from 1
    if min_index == 1 then
        local expected_length = max_index
        local actual_length = 0
        for _ in pairs(value) do
            actual_length = actual_length + 1
        end
        if actual_length == expected_length then
            -- Check size limit to prevent memory exhaustion (max 1MB)
            if max_index > 1048576 then
                return quote_string("<bytes vector too large: " .. max_index .. " bytes>")
            end

            -- Convert bytes vector to string
            local chars = {}
            for i = 1, max_index do
                chars[i] = string.char(value[i])
            end
            return quote_string(table.concat(chars))
        end
    end

    return nil -- Not a valid bytes vector
end

-- Function to convert a value or table into a single-line string recursively
local function to_string(value)
    if value == nil then
        return "nil"
    end

    if is_null(value) then
        return "NULL"
    end

    if type(value) == "boolean" then
        return tostring(value)
    end

    if type(value) == "number" then
        return tostring(value)
    end

    if type(value) == "string" then
        return quote_string(value)
    end

    if type(value) == "table" then
        -- Check if it's a bytes vector first
        local bytes_string = bytes_vector_to_string(value)
        if bytes_string then
            return bytes_string
        end

        -- Regular table processing
        local result = "{"
        local keys = {}

        -- Collect all keys
        for k in pairs(value) do
            table.insert(keys, k)
        end

        -- Sort keys if they're all strings or numbers
        local can_sort = true
        for _, k in ipairs(keys) do
            if type(k) ~= "string" and type(k) ~= "number" then
                can_sort = false
                break
            end
        end

        if can_sort then
            table.sort(keys, function(a, b)
                if type(a) == type(b) then
                    return a < b
                else
                    -- Put numbers before strings
                    return type(a) == "number"
                end
            end)
        end

        -- Process each key-value pair
        local first = true
        for _, k in ipairs(keys) do
            local v = value[k]

            -- Skip nil and NULL values
            if v ~= nil and not is_null(v) then
                if not first then
                    result = result .. ","
                end
                first = false

                local key_str
                if type(k) == "string" then
                    key_str = quote_string(k)
                else
                    key_str = "[" .. tostring(k) .. "]"
                end

                result = result .. key_str .. "=" .. to_string(v)
            end
        end

        result = result .. "}"
        return result
    end

    -- Handle function, userdata, thread
    return "<" .. type(value) .. ">"
end

local function eq_seq(key, seq)
    return {key = key, op = "eq_seq", value = seq}
end

local function ge_seq(key, seq)
    return {key = key, op = "ge_seq", value = seq}
end

local function eq_value(key, value)
    return {key = key, op = "eq_value", value = value}
end

local function put_op(key, value, ttl)
    local op = {op = "put", key = key, value = value}
    if ttl ~= nil then
        op.ttl = ttl
    end
    return op
end

local function get_op(key)
    return {op = "get", key = key}
end

local function delete_op(key)
    return {op = "delete", key = key}
end

local function new_txn(conditions, then_ops, else_ops)
    if conditions == nil then
        conditions = {}
    end
    if then_ops == nil then
        then_ops = {}
    end
    if else_ops == nil then
        else_ops = {}
    end
    return {
        conditions = conditions,
        ["then"] = then_ops,
        ["else"] = else_ops
    }
end

metactl.to_string = to_string
metactl.eq_seq = eq_seq
metactl.ge_seq = ge_seq
metactl.eq_value = eq_value
metactl.put_op = put_op
metactl.get_op = get_op
metactl.delete_op = delete_op
metactl.new_txn = new_txn
