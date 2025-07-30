-- Function to check if a value is NULL (mlua NULL constant)
function is_null(value)
    return value == NULL
end

-- Function to normalize NULL values to nil for easier handling
function normalize_value(value)
    if is_null(value) then
        return nil
    end
    return value
end

-- Function to check if a table represents a bytes vector and convert it to string
function bytes_vector_to_string(value)
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
                return '"<bytes vector too large: ' .. max_index .. ' bytes>"'
            end
            
            -- Convert bytes vector to string
            local chars = {}
            for i = 1, max_index do
                chars[i] = string.char(value[i])
            end
            return '"' .. table.concat(chars) .. '"'
        end
    end
    
    return nil -- Not a valid bytes vector
end

-- Function to convert a value or table into a single-line string recursively
function to_string(value)
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
        -- Escape quotes in the string
        return '"' .. string.gsub(value, '"', '\\"') .. '"'
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
                    key_str = '"' .. string.gsub(k, '"', '\\"') .. '"'
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
