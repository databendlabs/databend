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

-- Zipf distribution load generator for meta service
-- O(1) Zipf distribution using transformation method

local ZipfGenerator = {}
ZipfGenerator.__index = ZipfGenerator

function ZipfGenerator:new(num_keys, alpha)
    local obj = {
        num_keys = num_keys or 1000,        -- Total number of unique keys in the dataset
        alpha = alpha or 1.0,               -- Zipf exponent: higher values = more skewed distribution
        q_inv = 1.0 / (1.0 - alpha),        -- Inverse of (1-alpha)
        a_pow_q = 1.0 ^ (1.0 - alpha),      -- Pre-computed power of lower bound
        span = 0                            -- b^(1-alpha) - a^(1-alpha)
    }
    setmetatable(obj, self)

    -- Precompute constants for O(1) generation
    local b_pow_q = num_keys ^ (1.0 - alpha)
    obj.span = b_pow_q - obj.a_pow_q

    return obj
end

-- Inverse CDF of a power law truncated to [1, num_keys]:
-- t = (a^(1-alpha) + x * (b^(1-alpha) - a^(1-alpha)))^(1/(1-alpha))
-- Maps uniform x in [0, 1) to an index in [1, num_keys].
function ZipfGenerator:generate_key_index(x)
    local t = (self.a_pow_q + x * self.span) ^ self.q_inv
    return math.floor(t + 0.5)
end


-- Example usage
local function main()
    math.randomseed(os.time())

    local zipf = ZipfGenerator:new(10000, 1.2)

    print("Generating Zipf distribution access sequence...")
    print("Total keys: " .. zipf.num_keys)
    print("Alpha: " .. zipf.alpha)
    print()

    -- Generate sample sequence
    print("Sample access sequence (indices):")
    for i = 1, 20 do
        local index = zipf:generate_key_index(math.random())
        print(string.format("%2d: %d", i, index))
    end

    -- Statistics
    local counts = {}

    for i = 1, 10000 do
        local index = zipf:generate_key_index(math.random())
        counts[index] = (counts[index] or 0) + 1
    end

    print("\nTop 10 most accessed indices:")
    local sorted_counts = {}
    for index, count in pairs(counts) do
        table.insert(sorted_counts, {index = index, count = count})
    end

    table.sort(sorted_counts, function(a, b) return a.count > b.count end)

    for i = 1, math.min(10, #sorted_counts) do
        print(string.format("Index %d: %d accesses", sorted_counts[i].index, sorted_counts[i].count))
    end
end

if arg and arg[0] and arg[0]:match("zipf_load_generator%.lua$") then
    main()
end

return ZipfGenerator
