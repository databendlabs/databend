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
    -- Normalize defaults on the parameters themselves before they are read
    -- below; `x or default` inside the `obj` table literal would not help
    -- the *other* fields in the same literal, which still see the raw
    -- (possibly nil) parameter.
    num_keys = num_keys or 1000
    alpha = alpha or 1.0

    local obj = {
        num_keys = num_keys,                -- Total number of unique keys in the dataset
        alpha = alpha,                      -- Zipf exponent: higher values = more skewed distribution
        log_uniform = alpha == 1.0          -- alpha = 1 is a removable singularity of q_inv below
    }
    setmetatable(obj, self)

    if not obj.log_uniform then
        -- Precompute constants for O(1) generation
        obj.q_inv = 1.0 / (1.0 - alpha)        -- Inverse of (1-alpha)
        obj.a_pow_q = 1.0 ^ (1.0 - alpha)      -- Pre-computed power of lower bound
        local b_pow_q = num_keys ^ (1.0 - alpha)
        obj.span = b_pow_q - obj.a_pow_q       -- b^(1-alpha) - a^(1-alpha)
    end

    return obj
end

-- Inverse CDF of a power law truncated to [1, num_keys], mapping uniform x in
-- [0, 1) to an index in [1, num_keys]:
-- t = (a^(1-alpha) + x * (b^(1-alpha) - a^(1-alpha)))^(1/(1-alpha))
--
-- At alpha = 1 the exponent 1/(1-alpha) is undefined; the limit of the above
-- as alpha -> 1 is the log-uniform inverse CDF t = num_keys^x.
function ZipfGenerator:generate_key_index(x)
    local t
    if self.log_uniform then
        t = self.num_keys ^ x
    else
        t = (self.a_pow_q + x * self.span) ^ self.q_inv
    end
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
