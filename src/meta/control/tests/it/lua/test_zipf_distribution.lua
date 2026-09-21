-- Zipf distribution unit test.
--
-- Runs under the metactl Lua test harness (tests/it/lua.rs): the `metactl`
-- namespace is preloaded, so it uses the registered `metactl.ZipfGenerator`
-- instead of `require`. Failures are raised via `assert`.

local ZipfGenerator = metactl.ZipfGenerator

local NUM_SAMPLES = 100000
local NUM_BUCKETS = 100
local NUM_KEYS = 1000*1000
local ALPHA = 1.2
local INDICES_PER_BUCKET = NUM_KEYS / NUM_BUCKETS
local RANDOM_SEED_1 = 12345
local DISPLAY_TOP_BUCKETS = 20
local DISPLAY_BOTTOM_START = 81
local TOP_ANALYSIS_BUCKETS = 10
local BOTTOM_ANALYSIS_START = 91
local MIN_TOP_PERCENT = 50
local MAX_BOTTOM_PERCENT = 25
local COMPARISON_BUCKET_10 = 10
local COMPARISON_BUCKET_50 = 50

print("zipf: distribution shape over " .. NUM_SAMPLES .. " samples of "
    .. NUM_KEYS .. " keys, alpha " .. ALPHA)

local function test_zipf_distribution()
    math.randomseed(RANDOM_SEED_1)

    local zipf = ZipfGenerator:new(NUM_KEYS, ALPHA)
    local buckets = {}

    for i = 1, NUM_BUCKETS do
        buckets[i] = 0
    end

    for i = 1, NUM_SAMPLES do
        local uniform_x = math.random()
        local zipf_index = zipf:generate_key_index(uniform_x)
        -- NaN fails every comparison, so this also rejects NaN indices.
        assert(zipf_index >= 1 and zipf_index <= NUM_KEYS,
            "index out of range [1, " .. NUM_KEYS .. "]: " .. tostring(zipf_index))

        local bucket = math.min(NUM_BUCKETS, math.max(1, math.ceil(zipf_index / INDICES_PER_BUCKET)))
        buckets[bucket] = buckets[bucket] + 1
    end

    print("Zipf Distribution Test Results")
    print("==============================")
    print("Samples: " .. NUM_SAMPLES)
    print("Buckets: " .. NUM_BUCKETS .. " (each covers " .. INDICES_PER_BUCKET .. " indices)")
    print("Alpha: " .. zipf.alpha)
    print()

    local expected_total = 0
    local actual_total = 0

    for i = 1, DISPLAY_TOP_BUCKETS do
        local count = buckets[i]
        actual_total = actual_total + count
        local percentage = (count / NUM_SAMPLES) * 100
        print(string.format("Bucket %2d (indices %3d-%3d): %5d samples (%.2f%%)",
            i, (i-1)*INDICES_PER_BUCKET+1, i*INDICES_PER_BUCKET, count, percentage))
    end

    print("...")

    for i = DISPLAY_BOTTOM_START, NUM_BUCKETS do
        local count = buckets[i]
        actual_total = actual_total + count
        local percentage = (count / NUM_SAMPLES) * 100
        print(string.format("Bucket %2d (indices %3d-%3d): %5d samples (%.2f%%)",
            i, (i-1)*INDICES_PER_BUCKET+1, i*INDICES_PER_BUCKET, count, percentage))
    end

    print()
    print("Distribution Analysis:")
    print("=====================")

    local top_10_total = 0
    for i = 1, TOP_ANALYSIS_BUCKETS do
        top_10_total = top_10_total + buckets[i]
    end
    local top_10_percent = (top_10_total / NUM_SAMPLES) * 100

    local bottom_10_total = 0
    for i = BOTTOM_ANALYSIS_START, NUM_BUCKETS do
        bottom_10_total = bottom_10_total + buckets[i]
    end
    local bottom_10_percent = (bottom_10_total / NUM_SAMPLES) * 100

    print(string.format("Top 10 buckets: %.2f%% of samples", top_10_percent))
    print(string.format("Bottom 10 buckets: %.2f%% of samples", bottom_10_percent))
    print(string.format("Ratio (top/bottom): %.2f", top_10_percent / bottom_10_percent))

    assert(top_10_percent > MIN_TOP_PERCENT, "Top " .. TOP_ANALYSIS_BUCKETS .. " buckets should contain >" .. MIN_TOP_PERCENT .. "% of samples for Zipf distribution")
    assert(bottom_10_percent < MAX_BOTTOM_PERCENT, "Bottom " .. TOP_ANALYSIS_BUCKETS .. " buckets should contain <" .. MAX_BOTTOM_PERCENT .. "% of samples for Zipf distribution")
    assert(buckets[1] > buckets[COMPARISON_BUCKET_10], "First bucket should have more samples than " .. COMPARISON_BUCKET_10 .. "th bucket")
    assert(buckets[COMPARISON_BUCKET_10] > buckets[COMPARISON_BUCKET_50], COMPARISON_BUCKET_10 .. "th bucket should have more samples than " .. COMPARISON_BUCKET_50 .. "th bucket")

    print()
    print("✓ All Zipf distribution tests passed!")
    return true
end

local function test_uniform_input_conversion()
    local RANDOM_SEED_2 = 54321
    local UNIFORM_NUM_KEYS = 100
    local UNIFORM_ALPHA = 1.5
    local UNIFORM_TEST_SAMPLES = 10000
    local UNIFORM_BUCKETS = 10
    local UNIFORM_INDICES_PER_BUCKET = UNIFORM_NUM_KEYS / UNIFORM_BUCKETS
    local UNIFORM_EXPECTED_PER_BUCKET = 1000
    local UNIFORM_VARIANCE_MULTIPLIER = 5
    local UNIFORM_COMPARISON_BUCKET = 5

    math.randomseed(RANDOM_SEED_2)

    local zipf = ZipfGenerator:new(UNIFORM_NUM_KEYS, UNIFORM_ALPHA)
    local uniform_buckets = {}
    local zipf_buckets = {}

    for i = 1, UNIFORM_BUCKETS do
        uniform_buckets[i] = 0
        zipf_buckets[i] = 0
    end

    for i = 1, UNIFORM_TEST_SAMPLES do
        local uniform_x = math.random()
        local uniform_bucket = math.ceil(uniform_x * UNIFORM_BUCKETS)
        uniform_buckets[uniform_bucket] = uniform_buckets[uniform_bucket] + 1

        local zipf_index = zipf:generate_key_index(uniform_x)
        -- NaN fails every comparison, so this also rejects NaN indices.
        assert(zipf_index >= 1 and zipf_index <= UNIFORM_NUM_KEYS,
            "index out of range [1, " .. UNIFORM_NUM_KEYS .. "]: " .. tostring(zipf_index))
        local zipf_bucket = math.min(UNIFORM_BUCKETS, math.ceil(zipf_index / UNIFORM_INDICES_PER_BUCKET))
        zipf_buckets[zipf_bucket] = zipf_buckets[zipf_bucket] + 1
    end

    print("Uniform vs Zipf Conversion Test")
    print("===============================")
    print("Uniform input distribution:")
    for i = 1, UNIFORM_BUCKETS do
        local percentage = (uniform_buckets[i] / UNIFORM_TEST_SAMPLES) * 100
        print(string.format("  Bucket %d: %.1f%%", i, percentage))
    end

    print("Zipf output distribution:")
    for i = 1, UNIFORM_BUCKETS do
        local percentage = (zipf_buckets[i] / UNIFORM_TEST_SAMPLES) * 100
        print(string.format("  Bucket %d: %.1f%%", i, percentage))
    end

    local uniform_variance = 0
    local zipf_variance = 0
    for i = 1, UNIFORM_BUCKETS do
        uniform_variance = uniform_variance + (uniform_buckets[i] - UNIFORM_EXPECTED_PER_BUCKET)^2
        zipf_variance = zipf_variance + (zipf_buckets[i] - UNIFORM_EXPECTED_PER_BUCKET)^2
    end

    print(string.format("Uniform variance: %.0f", uniform_variance))
    print(string.format("Zipf variance: %.0f", zipf_variance))

    assert(zipf_variance > uniform_variance * UNIFORM_VARIANCE_MULTIPLIER, "Zipf distribution should have much higher variance than uniform")
    assert(zipf_buckets[1] > zipf_buckets[UNIFORM_COMPARISON_BUCKET], "Early buckets should dominate in Zipf distribution")

    print("✓ Uniform to Zipf conversion test passed!")
    return true
end

local function test_default_parameters()
    local DEFAULT_NUM_KEYS = 1000
    local DEFAULT_ALPHA = 1.0
    local SAMPLE_COUNT = 20

    -- Both constructor parameters are optional; alpha defaults to exactly
    -- 1.0, the removable singularity of the power-law formula (division by
    -- 1 - alpha), so this also exercises the log-uniform fallback.
    local zipf = ZipfGenerator:new()
    assert(zipf.num_keys == DEFAULT_NUM_KEYS, "default num_keys should be " .. DEFAULT_NUM_KEYS)
    assert(zipf.alpha == DEFAULT_ALPHA, "default alpha should be " .. DEFAULT_ALPHA)

    local seen = {}
    for i = 1, SAMPLE_COUNT do
        local index = zipf:generate_key_index(i / SAMPLE_COUNT)
        assert(index >= 1 and index <= DEFAULT_NUM_KEYS,
            "index out of range [1, " .. DEFAULT_NUM_KEYS .. "]: " .. tostring(index))
        seen[index] = true
    end
    local distinct = 0
    for _ in pairs(seen) do
        distinct = distinct + 1
    end
    assert(distinct > 1, "alpha = 1.0 should not collapse every sample to the same index")

    print("✓ Default parameters test passed!")
    return true
end

-- Each function asserts its own invariants; a failing assert raises a Lua error
-- that the Rust harness reports as a test failure. Do not call os.exit here.
test_zipf_distribution()
test_uniform_input_conversion()
test_default_parameters()
