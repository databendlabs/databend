local GRPC_ADDR = "127.0.0.1:9191"
local UPSERT_WORKERS = 64
local GET_WORKERS = 4
local OPERATIONS_PER_WORKER = 10000
local KEY_PREFIX = "bench_key_"
local VALUE_PREFIX = "bench_value_"
local PROGRESS_INTERVAL = 2.0

local stats = {
    upsert_ops = 0,
    get_ops = 0,
    upsert_errors = 0,
    get_errors = 0,
    start_time = nil,
    end_time = nil
}

local function generate_key(worker_id, op_id)
    return KEY_PREFIX .. worker_id .. "_" .. op_id
end

local function generate_value(worker_id, op_id)
    return VALUE_PREFIX .. worker_id .. "_" .. op_id .. "_data_" .. string.rep("x", 32)
end

local function atomic_inc(field)
    stats[field] = stats[field] + 1
end

local function upsert_worker(worker_id)
    local client = metactl.new_grpc_client(GRPC_ADDR)

    for i = 1, OPERATIONS_PER_WORKER do
        local key = generate_key(worker_id, i)
        local value = generate_value(worker_id, i)

        local result, err = client:upsert(key, value)
        if err then
            atomic_inc("upsert_errors")
        else
            atomic_inc("upsert_ops")
        end

        metactl.sleep(0.001)
    end

    print("Upsert worker " .. worker_id .. " completed")
end

local function get_worker(worker_id)
    local client = metactl.new_grpc_client(GRPC_ADDR)

    metactl.sleep(0.1)

    for i = 1, OPERATIONS_PER_WORKER do
        local target_worker = (worker_id + i) % UPSERT_WORKERS + 1
        local target_op = (i % OPERATIONS_PER_WORKER) + 1
        local key = generate_key(target_worker, target_op)

        local result, err = client:get(key)
        if err then
            atomic_inc("get_errors")
        else
            atomic_inc("get_ops")
        end

        metactl.sleep(0.001)
    end

    print("Get worker " .. worker_id .. " completed")
end

local function print_config()
    print("=== Databend Meta Benchmark Configuration ===")
    print("gRPC Address: " .. GRPC_ADDR)
    print("Upsert Workers: " .. UPSERT_WORKERS)
    print("Get Workers: " .. GET_WORKERS)
    print("Operations per Worker: " .. OPERATIONS_PER_WORKER)
    print("Total Upsert Operations: " .. (UPSERT_WORKERS * OPERATIONS_PER_WORKER))
    print("Total Get Operations: " .. (GET_WORKERS * OPERATIONS_PER_WORKER))
    print("============================================")
    print("")
end

local function print_results()
    local elapsed = stats.end_time - stats.start_time
    local total_ops = stats.upsert_ops + stats.get_ops
    local total_errors = stats.upsert_errors + stats.get_errors

    print("")
    print("=== Benchmark Results ===")
    print("Duration: " .. string.format("%.2f", elapsed) .. " seconds")
    print("")
    print("Upsert Operations:")
    print("  Successful: " .. stats.upsert_ops)
    print("  Errors: " .. stats.upsert_errors)
    print("  Rate: " .. string.format("%.1f", stats.upsert_ops / elapsed) .. " ops/sec")
    print("")
    print("Get Operations:")
    print("  Successful: " .. stats.get_ops)
    print("  Errors: " .. stats.get_errors)
    print("  Rate: " .. string.format("%.1f", stats.get_ops / elapsed) .. " ops/sec")
    print("")
    print("Total Operations:")
    print("  Successful: " .. total_ops)
    print("  Errors: " .. total_errors)
    print("  Overall Rate: " .. string.format("%.1f", total_ops / elapsed) .. " ops/sec")
    print("  Success Rate: " .. string.format("%.2f", (total_ops / (total_ops + total_errors)) * 100) .. "%")
    print("========================")
end

local function progress_reporter()
    local last_upsert_ops = 0
    local last_get_ops = 0

    while true do
        metactl.sleep(PROGRESS_INTERVAL)
        local current_upsert_ops = stats.upsert_ops
        local current_get_ops = stats.get_ops
        local current_upsert_errors = stats.upsert_errors
        local current_get_errors = stats.get_errors

        local upsert_qps = (current_upsert_ops - last_upsert_ops) / PROGRESS_INTERVAL
        local get_qps = (current_get_ops - last_get_ops) / PROGRESS_INTERVAL

        print(string.format("Progress: Upsert %d ops (%d err, %.1f qps), Get %d ops (%d err, %.1f qps)",
              current_upsert_ops, current_upsert_errors, upsert_qps,
              current_get_ops, current_get_errors, get_qps))

        last_upsert_ops = current_upsert_ops
        last_get_ops = current_get_ops
    end
end

local function run_benchmark()
    print_config()

    stats.start_time = os.clock()

    local progress_task = metactl.spawn(progress_reporter)

    local upsert_tasks = {}
    for i = 1, UPSERT_WORKERS do
        upsert_tasks[i] = metactl.spawn(function() upsert_worker(i) end)
    end

    local get_tasks = {}
    for i = 1, GET_WORKERS do
        get_tasks[i] = metactl.spawn(function() get_worker(i) end)
    end

    print("All workers started, waiting for completion...")

    for i = 1, UPSERT_WORKERS do
        upsert_tasks[i]:join()
    end
    print("All upsert workers completed")

    for i = 1, GET_WORKERS do
        get_tasks[i]:join()
    end
    print("All get workers completed")

    stats.end_time = os.clock()

    print_results()
end

print("Starting Databend Meta Benchmark...")
run_benchmark()
print("Benchmark completed!")
