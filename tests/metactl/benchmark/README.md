# Metactl Benchmark Tools

Performance benchmarking tools for Databend meta service operations.

## Files

- **`databend_meta_benchmark.lua`** - Concurrent benchmark script with configurable workers
- **`run_lua_benchmark.py`** - Python runner that sets up meta service and executes benchmark

## Usage

```bash
python run_lua_benchmark.py databend_meta_benchmark.lua
```

## Configuration

Edit constants in `databend_meta_benchmark.lua`:
- `UPSERT_WORKERS` - Number of concurrent upsert workers (default: 4)
- `GET_WORKERS` - Number of concurrent get workers (default: 4) 
- `OPERATIONS_PER_WORKER` - Operations per worker (default: 10,000)

## Output

Real-time progress reports and final statistics including:
- Operations per second for upsert/get operations
- Success rates and error counts
- Total throughput and benchmark duration