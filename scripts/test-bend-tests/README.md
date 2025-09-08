# Databend Test Cluster

Test setup for running a Databend cluster with 3 meta nodes and 1 query node.

## Structure

```
test-bend-tests/
├── configs/
│   ├── meta-node1.toml    # Meta node 1 (id=1, ports: grpc=9191, admin=29001, raft=27001)
│   ├── meta-node2.toml    # Meta node 2 (id=2, ports: grpc=9192, admin=29002, raft=27002)
│   ├── meta-node3.toml    # Meta node 3 (id=3, ports: grpc=9193, admin=29003, raft=27003)
│   └── query-node1.toml   # Query node 1 (HTTP=8000, MySQL=3307)
├── test_cluster.py        # Main test script
└── README.md             # This file
```

## Prerequisites

**IMPORTANT**: You must install the databend_test_helper library before running this test:

```bash
cd ../databend_test_helper
pip install -e .
```

## Usage

1. **Build Databend binaries (debug):**
   ```bash
   cd ../..  # Go to project root
   make build
   ```

2. **Run the test cluster:**
   ```bash
   cd scripts/test-bend-tests
   python test_cluster.py
   ```

If you get `ModuleNotFoundError: No module named 'databend_test_helper'`, you forgot step 1 in Prerequisites.

## What it does

1. **Starts meta nodes in sequence** (node1 → node2 → node3)
2. **Starts query node** (connects to all 3 meta nodes)
3. **Shows cluster status** and connection info
4. **Monitors health** - exits if any process dies
5. **Graceful shutdown** on Ctrl+C

## Connection info

Once running, you can connect to:
- **HTTP API**: `http://127.0.0.1:8000`
- **MySQL**: `mysql://root@127.0.0.1:3307` (no password)

## Logs

All logs are written to `_databend_data/logs/`:
- `meta1/`, `meta2/`, `meta3/` - Meta node logs
- `query1/` - Query node logs

## Data

All data is stored in `_databend_data/`:
- `meta1/`, `meta2/`, `meta3/` - Raft data
- `query1/` - Query data
- `cache/query1/` - Query cache
- `spill/query1/` - Query spill files


## Run

Bring up cluster:

```
python test_cluster.py
```

Run test:

```
python concurrent_sql_test.py --threads 10 -i 1000 --servers localhost:8000,localhost:8001,localhost:8002,localhost:8003,localhost:8004 --query='SELECT count(*) FROM yy a JOIN yy b ON a.number < b.number WHERE a.number * b.number < 1000000 and a.number < 10000;'

# run a single test:

bendsql  --user ana --password '123'  --query='SELECT count(*) FROM yy a JOIN yy b ON a.number < b.number WHERE a.number * b.number < 1000000 and a.number < 10000;'
```


Check pending status:
```
bendsql  --user ana --password '123'  --query='show processlist;'
# or
bendsql  --user ana --password '123'  --query='show processlist;' | grep '\[.*' -o | sort
```
