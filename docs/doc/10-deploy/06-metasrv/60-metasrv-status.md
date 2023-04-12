---
title: Meta Service HTTP APIs
sidebar_label: Meta Service HTTP APIs
description: 
  Meta Service HTTP APIs
---

In order to capture and keep track of various meta stats that are useful for your analysis, Databend provides a number of HTTP APIs.

:::note
Unless otherwise specified, these HTTP APIs use the port `28101` by default. To change the default port, edit the value of `admin_api_address` in the configuration file `databend-meta.toml`.
:::

## Cluster Node API

Returns all the meta nodes in the cluster.

### Request Endpoint

`http://<address>:<port>/v1/cluster/nodes`

### Response Example

```
[
  {
    name: "1",
    endpoint: { addr: "localhost", port: 28103 },
    grpc_api_addr: "0.0.0.0:9191",
  },
  {
    name: "2",
    endpoint: { addr: "localhost", port: 28104 },
    grpc_api_addr: "0.0.0.0:9192",
  },
];
```

## Cluster Status API

Returns the status information of each meta node in the cluster.

### Request Endpoint

`http://<address>:<port>/v1/cluster/status`

### Response Example

```json
{
  "id": 1,
  "endpoint": "localhost:28103",
  "db_size": 18899209,
  "state": "Follower",
  "is_leader": false,
  "current_term": 67,
  "last_log_index": 53067,
  "last_applied": { "term": 67, "index": 53067 },
  "leader": {
    "name": "2",
    "endpoint": { "addr": "localhost", "port": 28104 },
    "grpc_api_addr": "0.0.0.0:9192"
  },
  "voters": [
    {
      "name": "1",
      "endpoint": { "addr": "localhost", "port": 28103 },
      "grpc_api_addr": "0.0.0.0:9191"
    },
    {
      "name": "2",
      "endpoint": { "addr": "localhost", "port": 28104 },
      "grpc_api_addr": "0.0.0.0:9192"
    }
  ],
  "non_voters": [],
  "last_seq": 60665
}
```

## Meta Metrics API

Shows a bunch of metrics that Databend captures and tracks about the meta service performance. For more information about the meta service metrics, see [Databend Meta Metrics](50-metasrv-metrics.md).

### Request Endpoint

`http://<address>:<port>/v1/metrics`

### Response Example

```
# TYPE metasrv_meta_network_recv_bytes counter
metasrv_meta_network_recv_bytes 307163

# TYPE metasrv_server_leader_changes counter
metasrv_server_leader_changes 1

# TYPE metasrv_meta_network_req_success counter
metasrv_meta_network_req_success 3282

# TYPE metasrv_meta_network_sent_bytes counter
metasrv_meta_network_sent_bytes 1328402

# TYPE metasrv_server_node_is_health gauge
metasrv_server_node_is_health 1

# TYPE metasrv_server_is_leader gauge
metasrv_server_is_leader 1

# TYPE metasrv_server_proposals_applied gauge
metasrv_server_proposals_applied 810

# TYPE metasrv_server_last_seq gauge
metasrv_server_last_seq 753

# TYPE metasrv_server_current_term gauge
metasrv_server_current_term 1

# TYPE metasrv_meta_network_req_inflights gauge
metasrv_meta_network_req_inflights 0

# TYPE metasrv_server_proposals_pending gauge
metasrv_server_proposals_pending 0

# TYPE metasrv_server_last_log_index gauge
metasrv_server_last_log_index 810

# TYPE metasrv_server_current_leader_id gauge
metasrv_server_current_leader_id 1

# TYPE metasrv_meta_network_rpc_delay_seconds summary
metasrv_meta_network_rpc_delay_seconds{quantile="0"} 0.000227375
metasrv_meta_network_rpc_delay_seconds{quantile="0.5"} 0.0002439615242199244
metasrv_meta_network_rpc_delay_seconds{quantile="0.9"} 0.0002439615242199244
metasrv_meta_network_rpc_delay_seconds{quantile="0.95"} 0.0002439615242199244
metasrv_meta_network_rpc_delay_seconds{quantile="0.99"} 0.0002439615242199244
metasrv_meta_network_rpc_delay_seconds{quantile="0.999"} 0.0002439615242199244
metasrv_meta_network_rpc_delay_seconds{quantile="1"} 0.000563541
metasrv_meta_network_rpc_delay_seconds_sum 1.3146486719999995
metasrv_meta_network_rpc_delay_seconds_count 3283
```

## Snapshot Trigger API

For debugging use only. Forces raft to create and sync a snapshot to all nodes.

### Request Endpoint

`http://<address>:<port>/v1/ctrl/trigger_snapshot`

### Response Example

None.

## CPU and Memory Profiling APIs

Enables you to visualize performance data of your CPU and memory with [FlameGraph](https://github.com/brendangregg/FlameGraph). For more information, see [How to Profile Databend](../../90-contributing/07-how-to-profiling.md).