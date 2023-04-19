---
title: Databend Metrics
sidebar_label: Databend Metrics
description: 
  Meta and Query Service Metrics
---

Metrics are crucial to monitor the performance and health of the system. Databend collects and stores two types of metrics, Meta Metrics and Query Metrics, in the format of [Prometheus](http://prometheus.io/docs/instrumenting/exposition_formats/). Meta Metrics are used for real-time monitoring and debugging of the Metasrv component, while Query Metrics are used for monitoring the performance of the Databend-query component.

You can access the metrics through a web browser using the following URLs:

- Meta Metrics: `http://<admin_api_address>/v1/metrics`. Defaults to `0.0.0.0:28101/v1/metrics`.
- Query Metrics: `http://<metric_api_address>/metrics`. Defaults to `0.0.0.0:7070/metrics`.

:::tip
Alternatively, you can visualize the metrics with a third-party tool. For supported tools and integration tutorials, see **Monitor** > **Using 3rd-party Tools**.
:::
## Meta Metrics

Here's a list of Meta metrics captured by Databend.

### Server

These metrics describe the status of the `metasrv`. All these metrics are prefixed with `metasrv_server_`.

| Name              | Description                                       | Type    |
|-------------------|---------------------------------------------------|---------|
| current_leader_id | Current leader id of cluster, 0 means no leader.  | Gauge   |
| is_leader         | Whether or not this node is current leader.       | Gauge   |
| node_is_health    | Whether or not this node is health.               | Gauge   |
| leader_changes    | Number of leader changes seen.                    | Counter |
| applying_snapshot | Whether or not statemachine is applying snapshot. | Gauge   |
| proposals_applied | Total number of consensus proposals applied.      | Gauge   |
| last_log_index    | Index of the last log entry..                     | Gauge   |
| current_term      | Current term.                                     | Gauge   |
| proposals_pending | Total number of pending proposals.                | Gauge   |
| proposals_failed  | Total number of failed proposals.                 | Counter |
| watchers          | Total number of active watchers.                  | Gauge   |

`current_leader_id` indicate current leader id of cluster, 0 means no leader. If a cluster has no leader, it is unavailable.

`is_leader` indicate if this `metasrv` currently is the leader of cluster, and `leader_changes` show the total number of leader changes since start.If change leader too frequently, it will impact the performance of `metasrv`, also it signal that the cluster is unstable.

If and only if the node state is `Follower` or `Leader` , `node_is_health` is 1, otherwise is 0.

`proposals_applied` records the total number of applied write requests.

`last_log_index` records the last log index has been appended to this Raft node's log, `current_term` records the current term of the Raft node.

`proposals_pending` indicates how many proposals are queued to commit currently.Rising pending proposals suggests there is a high client load or the member cannot commit proposals.

`proposals_failed` show the total number of failed write requests, it is normally related to two issues: temporary failures related to a leader election or longer downtime caused by a loss of quorum in the cluster.

`watchers` show the total number of active watchers currently.

### Raft Network

These metrics describe the network status of raft nodes in the `metasrv`. All these metrics are prefixed with `metasrv_raft_network_`.

| Name                    | Description                                       | Labels                            | Type      |
|-------------------------|---------------------------------------------------|-----------------------------------|-----------|
| active_peers            | Current number of active connections to peers.    | id(node id),address(peer address) | Gauge     |
| fail_connect_to_peer    | Total number of fail connections to peers.        | id(node id),address(peer address) | Counter   |
| sent_bytes              | Total number of sent bytes to peers.              | to(node id)                       | Counter   |
| recv_bytes              | Total number of received bytes from peers.        | from(remote address)              | Counter   |
| sent_failures           | Total number of send failures to peers.           | to(node id)                       | Counter   |
| snapshot_send_success   | Total number of successful snapshot sends.        | to(node id)                       | Counter   |
| snapshot_send_failures  | Total number of snapshot send failures.           | to(node id)                       | Counter   |
| snapshot_send_inflights | Total number of inflight snapshot sends.          | to(node id)                       | Gauge     |
| snapshot_sent_seconds   | Total latency distributions of snapshot sends.    | to(node id)                       | Histogram |
| snapshot_recv_success   | Total number of successful receive snapshot.      | from(remote address)              | Counter   |
| snapshot_recv_failures  | Total number of snapshot receive failures.        | from(remote address)              | Counter   |
| snapshot_recv_inflights | Total number of inflight snapshot receives.       | from(remote address)              | Gauge     |
| snapshot_recv_seconds   | Total latency distributions of snapshot receives. | from(remote address)              | Histogram |

`active_peers` indicates how many active connection between cluster members, `fail_connect_to_peer` indicates the number of fail connections to peers. Each has the labels: id(node id) and address (peer address).

`sent_bytes` and `recv_bytes` record the sent and receive bytes to and from peers, and `sent_failures` records the number of fail sent to peers.

`snapshot_send_success` and `snapshot_send_failures` indicates the success and fail number of sent snapshot.`snapshot_send_inflights` indicate the inflight snapshot sends, each time send a snapshot, this field will increment by one, after sending snapshot is done, this field will decrement by one.

`snapshot_sent_seconds` indicate the total latency distributions of snapshot sends.

`snapshot_recv_success` and `snapshot_recv_failures` indicates the success and fail number of receive snapshot.`snapshot_recv_inflights` indicate the inflight receiving snapshot, each time receive a snapshot, this field will increment by one, after receiving snapshot is done, this field will decrement by one.

`snapshot_recv_seconds` indicate the total latency distributions of snapshot receives.

### Raft Storage

These metrics describe the storage status of raft nodes in the `metasrv`. All these metrics are prefixed with `metasrv_raft_storage_`.

| Name                    | Description                                | Labels              | Type    |
|-------------------------|--------------------------------------------|---------------------|---------|
| raft_store_write_failed | Total number of raft store write failures. | func(function name) | Counter |
| raft_store_read_failed  | Total number of raft store read failures.  | func(function name) | Counter |

`raft_store_write_failed` and `raft_store_read_failed` indicate the total number of raft store write and read failures.

### Meta Network

These metrics describe the network status of meta service in the `metasrv`. All these metrics are prefixed with `metasrv_meta_network_`.

| Name              | Description                                            | Type      |
|-------------------|--------------------------------------------------------|-----------|
| sent_bytes        | Total number of sent bytes to meta grpc client.        | Counter   |
| recv_bytes        | Total number of recv bytes from meta grpc client.      | Counter   |
| inflights         | Total number of inflight meta grpc requests.           | Gauge     |
| req_success       | Total number of success request from meta grpc client. | Counter   |
| req_failed        | Total number of fail request from meta grpc client.    | Counter   |
| rpc_delay_seconds | Latency distribution of meta-service API in second.    | Histogram |

## Query Metrics

Here's a list of Query metrics captured by Databend.

| Name                               |  Type   | Description                                                                 | Labels                                                                          |
|--------------------------------------|---------|-----------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| cluster_discovered_node_gauge        | gauge   | The number of nodes discovered in the current cluster.                      | tenant_id, cluster_id, flight_address and local_id(a inner cluster unique id)   |
| interpreter_usedtime                 | summary | Sql interpreter used time.                                                  |                                                                                 |
| meta_grpc_client_request_duration_ms | summary | The time used for requesting the remote meta service.                       | endpoint, request                                                               |
| meta_grpc_client_request_inflight    | gauge   | The currently on going request to remote meta service.                      |                                                                                 |
| meta_grpc_client_request_success     | counter | The total amount for successful request to remote meta service.             |                                                                                 |
| mysql_process_request_duration       | summary | MySQL interactive process request used.                                     |                                                                                 |
| opendal_bytes_total                  | counter | The total data size opendal handled in byte.                                | operation, service                                                              |
| opendal_errors_total                 | counter | The total error count of opendal operations.                                | operation, service                                                              |
| opendal_failures_total               | counter | The total failure count of opendal operations.                              | operation, service                                                              |
| opendal_requests_duration_seconds    | summary | The time used by opendal to request remote storage backend.                 | operation, service                                                              |
| opendal_requests_total               | counter | The total count of opendal operations.                                      | operation, service                                                              |
| query_duration_ms                    | summary | The time used by each single query.                                         | tenant, cluster, handler, kind                                                  |
| query_result_bytes                   | counter | The total returned data size of query result in byte.                       | tenant, cluster, handler, kind                                                  |
| query_result_rows                    | counter | The total returned data rows of query result.                               | tenant, cluster, handler, kind                                                  |
| query_scan_bytes                     | counter | The total scanned data size by query in byte.                               | tenant, cluster, handler, kind                                                  |
| query_scan_io_bytes                  | counter | The total scanned transferred data size by query in byte.                   | tenant, cluster, handler, kind                                                  |
| query_scan_partitions                | counter | The total scanned partitions by query.                                      | tenant, cluster, handler, kind                                                  |
| query_scan_rows                      | counter | The total scanned data rows by query.                                       | tenant, cluster, handler, kind                                                  |
| query_start                          | counter | The total count of query started.                                           | tenant, cluster, handler, kind                                                  |
| query_success                        | counter | The total count of query succeeded.                                         | tenant, cluster, handler, kind                                                  |
| query_total_partitions               | counter | The total partitions for query.                                             | tenant, cluster, handler, kind                                                  |
| query_write_bytes                    | counter | The total data size written by query in byte.                               | tenant, cluster, handler, kind                                                  |
| query_write_io_bytes                 | counter | The total data size written and transferred by query in byte.               | tenant, cluster, handler, kind                                                  |
| query_write_rows                     | counter | The total data rows written by query.                                       | tenant, cluster, handler, kind                                                  |
| session_close_numbers                | counter | The number of sessions have been disconnected since the server was started. | tenant, cluster_name                                                            |
| session_connect_numbers              | counter | The number of sessions have been connected since the server was started.    | tenant, cluster_name                                                            |