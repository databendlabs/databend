---
title: Databend Meta Metrics
sidebar_label: Meta Service Metrics
description: 
  Databend Meta Service Metrics
---

## Metrics

Metrics for real-time of monitoring and debugging of `metasrv`.

The simplest way to see the available metrics is to cURL the metrics HTTP API `HTTP_ADDRESS:HTTP_PORT/v1/metrics`, the API will returns a [Prometheus](http://prometheus.io/docs/instrumenting/exposition_formats/) format of metrics.

All the metrics is under `metasrv` prefix.

### Server

These metrics describe the status of the `metasrv`. All these metrics are prefixed with `metasrv_server_`.

| Name              | Description                                       | Type    |
| ----------------- | ------------------------------------------------- | ------- |
| current_leader_id | Current leader id of cluster, 0 means no leader.  | IntGauge   |
| is_leader         | Whether or not this node is current leader.       | Gauge   |
| node_is_health | Whether or not this node is health(in leader/follower state). | IntGauge |
| leader_changes    | Number of leader changes seen.                    | Counter |
| applying_snapshot | Whether or not statemachine is applying snapshot. | Gauge   |
| proposals_applied | Total number of consensus proposals applied.      | Gauge   |
| proposals_pending | Total number of pending proposals.                | Gauge   |
| proposals_failed  | Total number of failed proposals.                 | Counter |
| watchers          | Total number of active watchers.                  | Gauge   |

`current_leader_id` indicate current leader id of cluster, 0 means no leader. If a cluster has no leader, it is unavailable.

`is_leader` indicate if this `metasrv` currently is the leader of cluster, and `leader_changes` show the total number of leader changes since start.If change leader too frequently, it will impact the performance of `metasrv`, also it signal that the cluster is unstable.



If and only if the node state is `Follower` or `Leader` , `node_is_health` is 1, otherwise is 0.



`proposals_applied` records the total number of applied write requests.

`proposals_pending` indicates how many proposals are queued to commit currently.Rising pending proposals suggests there is a high client load or the member cannot commit proposals.

`proposals_failed` show the total number of failed write requests, it is normally related to two issues: temporary failures related to a leader election or longer downtime caused by a loss of quorum in the cluster.

`watchers` show the total number of active watchers currently.

### Raft Network

These metrics describe the network status of raft nodes in the `metasrv`. All these metrics are prefixed with `metasrv_raft_network_`.

| Name                    | Description                                       | Labels                            | Type          |
| ----------------------- | ------------------------------------------------- | --------------------------------- | ------------- |
| active_peers            | Current number of active connections to peers.    | id(node id),address(peer address) | GaugeVec      |
| fail_connect_to_peer    | Total number of fail connections to peers.        | id(node id),address(peer address) | CounterVec    |
| sent_bytes              | Total number of sent bytes to peers.              | to(node id)                       | CounterVec    |
| recv_bytes              | Total number of received bytes from peers.        | from(remote address)              | CounterVec    |
| sent_failures           | Total number of send failures to peers.           | to(node id)                       | CounterVec    |
| snapshot_send_success   | Total number of successful snapshot sends.        | to(node id)                       | IntCounterVec |
| snapshot_send_failures  | Total number of snapshot send failures.           | to(node id)                       | IntCounterVec |
| snapshot_send_inflights | Total number of inflight snapshot sends.          | to(node id)                       | IntGaugeVec   |
| snapshot_sent_seconds   | Total latency distributions of snapshot sends.    | to(node id)                       | HistogramVec  |
| snapshot_recv_success   | Total number of successful receive snapshot.      | from(remote address)              | IntCounterVec |
| snapshot_recv_failures  | Total number of snapshot receive failures.        | from(remote address)              | IntCounterVec |
| snapshot_recv_inflights | Total number of inflight snapshot receives.       | from(remote address)              | IntGaugeVec   |
| snapshot_recv_seconds   | Total latency distributions of snapshot receives. | from(remote address)              | HistogramVec  |

`active_peers` indicates how many active connection between cluster members, `fail_connect_to_peer` indicates the number of fail connections to peers. Each has the labels: id(node id) and address (peer address).

`sent_bytes` and `recv_bytes` record the sent and receive bytes to and from peers, and `sent_failures` records the number of fail sent to peers.

`snapshot_send_success` and `snapshot_send_failures` indicates the success and fail number of sent snapshot.`snapshot_send_inflights` indicate the inflight snapshot sends, each time send a snapshot, this field will increment by one, after sending snapshot is done, this field will decrement by one.

`snapshot_sent_seconds` indicate the total latency distributions of snapshot sends.

`snapshot_recv_success` and `snapshot_recv_failures` indicates the success and fail number of receive snapshot.`snapshot_recv_inflights` indicate the inflight receiving snapshot, each time receive a snapshot, this field will increment by one, after receiving snapshot is done, this field will decrement by one.

`snapshot_recv_seconds` indicate the total latency distributions of snapshot receives.

### Meta Network

These metrics describe the network status of meta service in the `metasrv`. All these metrics are prefixed with `metasrv_meta_network_`.



| Name             | Description                                            | Type       |
| ---------------- | ------------------------------------------------------ | ---------- |
| meta_sent_bytes  | Total number of sent bytes to meta grpc client.        | IntCounter |
| meta_recv_bytes  | Total number of recv bytes from meta grpc client.      | IntCounter |
| meta_inflights   | Total number of inflight meta grpc requests.           | IntGauge   |
| meta_req_success | Total number of success request from meta grpc client. | IntCounter |
| meta_req_failed  | Total number of fail request from meta grpc client.    | IntCounter |

