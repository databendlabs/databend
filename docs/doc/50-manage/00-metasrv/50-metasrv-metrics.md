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
| has_leader        | Whether or not a leader exists.                   | Gauge   |
| is_leader         | Whether or not this node is current leader.       | Gauge   |
| leader_changes    | Number of leader changes seen.                    | Counter |
| applying_snapshot | Whether or not statemachine is applying snapshot. | Gauge   |
| proposals_applied | Total number of consensus proposals applied.      | Gauge   |
| proposals_pending | Total number of pending proposals.                | Gauge   |
| proposals_failed  | Total number of failed proposals.                 | Counter |
| watchers          | Total number of active watchers.                  | Gauge   |

`has_leader` indicate if there is a leader in the cluster, if a member in the cluster has no leader, it is unavailable.

`is_leader` indicate if this `metasrv` currently is the leader of cluster, and `leader_changes` show the total number of leader changes since start.If change leader too frequently, it will impact the performance of `metasrv`, also it signal that the cluster is unstable.

`proposals_applied` records the total number of applied write requests.

`proposals_pending` indicates how many proposals are queued to commit currently.Rising pending proposals suggests there is a high client load or the member cannot commit proposals.

`proposals_failed` show the total number of failed write requests, it is normally related to two issues: temporary failures related to a leader election or longer downtime caused by a loss of quorum in the cluster.

`watchers` show the total number of active watchers currently.
