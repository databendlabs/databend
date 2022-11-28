---
title: Databend Query Metrics
sidebar_label: Databend Query Metrics
description:
  Databend Query Metrics
---

A `databend-query` server records metrics in table [system.metrics](../../13-sql-reference/70-system-tables/system-metrics.md).

The [metric_api_address](../07-query/10-query-config.md) to listen on that can be scraped by Prometheus and will return a [prometheus](http://prometheus.io/docs/instrumenting/exposition_formats/) format of metrics.

> Note: Default metric_api_address value is : 127.0.0.1:7070 , Prometheus can access this uri: http://127.0.0.1:7070/metrics

```sql
MySQL> DESC system.metrics;
+--------+---------+------+---------+-------+
| Field  | Type    | Null | Default | Extra |
+--------+---------+------+---------+-------+
| metric | VARCHAR | NO   |         |       |
| kind   | VARCHAR | NO   |         |       |
| labels | VARCHAR | NO   |         |       |
| value  | VARCHAR | NO   |         |       |
+--------+---------+------+---------+-------+
```

Metrics has four attributes: `metric`, `kind`, `labels`, `value`.

`metric` indicate the name of the metric contained in the databend-query.

`kind` records the kind of metric.
Available kinds:
* [`counter`](https://prometheus.io/docs/concepts/metric_types/#counter)
* [`gauge`](https://prometheus.io/docs/concepts/metric_types/#gauge)
* [`summary`](https://prometheus.io/docs/concepts/metric_types/#summary)
* `untyped` is part of `summary`

`labels` records the label of metric.

`value` records the value of metric.

## Metrics

:::info

Databend uses `summary` metrics to keep track of the total time spent handling various requests.
Value quantile contains: [0.0, 0.5, 0.9, 0.99, 0.999].

:::

| metric                               |  kind   | description                                                                 | labels                                                                          |
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
