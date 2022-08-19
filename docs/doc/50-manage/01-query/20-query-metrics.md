---
title: Databend Query Metrics
sidebar_label: Databend Query Metrics
description:
  Databend Query Metrics
---

A `databend-query` server records metrics in table [system.metrics](../../30-reference/30-sql/70-system-tables/system-metrics.md).

The [metric_api_address](../../50-manage/01-query/10-query-config.md) to listen on that can be scraped by Prometheus and will return a [Prometheus](http://prometheus.io/docs/instrumenting/exposition_formats/) format of metrics.

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

`kind` records the kind of metric. Now contains [`counter`](https://prometheus.io/docs/concepts/metric_types/#counter), [`gauge`](https://prometheus.io/docs/concepts/metric_types/#gauge), [`summary`](https://prometheus.io/docs/concepts/metric_types/#summary) and [`untyped`](https://prometheus.io/docs/concepts/metric_types/#metric-types).

`labels` records the label of metric.

`value` records the value of metric. 

## Metrics

| metric                               | Description                                                                                                                                                                                        |  kind   |
|--------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-------:|
| session_close_numbers                | The number of sessions have been disconnected since the server was started. The kind is counter. The labels contain cluster_name and tenant.                                                       | counter |
| session_connect_numbers              | The number of sessions have been connected since the server was started. The kind and labels are same as session_close_numbers.                                                                    | counter |
| cluster_discovered_node_gauge        | The number of nodes discovered in the current cluster. The kind is gauge. The lables contain tenant_id, cluster_id, flight_address and local_id(a inner cluster unique id).                        |  gauge  |
| parser_parse_usedtime_sum            | The sum of sql parse used time. The kind is untyped.                                                                                                                                               | untyped |
| parser_parse_usedtime_count          | The count of sql parse used. The kind is same as parser_parse_usedtime_sum.                                                                                                                        | untyped |
| interpreter_usedtime_sum             | The sum of sql interpreter used time. The kind is untyped.                                                                                                                                         | untyped |
| interpreter_usedtime_count           | The count of sql interpreter used. The kind is same as interpreter_usedtime_sum.                                                                                                                   | untyped |
| mysql_process_request_duration_sum   | The sum of MySQL interactive process request used time. The kind is untyped.                                                                                                                       | untyped |
| mysql_process_request_duration_count | The count of MySQL interactive process request used. The kind is same as mysql_process_request_duration_sum.                                                                                       | untyped |
| interpreter_usedtime                 | Sql interpreter used time. The kind is summary that used to track the distribution of a set of interpreter_usedtime. Value quantile contains: [0.0, 0.5, 0.9, 0.99, 0.999].                        | summary |
| mysql_process_request_duration       | MySQL interactive process request used. The kind is summary that used to track the distribution of a set of mysql_process_request_duration. Value quantile contains: [0.0, 0.5, 0.9, 0.99, 0.999]. | summary |
| parser_parse_usedtime                | Sql parse used time. The kind is summary that used to track the distribution of a set of parser_parse_usedtime. Value quantile contains: [0.0, 0.5, 0.9, 0.99, 0.999].                             | summary |
