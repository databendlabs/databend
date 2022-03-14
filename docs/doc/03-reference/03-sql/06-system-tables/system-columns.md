---
title: system.columns
---

Contains information about columns of tables.

```sql
mysql> select * from  columns;
+---------------------+----------+--------------+---------------+-------------+
| name                | database | table        | data_type     | is_nullable |
+---------------------+----------+--------------+---------------+-------------+
| name                | system   | configs      | String        |           0 |
| value               | system   | configs      | String        |           0 |
| group               | system   | configs      | String        |           0 |
| description         | system   | configs      | String        |           0 |
| name                | system   | clusters     | String        |           0 |
| host                | system   | clusters     | String        |           0 |
| port                | system   | clusters     | UInt16        |           0 |
| database            | system   | tables       | String        |           0 |
| name                | system   | tables       | String        |           0 |
| engine              | system   | tables       | String        |           0 |
| created_on          | system   | tables       | String        |           0 |
| id                  | system   | processes    | String        |           0 |
| type                | system   | processes    | String        |           0 |
| host                | system   | processes    | String        |           1 |
| user                | system   | processes    | String        |           1 |
| state               | system   | processes    | String        |           0 |
| database            | system   | processes    | String        |           0 |
| extra_info          | system   | processes    | String        |           1 |
| memory_usage        | system   | processes    | Int64         |           1 |
| v                   | system   | tracing      | Int64         |           0 |
| name                | system   | tracing      | String        |           0 |
| msg                 | system   | tracing      | String        |           0 |
| level               | system   | tracing      | Int8          |           0 |
| hostname            | system   | tracing      | String        |           0 |
| pid                 | system   | tracing      | Int64         |           0 |
| time                | system   | tracing      | String        |           0 |
| name                | system   | credits      | String        |           0 |
| version             | system   | credits      | String        |           0 |
| license             | system   | credits      | String        |           0 |
| name                | system   | databases    | String        |           0 |
| log_type            | system   | query_log    | Int8          |           0 |
| handler_type        | system   | query_log    | String        |           0 |
| tenant_id           | system   | query_log    | String        |           0 |
| cluster_id          | system   | query_log    | String        |           0 |
| sql_user            | system   | query_log    | String        |           0 |
| sql_user_quota      | system   | query_log    | String        |           0 |
| sql_user_privileges | system   | query_log    | String        |           0 |
| query_id            | system   | query_log    | String        |           0 |
| query_kind          | system   | query_log    | String        |           0 |
| query_text          | system   | query_log    | String        |           0 |
| event_date          | system   | query_log    | Date32        |           0 |
| event_time          | system   | query_log    | DateTime64(3) |           0 |
| current_database    | system   | query_log    | String        |           0 |
| databases           | system   | query_log    | String        |           0 |
| tables              | system   | query_log    | String        |           0 |
| columns             | system   | query_log    | String        |           0 |
| projections         | system   | query_log    | String        |           0 |
| written_rows        | system   | query_log    | UInt64        |           0 |
| written_bytes       | system   | query_log    | UInt64        |           0 |
| read_rows           | system   | query_log    | UInt64        |           0 |
| read_bytes          | system   | query_log    | UInt64        |           0 |
| result_rows         | system   | query_log    | UInt64        |           0 |
| result_bytes        | system   | query_log    | UInt64        |           0 |
| cpu_usage           | system   | query_log    | UInt32        |           0 |
| memory_usage        | system   | query_log    | UInt64        |           0 |
| client_info         | system   | query_log    | String        |           0 |
| client_address      | system   | query_log    | String        |           0 |
| exception_code      | system   | query_log    | Int32         |           0 |
| exception_text      | system   | query_log    | String        |           0 |
| stack_trace         | system   | query_log    | String        |           0 |
| server_version      | system   | query_log    | String        |           0 |
| extra               | system   | query_log    | String        |           0 |
| name                | system   | functions    | String        |           0 |
| is_aggregate        | system   | functions    | Boolean       |           0 |
| name                | system   | columns      | String        |           0 |
| database            | system   | columns      | String        |           0 |
| table               | system   | columns      | String        |           0 |
| data_type           | system   | columns      | String        |           0 |
| is_nullable         | system   | columns      | Boolean       |           0 |
| name                | system   | settings     | String        |           0 |
| value               | system   | settings     | String        |           0 |
| default_value       | system   | settings     | String        |           0 |
| description         | system   | settings     | String        |           0 |
| name                | system   | contributors | String        |           0 |
| name                | system   | users        | String        |           0 |
| hostname            | system   | users        | String        |           0 |
| password            | system   | users        | String        |           1 |
| password_type       | system   | users        | UInt8         |           0 |
| metric              | system   | metrics      | String        |           0 |
| kind                | system   | metrics      | String        |           0 |
| labels              | system   | metrics      | String        |           0 |
| value               | system   | metrics      | String        |           0 |
| dummy               | system   | one          | UInt8         |           0 |
| a                   | default  | t1           | Int32         |           1 |
+---------------------+----------+--------------+---------------+-------------+
```