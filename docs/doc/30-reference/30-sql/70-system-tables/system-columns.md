---
title: system.columns
---

Contains information about columns of tables.

```sql
DESC system.columns;
+--------------------+---------+------+---------+-------+
| Field              | Type    | Null | Default | Extra |
+--------------------+---------+------+---------+-------+
| name               | VARCHAR | NO   |         |       |
| database           | VARCHAR | NO   |         |       |
| table              | VARCHAR | NO   |         |       |
| type               | VARCHAR | NO   |         |       |
| default_kind       | VARCHAR | NO   |         |       |
| default_expression | VARCHAR | NO   |         |       |
| is_nullable        | BOOLEAN | NO   | false   |       |
| comment            | VARCHAR | NO   |         |       |

```

```sql
SELECT * FROM system.columns WHERE database='system';
+--------------------------+----------+--------------+-------------------+--------------+--------------------+-------------+---------+
| name                     | database | table        | type              | default_kind | default_expression | is_nullable | comment |
+--------------------------+----------+--------------+-------------------+--------------+--------------------+-------------+---------+
| dummy                    | system   | one          | TINYINT UNSIGNED  |              |                    |           0 |         |
| name                     | system   | contributors | VARCHAR           |              |                    |           0 |         |
| id                       | system   | processes    | VARCHAR           |              |                    |           0 |         |
| type                     | system   | processes    | VARCHAR           |              |                    |           0 |         |
| host                     | system   | processes    | VARCHAR           |              |                    |           1 |         |
| user                     | system   | processes    | VARCHAR           |              |                    |           1 |         |
| state                    | system   | processes    | VARCHAR           |              |                    |           0 |         |
| database                 | system   | processes    | VARCHAR           |              |                    |           0 |         |
| extra_info               | system   | processes    | VARCHAR           |              |                    |           1 |         |
| memory_usage             | system   | processes    | BIGINT            |              |                    |           0 |         |
| dal_metrics_read_bytes   | system   | processes    | BIGINT UNSIGNED   |              |                    |           1 |         |
| dal_metrics_write_bytes  | system   | processes    | BIGINT UNSIGNED   |              |                    |           1 |         |
| scan_progress_read_rows  | system   | processes    | BIGINT UNSIGNED   |              |                    |           1 |         |
| scan_progress_read_bytes | system   | processes    | BIGINT UNSIGNED   |              |                    |           1 |         |
| mysql_connection_id      | system   | processes    | INT UNSIGNED      |              |                    |           1 |         |
| name                     | system   | credits      | VARCHAR           |              |                    |           0 |         |
| version                  | system   | credits      | VARCHAR           |              |                    |           0 |         |
| license                  | system   | credits      | VARCHAR           |              |                    |           0 |         |
| group                    | system   | configs      | VARCHAR           |              |                    |           0 |         |
| name                     | system   | configs      | VARCHAR           |              |                    |           0 |         |
| value                    | system   | configs      | VARCHAR           |              |                    |           0 |         |
| description              | system   | configs      | VARCHAR           |              |                    |           0 |         |
| log_type                 | system   | query_log    | TINYINT           |              |                    |           0 |         |
| handler_type             | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| tenant_id                | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| cluster_id               | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| sql_user                 | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| sql_user_quota           | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| sql_user_privileges      | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| query_id                 | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| query_kind               | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| query_text               | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| event_date               | system   | query_log    | DATE              |              |                    |           0 |         |
| event_time               | system   | query_log    | TIMESTAMP(3)      |              |                    |           0 |         |
| current_database         | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| databases                | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| tables                   | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| columns                  | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| projections              | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| written_rows             | system   | query_log    | BIGINT UNSIGNED   |              |                    |           0 |         |
| written_bytes            | system   | query_log    | BIGINT UNSIGNED   |              |                    |           0 |         |
| written_io_bytes         | system   | query_log    | BIGINT UNSIGNED   |              |                    |           0 |         |
| written_io_bytes_cost_ms | system   | query_log    | BIGINT UNSIGNED   |              |                    |           0 |         |
| scan_rows                | system   | query_log    | BIGINT UNSIGNED   |              |                    |           0 |         |
| scan_bytes               | system   | query_log    | BIGINT UNSIGNED   |              |                    |           0 |         |
| scan_io_bytes            | system   | query_log    | BIGINT UNSIGNED   |              |                    |           0 |         |
| scan_io_bytes_cost_ms    | system   | query_log    | BIGINT UNSIGNED   |              |                    |           0 |         |
| scan_partitions          | system   | query_log    | BIGINT UNSIGNED   |              |                    |           0 |         |
| total_partitions         | system   | query_log    | BIGINT UNSIGNED   |              |                    |           0 |         |
| result_rows              | system   | query_log    | BIGINT UNSIGNED   |              |                    |           0 |         |
| result_bytes             | system   | query_log    | BIGINT UNSIGNED   |              |                    |           0 |         |
| cpu_usage                | system   | query_log    | INT UNSIGNED      |              |                    |           0 |         |
| memory_usage             | system   | query_log    | BIGINT UNSIGNED   |              |                    |           0 |         |
| client_info              | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| client_address           | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| exception_code           | system   | query_log    | INT               |              |                    |           0 |         |
| exception_text           | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| stack_trace              | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| server_version           | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| session_settings         | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| extra                    | system   | query_log    | VARCHAR           |              |                    |           0 |         |
| name                     | system   | columns      | VARCHAR           |              |                    |           0 |         |
| database                 | system   | columns      | VARCHAR           |              |                    |           0 |         |
| table                    | system   | columns      | VARCHAR           |              |                    |           0 |         |
| type                     | system   | columns      | VARCHAR           |              |                    |           0 |         |
| default_kind             | system   | columns      | VARCHAR           |              |                    |           0 |         |
| default_expression       | system   | columns      | VARCHAR           |              |                    |           0 |         |
| is_nullable              | system   | columns      | BOOLEAN           |              |                    |           0 |         |
| comment                  | system   | columns      | VARCHAR           |              |                    |           0 |         |
| Engine                   | system   | engines      | VARCHAR           |              |                    |           0 |         |
| Comment                  | system   | engines      | VARCHAR           |              |                    |           0 |         |
| name                     | system   | databases    | VARCHAR           |              |                    |           0 |         |
| name                     | system   | clusters     | VARCHAR           |              |                    |           0 |         |
| host                     | system   | clusters     | VARCHAR           |              |                    |           0 |         |
| port                     | system   | clusters     | SMALLINT UNSIGNED |              |                    |           0 |         |
| metric                   | system   | metrics      | VARCHAR           |              |                    |           0 |         |
| kind                     | system   | metrics      | VARCHAR           |              |                    |           0 |         |
| labels                   | system   | metrics      | VARCHAR           |              |                    |           0 |         |
| value                    | system   | metrics      | VARCHAR           |              |                    |           0 |         |
| name                     | system   | settings     | VARCHAR           |              |                    |           0 |         |
| value                    | system   | settings     | VARCHAR           |              |                    |           0 |         |
| default                  | system   | settings     | VARCHAR           |              |                    |           0 |         |
| level                    | system   | settings     | VARCHAR           |              |                    |           0 |         |
| description              | system   | settings     | VARCHAR           |              |                    |           0 |         |
| type                     | system   | settings     | VARCHAR           |              |                    |           0 |         |
| database                 | system   | tables       | VARCHAR           |              |                    |           0 |         |
| name                     | system   | tables       | VARCHAR           |              |                    |           0 |         |
| engine                   | system   | tables       | VARCHAR           |              |                    |           0 |         |
| created_on               | system   | tables       | VARCHAR           |              |                    |           0 |         |
| dropped_on               | system   | tables       | VARCHAR           |              |                    |           0 |         |
| num_rows                 | system   | tables       | BIGINT UNSIGNED   |              |                    |           1 |         |
| data_size                | system   | tables       | BIGINT UNSIGNED   |              |                    |           1 |         |
| data_compressed_size     | system   | tables       | BIGINT UNSIGNED   |              |                    |           1 |         |
| index_size               | system   | tables       | BIGINT UNSIGNED   |              |                    |           1 |         |
| v                        | system   | tracing      | BIGINT            |              |                    |           0 |         |
| name                     | system   | tracing      | VARCHAR           |              |                    |           0 |         |
| msg                      | system   | tracing      | VARCHAR           |              |                    |           0 |         |
| level                    | system   | tracing      | TINYINT           |              |                    |           0 |         |
| hostname                 | system   | tracing      | VARCHAR           |              |                    |           0 |         |
| pid                      | system   | tracing      | BIGINT            |              |                    |           0 |         |
| time                     | system   | tracing      | VARCHAR           |              |                    |           0 |         |
| name                     | system   | functions    | VARCHAR           |              |                    |           0 |         |
| is_builtin               | system   | functions    | BOOLEAN           |              |                    |           0 |         |
| is_aggregate             | system   | functions    | BOOLEAN           |              |                    |           0 |         |
| definition               | system   | functions    | VARCHAR           |              |                    |           0 |         |
| category                 | system   | functions    | VARCHAR           |              |                    |           0 |         |
| description              | system   | functions    | VARCHAR           |              |                    |           0 |         |
| syntax                   | system   | functions    | VARCHAR           |              |                    |           0 |         |
| example                  | system   | functions    | VARCHAR           |              |                    |           0 |         |
| name                     | system   | users        | VARCHAR           |              |                    |           0 |         |
| hostname                 | system   | users        | VARCHAR           |              |                    |           0 |         |
| auth_type                | system   | users        | VARCHAR           |              |                    |           0 |         |
| auth_string              | system   | users        | VARCHAR           |              |                    |           0 |         |
| name                     | system   | roles        | VARCHAR           |              |                    |           0 |         |
| inherited_roles          | system   | roles        | BIGINT UNSIGNED   |              |                    |           0 |         |
| name                     | system   | stages       | VARCHAR           |              |                    |           0 |         |
| stage_type               | system   | stages       | VARCHAR           |              |                    |           0 |         |
| stage_params             | system   | stages       | VARCHAR           |              |                    |           0 |         |
| copy_options             | system   | stages       | VARCHAR           |              |                    |           0 |         |
| file_format_options      | system   | stages       | VARCHAR           |              |                    |           0 |         |
| number_of_files          | system   | stages       | BIGINT UNSIGNED   |              |                    |           1 |         |
| creator                  | system   | stages       | VARCHAR           |              |                    |           1 |         |
| comment                  | system   | stages       | VARCHAR           |              |                    |           0 |         |
+--------------------------+----------+--------------+-------------------+--------------+--------------------+-------------+---------+

```