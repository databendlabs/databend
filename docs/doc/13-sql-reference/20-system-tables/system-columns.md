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
+--------------------------+----------+---------------------+------------------+-------------------+--------------+--------------------+-------------+---------+
| name                     | database | table               | column_type      | data_type         | default_kind | default_expression | is_nullable | comment |
+--------------------------+----------+---------------------+------------------+-------------------+--------------+--------------------+-------------+---------+
| id                       | system   | processes           | String           | VARCHAR           |              |                    | NO          |         |
| type                     | system   | processes           | String           | VARCHAR           |              |                    | NO          |         |
| host                     | system   | processes           | Nullable(String) | VARCHAR           |              |                    | YES         |         |
| user                     | system   | processes           | String           | VARCHAR           |              |                    | NO          |         |
| command                  | system   | processes           | String           | VARCHAR           |              |                    | NO          |         |
| database                 | system   | processes           | String           | VARCHAR           |              |                    | NO          |         |
| extra_info               | system   | processes           | String           | VARCHAR           |              |                    | NO          |         |
| memory_usage             | system   | processes           | Int64            | BIGINT            |              |                    | NO          |         |
| data_read_bytes          | system   | processes           | UInt64           | BIGINT UNSIGNED   |              |                    | NO          |         |
| data_write_bytes         | system   | processes           | UInt64           | BIGINT UNSIGNED   |              |                    | NO          |         |
| scan_progress_read_rows  | system   | processes           | UInt64           | BIGINT UNSIGNED   |              |                    | NO          |         |
| scan_progress_read_bytes | system   | processes           | UInt64           | BIGINT UNSIGNED   |              |                    | NO          |         |
| mysql_connection_id      | system   | processes           | Nullable(UInt32) | INT UNSIGNED      |              |                    | YES         |         |
....

```