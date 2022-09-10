---
title: system.query_log
---

A read-only in-memory table stores all the query logs.


## Columns

```
CREATE TABLE `query_log` (
  `log_type` TINYINT,
  `handler_type` VARCHAR,
  `tenant_id` VARCHAR,
  `cluster_id` VARCHAR,
  `sql_user` VARCHAR,
  `sql_user_quota` VARCHAR,
  `sql_user_privileges` VARCHAR,
  `query_id` VARCHAR,
  `query_kind` VARCHAR,
  `query_text` VARCHAR,
  `event_date` DATE,
  `event_time` TIMESTAMP(3),
  `current_database` VARCHAR,
  `databases` VARCHAR,
  `tables` VARCHAR,
  `columns` VARCHAR,
  `projections` VARCHAR,
  `written_rows` BIGINT UNSIGNED,
  `written_bytes` BIGINT UNSIGNED,
  `written_io_bytes` BIGINT UNSIGNED,
  `written_io_bytes_cost_ms` BIGINT UNSIGNED,
  `scan_rows` BIGINT UNSIGNED,
  `scan_bytes` BIGINT UNSIGNED,
  `scan_io_bytes` BIGINT UNSIGNED,
  `scan_io_bytes_cost_ms` BIGINT UNSIGNED,
  `scan_partitions` BIGINT UNSIGNED,
  `total_partitions` BIGINT UNSIGNED,
  `result_rows` BIGINT UNSIGNED,
  `result_bytes` BIGINT UNSIGNED,
  `cpu_usage` INT UNSIGNED,
  `memory_usage` BIGINT UNSIGNED,
  `client_info` VARCHAR,
  `client_address` VARCHAR,
  `exception_code` INT,
  `exception_text` VARCHAR,
  `stack_trace` VARCHAR,
  `server_version` VARCHAR,
  `session_settings` VARCHAR,
  `extra` VARCHAR
)
```

## Examples

```
*************************** 4. row ***************************
                log_type: 1
            handler_type: MySQL
               tenant_id: admin
              cluster_id:
                sql_user: root
          sql_user_quota: UserQuota<cpu:0,mem:0,store:0>
     sql_user_privileges: GRANT ALL ON *.*, ROLES: []
                query_id: eda2a82b-3667-4ffb-b436-953785178c39
              query_kind: Query
              query_text: select avg(number) from numbers(1000000)
              event_date: 2022-09-08
              event_time: 2022-09-08 03:32:39.517
        current_database: default
               databases:
                  tables:
                 columns:
             projections:
            written_rows: 0
           written_bytes: 0
        written_io_bytes: 0
written_io_bytes_cost_ms: 0
               scan_rows: 0
              scan_bytes: 0
           scan_io_bytes: 0
   scan_io_bytes_cost_ms: 0
         scan_partitions: 0
        total_partitions: 0

, skip_header=0, sql_dialect=PostgreSQL, storage_read_buffer_size=1048576, timezone=UTC, unquoted_ident_case_sensitive=0, wait_for_async_insert=1, wait_for_async_insert_timeout=100, scope: SESSION
                   extra:
                   
                   
*************************** 5. row ***************************
                log_type: 2
            handler_type: MySQL
               tenant_id: admin
              cluster_id:
                sql_user: root
          sql_user_quota: UserQuota<cpu:0,mem:0,store:0>
     sql_user_privileges: GRANT ALL ON *.*, ROLES: []
                query_id: eda2a82b-3667-4ffb-b436-953785178c39
              query_kind: Query
              query_text: select avg(number) from numbers(1000000)
              event_date: 2022-09-08
              event_time: 2022-09-08 03:32:39.519
        current_database: default
               databases:
                  tables:
                 columns:
             projections:
            written_rows: 0
           written_bytes: 0
        written_io_bytes: 0
written_io_bytes_cost_ms: 0
               scan_rows: 1000000
              scan_bytes: 8000000
           scan_io_bytes: 0
   scan_io_bytes_cost_ms: 0
         scan_partitions: 0
        total_partitions: 0
             result_rows: 1
            result_bytes: 9
               cpu_usage: 24
            memory_usage: 0
             client_info:
          client_address: 127.0.0.1:53304
          exception_code: 0
          exception_text:
             stack_trace:
          server_version:
        session_settings: compression=None, empty_as_default=1, enable_async_insert=0, enable_new_processor_framework=1, enable_planner_v2=1, field_delimiter=,, flight_client_timeout=60, group_by_two_level_threshold=10000, max_block_size=10000, max_execute_time=0, max_threads=24, quoted_ident_case_sensitive=1, record_delimiter=
, skip_header=0, sql_dialect=PostgreSQL, storage_read_buffer_size=1048576, timezone=UTC, unquoted_ident_case_sensitive=0, wait_for_async_insert=1, wait_for_async_insert_timeout=100, scope: SESSION
                   extra:
```
