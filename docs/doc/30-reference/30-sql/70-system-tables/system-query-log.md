---
title: system.query_log
---

A read-only in-memory table stores all the query logs;

```sql
SELECT * FROM system.query_log ORDER BY event_time DESC LIMIT 1\G
*************************** 1. row ***************************
                log_type: 1
            handler_type: MySQL
               tenant_id: test_tenant
              cluster_id: test_cluster
                sql_user: root
          sql_user_quota: UserQuota<cpu:0,mem:0,store:0>
     sql_user_privileges: GRANT ALL ON *.* TO 'root'@'127.0.0.1', ROLES: []
                query_id: da879c17-94bb-4163-b2ac-ff4786bbe69e
              query_kind: SelectPlan
              query_text: SELECT * from system.query_log order by event_time desc limit 1
              event_date: 2022-03-24
              event_time: 2022-03-24 11:13:27.414
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
             result_rows: 0
            result_bytes: 0
               cpu_usage: 10
            memory_usage: 1603
             client_info:
          client_address: 127.0.0.1:56744
          exception_code: 0
          exception_text:
             stack_trace:
          server_version:
        session_settings: enable_new_processor_framework=1, flight_client_timeout=60, max_block_size=10000, max_threads=8, storage_occ_backoff_init_delay_ms=5, storage_occ_backoff_max_delay_ms=20000, storage_occ_backoff_max_elapsed_ms=120000, storage_read_buffer_size=1048576, scope: SESSION
                   extra:
```
