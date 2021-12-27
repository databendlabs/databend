---
title: system.configs
---

Contains information about databend server configs.

```sql
mysql> select * from  configs;
+--------------------------------------+------------------+-------+-------------+
| name                                 | value            | group | description |
+--------------------------------------+------------------+-------+-------------+
| tenant_id                            |                  | query |             |
| cluster_id                           |                  | query |             |
| num_cpus                             | 16               | query |             |
| mysql_handler_host                   | 127.0.0.1        | query |             |
| mysql_handler_port                   | 3307             | query |             |
| max_active_sessions                  | 256              | query |             |
| clickhouse_handler_host              | 127.0.0.1        | query |             |
| clickhouse_handler_port              | 9000             | query |             |
| http_handler_host                    | 127.0.0.1        | query |             |
| http_handler_port                    | 8000             | query |             |
| flight_api_address                   | 127.0.0.1:9090   | query |             |
| http_api_address                     | 127.0.0.1:8080   | query |             |
| metric_api_address                   | 127.0.0.1:7070   | query |             |
| http_handler_tls_server_cert         |                  | query |             |
| http_handler_tls_server_key          |                  | query |             |
| http_handler_tls_server_root_ca_cert |                  | query |             |
| api_tls_server_cert                  |                  | query |             |
| api_tls_server_key                   |                  | query |             |
| api_tls_server_root_ca_cert          |                  | query |             |
| rpc_tls_server_cert                  |                  | query |             |
| rpc_tls_server_key                   |                  | query |             |
| rpc_tls_query_server_root_ca_cert    |                  | query |             |
| rpc_tls_query_service_domain_name    | localhost        | query |             |
| table_engine_csv_enabled             | false            | query |             |
| table_engine_parquet_enabled         | false            | query |             |
| table_engine_memory_enabled          | true             | query |             |
| database_engine_github_enabled       | true             | query |             |
| wait_timeout_mills                   | 5000             | query |             |
| max_query_log_size                   | 10000            | query |             |
| table_cache_enabled                  | false            | query |             |
| table_memory_cache_mb_size           | 256              | query |             |
| table_disk_cache_root                | _cache           | query |             |
| table_disk_cache_mb_size             | 1024             | query |             |
| log_level                            | INFO             | log   |             |
| log_dir                              | ./_logs          | log   |             |
| meta_embedded_dir                    | ./_meta_embedded | meta  |             |
| meta_address                         |                  | meta  |             |
| meta_username                        |                  | meta  |             |
| meta_password                        |                  | meta  |             |
| meta_client_timeout_in_second        | 10               | meta  |             |
| rpc_tls_meta_server_root_ca_cert     |                  | meta  |             |
| rpc_tls_meta_service_domain_name     | localhost        | meta  |             |
+--------------------------------------+------------------+-------+-------------+
```