---
title: system.tracing
---

Contains information about server log.

```sql
SELECT * FROM system.tracing LIMIT 1\G
*************************** 1. row ***************************
       v: 0
    name: databend-query-test_cluster@0.0.0.0:3307
     msg: Config { config_file: "scripts/ci/deploy/config/databend-query-node-1.toml", query: QueryConfig { tenant_id: "test_tenant", cluster_id: "test_cluster", num_cpus: 10, mysql_handler_host: "0.0.0.0", mysql_handler_port: 3307, max_active_sessions: 256, clickhouse_handler_host: "0.0.0.0", clickhouse_handler_port: 9001, http_handler_host: "0.0.0.0", http_handler_port: 8001, http_handler_result_timeout_millis: 10000, flight_api_address: "0.0.0.0:9091", admin_api_address: "0.0.0.0:8081", metric_api_address: "0.0.0.0:7071", http_handler_tls_server_cert: "", http_handler_tls_server_key: "", http_handler_tls_server_root_ca_cert: "", api_tls_server_cert: "", api_tls_server_key: "", api_tls_server_root_ca_cert: "", rpc_tls_server_cert: "", rpc_tls_server_key: "", rpc_tls_query_server_root_ca_cert: "", rpc_tls_query_service_domain_name: "localhost", table_engine_csv_enabled: true, table_engine_parquet_enabled: true, table_engine_memory_enabled: true, database_engine_github_enabled: true, wait_timeout_mills: 5000, max_query_log_size: 10000, table_cache_enabled: true, table_cache_snapshot_count: 256, table_cache_segment_count: 10240, table_cache_block_meta_count: 102400, table_memory_cache_mb_size: 1024, table_disk_cache_root: "_cache", table_disk_cache_mb_size: 10240, management_mode: false, jwt_key_file: "" }, log: LogConfig { log_level: "INFO", log_dir: "./_logs", log_query_enabled: false }, meta: {meta_address: "0.0.0.0:9191", meta_user: "root", meta_password: "******"}, storage: StorageConfig { storage_type: "disk", storage_num_cpus: 0, disk: FsStorageConfig { data_path: "stateless_test_data", temp_data_path: "" }, s3: {s3.storage.region: "", s3.storage.endpoint_url: "https://s3.amazonaws.com", s3.storage.bucket: "", s3.storage.access_key_id: "", s3.storage.secret_access_key: "", }, azure_storage_blob: {Azure.storage.container: "", } } }
   level: 30
hostname: localhost
     pid: 24640
    time: 2022-03-24T11:33:29.363161Z
```
