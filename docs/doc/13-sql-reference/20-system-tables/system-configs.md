---
title: system.configs
---

Contains information about Databend server configs.

```sql
mysql> SELECT * FROM system.configs;
+---------+----------------------------------------+--------------------------------+-------------+
| group   | name                                   | value                          | description |
+---------+----------------------------------------+--------------------------------+-------------+
| query   | tenant_id                              | admin                          |             |
| query   | cluster_id                             |                                |             |
| query   | num_cpus                               | 0                              |             |
| query   | mysql_handler_host                     | 127.0.0.1                      |             |
| query   | mysql_handler_port                     | 3307                           |             |
| query   | max_active_sessions                    | 256                            |             |
| query   | max_server_memory_usage                | 0                              |             |
| query   | max_memory_limit_enabled               | false                          |             |
| query   | clickhouse_handler_host                | 127.0.0.1                      |             |
| query   | clickhouse_handler_port                | 9000                           |             |
| query   | clickhouse_http_handler_host           | 127.0.0.1                      |             |
| query   | clickhouse_http_handler_port           | 8124                           |             |
| query   | http_handler_host                      | 127.0.0.1                      |             |
| query   | http_handler_port                      | 8000                           |             |
| query   | http_handler_result_timeout_secs       | 60                             |             |
| query   | flight_api_address                     | 127.0.0.1:9090                 |             |
| query   | admin_api_address                      | 127.0.0.1:8080                 |             |
| query   | metric_api_address                     | 127.0.0.1:7070                 |             |
| query   | http_handler_tls_server_cert           |                                |             |
| query   | http_handler_tls_server_key            |                                |             |
| query   | http_handler_tls_server_root_ca_cert   |                                |             |
| query   | api_tls_server_cert                    |                                |             |
| query   | api_tls_server_key                     |                                |             |
| query   | api_tls_server_root_ca_cert            |                                |             |
| query   | rpc_tls_server_cert                    |                                |             |
| query   | rpc_tls_server_key                     |                                |             |
| query   | rpc_tls_query_server_root_ca_cert      |                                |             |
| query   | rpc_tls_query_service_domain_name      | localhost                      |             |
| query   | table_engine_memory_enabled            | true                           |             |
| query   | wait_timeout_mills                     | 5000                           |             |
| query   | max_query_log_size                     | 10000                          |             |
| query   | management_mode                        | false                          |             |
| query   | jwt_key_file                           |                                |             |
| query   | jwt_key_files                          |                                |             |
| query   | users                                  |                                |             |
| query   | share_endpoint_address                 |                                |             |
| query   | share_endpoint_auth_token_file         |                                |             |
| query   | quota                                  | null                           |             |
| query   | internal_enable_sandbox_tenant         | false                          |             |
| log     | level                                  | INFO                           |             |
| log     | dir                                    | ./.databend/logs               |             |
| log     | query_enabled                          | false                          |             |
| log     | file.on                                | true                           |             |
| log     | file.level                             | INFO                           |             |
| log     | file.dir                               | ./.databend/logs               |             |
| log     | file.format                            | json                           |             |
| log     | stderr.on                              | false                          |             |
| log     | stderr.level                           | INFO                           |             |
| log     | stderr.format                          | text                           |             |
| meta    | embedded_dir                           | .databend/meta                 |             |
| meta    | endpoints                              |                                |             |
| meta    | username                               | root                           |             |
| meta    | password                               |                                |             |
| meta    | client_timeout_in_second               | 10                             |             |
| meta    | auto_sync_interval                     | 0                              |             |
| meta    | rpc_tls_meta_server_root_ca_cert       |                                |             |
| meta    | rpc_tls_meta_service_domain_name       | localhost                      |             |
| meta    | unhealth_endpoint_evict_time           | 120                            |             |
| cache   | enable_table_meta_cache                | true                           |             |
| cache   | table_meta_snapshot_count              | 256                            |             |
| cache   | table_meta_segment_count               | 10240                          |             |
| cache   | table_meta_statistic_count             | 256                            |             |
| cache   | enable_table_index_bloom               | true                           |             |
| cache   | table_bloom_index_meta_count           | 3000                           |             |
| cache   | table_bloom_index_filter_count         | 1048576                        |             |
| cache   | data_cache_storage                     | none                           |             |
| cache   | table_data_cache_population_queue_size | 65536                          |             |
| cache   | disk.max_bytes                         | 21474836480                    |             |
| cache   | disk.path                              | ./.databend/_cache             |             |
| cache   | table_data_deserialized_data_bytes     | 0                              |             |
| storage | type                                   | fs                             |             |
| storage | num_cpus                               | 0                              |             |
| storage | allow_insecure                         | false                          |             |
| storage | fs.data_path                           | _data                          |             |
| storage | gcs.endpoint_url                       | https://storage.googleapis.com |             |
| storage | gcs.bucket                             |                                |             |
| storage | gcs.root                               |                                |             |
| storage | gcs.credential                         |                                |             |
| storage | s3.region                              |                                |             |
| storage | s3.endpoint_url                        | https://s3.amazonaws.com       |             |
| storage | s3.access_key_id                       |                                |             |
| storage | s3.secret_access_key                   |                                |             |
| storage | s3.security_token                      |                                |             |
| storage | s3.bucket                              |                                |             |
| storage | s3.root                                |                                |             |
| storage | s3.master_key                          |                                |             |
| storage | s3.enable_virtual_host_style           | false                          |             |
| storage | s3.role_arn                            |                                |             |
| storage | s3.external_id                         |                                |             |
| storage | azblob.account_name                    |                                |             |
| storage | azblob.account_key                     |                                |             |
| storage | azblob.container                       |                                |             |
| storage | azblob.endpoint_url                    |                                |             |
| storage | azblob.root                            |                                |             |
| storage | hdfs.name_node                         |                                |             |
| storage | hdfs.root                              |                                |             |
| storage | obs.access_key_id                      |                                |             |
| storage | obs.secret_access_key                  |                                |             |
| storage | obs.bucket                             |                                |             |
| storage | obs.endpoint_url                       |                                |             |
| storage | obs.root                               |                                |             |
| storage | oss.access_key_id                      |                                |             |
| storage | oss.access_key_secret                  |                                |             |
| storage | oss.bucket                             |                                |             |
| storage | oss.endpoint_url                       |                                |             |
| storage | oss.presign_endpoint_url               |                                |             |
| storage | oss.root                               |                                |             |
| storage | cache.type                             | none                           |             |
| storage | cache.num_cpus                         | 0                              |             |
| storage | cache.fs.data_path                     | _data                          |             |
| storage | cache.moka.max_capacity                | 1073741824                     |             |
| storage | cache.moka.time_to_live                | 3600                           |             |
| storage | cache.moka.time_to_idle                | 600                            |             |
| storage | cache.redis.endpoint_url               |                                |             |
| storage | cache.redis.username                   |                                |             |
| storage | cache.redis.password                   |                                |             |
| storage | cache.redis.root                       |                                |             |
| storage | cache.redis.db                         | 0                              |             |
| storage | cache.redis.default_ttl                | 0                              |             |
+---------+----------------------------------------+--------------------------------+-------------+
```
