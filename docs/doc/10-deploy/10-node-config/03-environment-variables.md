---
title: Setting Environment Variables
---

Databend offers you the flexibility to configure your Meta and Query nodes in two ways: using environment variables or configuration files. Moreover, you have the option to utilize environment variables to point to custom configuration files. This capability empowers you to make modifications without disrupting the default setups. This is especially advantageous when you require swift adjustments, are working with containers, or need to safeguard sensitive data.

:::note
- A mapping relationship exists between the parameters set through environment variables and those specified in configuration files. In cases where a configuration parameter is defined both via an environment variable and in a configuration file, Databend will prioritize the value provided by the environment variable.

- Not all configurations can be managed solely through environment variables. In some cases, adjustments might necessitate modifications within the configuration files rather than relying on environment variables.
:::

## Setting Configuration File Paths

METASRV_CONFIG_FILE and CONFIG_FILE are environment variables used to designate the locations of your configuration files, [databend-meta.toml](https://github.com/datafuselabs/databend/blob/main/scripts/distribution/configs/databend-meta.toml) and [databend-query.toml](https://github.com/datafuselabs/databend/blob/main/scripts/distribution/configs/databend-query.toml). They provide a way to tailor the paths of configuration files in Databend. If you'd like to depart from the default setup and opt for custom configuration file locations, these variables empower you to specify the exact paths for your files.

```sql title='Example'
export METASRV_CONFIG_FILE='/etc/databend/databend-meta.toml'
export CONFIG_FILE='/etc/databend/databend-query.toml'
```

## Meta Environment Variables

Below is a list of available environment variables, each correspondingly mapped to parameters found in the configuration file [databend-meta.toml](https://github.com/datafuselabs/databend/blob/main/scripts/distribution/configs/databend-meta.toml). For detailed explanations of each parameter, see [Meta Configurations](01-metasrv-config.md).

| Environment Variable            	| Mapped to               	|
|---------------------------------	|-------------------------	|
| METASRV_LOG_DIR                 	| log_dir                 	|
| ADMIN_API_ADDRESS               	| admin_api_address       	|
| METASRV_GRPC_API_ADDRESS        	| grpc_api_address        	|
| METASRV_GRPC_API_ADVERTISE_HOST 	| grpc_api_advertise_host 	|
| KVSRV_ID                        	| id                      	|
| KVSRV_RAFT_DIR                  	| raft_dir                	|
| KVSRV_API_PORT                  	| raft_api_port           	|
| KVSRV_LISTEN_HOST               	| raft_listen_host        	|
| KVSRV_ADVERTISE_HOST            	| raft_advertise_host     	|
| KVSRV_SINGLE                    	| single                  	|

## Query Environment Variables

The parameters under the [query] and [storage] sections in the configuration file [databend-query.toml](https://github.com/datafuselabs/databend/blob/main/scripts/distribution/configs/databend-query.toml) can be configured using environment variables.

### Environment Variables for [query] Section

| Environment Variable               | Mapped to                    |
|------------------------------------|------------------------------|
| QUERY_MAX_ACTIVE_SESSIONS          | max_active_sessions          |
| QUERY_WAIT_TIMEOUT_MILLS           | wait_timeout_mills           |
| QUERY_FLIGHT_API_ADDRESS           | flight_api_address           |
| QUERY_ADMIN_API_ADDRESS            | admin_api_address            |
| QUERY_METRIC_API_ADDRESS           | metric_api_address           |
| QUERY_MYSQL_HANDLER_HOST           | mysql_handler_host           |
| QUERY_MYSQL_HANDLER_PORT           | mysql_handler_port           |
| QUERY_CLICKHOUSE_HTTP_HANDLER_HOST | clickhouse_http_handler_host |
| QUERY_CLICKHOUSE_HTTP_HANDLER_PORT | clickhouse_http_handler_port |
| QUERY_HTTP_HANDLER_HOST            | http_handler_host            |
| QUERY_HTTP_HANDLER_PORT            | http_handler_port            |
| QUERY_FLIGHT_SQL_HANDLER_HOST      | flight_sql_handler_host      |
| QUERY_FLIGHT_SQL_HANDLER_PORT      | flight_sql_handler_port      |
| QUERY_TENANT_ID                    | tenant_id                    |
| QUERY_CLUSTER_ID                   | cluster_id                   |
| QUERY_TABLE_ENGINE_MEMORY_ENABLED  | table_engine_memory_enabled  |

### Environment Variables for [storage] Section

| Environment Variable                 | Mapped to                    |
|--------------------------------------|------------------------------|
| STORAGE_TYPE                         | type                         |
| STORAGE_FS_DATA_PATH                 | fs.data_path                 |
| STORAGE_AZBLOB_ACCOUNT_KEY           | azblob.account_key           |
| STORAGE_AZBLOB_ACCOUNT_NAME          | azblob.account_name          |
| STORAGE_AZBLOB_CONTAINER             | azblob.container             |
| STORAGE_AZBLOB_ENDPOINT_URL          | azblob.endpoint_url          |
| STORAGE_AZBLOB_ROOT                  | azblob.root                  |
| STORAGE_COS_BUCKET                   | cos.bucket                   |
| STORAGE_COS_ENDPOINT_URL             | cos.endpoint_url             |
| STORAGE_COS_ROOT                     | cos.root                     |
| STORAGE_COS_SECRET_ID                | cos.secret_id                |
| STORAGE_COS_SECRET_KEY               | cos.secret_key               |
| STORAGE_FS_DATA_PATH                 | fs.data_path                 |
| STORAGE_GCS_BUCKET                   | gcs.bucket                   |
| STORAGE_GCS_CREDENTIAL               | gcs.credential               |
| STORAGE_GCS_ENDPOINT_URL             | gcs.endpoint_url             |
| STORAGE_GCS_ROOT                     | gcs.root                     |
| STORAGE_HDFS_NAME_NODE               | hdfs.name_node               |
| STORAGE_HDFS_ROOT                    | hdfs.root                    |
| STORAGE_NUM_CPUS                     | num_cpus                     |
| STORAGE_OSS_ACCESS_KEY_ID            | oss.access_key_id            |
| STORAGE_OSS_ACCESS_KEY_SECRET        | oss.access_key_secret        |
| STORAGE_OSS_BUCKET                   | oss.bucket                   |
| STORAGE_OSS_ENDPOINT_URL             | oss.endpoint_url             |
| STORAGE_OSS_PRE-SIGN_ENDPOINT_URL    | oss.presign_endpoint_url     |
| STORAGE_OSS_ROOT                     | oss.root                     |
| STORAGE_S3_ACCESS_KEY_ID             | s3.access_key_id             |
| STORAGE_S3_ALLOW_ANONYMOUS           | s3.allow_anonymous           |
| STORAGE_S3_BUCKET                    | s3.bucket                    |
| STORAGE_S3_ENABLE_VIRTUAL_HOST_STYLE | s3.enable_virtual_host_style |
| STORAGE_S3_ENDPOINT_URL              | s3.endpoint_url              |
| STORAGE_S3_EXTERNAL_ID               | s3.external_id               |
| STORAGE_S3_MASTER_KEY                | s3.master_key                |
| STORAGE_S3_REGION                    | s3.region                    |
| STORAGE_S3_ROLE_ARN                  | s3.role_arn                  |
| STORAGE_S3_ROOT                      | s3.root                      |
| STORAGE_S3_SECRET_ACCESS_KEY         | s3.secret_access_key         |
| STORAGE_S3_SECURITY_TOKEN            | s3.security_token            |
| STORAGE_STORAGE_NUM_CPUS             | storage_num_cpus             |
| STORAGE_WEBHDFS_DELEGATION           | webhdfs.delegation           |
| STORAGE_WEBHDFS_ENDPOINT_URL         | webhdfs.endpoint_url         |
| STORAGE_WEBHDFS_ROOT                 | webhdfs.root                 |
