---
title: Query Configurations
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.87"/>

This page describes the Query node configurations available in the [databend-query.toml](https://github.com/datafuselabs/databend/blob/main/scripts/distribution/configs/databend-query.toml) configuration file.

- Some parameters listed in the table below may not be present in [databend-query.toml](https://github.com/datafuselabs/databend/blob/main/scripts/distribution/configs/databend-query.toml). If you require these parameters, you can manually add them to the file.

- You can find [sample configuration files](https://github.com/datafuselabs/databend/tree/main/scripts/ci/deploy/config) on GitHub that set up Databend for various deployment environments. These files were created for internal testing ONLY. Please do NOT modify them for your own purposes. But if you have a similar deployment, it is a good idea to reference them when editing your own configuration files.

## [query] Section

The following is a list of the parameters available within the [query] section:

| Parameter                    | Description                                       |
|------------------------------|---------------------------------------------------|
| max_active_sessions          | Maximum number of active sessions.               |
| wait_timeout_mills           | Timeout in milliseconds for waiting.             |
| flight_api_address           | IP address and port for listening to Databend-Query cluster shuffle data. |
| admin_api_address            | Address for the Admin REST API.                  |
| metric_api_address           | Address for the Metrics REST API.                |
| mysql_handler_host           | Hostname for the MySQL query handler.            |
| mysql_handler_port           | Port for the MySQL query handler.                |
| clickhouse_http_handler_host | Hostname for the ClickHouse HTTP query handler.  |
| clickhouse_http_handler_port | Port for the ClickHouse HTTP query handler.      |
| http_handler_host            | Hostname for the HTTP API query handler.         |
| http_handler_port            | Port for the HTTP API query handler.             |
| flight_sql_handler_host      | Hostname for the Experimental Arrow Flight SQL API query handler. |
| flight_sql_handler_port      | Port for the Experimental Arrow Flight SQL API query handler. |
| tenant_id                    | Default tenant ID.                               |
| cluster_id                   | Default cluster ID.                              |
| table_engine_memory_enabled  | Flag to enable the Memory table engine.          |

## [[query.users]] Section

The following is a list of the parameters available within the [[query.users]] section. For more information about configuring admin users, see [Configuring Admin Users](../../13-sql-clients/00-admin-users.md).

| Parameter      | Description                              |
|----------------|------------------------------------------|
| name           | User name.                               |
| auth_type      | Authentication type (e.g., no_password, double_sha1_password, sha256_password). |
| auth_string    | Authentication string (e.g., SHA-1 or SHA-256 hash of the password). |

## [log] Section

This section can include three subsections: [log.file], [log.stderr], and [log.query].

### [log.file] Section

The following is a list of the parameters available within the [log.file] section:

| Parameter           | Description                                                                                         |
|---------------------|-----------------------------------------------------------------------------------------------------|
| on                  | Enables or disables file logging. Defaults to true.                                                 |
| dir                 | Path to store log files.                                                                            |
| level               | Log level: DEBUG, INFO, or ERROR. Defaults to INFO.                                                 |
| format              | Log format: json or text. Defaults to json.                                                         |

### [log.stderr] Section

The following is a list of the parameters available within the [log.stderr] section:

| Parameter           | Description                                                                                         |
|---------------------|-----------------------------------------------------------------------------------------------------|
| on                  | Enables or disables stderr logging. Defaults to false.                                              |
| level               | Log level: DEBUG, INFO, or ERROR. Defaults to DEBUG.                                                |
| format              | Log format: json or text. Defaults to text.                                                         |

### [log.query] Section

The following is a list of the parameters available within the [log.query] section:

| Parameter           | Description                                                                                         |
|---------------------|-----------------------------------------------------------------------------------------------------|
| on                  | Enables logging query execution details to the query-details folder in the log directory. Defaults to on. Consider disabling when storage space is limited. |

## [meta] Section

The following is a list of the parameters available within the [meta] section:

| Parameter                    | Description                                                                                           |
|------------------------------|-------------------------------------------------------------------------------------------------------|
| username                     | The username used to connect to the Meta service. Default: "root".                                  |
| password                     | The password used to connect to the Meta service. Databend recommends using the environment variable META_PASSWORD to provide the password. Default: "root". |
| endpoints                    | Sets one or more meta server endpoints that this query server can connect to. For robust connection to Meta, include multiple meta servers within the cluster as backups if possible. Example: ["192.168.0.1:9191", "192.168.0.2:9191"]. Default: ["0.0.0.0:9191"]. |
| client_timeout_in_second     | Sets the wait time (in seconds) before terminating the attempt to connect to a meta server. Default: 60. |
| auto_sync_interval           | Sets how often (in seconds) this query server should automatically sync up endpoints from the meta servers within the cluster. When enabled, Databend-query contacts a Databend-meta server periodically to obtain a list of grpc_api_advertise_host:grpc-api-port. To disable the sync up, set it to 0. Default: 60. |
| unhealth_endpoint_evict_time | Internal time (in seconds) for not querying an unhealthy meta node endpoint. Default: 120.           |

## [storage] Section

The following is a list of the parameters available within the [storage] section:

| Parameter | Description                                                                                     |
|-----------|-------------------------------------------------------------------------------------------------|
| type      | The type of storage used. It can be one of the following: fs, s3, azblob, gcs, oss, cos, hdfs, webhdfs. |


### [storage.fs] Section

The following is a list of the parameters available within the [storage.fs] section:

| Parameter   | Description                            |
|-------------|----------------------------------------|
| data_path   | The path to the data storage location. |

### [storage.s3] Section

The following is a list of the parameters available within the [storage.s3] section:

| Parameter                 | Description                                                                               |
|---------------------------|-------------------------------------------------------------------------------------------|
| bucket                    | The name of your Amazon S3-like storage bucket.                                           |
| endpoint_url              | The URL endpoint for the S3-like storage service. Defaults to "https://s3.amazonaws.com". |
| access_key_id             | The access key ID for authenticating with the storage service.                            |
| secret_access_key         | The secret access key for authenticating with the storage service.                        |
| enable_virtual_host_style | A boolean flag indicating whether to enable virtual host-style addressing.                |
| allow_anonymous           | A boolean flag indicating whether anonymous access is allowed (true or false).            |
| external_id               | External ID for authentication.                                                           |
| master_key                | Master key for authentication.                                                            |
| region                    | The region for the S3-like storage service.                                               |
| role_arn                  | ARN (Amazon Resource Name) for authentication.                                            |
| root                      | The root directory for HDFS.                                                              |
| security_token            | Security token for authentication.                                                        |

### [storage.azblob] Section

The following is a list of the parameters available within the [storage.azblob] section:

| Parameter    | Description                                                                                                |
|--------------|------------------------------------------------------------------------------------------------------------|
| endpoint_url | The URL endpoint for Azure Blob Storage (e.g., `https://<your-storage-account-name>.blob.core.windows.net)`. |
| container    | The name of your Azure storage container.                                                                  |
| account_name | The name of your Azure storage account.                                                                    |
| account_key  | The account key for authenticating with Azure Blob Storage.                                                |
| root         | The root directory for Azure Blob Storage.                                                                 |

### [storage.gcs] Section

The following is a list of the parameters available within the [storage.gcs] section:

| Parameter    | Description                                                   |
|--------------|---------------------------------------------------------------|
| bucket       | The name of your Google Cloud Storage bucket.                 |
| endpoint_url | The URL endpoint for Google Cloud Storage.                    |
| credential   | The credentials for authenticating with Google Cloud Storage. |
| root         | The root directory for Google Cloud Storage.                  |

### [storage.oss] Section

The following is a list of the parameters available within the [storage.oss] section:

| Parameter            | Description                                                       |
|----------------------|-------------------------------------------------------------------|
| bucket               | The name of your Alibaba Cloud OSS bucket.                        |
| endpoint_url         | The URL endpoint for Alibaba Cloud OSS.                           |
| access_key_id        | The access key ID for authenticating with Alibaba Cloud OSS.      |
| access_key_secret    | The access key secret for authenticating with Alibaba Cloud OSS.  |
| presign_endpoint_url | The URL endpoint for presigned operations with Alibaba Cloud OSS. |
| root                 | The root directory for Alibaba Cloud OSS.                         |

### [storage.cos] Section

The following is a list of the parameters available within the [storage.cos] section:

| Parameter    | Description                                                 |
|--------------|-------------------------------------------------------------|
| bucket       | The name of your Tencent Cloud Object Storage (COS) bucket. |
| endpoint_url | The URL endpoint for Tencent COS (optional).                |
| secret_id    | The secret ID for authenticating with Tencent COS.          |
| secret_key   | The secret key for authenticating with Tencent COS.         |
| root         | The root directory for Tencent Cloud Object Storage.        |

### [storage.hdfs] Section

The following is a list of the parameters available within the [storage.hdfs] section:

| Parameter      | Description                                       |
|----------------|---------------------------------------------------|
| name_node      | The name node address for Hadoop Distributed File System (HDFS). |
| root         | The root directory for HDFS.                                   |


### [storage.webhdfs] Section

The following is a list of the parameters available within the [storage.webhdfs] section:

| Parameter    | Description                                                    |
|--------------|----------------------------------------------------------------|
| endpoint_url | The URL endpoint for WebHDFS (Hadoop Distributed File System). |
| root         | The root directory for HDFS.                                   |
| delegation   | Delegation token for authentication and authorization.         |

## [cache] Section

The following is a list of the parameters available within the [cache] section:

| Parameter                | Description                                       |
|--------------------------|---------------------------------------------------|
| data_cache_storage       | The type of storage used for table data cache. Available options: "none" (disables table data cache), "disk" (enables disk cache). Defaults to "none".   |

### [cache.disk] Section

The following is a list of the parameters available within the [cache.disk] section:

| Parameter                | Description                                       |
|--------------------------|---------------------------------------------------|
| path                     | The path where the cache is stored when using disk cache. |
| max_bytes                | The maximum amount of cached data in bytes when using disk cache. Defaults to 21474836480 bytes (20 GB). |