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

The following is a list of the parameters available within the [log] section:

| Parameter           | Description                                                                                         |
|---------------------|-----------------------------------------------------------------------------------------------------|
| [log.file] on       | Enables or disables file logging. Defaults to true.                                                 |
| [log.file] dir      | Path to store log files.                                                                            |
| [log.file] level    | Log level: DEBUG, INFO, or ERROR. Defaults to INFO.                                                |
| [log.file] format   | Log format: json or text. Defaults to json.                                                         |
| [log.stderr] on     | Enables or disables stderr logging. Defaults to false.                                               |
| [log.stderr] level  | Log level: DEBUG, INFO, or ERROR. Defaults to DEBUG.                                               |
| [log.stderr] format | Log format: json or text. Defaults to text.                                                          |
| [log.query] on      | Enables logging query execution details to the query-details folder in the log directory. Defaults to on. Consider disabling when storage space is limited. |

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

| Parameter               | Description                                                                      |
|-------------------------|----------------------------------------------------------------------------------|
| bucket                  | The name of your Amazon S3-like storage bucket.                                |
| endpoint_url            | The URL endpoint for the S3-like storage service. Defaults to "https://s3.amazonaws.com". |
| access_key_id           | The access key ID for authenticating with the storage service.                  |
| secret_access_key       | The secret access key for authenticating with the storage service.              |
| enable_virtual_host_style | A boolean flag indicating whether to enable virtual host-style addressing.    |

### [storage.azblob] Section

The following is a list of the parameters available within the [storage.azblob] section:

| Parameter       | Description                                                                   |
|-----------------|-------------------------------------------------------------------------------|
| endpoint_url    | The URL endpoint for Azure Blob Storage (e.g., `https://<your-storage-account-name>.blob.core.windows.net`). |
| container       | The name of your Azure storage container.                                    |
| account_name    | The name of your Azure storage account.                                       |
| account_key     | The account key for authenticating with Azure Blob Storage.                   |



## 1. Logging Config

### log.file

  * on: Enables or disables `file` logging. Defaults to `true`.
  * dir: Path to store log files.
  * level: Log level (DEBUG | INFO | ERROR). Defaults to `INFO`.
  * format: Log format. Defaults to `json`.
    - `json`: Databend outputs logs in JSON format.
    - `text`: Databend outputs plain text logs.

### log.stderr

  * on: Enables or disables `stderr` logging. Defaults to `false`.
  * level: Log level (DEBUG | INFO | ERROR). Defaults to `DEBUG`.
  * format: Log format. Defaults to `text`.
    - `text`: Databend outputs plain text logs.
    - `json`: Databend outputs logs in JSON format.

### log.query

  * on: Enables logging query execution details to the **query-details** folder in the log directory. Defaults to `on`. Consider disabling when storage space is limited.

## 2. Meta Service Config

### username

* The username used to connect to the Meta service.
* Default: `"root"`
* Env variable: `META_USERNAME`

### password

* The password used to connect to the Meta service. Databend recommends using the environment variable to provide the password.
* Default: `"root"`
* Env variable: `META_PASSWORD`

### endpoints

* Sets one or more meta server endpoints that this query server can connect to. For a robust connection to Meta, include multiple meta servers within the cluster as backups if possible, for example, `["192.168.0.1:9191", "192.168.0.2:9191"]`.
* It is a list of `grpc_api_advertise_host:<grpc-api-port>` of databend-meta config. See [Databend-meta config: `grpc_api_advertise_host`](01-metasrv-config.md).
* This setting only takes effect when Databend works in cluster mode. You don't need to configure it for standalone Databend.
* Default: `["0.0.0.0:9191"]`
* Env variable: `META_ENDPOINTS`

### client_timeout_in_second

* Sets the wait time (in seconds) before terminating the attempt to connect a meta server.
* Default: 60

### auto_sync_interval

* Sets how often (in seconds) this query server should automatically sync up `endpoints` from the meta servers within the cluster. When enabled, databend-query tries to contact one of the databend-meta server to get a list of `grpc_api_advertise_host:<grpc-api-port>` periodically.
* If a databend-meta is **NOT** configured with `grpc_api_advertise_host`, it fills blank string `""` in the returned endpoint list. See [Databend-meta config: `grpc_api_advertise_host`](01-metasrv-config.md).
* If the returned endpoints list contains more than half invalid addresses, e.g., 2/3 are `""`: `["127.0.0.1:9191", "",""]`, databend-query will not update the `endpoint`.
* To disable the sync up, set it to 0.
* This setting only takes effect when Databend-query works with remote meta service(`endpoints` is not empty). You don't need to configure it for standalone Databend.
* Default: 60

### unhealth_endpoint_evict_time

* Internal(in seconds) time that not querying an unhealth meta node endpoint.
* Default: 120

## 3. Query config

### admin_api_address

* The IP address and port to listen on for admin the databend-query, e.g., `0.0.0.0::8080`.
* Default: `"127.0.0.1:8080"`
* Env variable: `QUERY_ADMIN_API_ADDRESS`

### metric_api_address

* The IP address and port to listen on that can be scraped by Prometheus, e.g., `0.0.0.0::7070`.
* Default: `"127.0.0.1:7070"`
* Env variable: `QUERY_METRIC_API_ADDRESS`

### flight_api_address

* The IP address and port to listen on for databend-query cluster shuffle data, e.g., `0.0.0.0::9090`.
* Default: `"127.0.0.1:9090"`
* Env variable: `QUERY_FLIGHT_API_ADDRESS`

### mysql_handler_host

* The IP address to listen on for MySQL handler, e.g., `0.0.0.0`.
* Default: `"127.0.0.1"`
* Env variable: `QUERY_MYSQL_HANDLER_HOST`

### mysql_handler_port

* The port to listen on for MySQL handler, e.g., `3307`.
* Default: `3307`
* Env variable: `QUERY_MYSQL_HANDLER_PORT`

### clickhouse_http_handler_host

* The IP address to listen on for ClickHouse HTTP handler, e.g., `0.0.0.0`.
* Default: `"127.0.0.1"`
* Env variable: `QUERY_CLICKHOUSE_HTTP_HANDLER_HOST`

### clickhouse_http_handler_port

* The port to listen on for ClickHouse HTTP handler, e.g., `8124`.
* Default: `8124`
* Env variable: `QUERY_CLICKHOUSE_HTTP_HANDLER_PORT`

### tenant_id

* Identifies the tenant and is used for storing the tenant's metadata.
* Default: `"admin"`
* Env variable: `QUERY_TENANT_ID`

### cluster_id

* Identifies the cluster that the databend-query node belongs to.
* Default: `""`
* Env variable: `QUERY_CLUSTER_ID`


## 4. Storage config

### type

* Which storage type(Must one of `"fs"` | `"s3"` | `"azblob"` | `"obs"`) should use for the databend-query, e.g., `"s3"`.
* Default: `""`
* Env variable: `STORAGE_TYPE`
* Required.

### storage.s3

#### bucket

* AWS S3 bucket name.
* Default: `""`
* Env variable: `STORAGE_S3_BUCKET`
* Required.

#### endpoint_url

* AWS S3(or MinIO S3-like) endpoint URL, e.g., `"https://s3.amazonaws.com"`.
* Default: `"https://s3.amazonaws.com"`
* Env variable: `STORAGE_S3_ENDPOINT_URL`

#### access_key_id

* AWS S3 access_key_id.
* Default: `""`
* Env variable: `STORAGE_S3_ACCESS_KEY_ID`
* Required.

#### secret_access_key

* AWS S3 secret_access_key.
* Default: `""`
* Env variable: `STORAGE_S3SECRET_ACCESS_KEY`
* Required.

### storage.azblob

#### endpoint_url

* Azure Blob Storage endpoint URL, e.g., `"https://<your-storage-account-name>.blob.core.windows.net"`.
* Default: `""`
* Env variable: `STORAGE_AZBLOB_ENDPOINT_URL`
* Required.

#### container

* Azure Blob Storage container name.
* Default: `""`
* Env variable: `STORAGE_AZBLOB_CONTAINER`
* Required.

#### account_name

* Azure Blob Storage account name.
* Default: `""`
* Env variable: `STORAGE_AZBLOB_ACCOUNT_NAME`
* Required.

#### account_key

* Azure Blob Storage account key.
* Default: `""`
* Env variable: `STORAGE_AZBLOB_ACCOUNT_KEY`
* Required.

## 5. Cache

This configuration determines whether to enable caching of **block data** to the local disk and how to configure the cache.

:::note

This need databend-query version >= v0.9.40-nightly.

:::

### data_cache_storage

* Type of storage to keep the table data cache, set to `disk` to enable the disk cache.
* Default: `"none"`, block data caching is not enabled.
* Env variable: `DATA_CACHE_STORAGE`
 
### cache.disk

#### path

* Table disk cache root path.
* Default: `"./.databend/_cache"`
* Env variable: `CACHE-DISK-PATH`

#### max_bytes

* Max bytes of cached raw table data.
* Default: `21474836480`
* Env variable: `CACHE-DISK-MAX-BYTES`

### Cache Config Example

Enable disk cache:
```shell

...

data_cache_storage = "disk"
[cache.disk]
# cache path
path = "./databend/_cache"
# max bytes of cached data 20G
max_bytes = 21474836480
```
