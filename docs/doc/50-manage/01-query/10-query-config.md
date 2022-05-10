---
title: Databend Query Configuration
sidebar_label: Databend Query Configuration
description: 
  Databend Query Configuration
---

A `databend-query` server is configured with a `toml` file or flags.

You can explore more flags with `./databend-query -h`.

## 1. Logging Config

### dir

* Path to a directory for storing hourly-rolling debug log.
* Default: `"./_logs"`
* Env variable: `LOG_DIR`

### level

* Log level, one of `"DEBUG" | "INFO" | "ERROR"`.
* Default: `"INFO"`
* Env variable: `LOG_LEVEL`

## 2. Meta Service Config

### address

* Meta service endpoint, which lets databend-query connect to get meta data, e.g., `192.168.0.1:9101`.
* Default: `""`
* Env variable: `META_ADDRESS`
* Required.

### username

* Meta service username when connecting to it.
* Default: `"root"`
* Env variable: `META_USERNAME`

### password

* Meta service password when connecting to it, `password` is not recommended here, please use Env variable instead.
* Default: `"root"`
* Env variable: `META_PASSWORD`


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

### clickhouse_handler_host

* The IP address to listen on for ClickHouse handler, e.g., `0.0.0.0`.
* Default: `"127.0.0.1"`
* Env variable: `QUERY_CLICKHOUSE_HANDLER_HOST`

### clickhouse_handler_port

* The port to listen on for ClickHouse handler, e.g., `3307`.
* Default: `9000`
* Env variable: `QUERY_CLICKHOUSE_HANDLER_PORT`

### tenant_id

* The ID for the databend-query server to store metadata to the Meta Service.
* Default: `"admin"`
* Env variable: `QUERY_TENANT_ID`

### cluster_id

* The ID for the databend-query server to construct a cluster.
* Default: `""`
* Env variable: `QUERY_CLUSTER_ID`


## 4. Storage config

### type 

* Which storage type(Only support `"fs"` or `"s3"`) should use for the databend-query, e.g., `"s3"`.
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


## A Toml File Demo

```toml title="databend-query.toml"
# Logging
[log]
level = "INFO"
dir = "benddata/_logs"

# Meta Service
[meta]
address = "127.0.0.1:9101"
username = "root"
password = "root"

[query]
# For admin RESET API.
admin_api_address = "127.0.0.1:8001"

# Metrics.
metric_api_address = "127.0.0.1:7071"

# Cluster flight RPC.
flight_api_address = "127.0.0.1:9091"

# Query MySQL Handler.
mysql_handler_host = "127.0.0.1"
mysql_handler_port = 3307

# Query ClickHouse Handler.
clickhouse_handler_host = "127.0.0.1"
clickhouse_handler_port = 9001

# Query HTTP Handler.
http_handler_host = "127.0.0.1"
http_handler_port = 8081

tenant_id = "tenant1"
cluster_id = "cluster1"

[storage]
# s3
type = "s3"

[storage.s3]
bucket = "databend"
endpoint_url = "https://s3.amazonaws.com"
access_key_id = "<your-key-id>"
secret_access_key = "<your-access-key>"
```
