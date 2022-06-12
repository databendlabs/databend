---
title: Deploying Databend on Your Laptop in Minutes
description: A quick Databend deployment guide
slug: deploy-databend-on-laptop
authors:
- name: Eric
  url: https://github.com/soyeric128
  image_url: https://github.com/soyeric128.png
  tags: [databend, deploy]
---

**Deploying Databend on Your Laptop in Minutes**


Deploying a data warehouse sounds like a big job to you? Definitely NOT. Databend can be deployed to your laptop and uses the local file system as storage. You can complete the deployment in a few minutes even if you're new to Databend.
Now let's get started!

:::tip

Databend requires a scalabe storage (for example, object storage) to work. This blog uses local file system to provide you a hands-on experience. Never use a local file system as storage for production purposes.

:::


## STEP 1. Downloading Databend

a. Create a folder named `databend` in the directory `/usr/local`. Then create the following subfolders in the folder `databend`:

* *bin*
* *data*
* *etc*
* *logs*

b. Download and extract the latest Databend package for your platform from [https://github.com/datafuselabs/databend/releases](https://github.com/datafuselabs/databend/releases).

c. Move the extracted files `databend-meta` and `databend-query` to the folder `/usr/local/databend/bin`.

## STEP 2. Deploying a Standalone databend-meta

a. Create a file named `databend-meta.toml` in the folder `/usr/local/databend/etc` with the following content:

```toml
dir = "metadata/_logs"
admin_api_address = "127.0.0.1:8101"
grpc_api_address = "127.0.0.1:9101"

[raft_config]
id = 1
single = true
raft_dir = "metadata/datas"

```

b. Open a terminal window and navigate to the folder `/usr/local/databend/bin`.

c. Run the following command to start databend-meta:

```shell
./databend-meta -c ../etc/databend-meta.toml > meta.log 2>&1 &
```

d. Run the following command to check if databend-meta was started successfully: 

```shell
curl -I  http://127.0.0.1:8101/v1/health
```

## STEP 3. Deploying a Standalone databend-query

a. Create a file named `databend-query.toml` in the folder `/usr/local/databend/etc` with the following content:

```toml
[log]
level = "INFO"
dir = "benddata/_logs"

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

[meta]
address = "127.0.0.1:9101"
username = "root"
password = "root"

[storage]
# s3
type = "fs"

[storage.fs]
data_path = "benddata/datas"
```

b. Open a terminal window and navigate to the folder `/usr/local/databend/bin`.

c. Run the following command to start databend-meta:

```shell
./databend-query -c ../etc/databend-query.toml > query.log 2>&1 &
```

d. Run the following command to check if databend-meta was started successfully: 

```shell
curl -I  http://127.0.0.1:8001/v1/health
```


There you go! You have successfully deployed Databend on your computer. If you have a SQL client on your computer, try the steps below to verify the deployment:

a. Create a connection to 127.0.0.1 from your SQL client. In the connection, set the port to `3307`, and set the username to `root`.

b. Run the following commands to check if the query is successful:

```sql
CREATE TABLE t1(a int);

INSERT INTO t1 VALUES(1), (2);

SELECT * FROM t1;
```
