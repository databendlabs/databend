---
title: Deploying with Self-Hosted Object Storage
sidebar_label: Deploying with Self-Hosted Object Storage
description:
  How to deploy Databend with self-hosted object storage
---

## Deploying with Self-Hosted Object Storage

Databend works with both self-hosted and cloud object storage solutions. This topic uses MinIO as an example to explain how to deploy Databend with self-hosted object storage. For a list of compatible self-hosted object storage solutions, see [Understanding Deployment Modes](./00-understanding-deployment-modes.md).

For the sake of simplicity, in this topic we will deploy [MinIO](https://min.io/) and Databend (including a Meta node and a Query node) on a same local machine.

### Deploying MinIO
In this section, we will run a standalone MinIO server on your local machine first, and then create a bucket for Databend in the MinIO Console.

#### Downloading MinIO
a. Follow the [MinIO Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide.html) to download and install the MinIO package to your local machine.

b. Open a terminal window and navigate to the folder where MinIO is stored.

c. Run the command *vim server.sh* to create a file with the following content:

```curl
~/minio$ cat server.sh
export MINIO_ROOT_USER=minioadmin
export MINIO_ROOT_PASSWORD=minioadmin
./minio server --address :9900 ./data
```
d. Run the following commands to start the MinIO server:

```curl
chmod +x server.sh
./server.sh
```

#### Creating a Bucket

a. In your browser, go to http://127.0.0.1:9900 and enter the credentials (minioadmin / minioadmin) to log in to the MinIO Console.

b. In the MinIO Console, create a bucket named *databend*.

### Deploying Databend

In this section, we will deploy a Meta node and a Query node on your local machine.

#### Downloading Databend
a. Create a folder named databend in the directory */usr/local*.

b. Download and extract the latest Databend package for your platform from https://github.com/datafuselabs/databend/releases.

c. Move the extracted folders *bin* and *etc* to the folder */usr/local/databend*.

#### Deploying a Meta node

a. Open the file *databend-meta-node.toml* in the folder */usr/local/databend/etc*, and replace *0.0.0.0* with *127.0.0.1* in the whole file.

b. Open a terminal window and navigate to the folder /usr/local/databend/bin.

c. Run the following command to start the Meta node:
```curl
./databend-meta -c ../etc/databend-meta.toml > meta.log 2>&1 &
```

d. Run the following command to check if the Meta node was started successfully:

```curl
curl -I  http://127.0.0.1:28101/v1/health
```

#### Deploying a Query node
a. Open the file *databend-query-node.toml* in the folder */usr/local/databend/etc*, and replace *0.0.0.0* with *127.0.0.1* in the whole file.

b. In the file databend-query-node.toml, replace the [storage] and [storage.fs] sections with the following content:

```toml
[storage]
# s3
type = "s3"

[storage.s3]
bucket = "databend"
endpoint_url = "http://127.0.0.1:9900"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
```
c. Open a terminal window and navigate to the folder */usr/local/databend/bin*.

d. Run the following command to start the Query node:

```curl
./databend-query -c ../etc/databend-query.toml > query.log 2>&1 &
```

e. Run the following command to check if the Query node was started successfully:

```curl
curl -I  http://127.0.0.1:8081/v1/health
```

### Verifying Deployment
In this section, we will run MySQL queries from a SQL client installed on your local machine.

a. Create a connection to 127.0.0.1 from your SQL client. In the connection, set the port to *3307*, and set the username to *root*.

b. Run the following commands to check if the query is successful:

```sql
CREATE TABLE t1(a int);

INSERT INTO t1 VALUES(1), (2);

SELECT * FROM t1;
```