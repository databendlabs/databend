---
title: Deploying with Cloud Object Storage
sidebar_label: Deploying with Cloud Object Storage
description:
  How to deploy Databend with cloud object storage
---

## Deploying with Cloud Object Storage

Databend works with both self-hosted and cloud object storage solutions. This topic explains how to deploy Databend with cloud object storage. For a list of compatible cloud object storage solutions, see [Understanding Deployment Modes](./00-understanding-deployment-modes.md). 

In this topic, we will deploy a standalone Databend on a local host, then connect it to your cloud object storage.

### Setting up Cloud Object Storage

Before deploying Databend, make sure you have successfully set up your object storage environment in the cloud, and the following tasks have been completed:

- Create a bucket or container named *databend*.
- Get the endpoint URL for connecting to the bucket or container you created.
- Get the Access Key ID and Secret Access Key for your account.

:::tip

**Need Help?** For information about how to manage buckets and Access Keys for your cloud object storage, refer to the user manual from the solution provider. Here are some useful links you may need:
- Amazon S3
  - https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html
  - https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html

- Tencent COS
  - https://cloud.tencent.com/document/product/436/13309
  - https://cloud.tencent.com/document/product/436/68282

- Alibaba OSS
  - https://www.alibabacloud.com/help/zh/object-storage-service/latest/create-buckets-2
  - https://help.aliyun.com/document_detail/53045.htm

- Wasabi
  - https://wasabi.com/wp-content/themes/wasabi/docs/Getting_Started/index.html#t=topics%2FGS-Buckets.htm%23TOC_Creating_a_Bucketbc-1&rhtocid=_5_0
  - https://wasabi.com/wp-content/themes/wasabi/docs/Getting_Started/index.html#t=topics%2FAssigning_an_Access_Key.htm

- QingCloud QingStore
  - https://docsv3.qingcloud.com/storage/object-storage/manual/console/bucket_manage/basic_opt/
  - https://docs.qingcloud.com/product/api/common/overview.html

- Azure Blob Storage
  - https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal#create-a-container
  - https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal#view-account-access-keys

:::

### Downloading Databend
a. Create a folder named *databend* in the directory */usr/local*.

b. Download and extract the latest Databend package for your platform from https://github.com/datafuselabs/databend/releases.

c. Move the extracted folders *bin* and *etc* to the folder */usr/local/databend*.

### Deploying a Meta Node
a. Open the file *databend-meta-node.toml* in the folder */usr/local/databend/etc*, and replace *0.0.0.0* with *127.0.0.1* within the whole file.

b. Open a terminal window and navigate to the folder */usr/local/databend/bin*.

c. Run the following command to start the Meta node:

```curl
./databend-meta -c ../etc/databend-meta.toml > meta.log 2>&1 &
```

d. Run the following command to check if the Meta node was started successfully:

```curl
curl -I  http://127.0.0.1:28101/v1/health
```

### Deploying a Query Node
a. Open the file *databend-query-node.toml* in the folder */usr/local/databend/etc*, and replace *0.0.0.0* with *127.0.0.1* within the whole file.

b. In the file *databend-query-node.toml*, set the parameter type in [storage] block to s3 first. Then comment out the [storage.fs] block,  uncomment the [storage.s3] block and set your values in it.

```toml
[storage]
# fs | s3 | azblob
type = "s3"

# Set a local folder to store your data.
# Comment out this block if you're NOT using local file system as storage.
#[storage.fs]
#data_path = "benddata/datas"

# To use S3-compatible object storage, uncomment this block and set your values.
[storage.s3]
bucket = "<your-bucket-name>"
endpoint_url = "<your-endpoint>"
access_key_id = "<your-key-id>"
secret_access_key = "<your-account-key>"

# To use Azure Blob storage, uncomment this block and set your values.
# [storage.azblob]
# endpoint_url = "https://<your-storage-account-name>.blob.core.windows.net"
# container = "<your-azure-storage-container-name>"
# account_name = "<your-storage-account-name>"
# account_key = "<your-account-key>"
```
:::tip

If you're using Azure Blob storage, set the parameter type in [storage] block to azblob first. Then comment out the [storage.fs] block,  uncomment the [storage.azblob] block and set your values in it.

:::

c. Open a terminal window and navigate to the folder */usr/local/databend/bin*.

d. Run the following command to start the Query node:

```curl
./databend-query -c ../etc/databend-query.toml > query.log 2>&1 &
```

e. Run the following command to check if the Query node was started successfully:
curl -I  http://127.0.0.1:8081/v1/health

### Verifying Deployment
In this section, we will run MySQL queries from a SQL client installed on your local machine.

a. Create a connection to 127.0.0.1 from your SQL client. In the connection, set the port to *3307*, and set the username to *root*.

b. Run the following commands to check if the query is successful:

```sql
CREATE TABLE t1(a int);

INSERT INTO t1 VALUES(1), (2);

SELECT * FROM t1;
```