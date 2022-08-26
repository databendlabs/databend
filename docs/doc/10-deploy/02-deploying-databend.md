---
title: Deploying a Standalone Databend
sidebar_label: Deploying a Standalone Databend
description:
  Deploying a Standalone Databend
---
import GetLatest from '@site/src/components/GetLatest';

## Deploying a Standalone Databend

Databend works with both self-hosted and cloud object storage solutions. This topic explains how to deploy Databend with your object storage. For a list of supported object storage solutions, see [Understanding Deployment Modes](./00-understanding-deployment-modes.md).

### Setting up Your Object Storage

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="operating-systems">

<TabItem value="MinIO" label="MinIO">

a. Follow the [MinIO Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide.html) to download and install the MinIO package to your local machine.

b. Open a terminal window and navigate to the folder where MinIO is stored.

c. Run the command `vim server.sh` to create a file with the following content:

```shell
~/minio$ cat server.sh
export MINIO_ROOT_USER=minioadmin
export MINIO_ROOT_PASSWORD=minioadmin
./minio server --address :9900 ./data
```
d. Run the following commands to start the MinIO server:

```shell
chmod +x server.sh
./server.sh
```

e. In your browser, go to http://127.0.0.1:9900 and enter the credentials (`minioadmin` / `minioadmin`) to log in to the MinIO Console.

f. In the MinIO Console, create a bucket named `databend`.

</TabItem>

<TabItem value="Amazon S3" label="Amazon S3">

Before deploying Databend, make sure you have successfully set up your object storage environment in the cloud, and the following tasks have been completed:

- Create a bucket or container named `databend`.
- Get the endpoint URL for connecting to the bucket or container you created.
- Get the Access Key ID and Secret Access Key for your account.

For information about how to manage buckets and Access Keys for your cloud object storage, refer to the user manual from the solution provider. Here are some useful links you may need:

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html
- https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html

</TabItem>

<TabItem value="Google GCS" label="Google GCS">

Before deploying Databend, make sure you have successfully set up your object storage environment in the cloud, and the following tasks have been completed:

- Create a bucket named `databend`.
- Get the Google Cloud Storage OAuth2 credential of your account.

For information about how to manage buckets and OAuth2 credentials in Google Cloud Storage, refer to the user manual from the solution provider. Here are some useful links you may need:

- https://cloud.google.com/storage/docs/creating-buckets
- https://cloud.google.com/storage/docs/authentication#apiauth

</TabItem>

<TabItem value="Tencent COS" label="Tencent COS">

Before deploying Databend, make sure you have successfully set up your object storage environment in the cloud, and the following tasks have been completed:

- Create a bucket or container named `databend`.
- Get the endpoint URL for connecting to the bucket or container you created.
- Get the Access Key ID and Secret Access Key for your account.

For information about how to manage buckets and Access Keys for your cloud object storage, refer to the user manual from the solution provider. Here are some useful links you may need:

- https://cloud.tencent.com/document/product/436/13309
- https://cloud.tencent.com/document/product/436/68282

</TabItem>

<TabItem value="Alibaba OSS" label="Alibaba OSS">

Before deploying Databend, make sure you have successfully set up your object storage environment in the cloud, and the following tasks have been completed:

- Create a bucket or container named `databend`.
- Get the endpoint URL for connecting to the bucket or container you created.
- Get the Access Key ID and Secret Access Key for your account.

For information about how to manage buckets and Access Keys for your cloud object storage, refer to the user manual from the solution provider. Here are some useful links you may need:

- https://www.alibabacloud.com/help/zh/object-storage-service/latest/create-buckets-2
- https://help.aliyun.com/document_detail/53045.htm

</TabItem>

<TabItem value="Wasabi" label="Wasabi">

Before deploying Databend, make sure you have successfully set up your object storage environment in the cloud, and the following tasks have been completed:

- Create a bucket or container named `databend`.
- Get the endpoint URL for connecting to the bucket or container you created.
- Get the Access Key ID and Secret Access Key for your account.

For information about how to manage buckets and Access Keys for your cloud object storage, refer to the user manual from the solution provider. Here are some useful links you may need:

- https://wasabi.com/wp-content/themes/wasabi/docs/Getting_Started/index.html#t=topics%2FGS-Buckets.htm%23TOC_Creating_a_Bucketbc-1&rhtocid=_5_0
- https://wasabi.com/wp-content/themes/wasabi/docs/Getting_Started/index.html#t=topics%2FAssigning_an_Access_Key.htm

</TabItem>

<TabItem value="QingCloud QingStore" label="QingCloud QingStore">

Before deploying Databend, make sure you have successfully set up your object storage environment in the cloud, and the following tasks have been completed:

- Create a bucket or container named `databend`.
- Get the endpoint URL for connecting to the bucket or container you created.
- Get the Access Key ID and Secret Access Key for your account.

For information about how to manage buckets and Access Keys for your cloud object storage, refer to the user manual from the solution provider. Here are some useful links you may need:

- https://docsv3.qingcloud.com/storage/object-storage/manual/console/bucket_manage/basic_opt/
- https://docs.qingcloud.com/product/api/common/overview.html

</TabItem>

<TabItem value="Azure Blob Storage" label="Azure Blob Storage">

Before deploying Databend, make sure you have successfully set up your object storage environment in the cloud, and the following tasks have been completed:

- Create a bucket or container named `databend`.
- Get the endpoint URL for connecting to the bucket or container you created.
- Get the Access Key ID and Secret Access Key for your account.

For information about how to manage buckets and Access Keys for your cloud object storage, refer to the user manual from the solution provider. Here are some useful links you may need:

- https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal#create-a-container
- https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal#view-account-access-keys

</TabItem>
</Tabs>

### Downloading Databend
a. Create a folder named `databend` in the directory `/usr/local`.

b. Download and extract the latest Databend release for your platform from [Github Release](https://github.com/datafuselabs/databend/releases):

<Tabs groupId="operating-systems">
<TabItem value="linux" label="Linux">

```shell
curl -LJO https://github.com/datafuselabs/databend/releases/download/${version}/databend-${version}-x86_64-unknown-linux-musl.tar.gz
```

</TabItem>
<TabItem value="mac" label="MacOS">

```shell
curl -LJO https://github.com/datafuselabs/databend/releases/download/${version}/databend-${version}-aarch64-apple-darwin.tar.gz
```

</TabItem>

<TabItem value="arm" label="Linux Arm">

```shell
curl -LJO https://github.com/datafuselabs/databend/releases/download/${version}/databend-${version}-aarch64-unknown-linux-musl.tar.gz
```

</TabItem>
</Tabs>

<Tabs groupId="operating-systems">
<TabItem value="linux" label="Linux">

```shell
tar xzvf databend-${version}-x86_64-unknown-linux-musl.tar.gz
```

</TabItem>
<TabItem value="mac" label="MacOS">

```shell
tar xzvf databend-${version}-aarch64-apple-darwin.tar.gz
```

</TabItem>

<TabItem value="arm" label="Linux Arm">

```shell
tar xzvf databend-${version}-aarch64-unknown-linux-musl.tar.gz
```

</TabItem>
</Tabs>

c. Move the extracted folders `bin` and `configs` to the folder `/usr/local/databend`.

### Deploying a Meta Node
a. Open the file `databend-meta.toml` in the folder `/usr/local/databend/configs`, and replace `127.0.0.1` with `0.0.0.0` within the whole file.

b. Open a terminal window and navigate to the folder `/usr/local/databend/bin`.

c. Run the following command to start the Meta node:

```shell
./databend-meta -c ../configs/databend-meta.toml > meta.log 2>&1 &
```

d. Run the following command to check if the Meta node was started successfully:

```shell
curl -I  http://127.0.0.1:28101/v1/health
```

### Deploying a Query Node
a. Open the file `databend-query.toml` in the folder `/usr/local/databend/configs`, and replace `127.0.0.1` with `0.0.0.0` within the whole file.

b. In the file `databend-query.toml`, set the parameter `type` in [storage] block to `s3` if you're using a S3 compatible object storage, or `azblob` if you're using Azure Blob storage.

```toml
[storage]
# fs | s3 | azblob | gcs
type = "s3"
```

c. Comment out the `[storage.fs]` block first, and then uncomment the `[storage.s3]` block if you're using a S3 compatible object storage, or uncomment the `[storage.azblob]` block if you're using Azure Blob storage.

```toml
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

# To use Google Cloud Storage, uncomment this block and set your values.
# [storage.gcs]
# bucket = "<your-bucket-name>"
# credential = "<your-credential>"
```

d. Set your values in the `[storage.s3]`, `[storage.azblob]` or `[storage.GCS]` block. Please note that the field `endpoint_url` refers to the service URL of your storage region and varies depending on the object storage solution you use:

<Tabs groupId="operating-systems">
<TabItem value="MinIO" label="MinIO">

```toml
[storage]
# s3
type = "s3"

[storage.s3]
endpoint_url = "http://127.0.0.1:9900"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
```

</TabItem>

<TabItem value="Amazon S3" label="Amazon S3">

```toml
[storage]
# s3
type = "s3"

[storage.s3]
# https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html
bucket = "databend"
endpoint_url = "https://s3.amazonaws.com"

# How to get access_key_id and secret_access_key:
# https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html
// highlight-next-line
access_key_id = "<your-key-id>"
// highlight-next-line
secret_access_key = "<your-access-key>"
```

</TabItem>

<TabItem value="Google GCS" label="Google GCS">

```toml
[storage]
# gcs
type = "gcs"

[storage.gcs]
# How to create a bucket:
# https://cloud.google.com/storage/docs/creating-buckets
// highlight-next-line
bucket = "databend-1.048596"

# GCS also supports changing the endpoint URL
# but the endpoint should be compatible with GCS's JSON API
# default:
# endpoint_url = "https://storage.googleapis.com/"

# working directory of GCS
# default:
# root = "/"

// highlight-next-line
credential = "<your-credential>"
```

</TabItem>

<TabItem value="Tencent COS" label="Tencent COS">

```toml
[storage]
# s3
type = "s3"

[storage.s3]
# How to create a bucket:
# https://cloud.tencent.com/document/product/436/13309
// highlight-next-line
bucket = "databend-1253727613"

# You can get the URL from the bucket detail page.
// highlight-next-line
endpoint_url = "https://cos.ap-beijing.myqcloud.com"

# How to get access_key_id and secret_access_key:
# https://cloud.tencent.com/document/product/436/68282
// highlight-next-line
access_key_id = "<your-key-id>"
// highlight-next-line
secret_access_key = "<your-access-key>"
```

:::tip
In this example COS region is `ap-beijing`.
:::

</TabItem>

<TabItem value="Alibaba OSS" label="Alibaba OSS">

```toml
[storage]
# s3
type = "s3"

[storage.s3]
# How to create a bucket:
// highlight-next-line
bucket = "databend"

# You can get the URL from the bucket detail page.
// highlight-next-line
# https://help.aliyun.com/document_detail/31837.htm
// highlight-next-line
# https://<bucket-name>.<region-id>[-internal].aliyuncs.com
// highlight-next-line
endpoint_url = "https://oss-cn-beijing-internal.aliyuncs.com"
enable_virtual_host_style = true

# How to get access_key_id and secret_access_key:
# https://help.aliyun.com/document_detail/53045.htm
// highlight-next-line
access_key_id = "<your-key-id>"
// highlight-next-line
secret_access_key = "<your-access-key>"
```

:::tip
In this example OSS region id is `oss-cn-beijing-internal`.
:::

</TabItem>

<TabItem value="Wasabi" label="Wasabi">

```toml
[storage]
# s3
type = "s3"

[storage.s3]
# How to create a bucket:
// highlight-next-line
bucket = "<your-bucket>"

# You can get the URL from:
# https://wasabi-support.zendesk.com/hc/en-us/articles/360015106031-What-are-the-service-URLs-for-Wasabi-s-different-regions-
// highlight-next-line
endpoint_url = "https://s3.us-east-2.wasabisys.com"

# How to get access_key_id and secret_access_key:
// highlight-next-line
access_key_id = "<your-key-id>"
// highlight-next-line
secret_access_key = "<your-access-key>"
```

:::tip
In this example Wasabi region is `us-east-2`.
:::

</TabItem>

<TabItem value="QingCloud QingStore" label="QingCloud QingStore">

```toml
[storage]
# s3
type = "s3"

[storage.s3]
bucket = "databend"

# You can get the URL from the bucket detail page.
# https://docsv3.qingcloud.com/storage/object-storage/intro/object-storage/#zone
endpoint_url = "https://s3.pek3b.qingstor.com"

# How to get access_key_id and secret_access_key:
# https://docs.qingcloud.com/product/api/common/overview.html
access_key_id = "<your-key-id>"
secret_access_key = "<your-access-key>"
```

:::tip
In this example QingStore region is `pek3b`.
:::

</TabItem>

<TabItem value="Azure Blob Storage" label="Azure Blob Storage">

```toml
[storage]
# azblob
type = "azblob"

[storage.azblob]
endpoint_url = "https://<your-storage-account-name>.blob.core.windows.net"

# https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal#create-a-container
container = "<your-azure-storage-container-name>"
account_name = "<your-storage-account-name>"

# https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal#view-account-access-keys
account_key = "<your-account-key>"
```

</TabItem>
</Tabs>

e. Open a terminal window and navigate to the folder `/usr/local/databend/bin`.

f. Run the following command to start the Query node:

```shell
./databend-query -c ../configs/databend-query.toml > query.log 2>&1 &
```

g. Run the following command to check if the Query node was started successfully:
```shell
curl -I  http://127.0.0.1:8081/v1/health
```

### Verifying Deployment

In this section, we will run some queries against Databend to verify the deployment.

a. Download and install a MySQL client on your local machine.

b. Create a connection to 127.0.0.1 from your SQL client. In the connection, set the port to `3307`, and set the username to `root`.

:::tip

**Create new users**. The `root` user only works when you access Databend from localhost. You will need to create new users and grant proper privileges first to connect to Databend remotely. For example, 

```sql
-- Create a user named "eric" with the password "databend"
CREATE USER eric IDENTIFIED BY 'databend';

-- Grant the ALL privilege on all existing tables in the default database to the user eric:
GRANT ALL ON default.* TO eric;
```

For more information about creating new users, see [CREATE USER](../30-reference/30-sql/00-ddl/30-user/01-user-create-user.md).

:::

c. Run the following commands and check if the query is successful:

```sql
CREATE TABLE t1(a int);

INSERT INTO t1 VALUES(1), (2);

SELECT * FROM t1;
```
<GetLatest/>
