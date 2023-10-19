---
title: Deploying a Standalone Databend
sidebar_label: Deploying a Standalone Databend
description: Deploying a Standalone Databend
---

import GetLatest from '@site/src/components/GetLatest';

## Deploying a Standalone Databend

Databend works with both self-hosted and cloud object storage solutions. This topic explains how to deploy Databend with your object storage. For a list of supported object storage solutions, see [Understanding Deployment Modes](./00-understanding-deployment-modes.md).

:::note
It is not recommended to deploy Databend on top of MinIO for production environments or performance testing.
:::

### Setting up Your Object Storage

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="operating-systems">


<TabItem value="Amazon S3" label="Amazon S3">

Before deploying Databend, make sure you have successfully set up your object storage environment in the cloud, and the following tasks have been completed:

- Create a bucket or container named `databend`.
- Get the endpoint URL for connecting to the bucket or container you created.
- Get the Access Key ID and Secret Access Key for your account.

For information about how to manage buckets and Access Keys for your cloud object storage, refer to the user manual from the solution provider. Here are some useful links you may need:

- <https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html>
- <https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html>

</TabItem>

<TabItem value="Google GCS" label="Google GCS">

Before deploying Databend, make sure you have successfully set up your object storage environment in the cloud, and the following tasks have been completed:

- Create a bucket named `databend`.
- Get the Google Cloud Storage OAuth2 credential of your account.

For information about how to manage buckets and OAuth2 credentials in Google Cloud Storage, refer to the user manual from the solution provider. Here are some useful links you may need:

- <https://cloud.google.com/storage/docs/creating-buckets>
- <https://cloud.google.com/storage/docs/authentication#apiauth>

</TabItem>

<TabItem value="Azure Blob" label="Azure Blob">

Before deploying Databend, make sure you have successfully set up your object storage environment in the cloud, and the following tasks have been completed:

- Create a bucket or container named `databend`.
- Get the endpoint URL for connecting to the bucket or container you created.
- Get the Access Key ID and Secret Access Key for your account.

For information about how to manage buckets and Access Keys for your cloud object storage, refer to the user manual from the solution provider. Here are some useful links you may need:

- <https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal#create-a-container>
- <https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal#view-account-access-keys>

</TabItem>

<TabItem value="Tencent COS" label="Tencent COS">

Before deploying Databend, make sure you have successfully set up your object storage environment in the cloud, and the following tasks have been completed:

- Create a bucket or container named `databend`.
- Get the endpoint URL for connecting to the bucket or container you created.
- Get the Access Key ID and Secret Access Key for your account.

For information about how to manage buckets and Access Keys for your cloud object storage, refer to the user manual from the solution provider. Here are some useful links you may need:

- <https://cloud.tencent.com/document/product/436/13309>
- <https://cloud.tencent.com/document/product/436/68282>

</TabItem>

<TabItem value="Alibaba OSS" label="Alibaba OSS">

Before deploying Databend, make sure you have successfully set up your object storage environment in the cloud, and the following tasks have been completed:

- Create a bucket or container named `databend`.
- Get the endpoint URL for connecting to the bucket or container you created.
- Get the Access Key ID and Secret Access Key for your account.

For information about how to manage buckets and Access Keys for your cloud object storage, refer to the user manual from the solution provider. Here are some useful links you may need:

- <https://www.alibabacloud.com/help/zh/object-storage-service/latest/create-buckets-2>
- <https://help.aliyun.com/document_detail/53045.htm>

</TabItem>


<TabItem value="QingCloud QingStor" label="QingCloud QingStor">

Before deploying Databend, make sure you have successfully set up your object storage environment in the cloud, and the following tasks have been completed:

- Create a bucket or container named `databend`.
- Get the endpoint URL for connecting to the bucket or container you created.
- Get the Access Key ID and Secret Access Key for your account.

For information about how to manage buckets and Access Keys for your cloud object storage, refer to the user manual from the solution provider. Here are some useful links you may need:

- <https://docsv3.qingcloud.com/storage/object-storage/manual/console/bucket_manage/basic_opt/>
- <https://docs.qingcloud.com/product/api/common/overview.html>

</TabItem>

<TabItem value="Wasabi" label="Wasabi">

Before deploying Databend, make sure you have successfully set up your object storage environment in the cloud, and the following tasks have been completed:

- Create a bucket or container named `databend`.
- Get the endpoint URL for connecting to the bucket or container you created.
- Get the Access Key ID and Secret Access Key for your account.

For information about how to manage buckets and Access Keys for your cloud object storage, refer to the user manual from the solution provider. Here are some useful links you may need:

- <https://docs.wasabi.com/docs/creating-a-bucket>
- <https://docs.wasabi.com/docs/access-keys-1>

</TabItem>

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

e. In your browser, go to <http://127.0.0.1:9900> and enter the credentials (`minioadmin` / `minioadmin`) to log into the MinIO Console.

f. In the MinIO Console, create a bucket named `databend`.

</TabItem>

<TabItem value="WebHDFS" label="WebHDFS">

Before deploying Databend, make sure you have successfully set up your Hadoop environment, and the following tasks have been completed:

- Enable the WebHDFS support on Hadoop.
- Get the endpoint URL for connecting to WebHDFS.
- Get the delegation token used for authentication (if needed).

For information about how to enable and manage WebHDFS on Apache Hadoop, please refer to the manual of WebHDFS. Here are some links you may find useful:

- <https://hadoop.apache.org/docs/r3.3.2/hadoop-project-dist/hadoop-hdfs/WebHDFS.html>

</TabItem>
</Tabs>

### Downloading Databend

a. Create a folder named `databend` in the directory `/usr/local`.

b. Download and extract the latest Databend release for your platform from [GitHub Release](https://github.com/datafuselabs/databend/releases):

<Tabs groupId="operating-systems">
<TabItem value="linux-x86_64" label="Linux(x86)">

```shell
curl -LJO https://github.com/datafuselabs/databend/releases/download/${version}/databend-${version}-x86_64-unknown-linux-musl.tar.gz
```

</TabItem>
<TabItem value="linux-arm64" label="Linux(arm)">

```shell
curl -LJO https://github.com/datafuselabs/databend/releases/download/${version}/databend-${version}-aarch64-unknown-linux-musl.tar.gz
```

</TabItem>
<TabItem value="mac-x86_64" label="MacOS(x86)">

```shell
curl -LJO https://github.com/datafuselabs/databend/releases/download/${version}/databend-${version}-x86_64-apple-darwin.tar.gz
```

</TabItem>
<TabItem value="mac-arm64" label="MacOS(arm)">

```shell
curl -LJO https://github.com/datafuselabs/databend/releases/download/${version}/databend-${version}-aarch64-apple-darwin.tar.gz
```

</TabItem>
</Tabs>

<Tabs groupId="operating-systems">
<TabItem value="linux-x86_64" label="Linux(x86)">

```shell
tar xzvf databend-${version}-x86_64-unknown-linux-musl.tar.gz
```

</TabItem>
<TabItem value="linux-arm64" label="Linux(arm)">

```shell
tar xzvf databend-${version}-aarch64-unknown-linux-musl.tar.gz
```

</TabItem>
<TabItem value="mac-x86_64" label="MacOS(x86)">

```shell
tar xzvf databend-${version}-x86_64-apple-darwin.tar.gz
```

</TabItem>
<TabItem value="mac-arm64" label="MacOS(arm)">

```shell
tar xzvf databend-${version}-aarch64-apple-darwin.tar.gz
```

</TabItem>
</Tabs>

c. Move the extracted folders `bin`, `configs`, and `scripts` to the folder `/usr/local/databend`.

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

b. In the file `databend-query.toml`, set the parameter *type* in the [storage] block and configure the access credentials and endpoint URL for connecting to your object storage. 

To configure your storage settings, please comment out the [storage.fs] section by adding '#' at the beginning of each line, and then uncomment the appropriate section for your object storage provider by removing the '#' symbol, and fill in the necessary values. If your desired storage provider is not listed, you can copy and paste the corresponding template below to the file and configure it accordingly.

<Tabs groupId="operating-systems">

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
bucket = "databend"

# GCS also supports changing the endpoint URL
# but the endpoint should be compatible with GCS's JSON API
# default:
# endpoint_url = "https://storage.googleapis.com"

# working directory of GCS
# default:
# root = "/"

// highlight-next-line
credential = "<your-credential>"
```

</TabItem>

<TabItem value="Azure Blob" label="Azure Blob">

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


<TabItem value="Tencent COS" label="Tencent COS">

```toml
[storage]
# s3
type = "cos"

[storage.cos]
# You can get the URL from the bucket detail page.
# The following is an example where the region is Beijing (ap-beijing):
// highlight-next-line
endpoint_url = "https://cos.ap-beijing.myqcloud.com"

# How to create a bucket:
# https://cloud.tencent.com/document/product/436/13309
// highlight-next-line
bucket = "databend"

# How to get secret_id and secret_key:
# https://cloud.tencent.com/document/product/436/68282
// highlight-next-line
secret_id = "<your-secret-id>"
// highlight-next-line
secret_key = "<your-secret-key>"
root = "<your-root-path>"
```
Tencent COS also supports loading configuration values from environment variables. This means that instead of specifying the configuration values directly in the configuration file, you can configure COS storage by setting the corresponding environment variables.

To do this, you can still use the same [storage.cos] section in the configuration file, but omit the settings secret_id, secret_key, and root. Instead, set the corresponding environment variables (TENCENTCLOUD_SECRETID, TENCENTCLOUD_SECRETKEY, and USER_CODE_ROOT) with the desired values.

```toml
[storage]
# s3
type = "cos"

[storage.cos]
# You can get the URL from the bucket detail page.
# The following is an example where the region is ap-beijing:
// highlight-next-line
endpoint_url = "https://cos.ap-beijing.myqcloud.com"

# How to create a bucket:
# https://cloud.tencent.com/document/product/436/13309
// highlight-next-line
bucket = "databend"
```

</TabItem>

<TabItem value="Alibaba OSS" label="Alibaba OSS">

```toml
[storage]
# s3
type = "oss"

[storage.oss]
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
# enable_virtual_host_style = true

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


<TabItem value="QingCloud QingStor" label="QingCloud QingStor">

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
In this example QingStor region is `pek3b`.
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


<TabItem value="MinIO" label="MinIO">

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

</TabItem>


<TabItem value="WebHDFS" label="WebHDFS">

```toml
[storage]
type = "webhdfs"
[storage.webhdfs]
endpoint_url = "https://hadoop.example.com:9870"
root = "/analyses/databend/storage"
# if your webhdfs needs authentication, uncomment and set with your value
# delegation = "<delegation-token>"
```

</TabItem>
</Tabs>

c. Configure an admin user with the [query.users] sections. For more information, see [Configuring Admin Users](../13-sql-clients/00-admin-users.md). To proceed with the default root user and the authentication type "no_password", ensure that you remove the '#' character before the following lines in the file `databend-query.toml`:

:::caution
Using "no_password" authentication for the root user in this tutorial is just an example and not recommended for production due to potential security risks.
:::

```toml title='databend-query.toml'
...
[[query.users]]
name = "root"
auth_type = "no_password"
...
```

d. Open a terminal window and navigate to the folder `/usr/local/databend/bin`.

e. Run the following command to start the Query node:

```shell
./databend-query -c ../configs/databend-query.toml > query.log 2>&1 &
```

f. Run the following command to check if the Query node was started successfully:

```shell
curl -I  http://127.0.0.1:8080/v1/health
```

### Verifying Deployment

In this section, we will run some queries against Databend to verify the deployment.

a. Download and install a MySQL client on your local machine.

b. Create a connection to 127.0.0.1 from your SQL client. In the connection, set the port to `3307`, and set the username to `root`.

c. Run the following commands and check if the query is successful:

```sql
CREATE TABLE t1(a int);

INSERT INTO t1 VALUES(1), (2);

SELECT * FROM t1;
```

### Starting and Stopping Databend

Each time you start and stop Databend, simply run the scripts in the folder `/usr/local/databend/scripts`:

```shell
# Start Databend
./scripts/start.sh

# Stop Databend
./scripts/stop.sh
```
:::tip
In case you encounter the subsequent error messages while attempting to start Databend:

```shell
==> query.log <==
: No getcpu support: percpu_arena:percpu
: option background_thread currently supports pthread only
Databend Query start failure, cause: Code: 1104, Text = failed to create appender: Os { code: 13, kind: PermissionDenied, message: "Permission denied" }.
```
Run the following commands and try starting Databend again:

```shell
sudo mkdir /var/log/databend
sudo mkdir /var/lib/databend
sudo chown -R $USER /var/log/databend
sudo chown -R $USER /var/lib/databend
```
:::
<GetLatest/>

## Next Steps

After deploying Databend, you might need to learn about the following topics:

- [SQL Clients](/doc/sql-clients): Learn to connect to Databend using SQL clients.
- [Manage Settings](../13-sql-reference/42-manage-settings.md): Optimize Databend for your needs.
- [Load & Unload Data](/doc/load-data): Manage data import/export in Databend.
- [Visualize](/doc/visualize): Integrate Databend with visualization tools for insights.

