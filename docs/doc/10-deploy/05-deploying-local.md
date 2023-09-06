---
title: Docker and Local Deployments
sidebar_label: Docker and Local Deployments
description:
  Deploying Databend locally or with Docker
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.97"/>

To quickly access Databend features and gain practical expertise, you have the following deployment options:

- [Deploying Databend on Docker](#deploying-databend-on-docker): You can deploy Databend along with [MinIO](https://min.io/) on Docker for a containerized setup.
- [Deploying a Local Databend](#deploying-a-local-databend): You can opt for a local deployment and use the file system as storage if object storage is unavailable.
- [Deploying databend-local](#deploying-databend-local): databend-local is a simplified version of Databend for easy SQL interaction and testing directly from the command line, without the need for a full Databend deployment. It's perfect for developers and testers looking for a lightweight, hassle-free way to explore Databend features.

:::note non-production use only
- Object storage is a requirement for production use of Databend. The file system should only be used for evaluation, testing, and non-production scenarios. 

- It is not recommended to deploy Databend on top of MinIO for production environments or performance testing purposes.

- databend-local runs a temporary databend-query process. Data storage is in a temporary directory, and all resources, including data, are deleted when the process ends. Be cautious to prevent unintended data loss.
:::

## Deploying Databend on Docker

Before you start, ensure that you have Docker installed on your system.

### Step 1. Deploying MinIO

1. Pull and run the MinIO image as a container with the following command:

```shell
mkdir -p ${HOME}/minio/data

docker run \
   -p 9000:9000 \
   -p 9090:9090 \
   --user $(id -u):$(id -g) \
   --name minio1 \
   -e "MINIO_ROOT_USER=ROOTUSER" \
   -e "MINIO_ROOT_PASSWORD=CHANGEME123" \
   -v ${HOME}/minio/data:/data \
   quay.io/minio/minio server /data --console-address ":9090"
```
Please be aware that the command above also sets the root user credentials (ROOTUSER/CHANGEME123) which you will need to provide for authentication in the next steps. If you make changes to the root user credentials at this point, ensure that you maintain consistency throughout the entire process.

You can confirm that the MinIO container has started successfully by checking for the following message in the terminal:

```shell
Unable to find image 'quay.io/minio/minio:latest' locally
latest: Pulling from minio/minio
68c8102008d3: Pull complete 
be9f9df177bb: Pull complete 
3af46996e2ef: Pull complete 
c8b0b68d12b4: Pull complete 
4273a1648411: Pull complete 
2fd0bc041cb4: Pull complete 
Digest: sha256:ab5296018bfca75d45f451e050f6c79c6e8b9927bbc444274a74123ea7921021
Status: Downloaded newer image for quay.io/minio/minio:latest
Formatting 1st pool, 1 set(s), 1 drives per set.
WARNING: Host local has more than 0 drives of set. A host failure will result in data becoming unavailable.
MinIO Object Storage Server
Copyright: 2015-2023 MinIO, Inc.
License: GNU AGPLv3 <https://www.gnu.org/licenses/agpl-3.0.html>
Version: RELEASE.2023-04-13T03-08-07Z (go1.20.3 linux/arm64)

Status:         1 Online, 0 Offline. 
API: http://172.17.0.2:9000  http://127.0.0.1:9000 
Console: http://172.17.0.2:9090 http://127.0.0.1:9090 

Documentation: https://min.io/docs/minio/linux/index.html
Warning: The standard parity is set to 0. This can lead to data loss.
```

2. Open your web browser and visit http://127.0.0.1:9090/ (login credentials: ROOTUSER/CHANGEME123). Create a bucket named **databend**.

### Step 2. Deploying Databend

Pull and run the Databend image as a container with the following command:

```shell
docker run \
    -p 8000:8000 \
    -p 3307:3307 \
    -v meta_storage_dir:/var/lib/databend/meta \
    -v query_storage_dir:/var/lib/databend/query \
    -v log_dir:/var/log/databend \
    -e QUERY_DEFAULT_USER=databend \
    -e QUERY_DEFAULT_PASSWORD=databend \
    -e QUERY_STORAGE_TYPE=s3 \
    -e AWS_S3_ENDPOINT=http://172.17.0.2:9000 \
    -e AWS_S3_BUCKET=databend \
    -e AWS_ACCESS_KEY_ID=ROOTUSER \
    -e AWS_SECRET_ACCESS_KEY=CHANGEME123 \
    datafuselabs/databend
```

When starting the Databend Docker container, you can specify the username and password using the environment variables QUERY_DEFAULT_USER and QUERY_DEFAULT_PASSWORD. If these variables are not provided, a default root user will be created without a password. The command above creates a SQL user (databend/databend) which you will need to use to connect to Databend in the next step. If you make changes to the SQL user at this point, ensure that you maintain consistency throughout the entire process.

### Step 3. Connecting to Databend

To establish a connection with Databend, you'll use the BendSQL CLI tool in this step. For instructions on how to install and operate BendSQL, see [BendSQL](../13-sql-clients/01-bendsql.md).

1. To establish a connection with Databend using the SQL user (databend/databend), run the following command:

```shell
eric@bogon ~ % bendsql -udatabend -pdatabend
Welcome to BendSQL 0.3.11-17b0d8b(2023-06-08T15:23:29.206137000Z).
Trying connect to localhost:8000 as user databend.
Connected to DatabendQuery v1.1.75-nightly-59eea5df495245b9475f81a28c7b688f013aac05(rust-1.72.0-nightly-2023-06-28T01:04:32.054683000Z)
```

2. To verify the deployment, you can create a table and insert some data with BendSQL:

```shell
databend@localhost> CREATE DATABASE eric;
Processed in (0.083 sec)

databend@localhost> CREATE TABLE mytable(a int);
Processed in (0.051 sec)

databend@localhost> INSERT INTO mytable VALUES(1);
1 rows affected in (0.242 sec)

databend@localhost> INSERT INTO mytable VALUES(2);
1 rows affected in (0.060 sec)

databend@localhost> INSERT INTO mytable VALUES(3);
1 rows affected in (0.053 sec)
```

As the table data is stored in the bucket, you will notice an increase in the bucket size from 0.

![Alt text](../../public/img/deploy/minio-deployment-verify.png)

## Deploying a Local Databend

The following steps will guide you through the process of locally deploying Databend.

### Step 1. Downloading Databend

1. Download the installation package suitable for your platform from the [Download](https://databend.rs/download) page.

2. Extract the installation package to a local directory.

### Step 2. Starting Databend

1. Configure an admin user. You will utilize this account to connect to Databend. For more information, see [Configuring Admin Users](../13-sql-clients/00-admin-users.md). For this example, uncomment the following lines to choose this account:

```sql
[[query.users]]
name = "root"
auth_type = "no_password"
```

2. Open a terminal and navigate to the folder where the extracted files and folders are stored.

3. Run the script **start.sh** in the folder **scripts**:

    MacOS might prompt an error saying "*databend-meta can't be opened because Apple cannot check it for malicious software.*". To proceed, open **System Settings** on your Mac, select **Privacy & Security** on the left menu, and click **Open Anyway** for databend-meta in the **Security** section on the right side. Do the same for the error on databend-query.

```shell
./scripts/start.sh
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

3. Run the following command to verify Databend has started successfully:

```shell
ps aux | grep databend

---
eric             12789   0.0  0.0 408495808   1040 s003  U+    2:16pm   0:00.00 grep databend
eric             12781   0.0  0.5 408790416  38896 s003  S     2:15pm   0:00.05 bin/databend-query --config-file=configs/databend-query.toml
eric             12776   0.0  0.3 408654368  24848 s003  S     2:15pm   0:00.06 bin/databend-meta --config-file=configs/databend-meta.toml
```

### Step 3. Connecting to Databend

To establish a connection with Databend, you'll use the BendSQL CLI tool in this step. For instructions on how to install and operate BendSQL, see [BendSQL](../13-sql-clients/01-bendsql.md).

1. To establish a connection with a local Databend, execute the following command:

```shell
eric@bogon ~ % bendsql      
Welcome to BendSQL 0.3.11-17b0d8b(2023-06-08T15:23:29.206137000Z).
Trying connect to localhost:8000 as user root.
Connected to DatabendQuery v1.1.75-nightly-59eea5df495245b9475f81a28c7b688f013aac05(rust-1.72.0-nightly-2023-06-28T01:04:32.054683000Z)
```

2. Query the Databend version to verify the connection:

```sql
root@localhost> SELECT VERSION();

SELECT
  VERSION()

┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                          version()                                                         │
│                                                           String                                                           │
├────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ DatabendQuery v1.1.75-nightly-59eea5df495245b9475f81a28c7b688f013aac05(rust-1.72.0-nightly-2023-06-28T01:04:32.054683000Z) │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
1 row in 0.024 sec. Processed 1 rows, 1B (41.85 rows/s, 41B/s)
```

## Deploying databend-local

### Deployment Steps

1. Download the installation package suitable for your platform from the [Download](https://databend.rs/download) page, and extract the **databend-query** binary located in the **bin** folder from the installation package.
2. Add the path to the **databend-query** binary to your PATH environment variable. For example, if your **databend-query** binary is located in the folder */Users/eric/Downloads/data*, set the PATH environment variable as follows:

```bash
macdeMacBook-Pro:rsdoc eric$ export PATH=/Users/eric/Downloads/data:$PATH
```

3. Create an alias called "bend-local" for the "databend-query local" command:

```bash
macdeMacBook-Pro:rsdoc eric$ alias bend-local="databend-query local"
```

4. Run databend-local:

```bash
macdeMacBook-Pro:rsdoc eric$ bend-local
Welcome to Databend, version v1.2.100-nightly-29d6bf3217(rust-1.72.0-nightly-2023-09-05T16:14:14.152454000Z).

databend-local:) 
```

To exit databend-local, simple type "exit":

```bash
databend-local:) exit
Bye~
macdeMacBook-Pro:rsdoc eric$
```

To view available arguments for databend-local:

```bash
macdeMacBook-Pro:rsdoc eric$ bend-local --help
Usage: databend-query local [OPTIONS]

Options:
  -q, --query <QUERY>                  [default: ]
      --output-format <OUTPUT_FORMAT>  [default: ]
  -h, --help                           Print help
```

| Argument            | Description                                          |
|---------------------|------------------------------------------------------|
| -q, --query         | Specifies the query to be executed.                 |
| --output-format     | Determines the file format for saving query results. |
| -h, --help          | Displays usage instructions.                          |

### Usage Examples

The following examples shed light on how to use databend-local.

#### Example 1: Querying from Command-Line

```bash
macdeMacBook-Pro:rsdoc eric$ bend-local
Welcome to Databend, version v1.2.100-nightly-29d6bf3217(rust-1.72.0-nightly-2023-09-05T16:14:14.152454000Z).

databend-local:) select max(a) from range(1,1000) t(a);
┌────────────┐
│   max(a)   │
│ Int64 NULL │
├────────────┤
│ 999        │
└────────────┘
1 row result in 0.013 sec. Processed 999 rows, 999 B (76.89 thousand rows/s, 600.67 KiB/s)
```

#### Example 2: Saving Results to a File

This example demonstrates how to create a Parquet file in a single command.

```bash
bend-local --query "select number, number + 1 as b from numbers(10)" --output-format parquet > /tmp/a.parquet
```

#### Example 3: Analyzing Data using Shell Pipe Mode

This example demonstrates the use of shell pipe mode to analyze data. The $STDIN macro interprets stdin as a temporary stage table.

```bash
echo '3,4' | bend-local -q "select \$1 a, \$2 b  from \$STDIN  (file_format => 'csv') " --output-format table

SELECT $1 AS a, $2 AS b FROM 'fs:///dev/fd/0' (FILE_FORMAT => 'csv')

┌─────────────────┐
│    a   │    b   │
│ String │ String │
├────────┼────────┤
│ '3'    │ '4'    │
└─────────────────┘
```

#### Example 4: Reading Staged Files

This example demonstrates how to read data from staged files.

```bash
bend-local --query "select count() from 'fs:///tmp/a.parquet'  (file_format => 'parquet')"

10
```

#### Example 5: Analyzing System Processes

This example is about analyzing system processes to find memory usage per user.

```bash
ps aux | tail -n +2 | awk '{ printf("%s\t%s\n", $1, $4) }' | bend-local -q "select  \$1 as user,  sum(\$2::double) as memory  from \$STDIN  (file_format => 'tsv')  group by user  "

_fpsd   0.0
_hidd   0.0
_nearbyd        0.1
_timed  0.0
_netbios        0.0
_trustd 0.1
root    5.899999999999998
_biome  0.1
...
```

#### Example 6: Data Transformation

This example demonstrates data transformation from one format to another, supporting formats CSV, TSV, Parquet, and NDJSON.

```bash
bend-local -q 'select rand() as a, rand() as b from numbers(100)' > /tmp/a.tsv

cat /tmp/a.tsv | bend-local -q "select \$1 a, \$2 b  from \$STDIN  (file_format => 'tsv') " --output-format parquet > /tmp/a.parquet
```

## Next Steps

After deploying Databend, you might need to learn about the following topics:

- [SQL Clients](/doc/integrations/clients): Learn to connect to Databend using SQL clients.
- [Manage Settings](../13-sql-reference/42-manage-settings.md): Optimize Databend for your needs. 
- [Load & Unload Data](/doc/load): Manage data import/export in Databend.
- [Visualize](/doc/integrations): Integrate Databend with visualization tools for insights.