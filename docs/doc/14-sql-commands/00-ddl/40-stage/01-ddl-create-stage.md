---
title: CREATE STAGE
---

Creates an internal or external stage.

## Syntax

```sql
-- Internal stage
CREATE STAGE [ IF NOT EXISTS ] <internal_stage_name>
  [ FILE_FORMAT = (
         FORMAT_NAME = '<your-custom-format>'
         | TYPE = { CSV | TSV | NDJSON | PARQUET | XML } [ formatTypeOptions ]
       ) ]
  [ COPY_OPTIONS = ( copyOptions ) ]
  [ COMMENT = '<string_literal>' ]

-- External stage
CREATE STAGE [ IF NOT EXISTS ] <external_stage_name>
    externalStageParams
  [ FILE_FORMAT = (
         FORMAT_NAME = '<your-custom-format>'
         | TYPE = { CSV | TSV | NDJSON | PARQUET | XML } [ formatTypeOptions ]
       ) ]
  [ COPY_OPTIONS = ( copyOptions ) ]
  [ COMMENT = '<string_literal>' ]
```

### externalStageParams

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="externalstageparams">

<TabItem value="Amazon S3-compatible Storage" label="Amazon S3-like Storage Services">

```sql
externalStageParams ::=
  's3://<bucket>[<path/>]'
  CONNECTION = (
        <connection_parameters>
  )
```

For the connection parameters available for accessing Amazon S3-like storage services, see [Connection Parameters](/13-sql-reference/51-connect-parameters.md).

:::note
To create an external stage on Amazon S3, you can also use an IAM user account, enabling you to define fine-grained access controls for the stage, including specifying actions such as read or write access to specific S3 buckets. See [Example 3: Create External Stage with AWS IAM User](#example-3-create-external-stage-with-aws-iam-user).
:::
</TabItem>

<TabItem value="Azure Blob Storage" label="Azure Blob Storage">

```sql
externalStageParams ::=
  'azblob://<container>[<path/>]'
  CONNECTION = (
        <connection_parameters>
  )
```

For the connection parameters available for accessing Azure Blob Storage, see [Connection Parameters](/13-sql-reference/51-connect-parameters.md).
</TabItem>

<TabItem value="Google Cloud Storage" label="Google Cloud Storage">

```sql
externalLocation ::=
  'gcs://<bucket>[<path>]'
  CONNECTION = (
        <connection_parameters>
  )
```

For the connection parameters available for accessing Google Cloud Storage, see [Connection Parameters](/13-sql-reference/51-connect-parameters.md).
</TabItem>

<TabItem value="Alibaba Cloud OSS" label="Alibaba Cloud OSS">

```sql
externalLocation ::=
  'oss://<bucket>[<path>]'
  CONNECTION = (
        <connection_parameters>
  )
```

For the connection parameters available for accessing Alibaba Cloud OSS, see [Connection Parameters](/13-sql-reference/51-connect-parameters.md).
</TabItem>

<TabItem value="Tencent Cloud Object Storage" label="Tencent Cloud Object Storage">

```sql
externalLocation ::=
  'cos://<bucket>[<path>]'
  CONNECTION = (
        <connection_parameters>
  )
```

For the connection parameters available for accessing Tencent Cloud Object Storage, see [Connection Parameters](/13-sql-reference/51-connect-parameters.md).
</TabItem>

<TabItem value="HDFS" label="HDFS">

```sql
externalLocation ::=
  "hdfs://<endpoint_url>[<path>]"
  CONNECTION = (
        <connection_parameters>
  )
```

For the connection parameters available for accessing HDFS, see [Connection Parameters](/13-sql-reference/51-connect-parameters.md).
</TabItem>

<TabItem value="WebHDFS" label="WebHDFS">

```sql
externalLocation ::=
  "webhdfs://<endpoint_url>[<path>]"
  CONNECTION = (
        <connection_parameters>
  )
```

For the connection parameters available for accessing WebHDFS, see [Connection Parameters](/13-sql-reference/51-connect-parameters.md).
</TabItem>
</Tabs>

### FILE_FORMAT

See [Input & Output File Formats](../../../13-sql-reference/50-file-format-options.md) for details.

### copyOptions

```sql
copyOptions ::=
  [ SIZE_LIMIT = <num> ]
  [ PURGE = <bool> ]
```

| Parameters           | Description                                                                                                                   | Required |
|----------------------|-------------------------------------------------------------------------------------------------------------------------------|----------|
| `SIZE_LIMIT = <num>` | Number (> 0) that specifies the maximum rows of data to be loaded for a given COPY statement. Default `0`                     | Optional |
| `PURGE = <bool>`     | True specifies that the command will purge the files in the stage if they are loaded successfully into table. Default `false` | Optional |

## Examples

### Example 1: Create Internal Stage

This example creates an internal stage named *my_internal_stage*:

```sql
CREATE STAGE my_internal_stage;

DESC STAGE my_internal_stage;

name             |stage_type|stage_params                                                  |copy_options                                                                                                                                                  |file_format_options             |number_of_files|creator           |comment|
-----------------+----------+--------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------+---------------+------------------+-------+
my_internal_stage|Internal  |StageParams { storage: Fs(StorageFsConfig { root: "_data" }) }|CopyOptions { on_error: AbortNum(1), size_limit: 0, max_files: 0, split_size: 0, purge: false, single: false, max_file_size: 0, disable_variant_check: false }|Parquet(ParquetFileFormatParams)|              0|'root'@'127.0.0.1'|       |

```

### Example 2: Create External Stage with AWS Access Key

This example creates an external stage named *my_s3_stage* on Amazon S3:

```sql
CREATE STAGE my_s3_stage URL='s3://load/files/' CONNECTION = (ACCESS_KEY_ID = '<your-access-key-id>' SECRET_ACCESS_KEY = '<your-secret-access-key>');

DESC STAGE my_s3_stage;
+-------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+
| name        | stage_type | stage_params                                                                                                                                                           | copy_options                                  | file_format_options                                                                                                | comment |
+-------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+
| my_s3_stage | External   | StageParams { storage: S3(StageS3Storage { bucket: "load", path: "/files/", credentials_aws_key_id: "", credentials_aws_secret_key: "", encryption_master_key: "" }) } | CopyOptions { on_error: None, size_limit: 0 } | FileFormatOptions { format: Csv, skip_header: 0, field_delimiter: ",", record_delimiter: "\n", compression: None } |         |
+-------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+
```

### Example 3: Create External Stage with AWS IAM User

This example creates an external stage named *iam_external_stage* on Amazon S3 with an AWS Identity and Access Management (IAM) user.

#### Step 1: Create Access Policy for S3 Bucket

The procedure below creates an access policy named *databend-access* for the bucket *databend-toronto* on Amazon S3:

1. Log into the AWS Management Console, then select **Services** > **Security, Identity, & Compliance** > **IAM**.
2. Select **Account settings** in the left navigation pane, and go to the **Security Token Service (STS)** section on the right page. Make sure the status of AWS region where your account belongs is **Active**.
3. Select **Policies** in the left navigation pane, then select **Create policy** on the right page.
4. Click the **JSON** tab, copy and paste the following code to the editor, then save the policy as *databend_access*.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllObjectActions",
      "Effect": "Allow",
      "Action": [
        "s3:*Object"
      ],
      "Resource": "arn:aws:s3:::databend-toronto/*"
    },
    {
      "Sid": "ListObjectsInBucket",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::databend-toronto"
    }
  ]
}
```

#### Step 2: Create IAM User

The procedure below creates an IAM user named *databend* and attach the access policy *databend-access* to the user.

1. Select **Users** in the left navigation pane, then select **Add users** on the right page.
2. Configure the user:
    - Set the user name to *databend*.
    - When setting permissions for the user, click **Attach policies directly**, then search for and select the access policy *databend-access*.
3. After the user is created, click the user name to open the details page and select the **Security credentials** tab.
4. In the **Access keys** section, click **Create access key**.
5. Select **Third-party service** for the use case, and tick the checkbox below to confirm creation of the access key.
6. Copy and save the generated access key and secret access key to a safe place.

#### Step 3: Create External Stage

Use the access key and secret access key generated for the IAM user *databend* to create an external stage.

```sql
CREATE STAGE iam_external_stage url = 's3://databend-toronto' CONNECTION =(aws_key_id='<your-access-key-id>' aws_secret_key='<your-secret-access-key>' region='us-east-2');
```

### Example 4: Create External Stage on Cloudflare R2

[Cloudflare R2](https://www.cloudflare.com/en-ca/products/r2/) is an object storage service introduced by Cloudflare that is fully compatible with Amazon's AWS S3 service. This example creates an external stage named *r2_stage* on Cloudflare R2.

#### Step 1: Create Bucket

The procedure below creates a bucket named *databend* on Cloudflare R2.

1. Log into the Cloudflare dashboard, and select **R2** in the left navigation pane.
2. Click **Create bucket** to create a bucket, and set the bucket name to *databend*. Once the bucket is successfully created, you can find the bucket endpoint right below the bucket name when you view the bucket details page.

#### Step 2: Create R2 API Token

The procedure below creates an R2 API token that includes an Access Key ID and a Secret Access Key.

1. Click **Manage R2 API Tokens** on **R2** > **Overview**.
2. Click **Create API token** to create an API token.
3. When configuring the API token, select the necessary permission and set the **TTL** as needed.
4. Click **Create API Token** to obtain the Access Key ID and Secret Access Key. Copy and save them to a safe place.

#### Step 3: Create External Stage

Use the created Access Key ID and Secret Access Key to create an external stage named *r2_stage*.

```sql
CREATE STAGE r2_stage
  URL='s3://databend/'
  CONNECTION = (
    REGION = 'auto'
    ENDPOINT_URL = '<your-bucket-endpoint>'
    ACCESS_KEY_ID = '<your-access-key-id>'
    SECRET_ACCESS_KEY = '<your-secret-access-key>');
```
