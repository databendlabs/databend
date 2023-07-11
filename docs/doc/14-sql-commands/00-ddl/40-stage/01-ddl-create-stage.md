---
title: CREATE STAGE
---

Creates an internal or external stage.

## Syntax

```sql
-- Internal stage
CREATE STAGE [ IF NOT EXISTS ] <internal_stage_name>
  [ FILE_FORMAT = ( { TYPE = { PARQUET | CSV | TSV | NDJSON } [ formatTypeOptions ] ) } ]
  [ COPY_OPTIONS = ( copyOptions ) ]
  [ COMMENT = '<string_literal>' ]

-- External stage
CREATE STAGE [ IF NOT EXISTS ] <external_stage_name>
    externalStageParams
  [ FILE_FORMAT = ( { TYPE = { PARQUET | CSV | TSV | NDJSON } [ formatTypeOptions ] ) } ]
  [ COPY_OPTIONS = ( copyOptions ) ]
  [ COMMENT = '<string_literal>' ]
```

### externalStageParams

:::note
If the `ENDPOINT_URL` parameter is not specified in the command, Databend will create the stage on Amazon S3 by default. Therefore, when you create an external stage on an S3-compatible object storage or other object storage solutions, be sure to include the `ENDPOINT_URL` parameter.
:::

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="externalstageparams">

<TabItem value="Amazon S3-compatible Storage" label="Amazon S3-compatible">

```sql
externalStageParams ::=
  URL = 's3://<bucket>[<path/>]'
  CONNECTION = (
        ENDPOINT_URL = 'https://<endpoint-URL>'
        ACCESS_KEY_ID = '<your-access-key-ID>'
        SECRET_ACCESS_KEY = '<your-secret-access-key>'
        ROLE_ARN = '<your-ARN-of-IAM-role>'
        EXTERNAL_ID = '<your-external-ID>'
        REGION = '<region-name>'
        ENABLE_VIRTUAL_HOST_STYLE = 'true'|'false'
  )
```

:::note
To create an external stage on Amazon S3, you can also use an IAM user account, enabling you to define fine-grained access controls for the stage, including specifying actions such as read or write access to specific S3 buckets. See [Example 3: Create External Stage with AWS IAM User](#example-3-create-external-stage-with-aws-iam-user).
:::

| Parameter                 | Description                                                                                                                                                                           | Required |
|---------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| URL                       | External files located at the AWS S3 compatible object storage.                                                                                                                       | Required |
| ENDPOINT_URL              | The bucket endpoint URL starting with "https://". To use a URL starting with "http://", set `allow_insecure` to `true` in the [storage] block of the file `databend-query-node.toml`. | Optional |
| ACCESS_KEY_ID             | Your access key ID for connecting the AWS S3 compatible object storage. If not provided, Databend will access the bucket anonymously.                                                 | Optional |
| SECRET_ACCESS_KEY         | Your secret access key for connecting the AWS S3 compatible object storage.                                                                                                           | Optional |
| ROLE_ARN                  | Amazon Resource Name (ARN) of an AWS Identity and Access Management (IAM) role.                                                                                                       | Optional |
| EXTERNAL_ID               | Your external ID for authentication when accessing specific Amazon S3 buckets.                                                                                                        | Optional |
| REGION                    | AWS region name. For example, us-east-1.                                                                                                                                              | Optional |
| ENABLE_VIRTUAL_HOST_STYLE | If you use virtual hosting to address the bucket, set it to "true".                                                                                                                   | Optional |

</TabItem>

<TabItem value="Azure Blob Storage" label="Azure Blob">

```sql
externalStageParams ::=
  URL = 'azblob://<container>[<path/>]'
  CONNECTION = (
        ENDPOINT_URL = 'https://<endpoint-URL>'
        ACCOUNT_NAME = '<your-account-name>'
        ACCOUNT_KEY = '<your-account-key>'
  )
```

| Parameter    | Description                                                                                                                                                                              | Required |
|--------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| URL          | External files located at the Azure Blob storage.                                                                                                                                        | Required |
| ENDPOINT_URL | The container endpoint URL starting with "https://". To use a URL starting with "http://", set `allow_insecure` to `true` in the [storage] block of the file `databend-query-node.toml`. | Required |
| ACCOUNT_NAME | Your account name for connecting the Azure Blob storage. If not provided, Databend will access the container anonymously.                                                                | Optional |
| ACCOUNT_KEY  | Your account key for connecting the Azure Blob storage.                                                                                                                                  | Optional |

</TabItem>

<TabItem value="Google Cloud Storage" label="Google Cloud">

```sql
externalLocation ::=
  URL = 'gcs://<bucket>[<path>]'
  CONNECTION = (
        ENDPOINT_URL = 'https://<endpoint-URL>'
        CREDENTIAL = '<your-credential>'
  )
```

| Parameter    | Description                                                                                                                                                                              | Required |
|--------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| URL          | External files located at the Google Cloud Storage                                                                                                                                       | Required |
| ENDPOINT_URL | The container endpoint URL starting with "https://". To use a URL starting with "http://", set `allow_insecure` to `true` in the [storage] block of the file `databend-query-node.toml`. | Required |
| CREDENTIAL   | Your credential for connecting the GCS. If not provided, Databend will access the container anonymously.                                                                                 | Optional |

</TabItem>

<TabItem value="Huawei Object Storage" label="Huawei OBS">

```sql
externalLocation ::=
  URL = 'obs://<bucket>[<path>]'
  CONNECTION = (
        ENDPOINT_URL = 'https://<endpoint-URL>'
        ACCESS_KEY_ID = '<your-access-key-id>'
        SECRET_ACCESS_KEY = '<your-secret-access-key>'
  )
```

| Parameter         | Description                                                                                                                                                                              | Required |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| URL               | External files located at the obs                                                                                                                                                        | Required |
| ENDPOINT_URL      | The container endpoint URL starting with "https://". To use a URL starting with "http://", set `allow_insecure` to `true` in the [storage] block of the file `databend-query-node.toml`. | Required |
| ACCESS_KEY_ID     | Your access key ID for connecting the OBS. If not provided, Databend will access the bucket anonymously.                                                                                 | Optional |
| SECRET_ACCESS_KEY | Your secret access key for connecting the OBS.                                                                                                                                           | Optional |

</TabItem>

<TabItem value="WebHDFS Storage" label="WebHDFS">

```sql
externalLocation ::=
  URL = "webhdfs://<endpoint_url[<path>]"
  CONNECTION = (
    [ HTTPS = 'true'|'false' ]
    [ DELEGATION = '<your-delegation-token>' ]
  )
```

| Parameter  | Description                                                                                                                                                                                                                                         | Required |
|------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| URL        | External endpoint and location of files.                                                                                                                                                                                                            | Required |
| HTTPS      | Connect to RESTful API with HTTP or HTTPS, set to `true` will use `HTTPS`, else `HTTP`. Use `HTTPS` by default. To use a URL starting with "http://", set `allow_insecure` to `true` in the [storage] block of the file `databend-query-node.toml`. | Optional |
| DELEGATION | Delegation token of WebHDFS for authentication. If not set, no tokens will be used in further operations.                                                                                                                                           | Optional |

</TabItem>

</Tabs>

### formatTypeOptions

For details about `FILE_FORMAT`, see [Input & Output File Formats](../../../13-sql-reference/50-file-format-options.md).

### copyOptions

```
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
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:DeleteObject",
                "s3:DeleteObjectVersion",
                "s3:ListBucket"
            ],
            "Resource": "arn:aws:s3:::databend-toronto/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::databend-toronto/*",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "/*"
                    ]
                }
            }
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