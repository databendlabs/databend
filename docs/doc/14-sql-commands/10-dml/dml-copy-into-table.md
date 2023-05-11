---
title: "COPY INTO <table>"
sidebar_label: "COPY INTO <table>"
---

The `COPY INTO` command in Databend allows you to load data from files located in multiple locations. This is the recommended method for loading a large amount of data into Databend.

One of its key features is that it provides idempotency by keeping track of files that have already been processed for a default period of 7 days, you can customize this behavior using the `load_file_metadata_expire_hours` global setting.

The files must already be staged in one of the following locations:
- Named internal stage.
- Named external stage that references an external location (Amazon S3-compatible Storage, Google Cloud Storage, or Microsoft Azure).
- External location:
  - Buckets created in `Supported Object Storage Solutions`.
  - Remote servers from where you can access the files by their URL (starting with "https://...").
  - [IPFS](https://ipfs.tech).

## Syntax

```sql
/* Standard data load */
COPY INTO [<database>.]<table_name>
     FROM { internalStage | externalStage | externalLocation }
[ FILES = ( '<file_name>' [ , '<file_name>' ] [ , ... ] ) ]
[ PATTERN = '<regex_pattern>' ]
[ FILE_FORMAT = ( TYPE = { CSV | TSV | NDJSON | PARQUET} [ formatTypeOptions ] ) ]
[ copyOptions ]

/* Data load with transformation(Only support Parquet format) */
COPY INTO [<database>.]<table_name> [ ( <col_name> [ , <col_name> ... ] ) ]
     FROM ( SELECT [<file_col> ... ]
            FROM { internalStage | externalStage } )
[ FILES = ( '<file_name>' [ , '<file_name>' ] [ , ... ] ) ]
[ PATTERN = '<regex_pattern>' ]
[ FILE_FORMAT = ( TYPE = {PARQUET} [ formatTypeOptions ] ) ]
[ copyOptions ]
```

### internalStage

```sql
internalStage ::= @<internal_stage_name>[/<path>]
```

### externalStage

```sql
externalStage ::= @<external_stage_name>[/<path>]
```

Also see [How to Create External Stage](../00-ddl/40-stage/01-ddl-create-stage.md).

### externalLocation

Databend's external location is a feature that allows users to access data stored outside of Databend's internal storage system, such as in cloud storage services like AWS S3 or Azure Blob Storage.

By defining an external location, users can query data stored in external systems directly from Databend without having to load it into Databend's internal storage.

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="externallocation">

<TabItem value="Amazon S3-compatible Storage" label="Amazon S3-compatible Storage">

```sql
externalLocation ::=
  's3://<bucket>[<path>]'
  CONNECTION = (
        ENDPOINT_URL = 'https://<endpoint-URL>'
        ACCESS_KEY_ID = '<your-access-key-ID>'
        SECRET_ACCESS_KEY = '<your-secret-access-key>'
        ROLE_ARN = '<your-ARN-of-IAM-role>'
        EXTERNAL_ID = '<your-external-ID>'
        SESSION_TOKEN = '<your-session-token>'
        REGION = '<region-name>'
        ENABLE_VIRTUAL_HOST_STYLE = 'true|false'
  )
```

:::info
To access your Amazon S3 buckets, you can use one of two methods: providing AWS access keys and secrets, or specifying an AWS IAM role and external ID for authentication. 

By specifying an AWS IAM role and external ID, you can provide more granular control over which S3 buckets a user can access. This means that if the IAM role has been granted permissions to access only specific S3 buckets, then the user will only be able to access those buckets. An external ID can further enhance security by providing an additional layer of verification. For more information, see https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-role.html
:::

| Parameter                 | Description                                                                                                                                                                           | Required     |
|---------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|
| `s3://<bucket>[<path>]`   | External files located at the AWS S3 compatible object storage.                                                                                                                       | **Required** |
| ENDPOINT_URL              | The bucket endpoint URL starting with "https://". To use a URL starting with "http://", set `allow_insecure` to `true` in the [storage] block of the file `databend-query-node.toml`. | **Required** |
| ACCESS_KEY_ID             | Your access key ID for connecting the AWS S3 compatible object storage. If not provided, Databend will access the bucket anonymously.                                                 | Optional     |
| SECRET_ACCESS_KEY         | Your secret access key for connecting the AWS S3 compatible object storage.                                                                                                           | Optional     |
| ROLE_ARN                  | Amazon Resource Name (ARN) of an AWS Identity and Access Management (IAM) role.                                                                                                       | Optional     |
| EXTERNAL_ID               | Your external ID for authentication when accessing specific Amazon S3 buckets.                                                                                                        | Optional     |
| SESSION_TOKEN             | Your temporary credential for connecting the AWS S3 service.                                                                                                                          | Optional     |
| REGION                    | AWS region name. For example, us-east-1.                                                                                                                                              | Optional     |
| ENABLE_VIRTUAL_HOST_STYLE | If you use virtual hosting to address the bucket, set it to "true".                                                                                                                   | Optional     |


</TabItem>

<TabItem value="Azure Blob Storage" label="Azure Blob Storage">

```sql
externalLocation ::=
  'azblob://<container>[<path>]'
  CONNECTION = (
        ENDPOINT_URL = 'https://<endpoint-URL>'
        ACCOUNT_NAME = '<your-account-name>'
        ACCOUNT_KEY = '<your-account-key>'
  )
```

| Parameter                      | Description                                                                                                                                                                              | Required     |
|--------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|
| `azblob://<container>[<path>]` | External files located at the Azure Blob storage.                                                                                                                                        | **Required** |
| ENDPOINT_URL                   | The container endpoint URL starting with "https://". To use a URL starting with "http://", set `allow_insecure` to `true` in the [storage] block of the file `databend-query-node.toml`. | **Required** |
| ACCOUNT_NAME                   | Your account name for connecting the Azure Blob storage. If not provided, Databend will access the container anonymously.                                                                | Optional     |
| ACCOUNT_KEY                    | Your account key for connecting the Azure Blob storage.                                                                                                                                  | Optional     |

</TabItem>

<TabItem value="Google Cloud Storage" label="Google Cloud Storage">

```sql
externalLocation ::=
  'gcs://<bucket>[<path>]'
  CONNECTION = (
        ENDPOINT_URL = 'https://<endpoint-URL>'
        CREDENTIAL = '<your-credential>'
  )
```

| Parameter                | Description                                                                                                                                                                              | Required     |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|
| `gcs://<bucket>[<path>]` | External files located at the Google Cloud Storage                                                                                                                                       | **Required** |
| ENDPOINT_URL             | The container endpoint URL starting with "https://". To use a URL starting with "http://", set `allow_insecure` to `true` in the [storage] block of the file `databend-query-node.toml`. | Optional     |
| CREDENTIAL               | Your credential for connecting the GCS. If not provided, Databend will access the container anonymously.                                                                                 | Optional     |

</TabItem>

<TabItem value="Huawei Object Storage" label="Huawei Object Storage">

```sql
externalLocation ::=
  'obs://<bucket>[<path>]'
  CONNECTION = (
        ENDPOINT_URL = 'https://<endpoint-URL>'
        ACCESS_KEY_ID = '<your-access-key-id>'
        SECRET_ACCESS_KEY = '<your-secret-access-key>'
  )
```

| Parameter                | Description                                                                                                                                                                              | Required     |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|
| `obs://<bucket>[<path>]` | External files located at the obs                                                                                                                                                        | **Required** |
| ENDPOINT_URL             | The container endpoint URL starting with "https://". To use a URL starting with "http://", set `allow_insecure` to `true` in the [storage] block of the file `databend-query-node.toml`. | **Required** |
| ACCESS_KEY_ID            | Your access key ID for connecting the OBS. If not provided, Databend will access the bucket anonymously.                                                                                 | Optional     |
| SECRET_ACCESS_KEY        | Your secret access key for connecting the OBS.                                                                                                                                           | Optional     |

</TabItem>

<TabItem value="Remote Files" label="Remote Files">

```sql
externalLocation ::=
  'https://<url>'
```

You can use glob patterns to specify moran than one file. For example, use

- `ontime_200{6,7,8}.csv` to represents `ontime_2006.csv`,`ontime_2007.csv`,`ontime_2008.csv`.
- `ontime_200[6-8].csv` to represents `ontime_2006.csv`,`ontime_2007.csv`,`ontime_2008.csv`.

</TabItem>

<TabItem value="IPFS" label="IPFS">

```sql
externalLocation ::=
  'ipfs://<your-ipfs-hash>'
  CONNECTION = (ENDPOINT_URL = 'https://<your-ipfs-gateway>')
```

</TabItem>

</Tabs>

### FILES = ( 'file1' [ , 'file2' ... ] )

Specify a list of one or more files names (separated by commas) to be loaded.

### PATTERN = 'regex_pattern'

A [PCRE2](https://www.pcre.org/current/doc/html/)-based regular expression pattern string, enclosed in single quotes, specifying the file names to match. Click [here](#loading-data-with-pattern-matching) to see an example. For PCRE2 syntax, see http://www.pcre.org/current/doc/html/pcre2syntax.html. 

### FILE_FORMAT

See [Input & Output File Formats](../../13-sql-reference/50-file-format-options.md).

### copyOptions

```sql
copyOptions ::=
  [ SIZE_LIMIT = <num> ]
  [ PURGE = <bool> ]
  [ FORCE = <bool> ]
  [ DISABLE_VARIANT_CHECK = <bool> ]
  [ ON_ERROR = { continue | abort } ]
```

| Parameter             | Description                                                                                                                                             | Required |
|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| SIZE_LIMIT            | Specifies the maximum rows of data to be loaded for a given COPY statement. Defaults to `0` meaning no limits.                                          | Optional |
| PURGE                 | If `True`, the command will purge the files in the stage after they are loaded successfully into the table. Default: `False`.                           | Optional |
| FORCE                 | Defaults to `False` meaning the command will skip duplicate files in the stage when copying data. If `True`, duplicate files will not be skipped.       | Optional |
| DISABLE_VARIANT_CHECK | If `True`, this will allow the variant field to insert invalid JSON strings. Default: `False`.                                                           | Optional |
| ON_ERROR              | Provides options to handle a file containing errors. Select `continue` to skip the file and continue, or `abort` (default) to abort the load operation. | Optional |

:::info
The parameter ON_ERROR currently does not work for parquet files.
:::

## Examples

### 1. Loading Data from an Internal Stage

```sql
COPY INTO mytable
FROM @my_internal_s1
PATTERN = '.*[.]parquet'
FILE_FORMAT = (TYPE = PARQUET);
```

### 2. Loading Data from an External Stage

```sql
COPY INTO mytable
FROM @my_external_s1
PATTERN = 'books.*parquet'
FILE_FORMAT = (TYPE = PARQUET);
```

```sql
COPY INTO mytable
FROM @my_external_s1
PATTERN = '.*[.]parquet'
FILE_FORMAT = (TYPE = PARQUET);
```

### 3. Loading Data from External Locations

<Tabs groupId="external-example">

<TabItem value="AWS S3-compatible Storage" label="AWS S3-compatible Storage">

This example reads 10 rows from a CSV file and inserts them into a table:

```sql
-- Authenticated by AWS access keys and secrets.
COPY INTO mytable
FROM 's3://mybucket/data.csv'
CONNECTION = (
    ENDPOINT_URL = 'https://<endpoint-URL>',
    ACCESS_KEY_ID = '<your-access-key-ID>',
    SECRET_ACCESS_KEY = '<your-secret-access-key>'
)
FILE_FORMAT = (type = CSV, field_delimiter = ',', record_delimiter = '\n', skip_header = 1)
SIZE_LIMIT = 10;
```

This example loads data from a CSV file without specifying the endpoint URL:

```sql
COPY INTO mytable
FROM 's3://mybucket/data.csv'
FILE_FORMAT = (type = CSV, field_delimiter = ',', record_delimiter = '\n', skip_header = 1)
SIZE_LIMIT = 10;
```

</TabItem>

<TabItem value="Azure Blob Storage" label="Azure Blob Storage">

This example reads data from a CSV file and inserts it into a table:

```sql
COPY INTO mytable
FROM 'azblob://mybucket/data.csv'
CONNECTION = (
    ENDPOINT_URL = 'https://<account_name>.blob.core.windows.net',
    ACCOUNT_NAME = '<account_name>',
    ACCOUNT_KEY = '<account_key>'
)
FILE_FORMAT = (type = CSV);
```
</TabItem>

<TabItem value="Remote Files" label="Remote Files">

As shown in this example, data is loaded from three remote CSV files, but a file will be skipped if it contains errors:

```sql
COPY INTO mytable
FROM 'https://repo.databend.rs/dataset/stateful/ontime_200{6,7,8}_200.csv'
FILE_FORMAT = (type = CSV)
ON_ERROR = continue;
```
</TabItem>

<TabItem value="IPFS Files" label="IPFS Files">

This example reads data from a CSV file on IPFS and inserts it into a table:

```sql
COPY INTO mytable
FROM 'ipfs://<your-ipfs-hash>'
CONNECTION = (endpoint_url = 'https://<your-ipfs-gateway>')
FILE_FORMAT = (type = CSV, field_delimiter = ',', record_delimiter = '\n', skip_header = 1);
```
</TabItem>

</Tabs>

### 4. Loading Data with Pattern Matching

This example uses pattern matching to only load from CSV files containing `sales` in their names:

```sql
COPY INTO mytable
FROM 's3://mybucket/'
PATTERN = '.*sales.*[.]csv'
FILE_FORMAT = (type = CSV, field_delimiter = ',', record_delimiter = '\n', skip_header = 1);
```
Where `.*` is interpreted as `zero or more occurrences of any character`. The square brackets escape the period character `(.)` that precedes a file extension.

If you want to load from all the CSV files, use `PATTERN = '.*[.]csv'`:
```sql
COPY INTO mytable
FROM 's3://mybucket/'
PATTERN = '.*[.]csv'
FILE_FORMAT = (type = CSV, field_delimiter = ',', record_delimiter = '\n', skip_header = 1);
```

### 5. Loading Data with AWS IAM Role

```sql
-- Authenticated by AWS IAM role and external ID.
COPY INTO mytable
FROM 's3://mybucket/'
CONNECTION = (
    ENDPOINT_URL = 'https://<endpoint-URL>',
    ROLE_ARN = 'arn:aws:iam::123456789012:role/my_iam_role',
    EXTERNAL_ID = '123456'
)
PATTERN = '.*[.]csv'
FILE_FORMAT = (type = CSV, field_delimiter = ',', record_delimiter = '\n', skip_header = 1);
```

### 6. Loading Data with Compression

This example reads 10 rows from a CSV file compressed as GZIP and inserts them into a table:

```sql
COPY INTO mytable
FROM 's3://mybucket/data.csv.gz'
CONNECTION = (
    ENDPOINT_URL = 'https://<endpoint-URL>',
    ACCESS_KEY_ID = '<your-access-key-ID>',
    SECRET_ACCESS_KEY = '<your-secret-access-key>'
)
FILE_FORMAT = (type = CSV, field_delimiter = ',', record_delimiter = '\n', skip_header = 1, compression = AUTO)
SIZE_LIMIT = 10;
```

### 7. Loading Parquet Files

```sql
COPY INTO mytable
FROM 's3://mybucket/'
CONNECTION = (
    ACCESS_KEY_ID = '<your-access-key-ID>',
    SECRET_ACCESS_KEY = '<your-secret-access-key>'
)
PATTERN = '.*[.]parquet'
FILE_FORMAT = (TYPE = PARQUET);
```