---
title: "COPY INTO <table>"
sidebar_label: "COPY INTO <table>"
---

The `COPY INTO` command in Databend allows you to load data from files located in multiple locations. This is the recommended method for loading a large amount of data into Databend.

One of its key features is that it provides idempotency by keeping track of files that have already been processed for a default period of 7 days, you can customize this behavior using the `load_file_metadata_expire_hours` global setting.

The files must exist in one of the following locations:

- User / Internal / External stages: See [Understanding Stages](../../12-load-data/00-stage/00-whystage.md) to learn about stages in Databend.
- Buckets or containers created in a storage service.
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

### externalLocation

This allows you to access data stored outside of Databend, such as in cloud storage services like AWS S3 or Azure Blob Storage. By specifying an external location, you can query data stored there directly from Databend without the need to load it into Databend.

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="externallocation">

<TabItem value="Amazon S3-like Storage Services" label="Amazon S3-like Storage Services">

```sql
externalLocation ::=
  's3://<bucket>[<path>]'
  CONNECTION = (
        <connection_parameters>
  )
```
For the connection parameters available for accessing Amazon S3-like storage services, see [Connection Parameters](/13-sql-reference/51-connect-parameters.md).
</TabItem>

<TabItem value="Azure Blob Storage" label="Azure Blob Storage">

```sql
externalLocation ::=
  'azblob://<container>[<path>]'
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

<TabItem value="Hadoop Distributed File System (HDFS)" label="HDFS">

```sql
externalLocation ::=
  'hdfs://<endpoint_url>[<path>]'
  CONNECTION = (
        <connection_parameters>
  )
```

For the connection parameters available for accessing HDFS, see [Connection Parameters](/13-sql-reference/51-connect-parameters.md).
</TabItem>

<TabItem value="WebHDFS" label="WebHDFS">

```sql
externalLocation ::=
  'webhdfs://<endpoint_url>[<path>]'
  CONNECTION = (
        <connection_parameters>
  )
```

For the connection parameters available for accessing WebHDFS, see [Connection Parameters](/13-sql-reference/51-connect-parameters.md).
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
  [ ON_ERROR = { continue | abort | abort_N } ]
  [ MAX_FILES = <num> ]
```

| Parameter             | Description                                                                                                                                                                                                              | Required |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| SIZE_LIMIT            | Specifies the maximum rows of data to be loaded for a given COPY statement. Defaults to `0` meaning no limits.                                                                                                           | Optional |
| PURGE                 | If `True`, the command will purge the files in the stage after they are loaded successfully into the table. Default: `False`.                                                                                            | Optional |
| FORCE                 | Defaults to `False` meaning the command will skip duplicate files in the stage when copying data. If `True`, duplicate files will not be skipped.                                                                        | Optional |
| DISABLE_VARIANT_CHECK | If `True`, this will allow the variant field to insert invalid JSON strings. Default: `False`.                                                                                                                           | Optional |
| ON_ERROR              | Decides how to handle a file that contains errors: 'continue' to skip and proceed, 'abort' to terminate on error, 'abort_N' to terminate when errors ≥ N. Default is 'abort'. Note: 'abort_N' not available for Parquet files. | Optional |
| MAX_FILES             | Sets the maximum number of files to load that have not been loaded already. The value can be set up to 500; any value greater than 500 will be treated as 500.                                                                                                                                             | Optional |

:::tip
When importing large volumes of data, such as logs, it is recommended to set both `PURGE` and `FORCE` to True. This ensures efficient data import without the need for interaction with the Meta server (updating the copied-files set). However, it is important to be aware that this may lead to duplicate data imports.
:::

### Output

The command returns the following columns:

| Column           | DataType | Nullable | Description                                |
|------------------|----------|----------|--------------------------------------------|
| FILE             | VARCHAR  | No       | The relative path to the source file       |
| ROWS_LOADED      | INT      | NO       | Number of rows loaded from the source file |
| ERRORS_SEEN      | INT      | NO       | Number of error rows in the source file    |
| FIRST_ERROR      | VARCHAR  | YES      | First error of the source file             |
| FIRST_ERROR_LINE | INT      | YES      | Line number of the first error             |


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
    ENDPOINT_URL = 'https://<endpoint-URL>'
    ACCESS_KEY_ID = '<your-access-key-ID>'
    SECRET_ACCESS_KEY = '<your-secret-access-key>'
)
FILE_FORMAT = (type = CSV field_delimiter = ',' record_delimiter = '\n' skip_header = 1)
SIZE_LIMIT = 10;
```

This example loads data from a CSV file without specifying the endpoint URL:

```sql
COPY INTO mytable
FROM 's3://mybucket/data.csv'
FILE_FORMAT = (type = CSV field_delimiter = ',' record_delimiter = '\n' skip_header = 1)
SIZE_LIMIT = 10;
```

</TabItem>

<TabItem value="Azure Blob Storage" label="Azure Blob Storage">

This example reads data from a CSV file and inserts it into a table:

```sql
COPY INTO mytable
FROM 'azblob://mybucket/data.csv'
CONNECTION = (
    ENDPOINT_URL = 'https://<account_name>.blob.core.windows.net'
    ACCOUNT_NAME = '<account_name>'
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
FILE_FORMAT = (type = CSV field_delimiter = ',' record_delimiter = '\n' skip_header = 1);
```
</TabItem>

</Tabs>

### 4. Loading Data with Pattern Matching

This example uses pattern matching to only load from CSV files containing `sales` in their names:

```sql
COPY INTO mytable
FROM 's3://mybucket/'
PATTERN = '.*sales.*[.]csv'
FILE_FORMAT = (type = CSV field_delimiter = ',' record_delimiter = '\n' skip_header = 1);
```
Where `.*` is interpreted as `zero or more occurrences of any character`. The square brackets escape the period character `(.)` that precedes a file extension.

If you want to load from all the CSV files, use `PATTERN = '.*[.]csv'`:
```sql
COPY INTO mytable
FROM 's3://mybucket/'
PATTERN = '.*[.]csv'
FILE_FORMAT = (type = CSV field_delimiter = ',' record_delimiter = '\n' skip_header = 1);
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
FILE_FORMAT = (type = CSV field_delimiter = ',' record_delimiter = '\n' skip_header = 1);
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
FILE_FORMAT = (type = CSV field_delimiter = ',' record_delimiter = '\n' skip_header = 1 compression = AUTO)
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

### 8. Controlling Parallel Processing

In Databend, the *max_threads* setting specifies the maximum number of threads that can be utilized to execute a request. By default, this value is typically set to match the number of CPU cores available on the machine.

When loading data into Databend with COPY INTO, you can control the parallel processing capabilities by injecting hints into the COPY INTO command and setting the *max_threads* parameter. For example:

```sql
COPY /*+ set_var(max_threads=6) */ INTO mytable FROM @mystage/ pattern='.*[.]parq' FILE_FORMAT=(TYPE=parquet);
```

For more information about injecting hints, see [SET_VAR](../80-setting-cmds/03-set-var.md).
