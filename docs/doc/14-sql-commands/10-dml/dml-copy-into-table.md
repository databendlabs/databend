---
title: "COPY INTO <table>"
sidebar_label: "COPY INTO <table>"
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.128"/>

COPY INTO allows you to load data from files located in one of the following locations:

- User / Internal / External stages: See [Understanding Stages](../../12-load-data/00-stage/00-whystage.md) to learn about stages in Databend.
- Buckets or containers created in a storage service.
- Remote servers from where you can access the files by their URL (starting with "https://...").
- [IPFS](https://ipfs.tech).

See also: [`COPY INTO <location>`](dml-copy-into-location.md)

## Syntax

```sql
COPY INTO [<database>.]<table_name>
     FROM { userStage | internalStage | externalStage | externalLocation }
[ FILES = ( '<file_name>' [ , '<file_name>' ] [ , ... ] ) ]
[ PATTERN = '<regex_pattern>' ]
[ FILE_FORMAT = ( TYPE = { CSV | TSV | PARQUET} [ formatTypeOptions ] ) ]
[ copyOptions ]
```

### FROM ...

The FROM clause specifies the source location (user stage, internal stage, external stage, or external location) from which data will be loaded into the specified table using the COPY INTO command.

When you load data from a staged file and the stage path contains special characters such as spaces or parentheses, you can enclose the entire path in single quotes, as demonstrated in the following SQL statements:

```sql
COPY INTO mytable FROM 's3://mybucket/dataset(databend)/' ...

COPY INTO mytable FROM 's3://mybucket/dataset databend/' ...
```

#### userStage

```sql
userStage ::= @~[/<path>]
```

#### internalStage

```sql
internalStage ::= @<internal_stage_name>[/<path>]
```

#### externalStage

```sql
externalStage ::= @<external_stage_name>[/<path>]
```

#### externalLocation

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="externallocation">

<TabItem value="Amazon S3-like Storage" label="Amazon S3-like Storage">

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

You can use glob patterns to specify more than one file. For example, use

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

### FILES

FILES specifies one or more file names (separated by commas) to be loaded.

### PATTERN

A [PCRE2](https://www.pcre.org/current/doc/html/)-based regular expression pattern string, enclosed in single quotes, specifying the file names to match. Click [here](#loading-data-with-pattern-matching) to see an example. For PCRE2 syntax, see http://www.pcre.org/current/doc/html/pcre2syntax.html.

:::note
Suppose there is a file `@<stage_name>/<path>/<sub_path>`, to include it, `<sub_path>` needs to match `^<regex_pattern>$`.
:::

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
| FORCE                 | COPY INTO ensures idempotence by automatically tracking and preventing the reloading of files for a default period of 7 days. This can be customized using the `load_file_metadata_expire_hours` setting to control the expiration time for file metadata.<br/>This parameter defaults to `False` meaning COPY INTO will skip duplicate files when copying data. If `True`, duplicate files will not be skipped.                                                                        | Optional |
| DISABLE_VARIANT_CHECK | If `True`, this will allow the variant field to insert invalid JSON strings. Default: `False`.                                                                                                                           | Optional |
| ON_ERROR              | Decides how to handle a file that contains errors: 'continue' to skip and proceed, 'abort' to terminate on error, 'abort_N' to terminate when errors ≥ N. Default is 'abort'. Note: 'abort_N' not available for Parquet files. | Optional |
| MAX_FILES             | Sets the maximum number of files to load that have not been loaded already. The value can be set up to 500; any value greater than 500 will be treated as 500.                                                                                                                                             | Optional |

:::tip
When importing large volumes of data, such as logs, it is recommended to set both `PURGE` and `FORCE` to True. This ensures efficient data import without the need for interaction with the Meta server (updating the copied-files set). However, it is important to be aware that this may lead to duplicate data imports.
:::

## Output

COPY INTO provides a summary of the data loading results with these columns:

| Column           | Type     | Nullable | Description                                |
|------------------|----------|----------|--------------------------------------------|
| FILE             | VARCHAR  | NO       | The relative path to the source file.       |
| ROWS_LOADED      | INT      | NO       | The number of rows loaded from the source file. |
| ERRORS_SEEN      | INT      | NO       | Number of error rows in the source file    |
| FIRST_ERROR      | VARCHAR  | YES      | The first error found in the source file.             |
| FIRST_ERROR_LINE | INT      | YES      | Line number of the first error.             |

## Managing Parallel Processing

In Databend, the `max_threads` setting specifies the maximum number of threads that can be utilized to execute a request. By default, this value is typically set to match the number of CPU cores available on the machine.

When loading data into Databend using the COPY INTO command, you can exert control over the parallel processing capabilities by injecting hints into the COPY INTO command and setting the max_threads parameter. For example:

```sql
COPY /*+ set_var(max_threads=6) */ INTO mytable FROM @mystage/ pattern='.*[.]parq' FILE_FORMAT=(TYPE=parquet);
```
For more detailed information on injecting hints, see [SET_VAR](../80-setting-cmds/03-set-var.md).

COPY INTO also supports distributed execution in cluster environments. You can enable distributed COPY INTO by setting `ENABLE_DISTRIBUTED_COPY_INTO` to 1. This helps enhance data loading performance and scalability in cluster environments.

```sql
SET ENABLE_DISTRIBUTED_COPY_INTO = 1;
```

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
FROM 'https://ci.databend.org/dataset/stateful/ontime_200{6,7,8}_200.csv'
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
