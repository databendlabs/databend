---
title: 'COPY INTO <table> FROM STAGED FILES'
sidebar_label: 'COPY INTO <table> FROM STAGED FILES'
description:
  'Loads data from staged files'
---

`COPY` moves data between Databend tables and object storage systems (AWS S3 compatible object storage services and Azure Blob storage).

This command loads data into a table from files staged in one of the following locations:

* Named internal stage, files can be staged using the [PUT to Stage](../../00-api/10-put-to-stage.md).
* Named external stage that references an external location (AWS S3 compatible object storage services and Azure Blob storage).
* External location. This includes AWS S3 compatible object storage services and Azure Blob storage.

`COPY` can also load data into a table from one or more remote files by their URL. See [COPY INTO \<table\> FROM REMOTE FILES](dml-copy-into-table-url.md).

## Syntax

```sql
COPY INTO [<database>.]<table_name>
FROM { internalStage | externalStage | externalLocation }
[ FILES = ( '<file_name>' [ , '<file_name>' ] [ , ... ] ) ]
[ PATTERN = '<regex_pattern>' ]
[ FILE_FORMAT = ( TYPE = { CSV | JSON | NDJSON | PARQUET } [ formatTypeOptions ] ) ]
[ copyOptions ]
```

Where:

### internalStage

```sql
internalStage ::= @<internal_stage_name>[/<path>]
```

### externalStage

```sql
externalStage ::= @<external_stage_name>[/<path>]
```

### externalLocation

AWS S3 compatible object storage services:

```sql
externalLocation ::=
  's3://<bucket>[<path>]'
  CONNECTION = (
        ENDPOINT_URL = 'https://<endpoint-URL>'
        ACCESS_KEY_ID = '<your-access-key-ID>'
        SECRET_ACCESS_KEY = '<your-secret-access-key>'
        REGION = '<region-name>'
        ENABLE_VIRTUAL_HOST_STYLE = true|false
  )
```

| Parameter                 	| Description                                                                 	| Required 	|
|---------------------------	|-----------------------------------------------------------------------------	|----------	|
| `s3://<bucket>[<path>]`    	| External files located at the AWS S3 compatible object storage.             	| Optional 	|
| ENDPOINT_URL              	| The bucket endpoint URL starting with "https://". To use a URL starting with "http://", set `allow_insecure` to `true` in the [storage] block of the file `databend-query-node.toml`.                                  	| Optional 	|
| ACCESS_KEY_ID             	| Your access key ID for connecting the AWS S3 compatible object storage. If not provided, Databend will access the bucket anonymously.    	| Optional 	|
| SECRET_ACCESS_KEY         	| Your secret access key for connecting the AWS S3 compatible object storage. 	| Optional 	|
| REGION                    	| AWS region name. For example, us-east-1.                                    	| Optional 	|
| ENABLE_VIRTUAL_HOST_STYLE 	| If you use virtual hosting to address the bucket, set it to "true".                               	| Optional 	|

Azure Blob storage：

```sql
externalLocation ::=
  'azblob://<container>[<path>]'
  CONNECTION = (
        ENDPOINT_URL = 'https://<endpoint-URL>'
        ACCOUT_NAME = '<your-account-name>'
        ACCOUNT_KEY = '<your-account-key>'
  )
```

| Parameter                  	| Description                                              	| Required 	|
|----------------------------	|----------------------------------------------------------	|----------	|
| `azblob://<container>[<path>]` 	| External files located at the Azure Blob storage.        	| Required 	|
| ENDPOINT_URL               	| The container endpoint URL starting with "https://". To use a URL starting with "http://", set `allow_insecure` to `true` in the [storage] block of the file `databend-query-node.toml`.    	| Optional 	|
| ACCOUNT_NAME               	| Your account name for connecting the Azure Blob storage. If not provided, Databend will access the container anonymously.	| Optional 	|
| ACCOUNT_KEY                	| Your account key for connecting the Azure Blob storage.  	| Optional 	|


### FILES = ( 'file_name' [ , 'file_name' ... ] )

Specifies a list of one or more files names (separated by commas) to be loaded.

### PATTERN = 'regex_pattern'

A regular expression pattern string, enclosed in single quotes, specifying the file names to match.

### formatTypeOptions

```
formatTypeOptions ::=
  RECORD_DELIMITER = '<character>' 
  FIELD_DELIMITER = '<character>' 
  SKIP_HEADER = <integer>
  COMPRESSION = AUTO | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | NONE
```

#### `RECORD_DELIMITER = '<character>'`

Description: One character that separate records in an input file.

Default: `'\n'`

#### `FIELD_DELIMITER = '<character>'`

Description: One character that separate fields in an input file.

Default: `','` (comma)

#### `SKIP_HEADER = '<integer>'`

Description: Number of lines at the start of the file to skip.

Default: `0`

#### `COMPRESSION = AUTO | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | XZ | NONE`

Description: String that represents the compression algorithm.

Default: `NONE`

Values:

| Values        | Notes                                                           | 
|---------------|-----------------------------------------------------------------|
| `AUTO`        | Auto detect compression via file extensions                     |
| `GZIP`        |                                                                 |
| `BZ2`         |                                                                 |
| `BROTLI`      | Must be specified if loading/unloading Brotli-compressed files. |
| `ZSTD`        | Zstandard v0.8 (and higher) is supported.                       |
| `DEFLATE`     | Deflate-compressed files (with zlib header, RFC1950).           |
| `RAW_DEFLATE` | Deflate-compressed files (without any header, RFC1951).         |
| `XZ` |                                                                 |
| `NONE`        | Indicates that the files have not been compressed.              |

### copyOptions
```
copyOptions ::=
  [ SIZE_LIMIT = <num> ]
```

| Parameters  | Description | Required |
| ----------- | ----------- | --- |
| `SIZE_LIMIT = <num>` | Number (> 0) that specifies the maximum rows of data to be loaded for a given COPY statement. Default `0` | Optional |

## Examples

### Loading Files from Internal Stage

First, create a named internal stage:

```sql
CREATE STAGE my_internal_s1;
```

Then, PUT a local file to `my_internal_s1` stage with [PUT to Stage](../../00-api/10-put-to-stage.md) API:

```shell
curl  -H "stage_name:my_internal_s1" -F "upload=@books.parquet" -XPUT "http://localhost:8081/v1/upload_to_stage"
```

Finally, copy the file into `mytable` from the `my_internal_s1` named internal stage:

```sql
LIST @my_internal_s1;
COPY INTO mytable FROM @my_internal_s1 pattern = 'books.*parquet' file_format = (type = 'PARQUET');
```

### Loading Files from External Stage

First, create a named external stage:

```sql
CREATE STAGE my_external_s1 url = 's3://testbucket/admin/data/' credentials=(aws_key_id='minioadmin' aws_secret_key='minioadmin');
```

Then, copy the file into `mytable` from the `my_external_s1` named external stage:

```sql
COPY INTO mytable FROM @my_external_s1 pattern = 'books.*parquet' file_format = (type = 'PARQUET');
```

### Loading Files Directly from External Location

**AWS S3 compatible object storage services**

This example reads 10 rows from a CSV file and inserts them into a table:

```sql
COPY INTO mytable
  FROM 's3://mybucket/data.csv'
  CONNECTION = (
        ENDPOINT_URL = 'https://<endpoint-URL>'
        ACCESS_KEY_ID = '<your-access-key-ID>'
        SECRET_ACCESS_KEY = '<your-secret-access-key>')
  FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1) size_limit=10;
```

This example reads 10 rows from a CSV file compressed as GZIP and inserts them into a table:

```sql
COPY INTO mytable
  FROM 's3://mybucket/data.csv.gz'
  CONNECTION = (
        ENDPOINT_URL = 'https://<endpoint-URL>'
        ACCESS_KEY_ID = '<your-access-key-ID>'
        SECRET_ACCESS_KEY = '<your-secret-access-key>')
  FILE_FORMAT = (type = 'CSV' field_delimiter = ',' record_delimiter = '\n' skip_header = 1 compression = GZIP) size_limit=10;
```

This example moves data from a CSV file without specifying the endpoint URL:
```sql
COPY INTO mytable
  FROM 's3://mybucket/data.csv'
  FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1) size_limit=10;
```

**Azure Blob storage**

This example reads data from a CSV file and inserts them into a table:
```sql
COPY INTO mytable
    FROM 'azblob://mybucket/data.csv'
    CONNECTION = (
        ENDPOINT_URL = 'https://<account_name>.blob.core.windows.net'
        ACCOUNT_NAME = '<account_name>'
        ACCOUNT_KEY = '<account_key>'
    )
```