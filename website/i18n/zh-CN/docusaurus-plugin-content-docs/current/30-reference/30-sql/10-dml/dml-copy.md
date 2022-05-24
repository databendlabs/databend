---
title: 'COPY INTO <table>'
sidebar_label: 'COPY INTO <table>'
description: '''Load Data using COPY INTO <table>'' ---'
---

`COPY` moves data between Databend tables and object storage system(Such as Amazon S3-like) files.

Loads data from staged files into a table, the files must be staged in one of the following locations:

* Named internal stage, files can be staged using the [PUT to Stage](../../00-api/10-put-to-stage.md).
* Named external stage that references an external location (Amazon S3 S3-like object storage system).
* External location (Amazon S3-like object storage system).

## Syntax

```sql
COPY INTO [<database>.]<table_name>
FROM { internalStage | externalStage | externalLocation }
[ FILES = ( '<file_name>' [ , '<file_name>' ] [ , ... ] ) ]
[ PATTERN = '<regex_pattern>' ]
[ FILE_FORMAT = ( TYPE = { CSV | JSON | PARQUET } [ formatTypeOptions ] } ) ]
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

### externalLocation (for Amazon S3-like)

```
externalLocation (for Amazon S3) ::=
  's3://<bucket>[/<path>]'
  [ { CREDENTIALS = ( {  { AWS_KEY_ID = '<string>' AWS_SECRET_KEY = '<string>' } } ) } ]
```

| Parameters                                                                                              | Description                                                                                                             | Required |
| ------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- | -------- |
| `s3://<bucket>/[<path>]`                                                                    | Files are in the specified external location (S3-like bucket)                                                           | YES      |
| `[ { CREDENTIALS = ( {  { AWS_KEY_ID = '<string>' AWS_SECRET_KEY = '<string>' } } ) } ]' ]` | The credentials for connecting to AWS and accessing the private/protected S3 bucket where the files to load are staged. | Optional |
| `[ ENDPOINT_URL = '<endpoint_url>' ]`                                                             | S3-compatible endpoint URL like MinIO, default is `https://s3.amazonaws.com`                                            | Optional |

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
```

| Parameters                               | Description                                                            | Required |
| ---------------------------------------- | ---------------------------------------------------------------------- | -------- |
| `RECORD_DELIMITER = '<character>'` | One characters that separate records in an input file. Default `'\n'` | Optional |
| `FIELD_DELIMITER = '<character>'`  | One characters that separate fields in an input file. Default `','`    | Optional |
| `SKIP_HEADER = <integer>`          | Number of lines at the start of the file to skip. Default `0`          | Optional |

### copyOptions
```
copyOptions ::=
  [ SIZE_LIMIT = <num> ]
```

| Parameters                 | Description                                                                                               | Required |
| -------------------------- | --------------------------------------------------------------------------------------------------------- | -------- |
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

Final, copy the file into `mytable` from the `my_internal_s1` named internal stage:
```sql
LIST @my_internal_s1;
COPY INTO mytable FROM '@my_internal_s1' pattern = 'books.*parquet' file_format = (type = 'PARQUET');
```

### Loading Files from External Stage

First, create a named external stage:
```sql
CREATE STAGE my_external_s1 url = 's3://testbucket/admin/data/' credentials=(aws_key_id='minioadmin' aws_secret_key='minioadmin');
```
Then, copy the file into `mytable` from the `my_external_s1` named external stage:
```sql
COPY INTO mytable FROM '@my_external_s1' pattern = 'books.*parquet' file_format = (type = 'PARQUET');
```

### Loading Files Directly from External Location

**Amazon S3**

Try to read 10 rows from csv and insert into the `mytable`.
```sql
COPY INTO mytable
  FROM s3://mybucket/data.csv
  credentials=(aws_key_id='<AWS_ACCESS_KEY_ID>' aws_secret_key='<AWS_SECRET_ACCESS_KEY>')
  FILE_FORMAT = (type = "CSV" field_delimiter = ','  record_delimiter = '\n' skip_header = 1) size_limit=10;
```
