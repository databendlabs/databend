---
title: CREATE STAGE
---

Creates a user stage.

## Syntax

```sql
-- Internal stage
CREATE STAGE [ IF NOT EXISTS ] <internal_stage_name>
  [ FILE_FORMAT = ( { TYPE = { CSV | PARQUET } [ formatTypeOptions ] ) } ]
  [ COPY_OPTIONS = ( copyOptions ) ]
  [ COMMENT = '<string_literal>' ]

-- External stage
CREATE STAGE [ IF NOT EXISTS ] <external_stage_name>
    externalStageParams
  [ FILE_FORMAT = ( { TYPE = { CSV | PARQUET } [ formatTypeOptions ] ) } ]
  [ COPY_OPTIONS = ( copyOptions ) ]
  [ COMMENT = '<string_literal>' ]
```

Where:

### externalStageParams

AWS S3 compatible object storage services:

```sql
externalStageParams ::=
  URL = 's3://<bucket>[<path>]'
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
| URL    	| External files located at the AWS S3 compatible object storage.             	| Optional 	|
| ENDPOINT_URL              	| The bucket endpoint URL starting with "https://". To use a URL starting with "http://", set `allow_insecure` to `true` in the [storage] block of the file `databend-query-node.toml`.                                  	| Optional 	|
| ACCESS_KEY_ID             	| Your access key ID for connecting the AWS S3 compatible object storage. If not provided, Databend will access the bucket anonymously.    	| Optional 	|
| SECRET_ACCESS_KEY         	| Your secret access key for connecting the AWS S3 compatible object storage. 	| Optional 	|
| REGION                    	| AWS region name. For example, us-east-1.                                    	| Optional 	|
| ENABLE_VIRTUAL_HOST_STYLE 	| If you use virtual hosting to address the bucket, set it to "true".                               	| Optional 	|

Azure Blob storage：

```sql
externalStageParams ::=
  URL = 'azblob://<container>[<path>]'
  CONNECTION = (
        ENDPOINT_URL = 'https://<endpoint-URL>'
        ACCOUT_NAME = '<your-account-name>'
        ACCOUNT_KEY = '<your-account-key>'
  )
```

| Parameter                  	| Description                                              	| Required 	|
|----------------------------	|----------------------------------------------------------	|----------	|
| URL 	| External files located at the Azure Blob storage.        	| Required 	|
| ENDPOINT_URL               	| The container endpoint URL starting with "https://". To use a URL starting with "http://", set `allow_insecure` to `true` in the [storage] block of the file `databend-query-node.toml`.    	| Optional 	|
| ACCOUNT_NAME               	| Your account name for connecting the Azure Blob storage. If not provided, Databend will access the container anonymously.	| Optional 	|
| ACCOUNT_KEY                	| Your account key for connecting the Azure Blob storage.  	| Optional 	|

### formatTypeOptions
```
formatTypeOptions ::=
  RECORD_DELIMITER = '<character>'
  FIELD_DELIMITER = '<character>'
  SKIP_HEADER = <integer>
```

| Parameters  | Description | Required |
| ----------- | ----------- | --- |
| `RECORD_DELIMITER = '<character>'`  | One characters that separate records in an input file. Default `'\n'` | Optional |
| `FIELD_DELIMITER = '<character>'`  | One characters that separate fields in an input file. Default `','` | Optional |
| `SKIP_HEADER = <integer>`  | Number of lines at the start of the file to skip. Default `0` | Optional |

### copyOptions
```
copyOptions ::=
  [ SIZE_LIMIT = <num> ]
  [ PURGE = <bool> ]
```

| Parameters  | Description | Required |
| ----------- | ----------- | --- |
| `SIZE_LIMIT = <num>` | Number (> 0) that specifies the maximum rows of data to be loaded for a given COPY statement. Default `0` | Optional |
| `PURGE = <bool>` | True specifies that the command will purge the files in the stage if they are loaded successfully into table. Default `false` | Optional |


## Examples

### Internal Stages

```sql
CREATE STAGE my_internal_stage;
```


### External Stages
```sql
CREATE STAGE my_s3_stage url='s3://load/files/' connection=(aws_key_id='1a2b3c' aws_secret_key='4x5y6z');
```

```sql
DESC STAGE my_s3_stage;
+-------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+
| name        | stage_type | stage_params                                                                                                                                                           | copy_options                                  | file_format_options                                                                                                | comment |
+-------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+
| my_s3_stage | External   | StageParams { storage: S3(StageS3Storage { bucket: "load", path: "/files/", credentials_aws_key_id: "", credentials_aws_secret_key: "", encryption_master_key: "" }) } | CopyOptions { on_error: None, size_limit: 0 } | FileFormatOptions { format: Csv, skip_header: 0, field_delimiter: ",", record_delimiter: "\n", compression: None } |         |
+-------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+
```
