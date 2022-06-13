---
title: CREATE STAGE
---

Create a user stage.

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

### externalStageParams (for Amazon S3)
```
externalLocation (for Amazon S3) ::=
  URL = 's3://<bucket>[/<path>]'
  [ { CREDENTIALS = ( {  { AWS_KEY_ID = '<string>' AWS_SECRET_KEY = '<string>' } } ) } ]
```

| Parameters  | Description | Required |
| ----------- | ----------- | --- |
| URL | Files are in the specified external location (S3-like bucket) | YES |
| `[ { CREDENTIALS = ( {  { AWS_KEY_ID = '<string>' AWS_SECRET_KEY = '<string>' } } ) } ]' ]`  | The credentials for connecting to AWS and accessing the private/protected S3 bucket where the files to load are staged. |  Optional |

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
```

| Parameters  | Description | Required |
| ----------- | ----------- | --- |
| `SIZE_LIMIT = <num>` | Number (> 0) that specifies the maximum rows of data to be loaded for a given COPY statement. Default `0` | Optional |


## Examples

### Internal Stages

```sql
CREATE STAGE my_internal_stage;
```


### External Stages
```sql
CREATE STAGE my_s3_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z');
```

```sql
DESC STAGE my_s3_stage;
+-------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+
| name        | stage_type | stage_params                                                                                                                                                           | copy_options                                  | file_format_options                                                                                                | comment |
+-------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+
| my_s3_stage | External   | StageParams { storage: S3(StageS3Storage { bucket: "load", path: "/files/", credentials_aws_key_id: "", credentials_aws_secret_key: "", encryption_master_key: "" }) } | CopyOptions { on_error: None, size_limit: 0 } | FileFormatOptions { format: Csv, skip_header: 0, field_delimiter: ",", record_delimiter: "\n", compression: None } |         |
+-------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+
```
