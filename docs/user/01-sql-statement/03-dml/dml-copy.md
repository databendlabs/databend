---
title: COPY INTO table
---

Loads data from staged files to a table. The files must be staged in one of the following locations:

* Named internal stage. (TODO)
* External location (Amazon S3-Like, Google Cloud Storage, or Microsoft Azure).

## Syntax

```sql
COPY INTO [<database>.]<table_name>
FROM { externalLocation }
[ FILES = ( '<file_name>' [ , '<file_name>' ] [ , ... ] ) ]
[ FILE_FORMAT = ( TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ]
[ copyOptions ]
[ VALIDATION_MODE = RETURN_<n>_ROWS | RETURN_ERRORS | RETURN_ALL_ERRORS ]
```

Where:

**externalLocation (for Amazon S3)**
```
externalLocation (for Amazon S3) ::=
  's3://<bucket>[/<path>]'
  [ ENDPOINT_URL = '<endpoint_url>' ]
  [ { CREDENTIALS = ( {  { AWS_KEY_ID = '<string>' AWS_SECRET_KEY = '<string>' } } ) } ]
```

| Parameters  | Description | Required |
| ----------- | ----------- | --- |
| `s3://<bucket>/[<path>]`  | Files are in the specified external location (S3-like bucket) | YES |
| `[ ENDPOINT_URL = '<endpoint_url>' ]`  | S3-compatible endpoint URL like MinIO, default is `https://s3.amazonaws.com` |  Optional |
| `[ { CREDENTIALS = ( {  { AWS_KEY_ID = '<string>' AWS_SECRET_KEY = '<string>' } } ) } ]' ]`  | The credentials for connecting to AWS and accessing the private/protected S3 bucket where the files to load are staged. |  Optional |
| `[ ENDPOINT_URL = '<endpoint_url>' ]`  | S3-compatible endpoint URL like MinIO, default is `https://s3.amazonaws.com` |  Optional |


**formatTypeOptions**
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

**copyOptions**
```
copyOptions ::=
  [ SIZE_LIMIT = <num> ]
```

| Parameters  | Description | Required |
| ----------- | ----------- | --- |
| `SIZE_LIMIT = <num>` | Number (> 0) that specifies the maximum rows of data to be loaded for a given COPY statement | Optional |

## Examples

### Loading Files Directly from an External Location

**Amazon S3**

Try to read 10 rows from csv and insert into the `mytable`.
```sql
mysql> copy into mytable
  from s3://mybucket/data.csv
  credentials=(aws_key_id='<AWS_ACCESS_KEY_ID>' aws_secret_key='<AWS_SECRET_ACCESS_KEY>')
  FILE_FORMAT = (type = "CSV" field_delimiter = ','  record_delimiter = '\n' skip_header = 1) size_limit=10;
```
