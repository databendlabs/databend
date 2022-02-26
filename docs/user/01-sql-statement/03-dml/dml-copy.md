---
title: COPY INTO table
---

Loads data from staged files to a table. The files must be staged in one of the following locations:

* Named internal stage. (TODO)
* External location (Amazon S3, Google Cloud Storage, or Microsoft Azure).

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
```
externalLocation (for Amazon S3) ::=
  's3://<bucket>[/<path>]'
  [ { CREDENTIALS = ( {  { AWS_KEY_ID = '<string>' AWS_SECRET_KEY = '<string>' } } ) } ]
  [ ENCRYPTION = ( [ MASTER_KEY = '<string>' ] ) ]
```

```
formatTypeOptions ::=
  RECORD_DELIMITER = '<character>' 
  FIELD_DELIMITER = '<character>' 
  SKIP_HEADER = <integer>
```

```
copyOptions ::=
  ON_ERROR = { CONTINUE | SKIP_FILE | SKIP_FILE_<num>| ABORT_STATEMENT }
  SIZE_LIMIT = <num>
```

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
