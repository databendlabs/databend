---
title: 'COPY INTO <location>'
sidebar_label: 'COPY INTO <location>'
description:
  'Unload Data using COPY INTO <location>'
---

`COPY` moves data between Databend and object storage systems (such as Amazon S3).

Unloads data from a table (or query) into one or more files in one of the following locations:

* Named internal stage: The files can be downloaded from the stage using the GET command.
* Named external stage: An external location (including Amazon S3).
* External location: An object storage system (including Amazon S3).


## Syntax

```sql
COPY INTO { internalStage | externalStage | externalLocation }
FROM { [<database_name>.]<table_name> | ( <query> ) }
[ FILE_FORMAT = ( { TYPE = { CSV | JSON | NDJSON | PARQUET } [ formatTypeOptions ] } ) ]
[ copyOptions ]
[ VALIDATION_MODE = RETURN_ROWS ]
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

### externalLocation (for Amazon S3)

```
externalLocation (for Amazon S3) ::=
  's3://<bucket>[/<path>]'
  [ { CREDENTIALS = ( {  { AWS_KEY_ID = '<string>' AWS_SECRET_KEY = '<string>' } } ) } ]
```

| Parameter  | Description | Required |
| ----------- | ----------- | --- |
| `s3://<bucket>/[<path>]`  | Files are in the specified external location (S3-like bucket) | YES |
| `[ { CREDENTIALS = ( {  { AWS_KEY_ID = '<string>' AWS_SECRET_KEY = '<string>' } } ) } ]' ]`  | The credentials for connecting to AWS and accessing the private/protected S3 bucket where the files to load are staged. |  Optional |
| `[ ENDPOINT_URL = '<endpoint_url>' ]`  | S3-compatible endpoint URL like MinIO, default is `https://s3.amazonaws.com` |  Optional |


### formatTypeOptions
```
formatTypeOptions ::=
  RECORD_DELIMITER = '<character>' 
  FIELD_DELIMITER = '<character>' 
  SKIP_HEADER = <integer>
```

| Parameters  | Description | Required |
| ----------- | ----------- | --- |
| `RECORD_DELIMITER = '<character>'`  | One or more characters that separate records in the output file. Default `'\n'` | Optional |
| `FIELD_DELIMITER = '<character>'`  | One or more characters that separate fields in the output file. Default `','` | Optional |
| `SKIP_HEADER = <integer>`  | Number of lines at the start of the file to skip. Default `0` | Optional |

### copyOptions
```
copyOptions ::=
  [ SIZE_LIMIT = <num> ]
```

| Parameters  | Description | Required |
| ----------- | ----------- | --- |
| `SIZE_LIMIT = <num>` | Number (> 0) that specifies the maximum rows of data to be unloaded for a given COPY statement. Default `0` | Optional |

## Examples


### Unloading into an Internal Stage

```sql
-- Create a table
CREATE TABLE test_table (
     id INTEGER,
     name VARCHAR,
     age INT
);

-- Insert data into the table
insert into test_table (id,name,age) values(1,'2',3), (4, '5', 6);

-- Create an internal stage
CREATE STAGE s2;


-- Unload the data in the table into the stage as a CSV file
copy into @s2 from test_table FILE_FORMAT = (type = 'CSV');

-- Unload the data from a query into the stage as a Parquet file
copy into @s2 from (select name, age, id from test_table limit 100) FILE_FORMAT = (type = 'PARQUET');
```

