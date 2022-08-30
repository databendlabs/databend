---
title: 'COPY INTO <table> FROM REMOTE FILES'
sidebar_label: 'COPY INTO <table> FROM REMOTE FILES'
description:
  'Loads data from remote files by URL'
---

This command loads data into a table from one or more remote files by their URL.

`COPY` can also load data into a table from files staged in an object storage system (for example, AWS S3 compatible object storage services, or Azure Blob storage). See [COPY INTO `<table>` FROM STAGED FILES](dml-copy-into-table.md).

## Syntax

```sql
COPY INTO [<database>.]<table_name>
FROM 'https://<site>/<directory>/<filename>'
[ FILE_FORMAT = ( TYPE = { CSV | JSON | NDJSON | PARQUET } [ formatTypeOptions ] ) ]
```

### filename

You can specify a single file to load data from the file, for example, `mydata_2022_10000.csv`. 

You can also specify multiple files with sequential numbers in their names using a pair of square or curly brackets to load data from them. See the examples below:

- `mydata_2022_{10001,10002}.csv`: This will include the files `mydata_2022_10001.csv` and `mydata_2022_10002.csv`.

- `mydata_2022_[10003-10005].csv`: This will include the files `mydata_2022_10003.csv`, `mydata_2022_10004.csv`, and `mydata_2022_10005.csv`. 

### formatTypeOptions

```sql
formatTypeOptions ::=
  RECORD_DELIMITER = '<character>' 
  FIELD_DELIMITER = '<character>' 
  SKIP_HEADER = <integer>
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

## Examples

This example loads data into the table `ontime200` from a remote file named `ontime_2006_200.csv`:
```sql
copy into ontime200 from 'https://repo.databend.rs/dataset/stateful/ontime_2006_200.csv' FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1)
```

This example loads data into the table `ontime200` from the remote files `ontime_2006_200.csv`, `ontime_2007_200.csv`, and `ontime_2008_200.csv`:

```sql
copy into ontime200 from 'https://repo.databend.rs/dataset/stateful/ontime_200{6,7,8}_200.csv' FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1)
```

This example does the same as the preceding one:

```sql 
copy into ontime200 from 'https://repo.databend.rs/dataset/stateful/ontime_200[6-8]_200.csv' FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1)
```
