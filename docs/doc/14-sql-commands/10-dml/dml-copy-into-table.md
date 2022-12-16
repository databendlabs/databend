---
title: "COPY INTO <table>"
sidebar_label: "COPY INTO <table>"
---

This command loads data into Databend from files in a variety of locations.

See Also: [COPY INTO location](dml-copy-into-location.md)

## Supported File Locations

Your data files must be located in one of these locations for COPY INTO to work:

- **Named internal stage**: Databend internal named stages. Files can be staged using the [PUT to Stage](../../11-integrations/00-api/10-put-to-stage.md) API.
- **Named external stage**: Stages created in [Supported Object Storage Solutions](../../10-deploy/00-understanding-deployment-modes.md#supported-object-storage-solutions).
- **External location**:
  - Buckets created in [Supported Object Storage Solutions](../../10-deploy/00-understanding-deployment-modes.md#supported-object-storage-solutions).
  - Remote servers from where you can access the files by their URL (starting with "https://...").
  - [IPFS](https://ipfs.tech).

## Syntax

```sql
COPY INTO [<database>.]<table_name>
FROM { internalStage | externalStage | externalLocation }
[ FILES = ( '<file_name>' [ , '<file_name>' ] [ , ... ] ) ]
[ PATTERN = '<regex_pattern>' ]
[ FILE_FORMAT = ( TYPE = { CSV | TSV | NDJSON | PARQUET | XML} [ formatTypeOptions ] ) ]
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

**AWS S3 Compatible Object Storage Service**

```sql
externalLocation ::=
  's3://<bucket>[<path>]'
  CONNECTION = (
        ENDPOINT_URL = 'https://<endpoint-URL>'
        ACCESS_KEY_ID = '<your-access-key-ID>'
        SECRET_ACCESS_KEY = '<your-secret-access-key>'
        SESSION_TOKEN = '<your-session-token>'
        REGION = '<region-name>'
        ENABLE_VIRTUAL_HOST_STYLE = 'true|false'
  )
```

| Parameter                 | Description                                                                                                                                                                           | Required |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| `s3://<bucket>[<path>]`   | External files located at the AWS S3 compatible object storage.                                                                                                                       | Required |
| ENDPOINT_URL              | The bucket endpoint URL starting with "https://". To use a URL starting with "http://", set `allow_insecure` to `true` in the [storage] block of the file `databend-query-node.toml`. | Required |
| ACCESS_KEY_ID             | Your access key ID for connecting the AWS S3 compatible object storage. If not provided, Databend will access the bucket anonymously.                                                 | Optional |
| SECRET_ACCESS_KEY         | Your secret access key for connecting the AWS S3 compatible object storage.                                                                                                           | Optional |
| SESSION_TOKEN             | Your temporary credential for connecting the AWS S3 service                                                                                                                           | Optional |
| REGION                    | AWS region name. For example, us-east-1.                                                                                                                                              | Optional |
| ENABLE_VIRTUAL_HOST_STYLE | If you use virtual hosting to address the bucket, set it to "true".                                                                                                                   | Optional |

**Azure Blob storage**

```sql
externalLocation ::=
  'azblob://<container>[<path>]'
  CONNECTION = (
        ENDPOINT_URL = 'https://<endpoint-URL>'
        ACCOUT_NAME = '<your-account-name>'
        ACCOUNT_KEY = '<your-account-key>'
  )
```

| Parameter                      | Description                                                                                                                                                                              | Required |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| `azblob://<container>[<path>]` | External files located at the Azure Blob storage.                                                                                                                                        | Required |
| ENDPOINT_URL                   | The container endpoint URL starting with "https://". To use a URL starting with "http://", set `allow_insecure` to `true` in the [storage] block of the file `databend-query-node.toml`. | Required |
| ACCOUNT_NAME                   | Your account name for connecting the Azure Blob storage. If not provided, Databend will access the container anonymously.                                                                | Optional |
| ACCOUNT_KEY                    | Your account key for connecting the Azure Blob storage.                                                                                                                                  | Optional |

**Google Cloud Storage**

```sql
externalLocation ::=
  'gcs://<container>[<path>]'
  CONNECTION = (
        ENDPOINT_URL = 'https://<endpoint-URL>'
        CREDENTIAL = '<your-credential>'
  )
```

| Parameter                | Description                                                                                                                                                                              | Required |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| `gcs://<bucket>[<path>]` | External files located at the Google Cloud Storage                                                                                                                                       | Required |
| ENDPOINT_URL             | The container endpoint URL starting with "https://". To use a URL starting with "http://", set `allow_insecure` to `true` in the [storage] block of the file `databend-query-node.toml`. | Optional |
| CREDENTIAL               | Your credential for connecting the GCS. If not provided, Databend will access the container anonymously.                                                                                 | Optional |

**Huawei Object Storage**

```sql
externalLocation ::=
  'obs://<container>[<path>]'
  CONNECTION = (
        ENDPOINT_URL = 'https://<endpoint-URL>'
        ACCESS_KEY_ID = '<your-access-key-id>'
        SECRET_ACCESS_KEY = '<your-secret-access-key>'
  )
```

| Parameter                | Description                                                                                                                                                                              | Required |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| `obs://<bucket>[<path>]` | External files located at the obs                                                                                                                                                        | Required |
| ENDPOINT_URL             | The container endpoint URL starting with "https://". To use a URL starting with "http://", set `allow_insecure` to `true` in the [storage] block of the file `databend-query-node.toml`. | Required |
| ACCESS_KEY_ID            | Your access key ID for connecting the OBS. If not provided, Databend will access the bucket anonymously.                                                                                 | Optional |
| SECRET_ACCESS_KEY        | Your secret access key for connecting the OBS.                                                                                                                                           | Optional |

**Remote Files**

```sql
externalLocation ::=
  'https://<url>'
```

You can use glob patterns to specify moran than one file. For example, use

- `ontime_200{6,7,8}.csv` to represents `ontime_2006.csv`,`ontime_2007.csv`,`ontime_2008.csv`.
- `ontime_200[6-8].csv` to represents `ontime_2006.csv`,`ontime_2007.csv`,`ontime_2008.csv`.

**IPFS**

```sql
externalLocation ::=
  'ipfs://<your-ipfs-hash>'
  CONNECTION = (ENDPOINT_URL = 'https://<your-ipfs-gateway>')
```

### FILES = ( 'file_name' [ , 'file_name' ... ] )

Specifies a list of one or more files names (separated by commas) to be loaded.

### PATTERN = 'regex_pattern'

A [PCRE2](https://www.pcre.org/current/doc/html/)-based regular expression pattern string, enclosed in single quotes, specifying the file names to match. Click [here](#loading-data-with-pattern-matching) to see an example. For PCRE2 syntax, see http://www.pcre.org/current/doc/html/pcre2syntax.html. 

### formatTypeOptions

```
formatTypeOptions ::=
  RECORD_DELIMITER = '<character>'
  FIELD_DELIMITER = '<character>'
  SKIP_HEADER = <integer>
  QUOTE = '<character>'
  ESCAPE = '<character>'
  NAN_DISPLAY = '<string>'
  ROW_TAG = '<string>'
  COMPRESSION = AUTO | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | XZ | NONE
```

#### `TYPE = 'CSV'`

Comma Separated Values format ([RFC](https://www.rfc-editor.org/rfc/rfc4180)).

some notice:

1. a string field contains `Quote`|`Escape`|`RECORD_DELIMITER`|`RECORD_DELIMITER` must be quoted.
2. no character is escaped except `Quote` in quoted string.
3. no space between `FIELD_DELIMITER` and `Quote`.
4. no trailing `FIELD_DELIMITER` for a record.
5. Array/Struct field is serialized to a string as in SQL, and then the resulting string is output to CSV in quotes.
6. if you are generating CSV via programing, we highly recommend you to use the CSV lib of the programing language.
7. for text file unloaded from [MySQL](https://dev.mysql.com/doc/refman/8.0/en/load-data.html), the default format is
   TSV in databend. it is valid CSV only if  `ESCAPED BY` is empty and `ENCLOSED BY` is not empty.

##### `RECORD_DELIMITER = '<character>'`

**Description**: One character that separate records in an input file.
**Supported Values**: `\r\n` or One character including escaped char: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\xHH`
**Default**: `\n`

##### `FIELD_DELIMITER = '<character>'`

**Description**: One character that separate fields in an input file.
**Supported Values**: One character only, including escaped char: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\xHH`
**Default**: `,` (comma)

##### `Quote = '<character>'`

**Description**: One character to quote strings in CSV file.

for data loading, quote is not necessary unless a string contains `Quote`|`Escape`|`RECORD_DELIMITER`|`RECORD_DELIMITER`

**Supported Values**: `\'` or `\"`.

**Default**: `\"`

##### `ESCAPE = '<character>'`

**Description**: One character to escape quote in quoted strings.

**Supported Values**: `\'` or `\"` or `\\`.

**Default**: `\"`

##### `SKIP_HEADER = '<integer>'`

**Use**: Data loading only.

**Description**: Number of lines at the start of the file to skip.

**Default**: `0`

##### `NAN_DISPLAY = '<string>'`

**Supported Values**: must be literal `'nan'` or `'null'` (case-insensitive)
**Default**: `'NaN'`

##### `COMPRESSION = AUTO | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | XZ | NONE`

**Description**: String that represents the compression algorithm.

**Default**: `NONE`

**Supported Values**:

| Values        | Notes                                                           |
| ------------- | --------------------------------------------------------------- |
| `AUTO`        | Auto detect compression via file extensions                     |
| `GZIP`        |                                                                 |
| `BZ2`         |                                                                 |
| `BROTLI`      | Must be specified if loading/unloading Brotli-compressed files. |
| `ZSTD`        | Zstandard v0.8 (and higher) is supported.                       |
| `DEFLATE`     | Deflate-compressed files (with zlib header, RFC1950).           |
| `RAW_DEFLATE` | Deflate-compressed files (without any header, RFC1951).         |
| `XZ`          |                                                                 |
| `NONE`        | Indicates that the files have not been compressed.              |

#### `TYPE = 'TSV'`

1. these characters are escaped: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\\`, `\'`, `RECORD_DELIMITER`,`FIELD_DELIMITER`.
2. quoting/enclosing is not support now.
3. Array/Struct field is serialized to a string as in SQL, and then the resulting string is output to CSV in quotes.
4. Null is serialized as `\N`

##### `RECORD_DELIMITER = '<character>'`

**Description**: One character that separate records in an input file.

**Supported Values**: `\r\n` or One character including escaped char: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\xHH`

**Default**: `\n`

##### `FIELD_DELIMITER = '<character>'`

**Description**: One character that separate fields in an input file.

**Supported Values**: One character only, including escaped char: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\xHH`

**Default**: `\t` (TAB)

##### `COMPRESSION = AUTO | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | XZ | NONE`

same as `COMPRESSION` in `TYPE = 'CSV'`

#### `TYPE = 'NDJSON'`

##### `COMPRESSION = AUTO | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | XZ | NONE`

same as `COMPRESSION` in `TYPE = 'CSV'`

#### `TYPE = 'XML'`

##### `COMPRESSION = AUTO | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | XZ | NONE`

same as `COMPRESSION` in `TYPE = 'CSV'`

##### `ROW_TAG` = `<string>`

**Description**: used to select XML elements to be decoded as a record.

**Default**: `'row'`

#### `TYPE = 'Parquet'`

No options available now.

### copyOptions

```
copyOptions ::=
  [ SIZE_LIMIT = <num> ]
  [ PURGE = <bool> ]
  [ FORCE = <bool> ]
```

| Parameters           | Description                                                                                                                                       | Required |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| `SIZE_LIMIT = <num>` | Specifies the maximum rows of data to be loaded for a given COPY statement. Defaults to `0` meaning no limits.                                    | Optional |
| `PURGE = <bool>`     | If `True`, the command will purge the files in the stage after they are loaded successfully into the table. Default: `False`.                     | Optional |
| `FORCE = <bool>`     | Defaults to `False` meaning the command will skip duplicate files in the stage when copying data. If `True`, duplicate files will not be skipped. | Optional |

## Examples

### Loading Data from an Internal Stage

```sql
COPY INTO mytable FROM @my_internal_s1 pattern = 'books.*parquet' file_format = (type = 'PARQUET');
```

### Loading Data from an External Stage

```sql
COPY INTO mytable FROM @my_external_s1 pattern = 'books.*parquet' file_format = (type = 'PARQUET');
```

### Loading Data from External Locations

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
  FILE_FORMAT = (type = 'CSV' field_delimiter = ',' record_delimiter = '\n' skip_header = 1 compression = 'GZIP') size_limit=10;
```

This example loads data from a CSV file without specifying the endpoint URL:

```sql
COPY INTO mytable
  FROM 's3://mybucket/data.csv'
  FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1) size_limit=10;
```

This is an example loading data from a `Parquet` file:

```sql
COPY INTO mytable
  FROM 's3://mybucket/data.parquet'
  CONNECTION = (
      ACCESS_KEY_ID = '<your-access-key-ID>'
      SECRET_ACCESS_KEY = '<your-secret-access-key>')
  FILE_FORMAT = (type = 'PARQUET');
```

**Azure Blob storage**

This example reads data from a CSV file and inserts it into a table:

```sql
COPY INTO mytable
    FROM 'azblob://mybucket/data.csv'
    CONNECTION = (
        ENDPOINT_URL = 'https://<account_name>.blob.core.windows.net'
        ACCOUNT_NAME = '<account_name>'
        ACCOUNT_KEY = '<account_key>'
    )
    FILE_FORMAT = (type = 'CSV');
```

**Remote Files**

This example reads data from three remote CSV files and inserts it into a table:

```sql
COPY INTO mytable
    FROM 'https://repo.databend.rs/dataset/stateful/ontime_200{6,7,8}_200.csv'
    FILE_FORMAT = (type = 'CSV');
```

**IPFS**

This example reads data from a CSV file on IPFS and inserts it into a table:

```sql
COPY INTO mytable
    FROM 'ipfs://<your-ipfs-hash>' connection = (endpoint_url = 'https://<your-ipfs-gateway>')
    FILE_FORMAT = (type = 'CSV' field_delimiter = ',' record_delimiter = '\n' skip_header = 1);
```

### Loading Data with Pattern Matching

This example uses pattern matching to only load from CSV files containing `sales` in their names:

```sql
COPY INTO mytable
  FROM 's3://mybucket/'
  PATTERN = '.*sales.*[.]csv'
  FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1);
```
Where `.*` is interpreted as `zero or more occurrences of any character`. The square brackets escape the period character `(.)` that precedes a file extension.

If you want to load from all the CSV files, use `PATTERN = '.*[.]csv'`:
```sql
COPY INTO mytable
  FROM 's3://mybucket/'
  PATTERN = '.*[.]csv'
  FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1);
```

## Tutorials

Here are some tutorials to help you get started with COPY INTO:

- [Tutorial: Load from an internal stage](../../12-load-data/00-stage.md): In this tutorial, you will create an internal stage, stage a sample file, and then load data from the file into Databend with the COPY INTO command.
- [Tutorial: Load from an Amazon S3 bucket](../../12-load-data/01-s3.md): In this tutorial, you will upload a sample file to your Amazon S3 bucket, and then load data from the file into Databend with the COPY INTO command.
- [Tutorial: Load from a remote file](../../12-load-data/04-http.md): In this tutorial, you will load data from a remote sample file into Databend with the COPY INTO command.