---
title: Querying Data in Staged Files
description:  Learn how to use standard SQL to query data files in internal or external storage stages with Databend
---

Databend supports using standard SQL to query data files located in an internal stage or named external stage (Amazon S3, Google Cloud Storage, or Microsoft Azure). This can be useful for inspecting or viewing the contents of the staged files, particularly before loading or after unloading data.

The schema is automatically detected, same as [infer_schema](../15-sql-functions/112-table-functions/infer_schema.md).

## Query Syntax and Parameters

```sql
SELECT <columns> FROM
{@<stage_name>[/<path>] | '<uri>'} [(
  [ PATTERN => '<regex_pattern>']
  [ FILE_FORMAT => '<format_name>']
  [ FILES => ( 'file_name' [ , 'file_name' ... ] ) ]
  [ ENDPOINT_URL => <'url'> ]
  [ AWS_KEY_ID => <'aws_key_id'> ]
  [ AWS_KEY_SECRET => <'aws_key_secret'> ]
  [ ACCESS_KEY_ID => <'access_key_id'> ]
  [ ACCESS_KEY_SECRET => <'access_key_secret'> ]
  [ SECRET_ACCESS_KEY => <'secret_access_key'> ]
  [ SESSION_TOKEN => <'session_token'> ]
  [ REGION => <'region'> ]
  [ ENABLE_VIRTUAL_HOST_STYLE => true|false ]
)]
```

The function parameters are as follows:

### FILE_FORMAT = '<format_name>'

`<format_name>` should be one of the following:

1. A built-in file format (see [Input & Output File Formats](../13-sql-reference/50-file-format-options.md).
2. A named file format created by [CREATE FILE FORMAT](../14-sql-commands/00-ddl/100-file-format/01-ddl-create-file-format.md).

If not specified for named stages, the format of the stage should be used.

:::caution

Only parquet file format is currently supported.

:::

### PATTERN = '<regex_pattern>'

A [PCRE2](https://www.pcre.org/current/doc/html/)-based regular expression pattern string, enclosed in single quotes, specifying the file names to match. Click [here](#loading-data-with-pattern-matching) to see an example. For PCRE2 syntax, see http://www.pcre.org/current/doc/html/pcre2syntax.html.


### FILES  = ( 'file1' [ , 'file2' ... ] )

Specifies a list of one or more files names (separated by commas) to be read.

### Connection Options for `<uri>` only

These include:

- ENDPOINT_URL
- AWS_KEY_ID
- AWS_SECRET_KEY
- ACCESS_KEY_ID
- ACCESS_KEY_SECRET
- SECRET_ACCESS_KEY
- SESSION_TOKEN
- REGION
- ENABLE_VIRTUAL_HOST_STYLE

They are explained in [Create Stage](../14-sql-commands/00-ddl/40-stage/01-ddl-create-stage.md).

## Query Examples

### Example 1: Querying Columns in a Parquet File

Let's assume you have a Parquet file called "example.parquet" with the following data:

```text

| name  | age | city   |
|-------|-----|--------|
| Alice | 28  | London |
| Bob   | 35  | Berlin |
| Carol | 42  | Paris  |
```

You can query the "name" and "age" columns from this file using the following query:
```sql
SELECT name, age FROM @internal_stage/example.parquet;
```

### Example 2: Querying Data using Pattern Matching

Suppose you have a directory with several Parquet files, and you only want to query the files that end with ".parquet". You can do this using the PATTERN parameter.

Assuming the following file structure:

```text
data/
  ├── 2022-01-01.parquet
  ├── 2022-01-02.parquet
  ├── 2022-01-03.parquet
  ├── 2022-01-04.parquet
  └── 2022-01-05.parquet
```

You can query the "name" and "age" columns from all files in the "data" folder using the following query:

```sql
SELECT name, age FROM @internal_stage/data/
(file_format => 'parquet', pattern => '.*parquet');
```

### Example 3: Querying Data in an External Stage

Let's assume you have data in an Amazon S3 bucket and you want to query it using Databend. You can use the following query:
```sql
SELECT *  FROM 's3://bucket/' 
(
 access_key_id => 'your-access-key-id', 
 secret_access_key => 'your-secret-access-key',
 endpoint_url => 'your-object-storage-endpoint',
 file_format => 'parquet',
 pattern => '.*parquet'
);  
```

### Example 4: Querying Data from a URI

You can also use a URI to query data files from a remote location, like this:

```sql
SELECT count(*), author FROM 'https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.parquet'
(file_format => 'parquet')
GROUP BY author;
```

## Conclusion

We hope this document has provided you with a better understanding of how to use standard SQL to query data files in an internal or external storage stage with Databend. By using the query function, you can easily inspect or view the contents of staged files, making it easier to load and unload data. The examples we provided should help you get started with using this powerful feature.