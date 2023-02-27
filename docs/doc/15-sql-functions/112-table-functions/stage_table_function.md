---
title: Stage Table Function  
---

Databend supports using standard SQL to query data files located in an internal stage or named external stage.

The schema is automatically detected, same as [infer_schema](infer_schema.md).

## Syntax

```sql
SELECT <columns> FROM
{@<stage_name>[/<path>] | '<uri>'} [(
  [ PARTTERN => '<regex_pattern>']
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

1. A built-in file format (see [Input & Output File Formats](../../13-sql-reference/50-file-format-options.md).
2. A named file format created by [CREATE FILE FORMAT](../../14-sql-commands/00-ddl/100-file-format/01-ddl-create-file-format.md).

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

They are explained [Create Stage](../../14-sql-commands/00-ddl/40-stage/01-ddl-create-stage.md).

## Query Examples

### Select from Named Stage

```sql
-- New stage.
create stage lake;
    
-- Stage the data file into internal stage.
copy into @lake from (select * from numbers(10)) file_format = (type = PARQUET);

-- Show files in the internal stage.
list @lake;
+-------------------------------------------------------+------+------------------------------------+-------------------------------+---------+
| name                                                  | size | md5                                | last_modified                 | creator |
+-------------------------------------------------------+------+------------------------------------+-------------------------------+---------+
| data_8f414f66-5a94-42ad-ad52-a9076541799e_0_0.parquet |  258 | "7DCC9FFE04EA1F6882AED2CF9640D3D4" | 2023-02-24 09:55:46.000 +0000 | NULL    |
+-------------------------------------------------------+------+------------------------------------+-------------------------------+---------+

-- Query.
select min(number), max(number) from @lake (pattern => '.*parquet');

+-------------+-------------+
| min(number) | max(number) |
+-------------+-------------+
|           0 |           9 |
+-------------+-------------+
```

### Select from URI

The following options are available to select from for the URI: `s3`, `azblob` , `gcs`, `https`, and `ipfs`.

:::caution

`file_format` must be specified.

:::

```sql
select *  from 's3://bucket/test.parquet' 
( access_key_id => 'your-access-key-id', 
  secret_access_key => 'your-secret-access-key',
  endpoint_url => 'your-object-storage-endpoint',
  file_format => 'parquet');  
```

```sql
select count(*), author from 'https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.parquet' (file_format => 'parquet')
group by author;
+----------+---------------------+
| count(*) | author              |
+----------+---------------------+
|        1 | Jim Gray            |
|        1 | Michael Stonebraker |
+----------+---------------------+
```

