---
title: Stage Table Function  
---

SQL to query data files located in a stage or an uri.

The schema is automatically detected, same as [infer_schema](infer_schema.md).


## Syntax

Looks like a stage function, except used `@<stage_name>` or `'uri'` instead of using table_function name. 

```sql
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

where:

### FILE_FORMAT = '<format_name>'

`<format_name>` is one of

1. build-in file format,  see [Input & Output File Formats](../../13-sql-reference/75-file-format-options.md).
2. named file format create by [CREATE FILE FORMAT](../../14-sql-commands/00-ddl/100-file-format/01-ddl-create-file-format.md).

for named stage, use the format of the stage if not specified.

:::caution

Currently, only supports parquet file format.

:::

### PATTERN = '<regex_pattern>'

A [PCRE2](https://www.pcre.org/current/doc/html/)-based regular expression pattern string, enclosed in single quotes, specifying the file names to match. Click [here](#loading-data-with-pattern-matching) to see an example. For PCRE2 syntax, see http://www.pcre.org/current/doc/html/pcre2syntax.html.


### FILES  = ( 'file_name' [ , 'file_name' ... ] )

Specifies a list of one or more files names (separated by commas) to be read.


### Connection Options for `<uri>` only 

including: 

- ENDPOINT_URL
- AWS_KEY_ID 
- AWS_SECRET_KEY
- ACCESS_KEY_ID
- ACCESS_KEY_SECRET
- SECRET_ACCESS_KEY
- SESSION_TOKEN
- REGION
- ENABLE_VIRTUAL_HOST_STYLE

they are explained [Create Stage](../../14-sql-commands/00-ddl/40-stage/01-ddl-create-stage.md).

## Examples

### select from named stage

```shell
SELECT * FROM @my_stage/my_home(pattern => '.*parquet');
```

### select from uri 

`file_format` must be specified.

```shell
select *  from 's3://testbucket/admin/data/tuple.parquet' 
(aws_key_id => 'minioadmin', aws_secret_key => 'minioadmin', endpoint_url => 'http://127.0.0.1:9900/', file_format => 'parquet');  
```

