---
title: Querying Data in Staged Files
---

Databend allows you to directly query data in the files stored in one of the following locations without loading them into a table:

- User stage, internal stage, or external stage.
- Bucket or container created in your object storage, such as Amazon S3, Google Cloud Storage, and Microsoft Azure.
- Remote server.

During this process, the schema is automatically detected, just like with the [infer_schema](../../15-sql-functions/112-table-functions/infer_schema.md) function. This feature can be particularly useful for inspecting or viewing the contents of staged files, whether it's before or after loading data.

:::note
This feature is currently only available for the Parquet file format.
:::

## Syntax and Parameters

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

### FILE_FORMAT

The file format must be one of the following:

- Built-in file format, see [Input & Output File Formats](../../13-sql-reference/50-file-format-options.md).
- Named file format created by [CREATE FILE FORMAT](../../14-sql-commands/00-ddl/100-file-format/01-ddl-create-file-format.md).

### PATTERN

A [PCRE2](https://www.pcre.org/current/doc/html/)-based regular expression pattern string, enclosed in single quotes, specifying the file names to match. Click [here](#loading-data-with-pattern-matching) to see an example. For PCRE2 syntax, see http://www.pcre.org/current/doc/html/pcre2syntax.html.

### FILES

Specifies one or a list of file names (separated by commas).

### Others

To query data files in a bucket or container, provide necessary connection information with the following parameters:

- ENDPOINT_URL
- AWS_KEY_ID
- AWS_SECRET_KEY
- ACCESS_KEY_ID
- ACCESS_KEY_SECRET
- SECRET_ACCESS_KEY
- SESSION_TOKEN
- REGION
- ENABLE_VIRTUAL_HOST_STYLE

They are explained in [Create Stage](../../14-sql-commands/00-ddl/40-stage/01-ddl-create-stage.md).

## Examples

### Example 1: Querying Data in a Parquet File

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This example shows how to query data in a Parquet file stored in different locations. Click the tabs below to see details.

<Tabs groupId="query2stage">
<TabItem value="Stages" label="Stages">

Let's assume you have a sample file named [books.parquet](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.parquet) and you have uploaded it to your user stage, an internal stage named *my_internal_stage*, and an external stage named *my_external_stage*. To upload files to a stage, use the [File Upload API](../../11-integrations/00-api/10-put-to-stage.md).

```sql
-- Query file in user stage
SELECT * FROM @~/books.parquet;

-- Query file in internal stage
SELECT * FROM @my_internal_stage/books.parquet;

-- Query file in external stage
SELECT * FROM @my_external_stage/books.parquet;
```
</TabItem>
<TabItem value="Bucket" label="Bucket">

Let's assume you have a sample file named [books.parquet](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.parquet) stored in a bucket named *databend-toronto* on Amazon S3 in the region *us-east-2*. You can query the data by specifying the connection parameters:

```sql
SELECT *  FROM 's3://databend-toronto' 
(
 access_key_id => '<your-access-key-id>', 
 secret_access_key => '<your-secret-access-key>',
 endpoint_url => 'https://databend-toronto.s3.us-east-2.amazonaws.com',
 region => 'us-east-2',
 files => ('books.parquet')
);  
```
</TabItem>
<TabItem value="Remote" label="Remote">

Let's assume you have a sample file named [books.parquet](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.parquet) stored in a remote server. You can query the data by specifying the file URI:

```sql
SELECT * FROM 'https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.parquet';
```
</TabItem>
</Tabs>

### Example 2: Filtering Files with Pattern Matching

Let's assume you have a folder containing the following Parquet files with the same schema, as well as some files of other formats. You can use the PATTERN parameter to filter all the Parquet files in the folder.

```text
data/
  ├── 2022-01-01.parquet
  ├── 2022-01-02.parquet
  ├── 2022-01-03.parquet
  ├── 2022-01-04.parquet
  └── 2022-01-05.parquet
```

For example, you can query the "name" and "age" columns from all Parquet files in the folder:

```sql
SELECT name, age FROM @internal_stage/data/
(file_format => 'parquet', pattern => '.*parquet');
```