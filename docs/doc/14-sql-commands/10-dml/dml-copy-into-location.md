---
title: 'COPY INTO <location>'
sidebar_label: 'COPY INTO <location>'
description:
  'Unload Data using COPY INTO <location>'
---

This command unloads data from a table (or query) into one or more files in one of the following locations:

* User / Internal / External stages: See [Understanding Stages](../../12-load-data/00-stage/00-whystage.md) to learn about stages in Databend.
* Buckets or containers created in a storage service.

See Also: [COPY INTO table](dml-copy-into-table.md)

## Syntax

```sql
COPY INTO { internalStage | externalStage | externalLocation }
FROM { [<database_name>.]<table_name> | ( <query> ) }
[ FILE_FORMAT = ( { TYPE = { CSV | JSON | NDJSON | PARQUET } [ formatTypeOptions ] } ) ]
[ copyOptions ]
[ VALIDATION_MODE = RETURN_ROWS ]
```

### internalStage

```sql
internalStage ::= @<internal_stage_name>[/<path>]
```

### externalStage

```sql
externalStage ::= @<external_stage_name>[/<path>]
```

### externalLocation

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="externallocation">

<TabItem value="Amazon S3-like Storage Services" label="Amazon S3-like Storage Services">

```sql
externalLocation ::=
  's3://<bucket>[<path>]'
  CONNECTION = (
        <connection_parameters>
  )
```
For the connection parameters available for accessing Amazon S3-like storage services, see [Connection Parameters](/13-sql-reference/51-connect-parameters.md).
</TabItem>

<TabItem value="Azure Blob Storage" label="Azure Blob Storage">

```sql
externalLocation ::=
  'azblob://<container>[<path>]'
  CONNECTION = (
        <connection_parameters>
  )
```

For the connection parameters available for accessing Azure Blob Storage, see [Connection Parameters](/13-sql-reference/51-connect-parameters.md).
</TabItem>

<TabItem value="Google Cloud Storage" label="Google Cloud Storage">

```sql
externalLocation ::=
  'gcs://<bucket>[<path>]'
  CONNECTION = (
        <connection_parameters>
  )
```

For the connection parameters available for accessing Google Cloud Storage, see [Connection Parameters](/13-sql-reference/51-connect-parameters.md).
</TabItem>

<TabItem value="Alibaba Cloud OSS" label="Alibaba Cloud OSS">

```sql
externalLocation ::=
  'oss://<bucket>[<path>]'
  CONNECTION = (
        <connection_parameters>
  )
```

For the connection parameters available for accessing Alibaba Cloud OSS, see [Connection Parameters](/13-sql-reference/51-connect-parameters.md).
</TabItem>

<TabItem value="Tencent Cloud Object Storage" label="Tencent Cloud Object Storage">

```sql
externalLocation ::=
  'cos://<bucket>[<path>]'
  CONNECTION = (
        <connection_parameters>
  )
```

For the connection parameters available for accessing Tencent Cloud Object Storage, see [Connection Parameters](/13-sql-reference/51-connect-parameters.md).
</TabItem>

<TabItem value="Hadoop Distributed File System (HDFS)" label="HDFS">

```sql
externalLocation ::=
  'hdfs://<endpoint_url>[<path>]'
  CONNECTION = (
        <connection_parameters>
  )
```

For the connection parameters available for accessing HDFS, see [Connection Parameters](/13-sql-reference/51-connect-parameters.md).
</TabItem>

<TabItem value="WebHDFS" label="WebHDFS">

```sql
externalLocation ::=
  'webhdfs://<endpoint_url>[<path>]'
  CONNECTION = (
        <connection_parameters>
  )
```

For the connection parameters available for accessing WebHDFS, see [Connection Parameters](/13-sql-reference/51-connect-parameters.md).
</TabItem>
</Tabs>

### FILE_FORMAT

See [Input & Output File Formats](../../13-sql-reference/50-file-format-options.md).

### copyOptions
```sql
copyOptions ::=
  [ SINGLE = TRUE | FALSE ]
  [ MAX_FILE_SIZE = <num> ]
```

| Parameter       | Description                                                                                                               | Required |
|-----------------|---------------------------------------------------------------------------------------------------------------------------|----------|
| `SINGLE`        | When TRUE, the command unloads data into one single file. Default: FALSE.                                                 | Optional |
| `MAX_FILE_SIZE` | The maximum size (in bytes) of each file to be created.<br />Effective when `SINGLE` is FALSE. Default: 67108864 (64 MB). | Optional |

## Examples

The following examples unload data into an internal stage:

```sql
-- Create a table
CREATE TABLE test_table (
     id INTEGER,
     name VARCHAR,
     age INT
);

-- Insert data into the table
INSERT INTO test_table (id,name,age) VALUES(1,'2',3), (4, '5', 6);

-- Create an internal stage
CREATE STAGE s2;

-- Unload the data in the table into a CSV file on the stage
COPY INTO @s2 FROM test_table FILE_FORMAT = (TYPE = CSV);

-- Unload the data from a query into a parquet file on the stage
COPY INTO @s2 FROM (SELECT name, age, id FROM test_table LIMIT 100) FILE_FORMAT = (TYPE = PARQUET);
```