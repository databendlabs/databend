---
title: LIST_STAGE 
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.32"/>

Lists files in a stage. This allows you to filter files in a stage based on their extensions and obtain comprehensive details about each file. The function is similar to the DDL command [LIST STAGE FILES](../../14-sql-commands/00-ddl/40-stage/04-ddl-list-stage.md), but provides you the flexibility to retrieve specific file information with the SELECT statement, such as file name, size, MD5 hash, last modified timestamp, and creator, rather than all file information.

## Syntax

```sql
LIST_STAGE(
  LOCATION => '{ internalStage | externalStage | userStage }'
  [ PATTERN => '<regex_pattern>']
)
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

### userStage

```sql
userStage ::= @~[/<path>]
```

### PATTERN

See [COPY INTO table](/14-sql-commands/10-dml/dml-copy-into-table.md).


## Examples

```sql
SELECT * FROM list_stage(location => '@my_stage/', pattern => '.*[.]log');
+----------------+------+------------------------------------+-------------------------------+---------+
|      name      | size |                md5                 |         last_modified         | creator |
+----------------+------+------------------------------------+-------------------------------+---------+
| 2023/meta.log  |  475 | "4208ff530b252236e14b3cd797abdfbd" | 2023-04-19 20:23:24.000 +0000 | NULL    |
| 2023/query.log | 1348 | "1c6654b207472c277fc8c6207c035e18" | 2023-04-19 20:23:24.000 +0000 | NULL    |
+----------------+------+------------------------------------+-------------------------------+---------+

-- Equivalent to the following statement:
LIST @my_stage PATTERN = '.log';
```