---
title: LIST_STAGE 
---

List files in a stage.

Almost the same as DDL command [LIST STAGE FILES](../../14-sql-commands/00-ddl/40-stage/04-ddl-list-stage.md), but as a table function, allow more query processing.

## Syntax

```sql
LIST_STAGE(
  LOCATION => '{ internalStage | externalStage }'
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

### example

```sql
SELECT * FROM list_stage(location => '@my_stage/', pattern => '.log');
+----------------+------+------------------------------------+-------------------------------+---------+
|      name      | size |                md5                 |         last_modified         | creator |
+----------------+------+------------------------------------+-------------------------------+---------+
| 2023/meta.log  |  475 | "4208ff530b252236e14b3cd797abdfbd" | 2023-04-19 20:23:24.000 +0000 | NULL    |
| 2023/query.log | 1348 | "1c6654b207472c277fc8c6207c035e18" | 2023-04-19 20:23:24.000 +0000 | NULL    |
+----------------+------+------------------------------------+-------------------------------+---------+
```

is the same as

```sql
LIST @my_stage PATTERN = '.log';
```