---
title: INFER_SCHEMA
---

Automatically detects the file metadata schema and retrieves the column definitions.


:::caution

`infer_schema` currently only supports parquet file format.

:::

## Syntax

```sql
INFER_SCHEMA(
  LOCATION => '{ internalStage | externalStage }'
  [ PARTTERN => '<regex_pattern>']
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

### PATTERN = 'regex_pattern'

A [PCRE2](https://www.pcre.org/current/doc/html/)-based regular expression pattern string, enclosed in single quotes, specifying the file names to match. Click [here](#loading-data-with-pattern-matching) to see an example. For PCRE2 syntax, see http://www.pcre.org/current/doc/html/pcre2syntax.html.

## Examples

Generate a parquet file in a stage:

```sql
CREATE STAGE infer_parquet FILE_FORMAT = (TYPE = PARQUET);
COPY INTO @infer_parquet FROM (SELECT * FROM numbers(10)) FILE_FORMAT = (TYPE = PARQUET);
```

```sql
LIST @infer_parquet;
+-------------------------------------------------------+------+------------------------------------+-------------------------------+---------+
| name                                                  | size | md5                                | last_modified                 | creator |
+-------------------------------------------------------+------+------------------------------------+-------------------------------+---------+
| data_e0fd9cba-f45c-4c43-aa07-d6d87d134378_0_0.parquet |  258 | "7DCC9FFE04EA1F6882AED2CF9640D3D4" | 2023-02-09 05:21:52.000 +0000 | NULL    |
+-------------------------------------------------------+------+------------------------------------+-------------------------------+---------+
```

### `infer_schema`


```sql
SELECT * FROM INFER_SCHEMA(location => '@infer_parquet/data_e0fd9cba-f45c-4c43-aa07-d6d87d134378_0_0.parquet');
+-------------+-----------------+----------+----------+
| column_name | type            | nullable | order_id |
+-------------+-----------------+----------+----------+
| number      | BIGINT UNSIGNED |        0 |        0 |
+-------------+-----------------+----------+----------+
```

### `infer_schema` with Pattern Matching

```sql
SELECT * FROM infer_schema(location => '@infer_parquet/', pattern => '.*parquet');
+-------------+-----------------+----------+----------+
| column_name | type            | nullable | order_id |
+-------------+-----------------+----------+----------+
| number      | BIGINT UNSIGNED |        0 |        0 |
+-------------+-----------------+----------+----------+
```

### Create a Table From Parquet File

The `infer_schema` can only display the schema of a parquet file and cannot create a table from it. 

To create a table from a parquet file, use the [Stage Table Function](./stage_table_function.md) as following:

```sql
CREATE TABLE mytable AS SELECT * FROM @infer_parquet/ (pattern=>'.*parquet') LIMIT 0;

DESC mytable;
+--------+-----------------+------+---------+-------+
| Field  | Type            | Null | Default | Extra |
+--------+-----------------+------+---------+-------+
| number | BIGINT UNSIGNED | NO   | 0       |       |
+--------+-----------------+------+---------+-------+
```
