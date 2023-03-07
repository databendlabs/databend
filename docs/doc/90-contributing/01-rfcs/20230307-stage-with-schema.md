---
title: Stage With Schema
---

## Summary

stage support `schema` option.

## Motivation

Currently, when query/copy/insert data from files, we obtain the schema by 2 ways:

1. use the schema of the dest table (used in copy/insert).
2. use the schema of inferred from data (used in stage_table_function).

To support "transform during load", the first way can not be used.

Inferred schema, although convenient, has many drawbacks (detailed in 'drawbacks of infer_schema' section below).

## Guide-level explanation

basic form:

```sql
SELECT c1, c2 + c1, trim(c2) from @my_stage(schema=(c1 int, c2 float, c3 string))
```

it can be used to support "transform during load".

```sql
copy into my_table from (
    SELECT c1, c2 + c1, trim(c2)  FROM @my_stage(schema=(c1 int, c2 float, c3 string))
)
```

if the same schema is used frequently, user can create table for it and use table name.

```sql
SELECT  c1, c2 + c1, trim(c2)  FROM @my_stage(schema='db_name.table_name')
```

and user can get knowledge about the data with `desc <table>` instead of `infer schema` each time.

## Reference-level explanation

```sql
SELECT <exprs> FROM @<stage>/'<uri>'(SCHEMA= (<schema> | "<table_name>"), ..)
```

## Rationale and alternatives

### drawbacks of infer_schema

risk of wrong schema: schema infer depend on the file chosen to be inferred, but data may be bad.

only a rough schema can be inferred.

1. for CSV/TSV, all columns are `STRING` type.
  - user can only use column name `$1`, `$2`, which is not friendly when there is a lot of columns.
2. for ndjson, all columns is `VARINT` type.
3. even for parquet, columns of high level types like variant/datetime in string format can not be mapped directly.

this leads to overhead of cast: deserialize while read is faster than read into `TYPE1` and then cast to `TYPE2`.

overhead of infer_schema itself, at least 2 operations:

1. list dir 
2. read meta/head of a file.

limits to file source: infer schema can only be used in copy, not streaming insert.

### alternatives 

#### WITH_SCHEMA 

```sql

SELECT <exprs> FROM @<stage>|'<uri>' WITH_SCHEMA <data_schema> 
```

drawback: hard to read and parse when used with other table or nested query, e.g.:

```sql
select * (SELECT <exprs> FROM @stage1 (format='json')  WITH_SCHEMA <data_schema> t) join my_table2
```

#### WITH_TRANSFORM

```sql
insert into my_table from @my_stage WITH_TRANSFORM  t.c1, t.c2 + 1 FROM t(c1 int, c2 float, c3 string)
```

drawback: only applied in copy/insert, can not help with `stage table function`.

## Future possibilities

ignore fields with `_`. e.g.:

```sql
SELECT c1,  c3   FROM @my_stage(schema=(c1 int, _ , c3 int))
```
