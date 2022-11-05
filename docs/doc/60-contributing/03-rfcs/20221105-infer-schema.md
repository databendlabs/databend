---
title: Infer Schema
---

- RFC PR: [datafuselabs/databend#8645](https://github.com/datafuselabs/databend/pull/8645)
- Tracking Issue: [datafuselabs/databend#8646](https://github.com/datafuselabs/databend/issues/8646)

## Summary

Allowing infer schema from staged files or locations.

## Motivation

To load data from a stage or location, users need to create table first. However, sometimes users don't know the file schema, or the schema is too complex / too simple to be input by users.

Allowing infer schema from existing files makes our users' lives easier. Also, this feature will unlock the databend from implementing `select * from @my_stage`.

## Guide-level explanation

Users will be able to infer the schema of the staged file by:

```sql
INFER @my_stage/data.csv FILE_FORMAT = ( TYPE = CSV);

+-------------+---------+----------+
| COLUMN_NAME | TYPE    | NULLABLE |
|-------------+---------+----------|
| CONTINENT   | TEXT    | True     |
| COUNTRY     | VARIANT | True     |
+-------------+---------+----------+
```

Infer from an external location is also supported:

```sql
INFER 's3://mybucket/data.csv' FILE_FORMAT = ( TYPE = CSV );

+-------------+---------+----------+
| COLUMN_NAME | TYPE    | NULLABLE |
|-------------+---------+----------|
| CONTINENT   | TEXT    | True     |
| COUNTRY     | VARIANT | True     |
+-------------+---------+----------+
```

`CREATE TABLE` will support `CREATE TABLE <table> BY ( <Query> )` to accept the output from `INFER`, so users can create a table with `INFRE` directly.

For example:

```sql
CREATE TABLE test BY (
    INFER 's3://mybucket/data.csv' FILE_FORMAT = ( TYPE = CSV )
);
```

Users can also make some changes to the output of `INFER`:

```sql
CREATE TABLE test BY (
    SELECT UPPER(COLUMN_NAME), TYPE, TRUE from (
        INFER 's3://mybucket/data.csv' FILE_FORMAT = ( TYPE = CSV )
    )
);
```

## Reference-level explanation

We will adopt the `infer_schema` feature from `arrow2` to make this possible. Take `csv` as an example:

```rust
pub fn infer_schema<R: Read + Seek, F: Fn(&[u8]) -> DataType>(
    reader: &mut Reader<R>,
    max_rows: Option<usize>,
    has_header: bool,
    infer: &F
) -> Result<(Vec<Field>, usize)>
```

We will then convert the `Field` into data types that the databend supports.

## Drawbacks

None.

## Rationale and alternatives

None.

## Prior art

### Snowflake

Snowflake has `infer_schema` functions:

```sql
select *
  from table(
    infer_schema(
      location=>'@mystage'
      , file_format=>'my_parquet_format'
      )
    );

+-------------+---------+----------+---------------------+--------------------------+----------+
| COLUMN_NAME | TYPE    | NULLABLE | EXPRESSION          | FILENAMES                | ORDER_ID |
|-------------+---------+----------+---------------------+--------------------------|----------+
| continent   | TEXT    | True     | $1:continent::TEXT  | geography/cities.parquet | 0        |
| country     | VARIANT | True     | $1:country::VARIANT | geography/cities.parquet | 1        |
| COUNTRY     | VARIANT | True     | $1:COUNTRY::VARIANT | geography/cities.parquet | 2        |
+-------------+---------+----------+---------------------+--------------------------+----------+
```

Snowflake's `CREATE TABLE` support `USING TEMPLATE` to accept the output from `infer_schema`:

```sql
create table mytable
  using template (
    select array_agg(object_construct(*))
    within group (order by order_id)
      from table(
        infer_schema(
          location=>'@mystage',
          file_format=>'my_parquet_format'
        )
      ));
```

## Unresolved questions

None.

## Future possibilities

None.
