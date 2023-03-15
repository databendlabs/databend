---
title: Transforming Data During a Load
sidebar_label: Transforming Data During a Load
description: Learn how to use Databend to transform data while loading it into a table using the COPY INTO <table> command. 
---

Databend supports transforming data while loading it into a table using the `COPY INTO <table>` command, which simplifies your ETL pipeline for basic transformations. 

This feature helps you avoid the use of temporary tables to store pre-transformed data when reordering columns during a data load.

The `COPY` command supports:
- Column reordering, column omission, and casts using a SELECT statement. There is no requirement for your data files to have the same number and ordering of columns as your target table.

:::note
Transforming is only supported for Parquet format in the stage.
:::

## Load a Subset of Table Data

Load a subset of data into a table. The following example loads data from columns `id`, `name` of a staged Parquet file:

**Sample Data**
```text
id | name       | age
---|------------|----
 1 | John Doe   |  35
 2 | Jane Smith |  28
```

**Example**
```sql
-- create a table
CREATE TABLE my_table(id int, name string);

COPY INTO my_table
FROM (SELECT t.id, t.name FROM @mystage t)
FILE_FORMAT = (type = parquet) PATTERN='.*parquet';
````

## Reorder Columns During a Load

To reorder the columns from a staged Parquet file before loading it into a table, you can use the `COPY INTO` command with a `SELECT` statement. The following example reorders the columns `name` and `id`:

**Sample Data**
```text
id | name       | age
---|------------|----
 1 | John Doe   |  35
 2 | Jane Smith |  28
```

**Example**
````sql
CREATE TABLE my_table(name string, id int);

COPY INTO my_table
FROM (SELECT t.name, t.id FROM @mystage t)
FILE_FORMAT = (type = parquet) PATTERN='.*parquet';
````

## Convert Data Types During a Load

To convert staged data into other data types during a data load, you can use the appropriate conversion function in your `SELECT` statement.

The following example converts a timestamp into a date:

**Sample Data**
```text
id | name    | timestamp
---|---------|--------------------
 1 | John Doe| 2022-03-15 10:30:00
 2 | Jane Doe| 2022-03-14 09:00:00
```

**Example**
```sql
CREATE TABLE my_table(id int, name string, time date);

COPY INTO my_table
FROM (SELECT t.id, t.name, to_date(t.timestamp) FROM @mystage t)
FILE_FORMAT = (type = parquet) PATTERN='.*parquet';
```

## Conclusion

Transforming data during a load is a powerful feature of Databend that allows you to simplify your ETL pipeline and avoid the use of temporary tables. With the ability to transform data during a load, you can streamline your ETL pipeline and focus on the analysis of your data rather than the mechanics of moving it around.