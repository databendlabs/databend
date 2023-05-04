---
title: Transforming Data During Load
---

Databend provides a powerful feature that allows you to transform data while loading it into a table using the COPY INTO command. This functionality simplifies your ETL (extract, transform, load) pipeline with basic transformations. By transforming data during a load, you can avoid the use of temporary tables, streamline your ETL process, and improve performance.

The COPY INTO command supports column reordering, column omission, and casts using a SELECT statement. This means that your data files don't need to have the same number and ordering of columns as your target table. You can also convert data types during the load, which is particularly useful if you need to change the format of data in your source files to match the data type of the target table.

:::note
This feature is currently only available for the Parquet file format.
:::

## Loading Subset of Table Data

The following example loads data from columns `id`, `name` of a staged Parquet file:

**Sample Data**

```text
id | name       | age
---|------------|----
 1 | John Doe   |  35
 2 | Jane Smith |  28
```

**Example**

```sql
CREATE TABLE my_table(id int, name string);

COPY INTO my_table
FROM (SELECT t.id, t.name FROM @mystage t)
FILE_FORMAT = (type = parquet) PATTERN='.*parquet';
```

## Reordering Columns During Load

To reorder the columns from a staged Parquet file before loading it into a table, you can use the `COPY INTO` command with a `SELECT` statement. The following example reorders the columns `name` and `id`:

**Sample Data**

```text
id | name       | age
---|------------|----
 1 | John Doe   |  35
 2 | Jane Smith |  28
```

**Example**

```sql
CREATE TABLE my_table(name string, id int);

COPY INTO my_table
FROM (SELECT t.name, t.id FROM @mystage t)
FILE_FORMAT = (type = parquet) PATTERN='.*parquet';
```

## Converting Datatypes During Load

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