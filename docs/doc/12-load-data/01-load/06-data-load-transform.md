---
title: Transforming Data During Load
---

Databend provides a powerful feature that allows you to transform data while loading it into a table using the [COPY INTO](../../14-sql-commands/10-dml/dml-copy-into-table.md) command. This functionality simplifies your ETL (extract, transform, load) pipeline with basic transformations. By transforming data during a load, you can avoid the use of temporary tables, streamline your ETL process, and improve performance.

The COPY INTO command supports column reordering, column omission, and casts using a SELECT statement. This means that your data files don't need to have the same number and ordering of columns as your target table. You can also convert data types during the load, which is particularly useful if you need to change the format of data in your source files to match the data type of the target table.

:::note
This feature is currently only available for the Parquet file format.
:::

## Tutorials

This section provides three brief tutorials that offer practical guidance on how to transform data while loading it. Please note that these tutorials are independent of each other, and you don't need to complete them in order. Feel free to follow along based on your needs.

### Before You Begin

Download the sample file [employees.parquet](https://datasets.databend.rs/employees.parquet) and then upload it to your user stage using the [File Upload API](../../11-integrations/00-api/10-put-to-stage.md). If you query the file after upload, you will find that it contains these records:

```sql
SELECT * FROM @~/employees.parquet;

id|name        |age|onboarded          |
--+------------+---+-------------------+
 2|Jane Doe    | 35|2022-02-15 13:30:00|
 1|John Smith  | 28|2022-01-01 09:00:00|
 3|Mike Johnson| 42|2022-03-10 11:15:00|
```

### Tutorial 1 - Loading Subset of Table Data

In this tutorial, you will create a table that has fewer columns than the sample file, and then populate it with the corresponding data extracted from the sample file.

1. Create a table without the 'age' column.

```sql
CREATE TABLE employees_no_age (
  id INT,
  name VARCHAR,
  onboarded timestamp
);
```

2. Load data from the staged sample file, except for the 'age' column.

```sql
COPY INTO employees_no_age
FROM (SELECT t.id, t.name, t.onboarded FROM @~ t)
FILE_FORMAT = (type = parquet) PATTERN='.*parquet';
```

3. Check the loaded data:

```sql
SELECT * FROM employees_no_age;

id|name        |onboarded          |
--+------------+-------------------+
 2|Jane Doe    |2022-02-15 13:30:00|
 1|John Smith  |2022-01-01 09:00:00|
 3|Mike Johnson|2022-03-10 11:15:00|
```

### Tutorial 2 - Reordering Columns During Load

In this tutorial, you will create a table that has the same columns as the sample file but in a different order, and then populate it with the corresponding data extracted from the sample file.

1. Create a table where the 'name' and 'age' columns are swapped.

```sql
CREATE TABLE employees_new_order (
  id INT,
  age INT,
  name VARCHAR,
  onboarded timestamp
);
```

2. Load data from the staged sample file in the new order.

```sql
COPY INTO employees_new_order
FROM (SELECT t.id, t.age, t.name, t.onboarded FROM @~ t)
FILE_FORMAT = (type = parquet) PATTERN='.*parquet';
```

3. Check the loaded data:

```sql
SELECT * FROM employees_new_order;

id|age|name        |onboarded          |
--+---+------------+-------------------+
 3| 42|Mike Johnson|2022-03-10 11:15:00|
 2| 35|Jane Doe    |2022-02-15 13:30:00|
 1| 28|John Smith  |2022-01-01 09:00:00|
```

### Tutorial 3 - Converting Datatypes During Load

In this tutorial, you will create a table that has the same columns as the sample file, except for one which will have a different data type, and then populate it with the data extracted and converted from the sample file.

1. Create a table with the 'onboarded' column of Date type.

```sql
CREATE TABLE employees_date (
  id INT,
  name VARCHAR,
  age INT,
  onboarded date
);
```

2. Load data from the staged sample file and convert the 'onboarded' column to Date type.

```sql
COPY INTO employees_date
FROM (SELECT t.id, t.name, t.age, to_date(t.onboarded) FROM @~ t)
FILE_FORMAT = (type = parquet) PATTERN='.*parquet';
```

3. Check the loaded data:

```sql
SELECT * FROM employees_date;

id|name        |age|onboarded |
--+------------+---+----------+
 3|Mike Johnson| 42|2022-03-10|
 1|John Smith  | 28|2022-01-01|
 2|Jane Doe    | 35|2022-02-15|
```