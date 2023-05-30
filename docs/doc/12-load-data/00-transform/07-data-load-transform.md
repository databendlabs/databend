---
title: Transforming Data on Load
---

Databend offers a powerful feature that enables data transformation during the loading process using the [COPY INTO](../../14-sql-commands/10-dml/dml-copy-into-table.md) command. This functionality simplifies your ETL pipeline by incorporating basic transformations, eliminating the need for temporary tables. By transforming data during loading, you can streamline your ETL process effectively. Here are practical ways to enhance data loading with this feature:

- **Loading a subset of data columns**: Allows you to selectively import specific columns from a dataset, focusing on the data that is relevant to your analysis or application.

- **Reordering columns during load**: Enables you to change the order of columns while loading data, ensuring the desired column arrangement for better data organization or alignment with specific requirements.

- **Converting datatypes during load**: Provides the ability to convert datatypes of certain columns during the data loading process, allowing you to ensure consistency and compatibility with the desired data formats or analysis techniques.

- **Performing arithmetic operations during load**: Allows you to perform mathematical calculations and operations on specific columns as the data is being loaded, facilitating advanced data transformations or generating new derived data.

- **Loading data to a table with additional columns**: Enables you to load data into a table that already contains additional columns, accommodating the existing structure while mapping and inserting the data into the corresponding columns efficiently.

:::note
This feature is currently only available for the Parquet file format.
:::

## Tutorials

This section provides several brief tutorials that offer practical guidance on how to transform data while loading it. Each tutorial will walk you through the data loading process in two ways: loading directly from a remote file and loading from a staged file. Please note that these tutorials are independent of each other, and you don't need to complete them in order. Feel free to follow along based on your needs.

### Before You Begin

Download the sample file [employees.parquet](https://datasets.databend.rs/employees.parquet) and then upload it to your user stage using the [File Upload API](../../11-integrations/00-api/10-put-to-stage.md). If you query the file, you will find that it contains these records:

```sql
-- Query remote sample file directly
SELECT * FROM 'https://datasets.databend.rs/employees.parquet';

-- Query staged sample file
SELECT * FROM @~/employees.parquet;

id|name        |age|onboarded          |
--+------------+---+-------------------+
 2|Jane Doe    | 35|2022-02-15 13:30:00|
 1|John Smith  | 28|2022-01-01 09:00:00|
 3|Mike Johnson| 42|2022-03-10 11:15:00|
```

### Tutorial 1 - Loading a Subset of Data Columns

In this tutorial, you will create a table that has fewer columns than the sample file, and then populate it with the corresponding data extracted from the sample file.

1. Create a table without the 'age' column.

```sql
CREATE TABLE employees_no_age (
  id INT,
  name VARCHAR,
  onboarded timestamp
);
```

2. Load data from the remote or staged sample file, except for the 'age' column.

```sql
-- Load from remote file
COPY INTO employees_no_age
FROM (SELECT t.id, t.name, t.onboarded FROM 'https://datasets.databend.rs/employees.parquet' t)
FILE_FORMAT = (type = parquet) PATTERN='.*parquet';

-- Load from staged file
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

2. Load data from the remote or staged sample file in the new order.

```sql
-- Load from remote file
COPY INTO employees_new_order
FROM (SELECT t.id, t.age, t.name, t.onboarded FROM 'https://datasets.databend.rs/employees.parquet' t)
FILE_FORMAT = (type = parquet) PATTERN='.*parquet';

-- Load from staged file
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

2. Load data from the remote or staged sample file and convert the 'onboarded' column to Date type.

```sql
-- Load from remote file
COPY INTO employees_date
FROM (SELECT t.id, t.name, t.age, to_date(t.onboarded) FROM 'https://datasets.databend.rs/employees.parquet' t)
FILE_FORMAT = (type = parquet) PATTERN='.*parquet';

-- Load from staged file
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

### Tutorial 4 - Performing Arithmetic Operations During Load

In this tutorial, you will create a table with the same columns as the sample file. You will then extract and convert data from the sample file, perform arithmetic operations on the extracted data, and populate the table with the results.

1. Create a table containing exactly the same columns with the sample file:

```sql
CREATE TABLE employees_new_age (
  id INT,
  name VARCHAR,
  age INT,
  onboarded timestamp
);
```

2. Load data from the remote or staged sample file and perform an arithmetic operation on the 'age' column to increment its values by 1 before inserting it into the target table.

```sql
-- Load from remote file
COPY INTO employees_new_age
FROM (SELECT t.id, t.name, t.age+1, t.onboarded FROM 'https://datasets.databend.rs/employees.parquet' t)
FILE_FORMAT = (type = parquet) PATTERN='.*parquet';

-- Load from staged file
COPY INTO employees_new_age
FROM (SELECT t.id, t.name, t.age+1, t.onboarded FROM @~ t)
FILE_FORMAT = (type = parquet) PATTERN='.*parquet';
```

3. Check the loaded data:

```sql
SELECT * FROM employees_new_age

id|name        |age|onboarded          |
--+------------+---+-------------------+
 3|Mike Johnson| 43|2022-03-10 11:15:00|
 2|Jane Doe    | 36|2022-02-15 13:30:00|
 1|John Smith  | 29|2022-01-01 09:00:00|
```

### Tutorial 5 - Loading to a Table with Additional Columns

In this tutorial, you will a new table that includes additional columns compared to the sample file. You'll then extract data from the sample file, and finally populate the new table with the transformed data.

1. Create a table containing more columns than the sample file: 

```sql
CREATE TABLE employees_plus (
  id INT,
  name VARCHAR,
  age INT,
  onboarded timestamp,
  lastday timestamp
);
```

2. Load data from the staged sample file:

```sql
-- Load from remote file
COPY INTO employees_plus (id, name, age, onboarded)
FROM (SELECT t.id, t.name, t.age, t.onboarded FROM 'https://datasets.databend.rs/employees.parquet' t)
FILE_FORMAT = (type = parquet) PATTERN='.*parquet';

-- Load from staged file
COPY INTO employees_plus (id, name, age, onboarded)
FROM (SELECT t.id, t.name, t.age, t.onboarded FROM @~ t)
FILE_FORMAT = (type = parquet) PATTERN='.*parquet';
```

3. Check the loaded data:

```sql
SELECT * FROM employees_plus;

id|name        |age|onboarded          |lastday            |
--+------------+---+-------------------+-------------------+
 3|Mike Johnson| 42|2022-03-10 11:15:00|1970-01-01 00:00:00|
 1|John Smith  | 28|2022-01-01 09:00:00|1970-01-01 00:00:00|
 2|Jane Doe    | 35|2022-02-15 13:30:00|1970-01-01 00:00:00|
```