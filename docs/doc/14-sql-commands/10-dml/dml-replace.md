---
title: REPLACE INTO
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.55"/>

REPLACE INTO can either insert multiple new rows into a table or update existing rows if those rows already exist, using the following sources of data:

- Direct values

- Query results

- Staged files: Databend enables you to replace data into a table from staged files with the REPLACE INTO statement. This is achieved through Databend's capacity to [Query Staged Files](../../12-load-data/00-transform/05-querying-stage.md) and subsequently incorporate the query result into the table.

:::tip atomic operations
Databend ensures data integrity with atomic operations. Inserts, updates, replaces, and deletes either succeed completely or fail entirely.
:::

## Syntax

```sql
REPLACE INTO <table_name> [ ( <col_name> [ , ... ] ) ]
    ON (<CONFLICT KEY>) ...
```

REPLACE INTO updates existing rows when the specified conflict key is found in the table and inserts new rows if the conflict key is not present. The conflict key is a column or combination of columns in a table that uniquely identifies a row and is used to determine whether to insert a new row or update an existing row in the table using the REPLACE INTO statement. See an example below:

```sql
CREATE TABLE employees (
    employee_id INT,
    employee_name VARCHAR(100),
    employee_salary DECIMAL(10, 2),
    employee_email VARCHAR(255)
);

-- This REPLACE INTO inserts a new row
REPLACE INTO employees (employee_id, employee_name, employee_salary, employee_email) ON (employee_email)
VALUES (123, 'John Doe', 50000, 'john.doe@example.com');

-- This REPLACE INTO updates the inserted row
REPLACE INTO employees (employee_id, employee_name, employee_salary, employee_email) ON (employee_email)
VALUES (123, 'John Doe', 60000, 'john.doe@example.com');
```

## Distributed REPLACE INTO

REPLACE INTO supports distributed execution in cluster environments. You can enable distributed REPLACE INTO by setting ENABLE_DISTRIBUTED_REPLACE_INTO to 1. This helps enhance data loading performance and scalability in cluster environments.

```sql
SET enable_distributed_replace_into = 1;
```

## Examples

### Example 1: Replace with Direct Values

This example replaces data with direct values:

```sql
CREATE TABLE employees(id INT, name VARCHAR, salary INT);

REPLACE INTO employees (id, name, salary) ON (id)
VALUES (1, 'John Doe', 50000);

SELECT  * FROM Employees;
+------+----------+--------+
| id   | name     | salary |
+------+----------+--------+
| 1    | John Doe |  50000 |
+------+----------+--------+
```

### Example 2: Replace with Query Results

This example replaces data with a query result:

```sql
CREATE TABLE employees(id INT, name VARCHAR, salary INT);

CREATE TABLE temp_employees(id INT, name VARCHAR, salary INT);

INSERT INTO temp_employees (id, name, salary) VALUES (1, 'John Doe', 60000);

REPLACE INTO employees (id, name, salary) ON (id)
SELECT id, name, salary FROM temp_employees WHERE id = 1;

SELECT  * FROM Employees;
+------+----------+--------+
| id   | name     | salary |
+------+----------+--------+
|    1 | John Doe |  60000 |
+------+----------+--------+
```

### Example 3: Replace with Staged Files

This example replaces data with a staged data file:

1. Create a table called "sample":

```sql
CREATE TABLE sample
(
    Id      INT,
    City    VARCHAR,
    Score   INT,
    Country VARCHAR DEFAULT 'China'
);
```

2. Create an internal stage and upload a sample CSV file called [sample_3_replace.csv](https://github.com/ZhiHanZ/databend/blob/0f333a13fc38548595ea58242a37c5f4a73e9c88/tests/data/sample_3_replace.csv) to the stage with PRESIGN:

```sql
CREATE STAGE s1 FILE_FORMAT = (TYPE = CSV);

PRESIGN UPLOAD @s1/sample_3_replace.csv;

| method | headers                               | url                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
|--------|---------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| PUT    | {"host":"s3.us-east-2.amazonaws.com"} | https://s3.us-east-2.amazonaws.com/query-storage-53b9412/tn3ftqihs/stage/internal/s1/sample_3_replace.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ASIAT2EUTJTKFPM7OYFZ%2F20230913%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20230913T023209Z&X-Amz-Expires=3600&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDsaCXVzLWVhc3QtMiJIMEYCIQDfDn6EBei4IHRhmFNldu%2FpafMhHwx%2B934HQDafsfFQOAIhAI38G%2FaKG3GFso8qHBCguoL3GvXUIDaKDJ3bJs5VBSwoKvwECCQQABoMMjYyMzA0NjQ4NDA0IgxyYEa7Xes%2Bb%2FnDT%2Fkq2QRkkQi83V9qVKyZJP0UoOBZEaFIS1qkPd2gEObVd3%2BA8yq8wVhdr749DmvZ7sXWlcXsXmXOjnl9cxwkvcJuXZ%2F1LVO5Kh3vTSF3dbNkbkIY3z9pEOX70llHFSenSSo8f44wqzsFkuLanwpzWjL2eFn%2B1boz7iDuWY7p2bb7ZtoTkYat4TrHQWpG2hPayk3Sn6ueAfBMCnYJ3oMy2a1G7F0onz3pM%2FFSRxCe7tsPMAEg2wP24YnXhKCUaq7xo8Gvy81FKNhhPr8XWYW0tHBON3aWh7t1q6mJw%2B3KeUtMI6Cdz1BsqhGpLgMUB%2BPctxHmlm2UVUk72LsmxioAKq4Fl48jFsMz7fwKjbheMqv6jKlzgu%2B8B4V6DCo2KqsTsip%2FoOevk4R4X5OTqA4FQ3Qy%2BX%2FtMUMKohXkXKYSJPP15XPOYsogXrQWhszK%2B%2FaUth%2FzY8GAzYf0MemnooACTkDE6A8v5uB%2FRoPQwSPCQ0Dwbn%2FNrLVC3c649l%2FWh7iy2FcE2CDm7yppj5XklttYuhwiuQ%2B2WnDcRn0yesqeTeRoDP0lBZyGj%2FlB7hATTqDZV2lSSFI737sU8BWBncOoTqBltaClBdtIQkTtmheDAMtNdQ8zvF5ZmFetF4eUU0D3AZ3FD90lTUZ6gSPGfVlIZbwY%2BBW%2FmG1tP5%2BaokXkMnywPaYvtep1HwR3cHg%2B8qoZW5o11yPCRAd0MEZmOaYO18JSYuwejam8pb%2F1BVbi%2B6a1W62ohAa4zCH29%2BGGISNqjLcKTZQOA6gEt7%2BZoUxd4mQ5wg4BxIpqEXL%2F0YpcMKm%2BhKgGOpoBWEV2udBO%2FX9wSP%2FAMK4KwmeIboZ1aQpwkBgUmtP%2FsgXErKghAm54PA7dK1n7sm%2FqOBQjXuRWTj%2B3iykJaT97dWoutgmqYgqj377TweIVffXF0cSHx1%2F3ri3aXmZ9fh4GAfcfhzs7NugH%2Fk2IkORKHHv3tGmlKGHLVp8XL0bXIqTsCthRRJvOwlYIaPumBhfaEA38PAs%2BSeEwwA%3D%3D&X-Amz-SignedHeaders=host&X-Amz-Signature=43e3aa7c2bb5a08ce8ae9746b70b1d1c743a57937fa2bd596b1170f00bcf4f34 |
```

```shell
curl -X PUT -T sample_3_replace.csv "https://s3.us-east-2.amazonaws.com/query-storage-53b9412/tn3ftqihs/stage/internal/s1/sample_3_replace.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ASIAT2EUTJTKFPM7OYFZ%2F20230913%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20230913T023209Z&X-Amz-Expires=3600&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDsaCXVzLWVhc3QtMiJIMEYCIQDfDn6EBei4IHRhmFNldu%2FpafMhHwx%2B934HQDafsfFQOAIhAI38G%2FaKG3GFso8qHBCguoL3GvXUIDaKDJ3bJs5VBSwoKvwECCQQABoMMjYyMzA0NjQ4NDA0IgxyYEa7Xes%2Bb%2FnDT%2Fkq2QRkkQi83V9qVKyZJP0UoOBZEaFIS1qkPd2gEObVd3%2BA8yq8wVhdr749DmvZ7sXWlcXsXmXOjnl9cxwkvcJuXZ%2F1LVO5Kh3vTSF3dbNkbkIY3z9pEOX70llHFSenSSo8f44wqzsFkuLanwpzWjL2eFn%2B1boz7iDuWY7p2bb7ZtoTkYat4TrHQWpG2hPayk3Sn6ueAfBMCnYJ3oMy2a1G7F0onz3pM%2FFSRxCe7tsPMAEg2wP24YnXhKCUaq7xo8Gvy81FKNhhPr8XWYW0tHBON3aWh7t1q6mJw%2B3KeUtMI6Cdz1BsqhGpLgMUB%2BPctxHmlm2UVUk72LsmxioAKq4Fl48jFsMz7fwKjbheMqv6jKlzgu%2B8B4V6DCo2KqsTsip%2FoOevk4R4X5OTqA4FQ3Qy%2BX%2FtMUMKohXkXKYSJPP15XPOYsogXrQWhszK%2B%2FaUth%2FzY8GAzYf0MemnooACTkDE6A8v5uB%2FRoPQwSPCQ0Dwbn%2FNrLVC3c649l%2FWh7iy2FcE2CDm7yppj5XklttYuhwiuQ%2B2WnDcRn0yesqeTeRoDP0lBZyGj%2FlB7hATTqDZV2lSSFI737sU8BWBncOoTqBltaClBdtIQkTtmheDAMtNdQ8zvF5ZmFetF4eUU0D3AZ3FD90lTUZ6gSPGfVlIZbwY%2BBW%2FmG1tP5%2BaokXkMnywPaYvtep1HwR3cHg%2B8qoZW5o11yPCRAd0MEZmOaYO18JSYuwejam8pb%2F1BVbi%2B6a1W62ohAa4zCH29%2BGGISNqjLcKTZQOA6gEt7%2BZoUxd4mQ5wg4BxIpqEXL%2F0YpcMKm%2BhKgGOpoBWEV2udBO%2FX9wSP%2FAMK4KwmeIboZ1aQpwkBgUmtP%2FsgXErKghAm54PA7dK1n7sm%2FqOBQjXuRWTj%2B3iykJaT97dWoutgmqYgqj377TweIVffXF0cSHx1%2F3ri3aXmZ9fh4GAfcfhzs7NugH%2Fk2IkORKHHv3tGmlKGHLVp8XL0bXIqTsCthRRJvOwlYIaPumBhfaEA38PAs%2BSeEwwA%3D%3D&X-Amz-SignedHeaders=host&X-Amz-Signature=43e3aa7c2bb5a08ce8ae9746b70b1d1c743a57937fa2bd596b1170f00bcf4f34"
```

```sql
LIST @s1;

| name                 | size | md5                                | last_modified                 | creator |
|----------------------|------|------------------------------------|-------------------------------|---------|
| sample_3_replace.csv | 83   | "42807a735a36f9bde392fee5834b22c4" | 2023-09-13 02:43:29.000 +0000 | NULL    |
```

3. Insert data from the staged CSV file with REPLACE INTO:

:::tip
You can specify the file format and various copy-related settings with the FILE_FORMAT and COPY_OPTIONS available in the [COPY INTO](dml-copy-into-table.md) command. When `purge` is set to `true`, the original file will only be deleted if the data update is successful. 
:::

```sql
REPLACE INTO sample (Id, City, Score) ON(Id) SELECT $1, $2, $3 FROM @s1 (FILE_FORMAT=>'csv');

-- Verify the inserted data
SELECT * FROM sample;

id|city       |score|country|
--+-----------+-----+-------+
 1|'Chengdu'  |   80|China  |
 3|'Chongqing'|   90|China  |
 6|'HangZhou' |   92|China  |
 9|'Changsha' |   91|China  |
10|'Hong Kong'|   88|China  |
```