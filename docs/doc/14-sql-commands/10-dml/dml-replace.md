---
title: REPLACE INTO
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.55"/>

REPLACE INTO either inserts a new row into a table or updates an existing row if the row already exists. 

When a row with the specified [conflict key](#what-is-conflict-key) already exists in the table, REPLACE INTO will replace that existing row with the new data provided. However, if the row does not exist, a new row will be inserted into the table with the specified data.

:::tip atomic operations
Databend ensures data integrity with atomic operations. Inserts, updates, replaces, and deletes either succeed completely or fail entirely.
:::

## Syntax

```sql
REPLACE INTO <table_name> [ ( <col_name> [ , ... ] ) ]
    ON (<CONFLICT KEY>) ...
```

### What is Conflict Key?

The conflict key is a column or combination of columns in a table that uniquely identifies a row and is used to determine whether to insert a new row or update an existing row in the table using the REPLACE INTO statement. It can be any column or combination of columns with a unique constraint, not just the primary key(Databend doesn't have primary key).

For example, in a table called "employees" with a unique constraint on the "employee_email" column, you can use the "employee_email" column as the conflict key in the REPLACE INTO statement:

```sql
REPLACE INTO employees (employee_id, employee_name, employee_salary, employee_email) ON (employee_email)
VALUES (123, 'John Doe', 50000, 'john.doe@example.com');
```

## Examples

Here are some examples that show how to use the `REPLACE INTO` statement in Databend:

### Replace with Values

```sql
CREATE TABLE employees(id INT, name VARCHAR, salary INT);
REPLACE INTO employees (id, name, salary) ON (id)
VALUES (1, 'John Doe', 50000);
```

```sql
SELECT  * FROM Employees;
+------+----------+--------+
| id   | name     | salary |
+------+----------+--------+
|    1 | John Doe |  50000 |
+------+----------+--------+
```

### Replace with Query Results

In this case, the data to be inserted or updated comes from a temporary table called `temp_employees`:

```sql
CREATE TABLE employees(id INT, name VARCHAR, salary INT);
-- Create a temp stage table
CREATE TABLE temp_employees(id INT, name VARCHAR, salary INT);
INSERT INTO temp_employees (id, name, salary) VALUES (1, 'John Doe', 60000);

REPLACE INTO employees (id, name, salary) ON (id)
SELECT id, name, salary FROM temp_employees WHERE id = 1;
```

```sql
SELECT  * FROM Employees;
+------+----------+--------+
| id   | name     | salary |
+------+----------+--------+
|    1 | John Doe |  60000 |
+------+----------+--------+
```

### Replace with Staged Files

Here is an example of using staged files for updating with a REPLACE INTO statement. 

First, create a table called "sample":

```sql
CREATE TABLE sample
(
    Id      INT,
    City    VARCHAR,
    Score   INT,
    Country VARCHAR DEFAULT 'China'
);
```

Then, create an internal stage and upload a sample CSV file called [sample_3_replace.csv](https://github.com/ZhiHanZ/databend/blob/0f333a13fc38548595ea58242a37c5f4a73e9c88/tests/data/sample_3_replace.csv) to the stage:

```sql
CREATE STAGE s1 FILE_FORMAT = (TYPE = CSV);
```

```shell
curl -u root: -H "stage_name:s1" -F "upload=@sample_3_replace.csv" -XPUT "http://localhost:8000/v1/upload_to_stage"

{"id":"b8305187-c816-4bb5-8350-c441b85baaf9","stage_name":"s1","state":"SUCCESS","files":["sample_3_replace.csv"]}   
```

```sql
LIST @s1;

name                |size|md5|last_modified                |creator|
--------------------+----+---+-----------------------------+-------+
sample_3_replace.csv|  83|   |2023-06-12 03:01:56.522 +0000|       |
```

Use Databend's [HTTP handler](../../11-integrations/00-api/01-mysql-handler.md) to replace data with from a staged CSV file:

```shell
curl -s -u root: -XPOST "http://localhost:8000/v1/query" --header 'Content-Type: application/json' -d '{"sql": "REPLACE INTO sample (Id, City, Score) ON(Id) VALUES", "stage_attachment": {"location": "@s1/sample_3_replace.csv", "copy_options": {"purge": "true"}}}'

{"id":"92182fc6-11b9-461b-8fbd-f82ecaa637ef","session_id":"f5caf18a-5dc8-422d-80b7-719a6da76039","session":{},"schema":[],"data":[],"state":"Succeeded","error":null,"stats":{"scan_progress":{"rows":5,"bytes":83},"write_progress":{"rows":5,"bytes":277},"result_progress":{"rows":0,"bytes":0},"total_scan":{"rows":0,"bytes":0},"running_time_ms":143.632441},"affect":null,"stats_uri":"/v1/query/92182fc6-11b9-461b-8fbd-f82ecaa637ef","final_uri":"/v1/query/92182fc6-11b9-461b-8fbd-f82ecaa637ef/final","next_uri":"/v1/query/92182fc6-11b9-461b-8fbd-f82ecaa637ef/final","kill_uri":"/v1/query/92182fc6-11b9-461b-8fbd-f82ecaa637ef/kill"}
```

Verify the inserted data:

```sql
SELECT * FROM sample;

id|city       |score|country|
--+-----------+-----+-------+
 1|'Chengdu'  |   80|China  |
 3|'Chongqing'|   90|China  |
 6|'HangZhou' |   92|China  |
 9|'Changsha' |   91|China  |
10|'Hong Kong‘|   88|China  |
```