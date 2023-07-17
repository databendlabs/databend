---
title: USE DATABASE
---

Selects a database for the current session. This statement allows you to specify and switch to a different database. Once you set the current database using this command, it remains the same until the end of the session unless you choose to change it.

## Syntax

```sql
USE <database_name>
```

## Examples

```sql
-- Create two databases
CREATE DATABASE database1;
CREATE DATABASE database2;

-- Select and use "database1" as the current database
USE database1;

-- Create a new table "table1" in "database1"
CREATE TABLE table1 (
  id INT,
  name VARCHAR(50)
);

-- Insert data into "table1"
INSERT INTO table1 (id, name) VALUES (1, 'John');
INSERT INTO table1 (id, name) VALUES (2, 'Alice');

-- Query all data from "table1"
SELECT * FROM table1;

-- Switch to "database2" as the current database
USE database2;

-- Create a new table "table2" in "database2"
CREATE TABLE table2 (
  id INT,
  city VARCHAR(50)
);

-- Insert data into "table2"
INSERT INTO table2 (id, city) VALUES (1, 'New York');
INSERT INTO table2 (id, city) VALUES (2, 'London');

-- Query all data from "table2"
SELECT * FROM table2;
```