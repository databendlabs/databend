---
title: DELETE
---

Removes one or more rows from a table.

:::tip atomic operations
Databend ensures data integrity with atomic operations. Inserts, updates, replaces, and deletes either succeed completely or fail entirely.
:::

## Syntax

```sql
DELETE FROM <table_name>
[WHERE <condition>]
```

:::tip
The DELETE statement does not support the USING clause yet.
:::

## Examples

```sql
-- create a table
CREATE TABLE bookstore (
  book_id INT,
  book_name VARCHAR
);

-- insert values
INSERT INTO bookstore VALUES (101, 'After the death of Don Juan');
INSERT INTO bookstore VALUES (102, 'Grown ups');
INSERT INTO bookstore VALUES (103, 'The long answer');
INSERT INTO bookstore VALUES (104, 'Wartime friends');
INSERT INTO bookstore VALUES (105, 'Deconstructed');

-- show the table before deletion
SELECT * FROM bookstore;

101|After the death of Don Juan
102|Grown ups
103|The long answer
104|Wartime friends
105|Deconstructed

-- delete a book (Id: 103)
DELETE FROM bookstore WHERE book_id = 103;

-- show the table again after deletion
SELECT * FROM bookstore;

101|After the death of Don Juan
102|Grown ups
104|Wartime friends
105|Deconstructed
```