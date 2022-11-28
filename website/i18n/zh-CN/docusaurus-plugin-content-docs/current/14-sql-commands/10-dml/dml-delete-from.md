---
title: DELETE
---

Removes one or more rows from a table.

## Syntax

```sql
DELETE FROM table_name
[WHERE search_ condition]
```

:::tip
The DELETE statement does not support the USING clause yet.
:::

## Examples

```sql
-- create a table
CREATE TABLE bookstore (
  bookId INTEGER PRIMARY KEY,
  bookName TEXT NOT NULL
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
DELETE from bookstore where bookId = 103;

-- show the table again after deletion
SELECT * FROM bookstore;

101|After the death of Don Juan
102|Grown ups
104|Wartime friends
105|Deconstructed
```