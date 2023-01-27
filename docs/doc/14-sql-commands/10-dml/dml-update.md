---
title: UPDATE
---

Updates statement modifies rows to new value in a table.

## Syntax

```sql
UPDATE <table_name>
SET <col_name> = <value> [ , <col_name> = <value> , ... ]
    [ FROM <table_name> ]
    [ WHERE <condition> ]
```

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

-- Update delete a book (Id: 103)
UPDATE bookstore SET book_name = 'The long answer (2nd)' WHERE book_id = 103;

-- show the table again after deletion
SELECT * FROM bookstore;

101|After the death of Don Juan
102|Grown ups
103|The long answer (2nd)
104|Wartime friends
105|Deconstructed
```