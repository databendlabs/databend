---
title: DATEDIFF
---

Databend does not provide a Datediff function yet, but it supports direct arithmetic operations on dates and times. For example, you can use the expression `TO_DATE(NOW())-2` to obtain the date from two days ago.

This flexibility of directly manipulating dates and times in Databend makes it convenient and versatile for handling date and time computations. See an example below:

```sql
CREATE TABLE tasks (
  task_name VARCHAR(50),
  start_date DATE,
  end_date DATE
);

INSERT INTO tasks (task_name, start_date, end_date)
VALUES
  ('Task 1', '2023-06-15', '2023-06-20'),
  ('Task 2', '2023-06-18', '2023-06-25'),
  ('Task 3', '2023-06-20', '2023-06-23');

SELECT task_name, end_date - start_date AS duration
FROM tasks;

task_name|duration|
---------+--------+
Task 1   |       5|
Task 2   |       7|
Task 3   |       3|
```
