---
title: ARG_MIN
---

Calculates the `arg` value for a minimum `val` value. If there are several different values of `arg` for minimum values of `val`, returns the first of these values encountered.

## Syntax

```sql
ARG_MIN(<arg>, <val>)
```

## Arguments

| Arguments | Description                                                                                       |
|-----------|---------------------------------------------------------------------------------------------------|
| `<arg>`   | Argument of [any data type that Databend supports](../../13-sql-reference/10-data-types/index.md) |
| `<val>`   | Value of [any data type that Databend supports](../../13-sql-reference/10-data-types/index.md)    |

## Return Type

`arg` value that corresponds to minimum `val` value.

 matches `arg` type.

## Example

Let's create a table students with columns id, name, and score, and insert some data:
```sql
CREATE TABLE students (
  id INT,
  name VARCHAR,
  score INT
);

INSERT INTO students (id, name, score) VALUES
  (1, 'Alice', 80),
  (2, 'Bob', 75),
  (3, 'Charlie', 90),
  (4, 'Dave', 80);
```

Now, we can use ARG_MIN to find the name of the student with the lowest score:
```sql
SELECT ARG_MIN(name, score) AS student_name
FROM students;
```

Result:
```sql
| student_name |
|--------------|
| Charlie      |
```