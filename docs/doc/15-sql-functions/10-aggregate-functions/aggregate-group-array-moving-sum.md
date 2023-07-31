---
title: GROUP_ARRAY_MOVING_SUM
---

The GROUP_ARRAY_MOVING_SUM function calculates the moving sum of input values. The function can take the window size as a parameter. If left unspecified, the function takes the window size equal to the number of input values.

## Syntax

```sql
GROUP_ARRAY_MOVING_SUM(<expr>)

GROUP_ARRAY_MOVING_SUM(<window_size>)(<expr>)
```

## Arguments

| Arguments        | Description              |
|------------------| ------------------------ |
| `<window_size>`  | Any numerical expression |
| `<expr>`         | Any numerical expression |

## Return Type

Returns an [Array](../../13-sql-reference/10-data-types/40-data-type-array-types.md) with elements that are of the same type as the original data.

## Examples

```sql
-- Create a table and insert sample data
CREATE TABLE hits (
  user_id INT,
  request_num INT
);

INSERT INTO hits (user_id, request_num)
VALUES (1, 10),
       (2, 15),
       (3, 20),
       (1, 13),
       (2, 21),
       (3, 25),
       (1, 30),
       (2, 41),
       (3, 45);

SELECT user_id, GROUP_ARRAY_MOVING_SUM(2)(request_num) AS request_num
FROM hits
GROUP BY user_id;

| user_id | request_num |
|---------|-------------|
|       1 | [10,23,43]  |
|       2 | [20,45,70]  |
|       3 | [15,36,62]  |
```
