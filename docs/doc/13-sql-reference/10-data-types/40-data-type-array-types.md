---
title: Array(T)
description: Array of defined data type.
---

## Array(T) Data Types

ARRAY(T) consists of defined variable-length inner T data type values, which is very similar to a semi-structured ARRAY, except that the inner data type needs to be defined rather than arbitrary. T can be any data type.

:::note
Databend uses a 1-based numbering convention for arrays. An array of n elements starts with array[1] and ends with array[n].
:::

### Example

```sql
CREATE TABLE array_int64_table(arr ARRAY(INT64));

DESC array_int64_table;
+-------+--------------+------+---------+-------+
| Field | Type         | Null | Default | Extra |
+-------+--------------+------+---------+-------+
| arr   | ARRAY(INT64) | NO   | []      |       |
+-------+--------------+------+---------+-------+

INSERT INTO array_int64_table VALUES([1,2,3,4]),([5,6,7,8]);

-- Get all elements of the array:
SELECT arr FROM array_int64_table;
+--------------+
| arr          |
+--------------+
| [1, 2, 3, 4] |
| [5, 6, 7, 8] |
+--------------+

-- Get the elements at index 1 of the array:
SELECT arr[1] FROM array_int64_table;
+--------+
| arr[1] |
+--------+
|    1   |
|    5   |
+--------+

-- No results returned for index 0: 
SELECT arr[0] FROM array_int64_table;
+--------+
| arr[0] |
+--------+
|        |
+--------+
```