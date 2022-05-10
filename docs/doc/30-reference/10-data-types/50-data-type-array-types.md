---
title: Array(T)
description: Array of defined data type.
---

## Array(T) Data Types

ARRAY(T) consists of defined variable-length inner T data type values, which is very similar to a semi-structured ARRAY, except that the inner data type needs to be defined rather than arbitrary. T can be any data type.

### Example

```sql
CREATE TABLE array_int64_table(arr ARRAY(INT64) NULL);
```

```sql
DESC array_int64_table;
```

Insert two values into the table, `[1,2,3,4]`, `[5,6,7,8]`:
```sql
INSERT INTO array_int64_table VALUES([1,2,3,4]),([5,6,7,8]);
```

Get all elements of the array:
```sql
SELECT arr FROM array_int64_table;
+--------------+
| arr          |
+--------------+
| [1, 2, 3, 4] |
| [5, 6, 7, 8] |
+--------------+
```

Get the element at index 0 of the array:
```sql
SELECT arr[0] FROM array_int64_table;
+--------+
| arr[0] |
+--------+
|      1 |
|      5 |
+--------+
```
