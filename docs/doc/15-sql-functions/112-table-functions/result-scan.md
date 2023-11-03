---
title: RESULT_SCAN
---

Returns the result set of a previous command in same session as if the result was a table.


## Syntax

```sql
RESULT_SCAN( { '<query_id>' | LAST_QUERY_ID() } )
```

## Examples

Create a simple table:

```sql
CREATE TABLE t1(a int);
```

Insert some values;

```sql
INSERT INTO t1(a) VALUES (1), (2), (3);
```

### `result_scan`


```shell
SELECT * FROM t1 ORDER BY a;
+-------+
|   a   |
+-------+
|   1   |
+-------+
|   2   |
+-------+
|   3   |
+-------+
```


```shell
SELECT * FROM RESULT_SCAN(LAST_QUERY_ID()) ORDER BY a;
+-------+
|   a   |
+-------+
|   1   |
+-------+
|   2   |
+-------+
|   3   |
+-------+
```

