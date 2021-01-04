---
id: logical-operators
title: Logical Operators 
---

## AND


```
mysql> SELECT * FROM system.numbers(10) WHERE number>=1 AND number<3;
+--------+
| number |
+--------+
|      1 |
|      2 |
+--------+
2 rows in set (0.00 sec)
```

## OR

```
mysql> SELECT * FROM system.numbers(10) WHERE number<2 OR number>8;
+--------+
| number |
+--------+
|      0 |
|      1 |
|      9 |
+--------+
3 rows in set (0.01 sec)
```
