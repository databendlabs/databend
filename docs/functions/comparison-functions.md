---
id: comparison-functions
title: Comparison Functions and Operators
---

Comparison functions always return true or false.

The following types can be compared:
* numbers
* strings and fixed strings

## =

Calculates the sum of the numbers.

```
mysql> SELECT * FROM system.numbers(3) WHERE number=1;
+--------+
| number |
+--------+
|      1 |
+--------+
1 row in set (0.00 sec)
```

## !=

```
mysql> SELECT * FROM system.numbers(3) WHERE number!=1;
+--------+
| number |
+--------+
|      0 |
|      2 |
+--------+
2 rows in set (0.00 sec)
```

## <

```
mysql> SELECT * FROM system.numbers(3) WHERE number<1;
+--------+
| number |
+--------+
|      0 |
+--------+
1 row in set (0.00 sec)
```

## <=


```
mysql> SELECT * FROM system.numbers(3) WHERE number<=1;
+--------+
| number |
+--------+
|      0 |
|      1 |
+--------+
2 rows in set (0.01 sec)
```

## >

```
mysql> SELECT * FROM system.numbers(3) WHERE number>1;
+--------+
| number |
+--------+
|      2 |
+--------+
1 row in set (0.01 sec)
```

## >=


```
mysql> SELECT * FROM system.numbers(3) WHERE number>=1;
+--------+
| number |
+--------+
|      1 |
|      2 |
+--------+
2 rows in set (0.00 sec)
```









