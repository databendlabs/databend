---
title: TRIM FUNCTIONS
title_includes: TRIM, LTRIM, RTRIM
---

Returns the string str with all remstr prefixes or suffixes removed.

## Syntax

```sql
TRIM(expr);
LTRIM(expr);
RTRIM(expr);
```

## Examples

```sql
MySQL [(none)]> SELECT TRIM('   aaa   ');
+-------------------+
| trim('   aaa   ') |
+-------------------+
| aaa               |
+-------------------+

MySQL [(none)]> SELECT LTRIM('   aaa   ');
+--------------------+
| LTRIM('   aaa   ') |
+--------------------+
| aaa                |
+--------------------+

MySQL [(none)]> SELECT RTRIM('   aaa   ');
+--------------------+
| RTRIM('   aaa   ') |
+--------------------+
|    aaa             |
+--------------------+
```
