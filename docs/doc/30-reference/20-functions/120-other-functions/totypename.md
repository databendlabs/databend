---
title: ToTypeName
---

ToTypeName function is used to return the name of a data type.

## Syntax

```sql
ToTypeName(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | Any expression. <br /> This may be a column name, the result of another function, or a math operation.

## Return Type

String

## Examples

```sql
mysql> SELECT ToTypeName(number) FROM numbers(2);
+--------------------+
| ToTypeName(number) |
+--------------------+
| UInt64             |
| UInt64             |
+--------------------+

mysql> SELECT ToTypeName(sum(number)) FROM numbers(2);
+-------------------------+
| ToTypeName(sum(number)) |
+-------------------------+
| UInt64                  |
+-------------------------+
```
