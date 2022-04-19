---
title: TYPEOF 
---

TYPEOF function is used to return the name of a data type.

## Syntax

```sql
TYPEOF( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | Any expression. <br /> This may be a column name, the result of another function, or a math operation.

## Return Type

String

## Examples

```sql
SELECT typeof(number) FROM numbers(2);
+--------------------+
| typeof(number)     |
+--------------------+
| UInt64             |
| UInt64             |
+--------------------+

SELECT typeof(sum(number)) FROM numbers(2);
+-------------------------+
| typeof(sum(number))     |
+-------------------------+
| UInt64                  |
+-------------------------+
```
