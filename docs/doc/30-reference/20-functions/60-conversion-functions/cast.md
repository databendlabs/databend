---
title: CAST
---

Convert a value from one data type to another data type.

## Syntax

```sql
CAST( <expr> AS <type>)
<expr>::<type>
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | A value to convert. |
| `<type>` | The target data type |

:::tip

[Databend Data Types](../../10-data-types/index.md)

:::

## Return Type

Converted value.

## Examples

```sql
SELECT CAST(1 AS VARCHAR);
+-------------------+
| cast(1 as String) |
+-------------------+
| 1                 |
+-------------------+

SELECT 1::VARCHAR;
+-----------+
| 1::String |
+-----------+
| 1         |
+-----------+

SELECT CAST(1 AS BIGINT UNSIGNED);
+-------------------+
| cast(1 as UInt64) |
+-------------------+
|                 1 |
+-------------------+

SELECT typeof(CAST(1 AS BIGINT UNSIGNED));
+-------------------------------+
| typeof(cast(1 as UInt64))     |
+-------------------------------+
| UInt64                        |
+-------------------------------+
```
