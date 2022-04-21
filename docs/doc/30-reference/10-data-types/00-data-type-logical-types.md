---
title: Boolean
description: Basic logical data type.
---

The BOOLEAN type represents a statement of truth (`true` or `false`).

## Boolean Data Types

| Name     | Storage Size | Description
| -------- | ------------ |-----------
| BOOLEAN  | 1 byte       | Logical boolean (true/false)

## Implicit Conversion

Boolean values can be implicitly converted from numeric values to boolean values.

Numeric Conversion:
* Zero (0) is converted to FALSE.
* Any non-zero value is converted to TRUE.

String Conversion:
* Strings converted to TRUE: `true`
* Strings converted to FALSE: `false`
* Conversion is case-insensitive.
* All other text strings cannot be converted to Boolean values, it will get `Code: 1010` error.

## Functions

See [Conditional Functions](/doc/reference/functions/conditional-functions).

## Example

```sql
SELECT 0::BOOLEAN, 1::BOOLEAN, 'true'::BOOLEAN, 'false'::BOOLEAN, 'True'::BOOLEAN;
+------------+------------+-----------------+------------------+-----------------+
| 0::Boolean | 1::Boolean | 'true'::Boolean | 'false'::Boolean | 'True'::Boolean |
+------------+------------+-----------------+------------------+-----------------+
|          0 |          1 |               1 |                0 |               1 |
+------------+------------+-----------------+------------------+-----------------+

SELECT 'xx'::BOOLEAN;
ERROR 1105 (HY000): Code: 1010, displayText = Cast error happens in casting from String to Boolean.
```
