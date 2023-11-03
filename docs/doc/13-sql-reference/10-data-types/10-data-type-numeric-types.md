---
title: Numeric
description: Basic Numeric data type.
---

## Integer Data Types

| Name     | Alias | Size    | Min Value            | Max Value           |
|----------|-------|---------|----------------------|---------------------|
| TINYINT  | INT8  | 1 byte  | -128                 | 127                 |
| SMALLINT | INT16 | 2 bytes | -32768               | 32767               |
| INT      | INT32 | 4 bytes | -2147483648          | 2147483647          |
| BIGINT   | INT64 | 8 bytes | -9223372036854775808 | 9223372036854775807 |

:::tip
If you want unsigned integer, please use `UNSIGNED` constraint, this is compatible with MySQL, for example:

```sql
CREATE TABLE test_numeric(tiny TINYINT, tiny_unsigned TINYINT UNSIGNED)
```
:::

## Floating-Point Data Types

| Name   | Size    | Min Value                | Max Value               |
|--------|---------|--------------------------|-------------------------|
| FLOAT  | 4 bytes | -3.40282347e+38          | 3.40282347e+38          |
| DOUBLE | 8 bytes | -1.7976931348623157E+308 | 1.7976931348623157E+308 |

## Functions

See [Numeric Functions](/doc/sql-functions/numeric-functions).

## Examples

```sql
CREATE TABLE test_numeric
(
    tiny              TINYINT,
    tiny_unsigned     TINYINT UNSIGNED,
    smallint          SMALLINT,
    smallint_unsigned SMALLINT UNSIGNED,
    int               INT,
    int_unsigned      INT UNSIGNED,
    bigint            BIGINT,
    bigint_unsigned   BIGINT UNSIGNED,
    float             FLOAT,
    double            DOUBLE
);

DESC test_numeric;

┌───────────────────────────────────────────────────────────────────┐
│       Field       │        Type       │  Null  │ Default │  Extra │
├───────────────────┼───────────────────┼────────┼─────────┼────────┤
│ tiny              │ TINYINT           │ YES    │ NULL    │        │
│ tiny_unsigned     │ TINYINT UNSIGNED  │ YES    │ NULL    │        │
│ smallint          │ SMALLINT          │ YES    │ NULL    │        │
│ smallint_unsigned │ SMALLINT UNSIGNED │ YES    │ NULL    │        │
│ int               │ INT               │ YES    │ NULL    │        │
│ int_unsigned      │ INT UNSIGNED      │ YES    │ NULL    │        │
│ bigint            │ BIGINT            │ YES    │ NULL    │        │
│ bigint_unsigned   │ BIGINT UNSIGNED   │ YES    │ NULL    │        │
│ float             │ FLOAT             │ YES    │ NULL    │        │
│ double            │ DOUBLE            │ YES    │ NULL    │        │
└───────────────────────────────────────────────────────────────────┘
```