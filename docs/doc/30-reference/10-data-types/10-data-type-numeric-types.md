---
title: Numeric
description: Basic Numeric data type.
---

## Integer Data Types

Basic Integer Numbers data types.

| Name      |  Aliases     | Storage Size | Min Value                   | Max Value                       | Description
|-----------|--------------| -------------| --------------------------- | ------------------------------- | -------
| TINYINT   |  INT8        | 1 byte       |  -128                       |  127                            |
| SMALLINT  |  INT16       | 2 bytes      |  -32768                     |  32767                          |
| INT       |  INT32       | 4 bytes      |  -2147483648                |  2147483647                     |
| BIGINT    |  INT64       | 8 bytes      |  -9223372036854775808       |  9223372036854775807            |

:::tip
If you want unsigned integer, please use `UNSIGNED` constraint, this is compatible with MySQL, for example:
```sql

CREATE TABLE test_numeric(tiny TINYINT, tiny_unsigned TINYINT UNSIGNED)
```
:::

## Floating-Point Data Types

Basic Float32/Float64 data types.

| Name      |  Aliases     | Storage Size | Min Value                   | Max Value                       | Description
|-----------|--------------| -------------| --------------------------- | ------------------------------- | -------
| FLOAT     |              | 4 bytes      |  -3.40282347e+38            | 3.40282347e+38                  |
| DOUBLE    |              | 8 bytes      |  -1.7976931348623157E+308   | 1.7976931348623157E+308         |

## Functions

See [Numeric Functions](/doc/reference/functions/numeric-functions).

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
+-------------------+-------------------+------+---------+-------+
| Field             | Type              | Null | Default | Extra |
+-------------------+-------------------+------+---------+-------+
| tiny              | TINYINT           | NO   | 0       |       |
| tiny_unsigned     | TINYINT UNSIGNED  | NO   | 0       |       |
| smallint          | SMALLINT          | NO   | 0       |       |
| smallint_unsigned | SMALLINT UNSIGNED | NO   | 0       |       |
| int               | INT               | NO   | 0       |       |
| int_unsigned      | INT UNSIGNED      | NO   | 0       |       |
| bigint            | BIGINT            | NO   | 0       |       |
| bigint_unsigned   | BIGINT UNSIGNED   | NO   | 0       |       |
| float             | FLOAT             | NO   | 0       |       |
| double            | DOUBLE            | NO   | 0       |       |
+-------------------+-------------------+------+---------+-------+
```

