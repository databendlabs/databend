---
title: CREATE FUNCTION
description:
  Create a new user-defined scalar function.
---


## CREATE FUNCTION

Creates a new UDF (user-defined function), the UDF can contain a SQL expression.

## Syntax

```sql
CREATE FUNCTION [ IF NOT EXISTS ] <name> AS ([ argname ]) -> '<function_definition>'
```

## Examples

```sql
CREATE FUNCTION a_plus_3 AS (a) -> a+3;

SELECT a_plus_3(2);
+---------+
| (2 + 3) |
+---------+
|       5 |
+---------+
```

```sql
-- Define lambda-style UDF
CREATE FUNCTION get_v1 AS (json) -> json["v1"];
CREATE FUNCTION get_v2 AS (json) -> json["v2"];

-- Create a time series table
CREATE TABLE json_table(time TIMESTAMP, data JSON);

-- Insert a time event
INSERT INTO json_table VALUES('2022-06-01 00:00:00.00000', PARSE_JSON('{"v1":1.5, "v2":20.5}'));

-- Get v1 and v2 value from the event
SELECT get_v1(data), get_v2(data) FROM json_table;
+------------+------------+
| data['v1'] | data['v2'] |
+------------+------------+
| 1.5        | 20.5       |
+------------+------------+

DROP FUNCTION get_v1;

DROP FUNCTION get_v2;

DROP TABLE json_table;
```
