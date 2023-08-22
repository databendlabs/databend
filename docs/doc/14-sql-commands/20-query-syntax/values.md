---
title: VALUES
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.65"/>

The VALUES clause is used to define a set of rows explicitly for use in queries. It allows you to provide a list of values that can be used as a temporary table in your SQL statements.

## Syntax

```sql
VALUES (value_1_1, value_1_2, ...), (value_2_1, value_2_2, ...), ...
```
- The VALUES clause is followed by sets of values enclosed in parentheses.
- Each set of values represents a row to be inserted into the temporary table.
- Within each set of values, the individual values are comma-separated and correspond to the columns of the temporary table.
- Databend automatically assigns default column names like *col0*, *col1*, *col2*, and so on when you insert multiple rows without specifying column names.

## Examples

These examples demonstrate using the VALUES clause to show city data in various formats: directly, ordered by population, and with structured column aliases:

```sql
VALUES ('Toronto', 2731571), ('Vancouver', 631486), ('Montreal', 1704694);

col0     |col1   |
---------+-------+
Toronto  |2731571|
Vancouver| 631486|
Montreal |1704694|

VALUES ('Toronto', 2731571), ('Vancouver', 631486), ('Montreal', 1704694) ORDER BY col1;

col0     |col1   |
---------+-------+
Vancouver| 631486|
Montreal |1704694|
Toronto  |2731571|

SELECT * FROM (
    VALUES ('Toronto', 2731571), 
           ('Vancouver', 631486), 
           ('Montreal', 1704694)
) AS CityPopulation(City, Population);

city     |population|
---------+----------+
Toronto  |   2731571|
Vancouver|    631486|
Montreal |   1704694|
```

These examples demonstrate how the VALUES clause can be used in a SELECT statement:

```sql
SELECT col1 
FROM (VALUES ('Toronto', 2731571), ('Vancouver', 631486), ('Montreal', 1704694));

col1   |
-------+
2731571|
 631486|
1704694|

SELECT col0 AS City, col1 AS Population
FROM (VALUES ('Toronto', 2731571), ('Vancouver', 631486), ('Montreal', 1704694))
ORDER BY col1 DESC
LIMIT 1;

city   |population|
-------+----------+
Toronto|   2731571|
```