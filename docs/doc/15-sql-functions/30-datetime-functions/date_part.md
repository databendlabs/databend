---
title: DATE_PART
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.153"/>

Retrieves the designated portion of a date, time, or timestamp.

See also: [EXTRACT](extract.md)

## Syntax

```sql
DATE_PART( YEAR | QUARTER | MONTH | WEEK | DAY | HOUR | MINUTE | SECOND, <date_or_time_expr> )
```

## Return Type

Integer.

## Examples

```sql
SELECT NOW();

now()                |
---------------------+
2023-10-13 08:40:36.0|

SELECT DATE_PART(DAY, NOW());

date_part(day, now())|
---------------------+
                   13|

SELECT DATE_PART(MONTH, TO_DATE('2022-05-13'));

date_part(month, to_date('2022-05-13'))|
---------------------------------------+
                                      5|
```