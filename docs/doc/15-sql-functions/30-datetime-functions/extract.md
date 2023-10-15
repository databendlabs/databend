---
title: EXTRACT
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.153"/>

Retrieves the designated portion of a date, time, or timestamp.

See also: [DATE_PART](date_part.md)

## Syntax

```sql
EXTRACT( YEAR | QUARTER | MONTH | WEEK | DAY | HOUR | MINUTE | SECOND FROM <date_or_time_expr> )
```

## Return Type

Integer.

## Examples

```sql
SELECT NOW();

now()                |
---------------------+
2023-10-13 08:40:36.0|

SELECT EXTRACT(DAY FROM NOW());

extract(day from now())|
-----------------------+
                     13|

SELECT EXTRACT(MONTH FROM TO_DATE('2022-05-13'));

extract(month from to_date('2022-05-13'))|
-----------------------------------------+
                                        5|
```