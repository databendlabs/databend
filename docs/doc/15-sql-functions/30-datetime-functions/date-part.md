---
title: DATE_PART
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.153"/>

Retrieves the designated portion of a date, time, or timestamp.

See also: [EXTRACT](extract.md)

## Syntax

```sql
DATE_PART( YEAR | QUARTER | MONTH | WEEK | DAY | HOUR | MINUTE | SECOND | DOW | DOY, <date_or_time_expr> )
```

- DOW: Day of Week.
- DOY: Day of Year.

## Return Type

Integer.

## Examples

```sql
SELECT NOW();

now()                |
---------------------+
2023-10-16 02:09:28.0|

SELECT DATE_PART(DAY, NOW());

date_part(day, now())|
---------------------+
                   16|

-- October 16, 2023, is a Monday
SELECT DATE_PART(DOW, NOW());

date_part(dow, now())|
---------------------+
                    1|

-- October 16, 2023, is the 289th day of the year
SELECT DATE_PART(DOY, NOW());

date_part(doy, now())|
---------------------+
                  289|

SELECT DATE_PART(MONTH, TO_DATE('2022-05-13'));

date_part(month, to_date('2022-05-13'))|
---------------------------------------+
                                      5|
```