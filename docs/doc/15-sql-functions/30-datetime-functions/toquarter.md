---
title: TO_QUARTER
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.153"/>

Retrieves the quarter (1, 2, 3, or 4) from a given date or timestamp.

## Syntax

```sql
TO_QUARTER( <date_or_time_expr> )
```

## Return Type

Integer.

## Examples

```sql
SELECT NOW();

now()                |
---------------------+
2023-10-13 08:40:36.0|

SELECT TO_QUARTER(NOW());

to_quarter(now())|
-----------------+
                4|

SELECT TO_QUARTER('2022-05-13');

to_quarter('2022-05-13')|
------------------------+
                       2|
```