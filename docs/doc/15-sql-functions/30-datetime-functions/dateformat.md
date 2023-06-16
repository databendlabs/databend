---
title: DATE_FORMAT
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.39"/>

Converts a date value to a specific string format. To customize the format of date and time in Databend, you can utilize specifiers. These specifiers allow you to define the desired format for date and time values. For a comprehensive list of supported specifiers, see [Formatting Date and Time](../../13-sql-reference/10-data-types/20-data-type-time-date-types.md#formatting-date-and-time).

## Syntax

```sql
DATE_FORMAT(<date>, <format>)
```

## Return Type

String.

## Examples

```sql
SELECT DATE_FORMAT('2022-12-25', 'Month/Day/Year: %m/%d/%Y')

+-------------------------------------------------------+
| date_format('2022-12-25', 'month/day/year: %m/%d/%y') |
+-------------------------------------------------------+
| Month/Day/Year: 12/25/2022                            |
+-------------------------------------------------------+
```