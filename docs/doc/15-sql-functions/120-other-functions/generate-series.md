---
title: GENERATE_SERIES
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.1.0"/>

Generates a dataset starting from a specified point, ending at another specified point, and optionally with an incrementing value. The GENERATE_SERIES function works with the following data types: 

- Integer
- Date
- Timestamp

## Syntax

```sql
GENERATE_SERIES(<start>, <stop>[, <step_interval>])
```

## Arguments

| Argument      	| Description                                                                                                                                                                                                	|
|---------------	|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
| start         	| The starting value, representing the first number, date, or timestamp in the sequence.                                                                                                                            	|
| stop          	| The ending value, representing the last number, date, or timestamp in the sequence.                                                                                                                               	|
| step_interval 	| The step interval, determining the difference between adjacent values in the sequence. For integer sequences, the default value is 1. For date sequences, the default step interval is 1 day. For timestamp sequences, the default step interval is 1 microsecond. 	|


:::note
When dealing with functions like GENERATE_SERIES and RANGE, a key distinction lies in their boundary traits. GENERATE_SERIES is bound by both the left and right sides, while RANGE is bound on the left side only. For example, utilizing RANGE(1, 11) is equivalent to GENERATE_SERIES(1, 10).
:::

## Return Type

Returns a list containing a continuous sequence of numeric values, dates, or timestamps from *start* to *stop*.

## Examples

### Example 1: Generating Numeric, Date, and Timestamp Data

```sql
SELECT * FROM GENERATE_SERIES(1, 10, 2);

generate_series|
---------------+
              1|
              3|
              5|
              7|
              9|

SELECT * FROM GENERATE_SERIES('2023-03-20'::date, '2023-03-27'::date);

generate_series|
---------------+
     2023-03-20|
     2023-03-21|
     2023-03-22|
     2023-03-23|
     2023-03-24|
     2023-03-25|
     2023-03-26|
     2023-03-27|

SELECT * FROM GENERATE_SERIES('2023-03-26 00:00'::timestamp, '2023-03-27 12:00'::timestamp, 86400000000);

generate_series    |
-------------------+
2023-03-26 00:00:00|
2023-03-27 00:00:00|
```

### Example 2: Filling Query Result Gaps 

This example uses the GENERATE_SERIES function and left join operator to handle gaps in query results caused by missing information in specific ranges.

```sql
CREATE TABLE t_metrics (
  date Date,
  value INT
);

INSERT INTO t_metrics VALUES
  ('2020-01-01', 200),
  ('2020-01-01', 300),
  ('2020-01-04', 300),
  ('2020-01-04', 300),
  ('2020-01-05', 400),
  ('2020-01-10', 700);

SELECT date, SUM(value), COUNT() FROM t_metrics GROUP BY date ORDER BY date;

date      |sum(value)|count()|
----------+----------+-------+
2020-01-01|       500|      2|
2020-01-04|       600|      2|
2020-01-05|       400|      1|
2020-01-10|       700|      1|
```

To close the gaps between January 1st and January 10th, 2020, use the following query:

```sql
SELECT t.date, COALESCE(SUM(t_metrics.value), 0), COUNT(t_metrics.value)
FROM generate_series(
  '2020-01-01'::Date,
  '2020-01-10'::Date
) AS t(date)
LEFT JOIN t_metrics ON t_metrics.date = t.date
GROUP BY t.date ORDER BY t.date;

date      |coalesce(sum(t_metrics.value), 0)|count(t_metrics.value)|
----------+---------------------------------+----------------------+
2020-01-01|                              500|                     2|
2020-01-02|                                0|                     0|
2020-01-03|                                0|                     0|
2020-01-04|                              600|                     2|
2020-01-05|                              400|                     1|
2020-01-06|                                0|                     0|
2020-01-07|                                0|                     0|
2020-01-08|                                0|                     0|
2020-01-09|                                0|                     0|
2020-01-10|                              700|                     1|
```