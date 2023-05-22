---
title: TO_TIMESTAMP
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.39"/>

TO_TIMESTAMP converts an expression to a date with time (timestamp/datetime).

The function can accept one or two arguments. If given one argument, the function extracts a date from the string. If the argument is an integer, the function interprets the integer as the number of seconds, milliseconds, or microseconds before (for a negative number) or after (for a positive number) the Unix epoch (midnight on January 1, 1970):

- If the integer is less than 31,536,000,000, it is treated as seconds.
- If the integer is greater than or equal to 31,536,000,000 and less than 31,536,000,000,000, it is treated as milliseconds.
- If the integer is greater than or equal to 31,536,000,000,000, it is treated as microseconds.

If given two arguments, the function converts the first string to a timestamp based on the format specified in the second string. To customize the format of date and time in Databend, you can utilize specifiers. These specifiers allow you to define the desired format for date and time values. For a comprehensive list of supported specifiers, see [Formatting Date and Time](../../13-sql-reference/10-data-types/20-data-type-time-date-types.md#formatting-date-and-time).

- The output timestamp reflects your Databend timezone.
- The timezone information must be included in the string you want to convert, otherwise NULL will be returned.

See also: [TO_DATE](todate.md)

## Syntax

```sql
-- Convert a string or integer to a timestamp
TO_TIMESTAMP(<expr>)

-- Convert a string to a timestamp using the given pattern
TO_TIMESTAMP(<expr, expr>)
```

## Return Type

Returns a timestamp in the format "YYYY-MM-DD hh:mm:ss.ffffff". If the given string matches this format but does not have the time part, it is automatically extended to this pattern. The padding value is 0.

## Examples

### Given a String Argument

```sql
SELECT TO_TIMESTAMP('2022-01-02T03:25:02.868894-07:00');

---
2022-01-02 10:25:02.868894

SELECT TO_TIMESTAMP('2022-01-02 02:00:11');

---
2022-01-02 02:00:11.000000

SELECT TO_TIMESTAMP('2022-01-02T02:00:22');

---
2022-01-02 02:00:22.000000

SELECT TO_TIMESTAMP('2022-01-02T01:12:00-07:00');

---
2022-01-02 08:12:00.000000

SELECT TO_TIMESTAMP('2022-01-02T01');

---
2022-01-02 01:00:00.000000
```

### Given an Integer Argument

```sql
SELECT TO_TIMESTAMP(1);

---
1970-01-01 00:00:01.000000

SELECT TO_TIMESTAMP(-1);

---
1969-12-31 23:59:59.000000
```

:::tip

Please note that a Timestamp value ranges from 1000-01-01 00:00:00.000000 to 9999-12-31 23:59:59.999999. Databend would return an error if you run the following statement:

```sql
SELECT TO_TIMESTAMP(9999999999999999999);
```
:::

### Given Two Arguments

```sql
SET GLOBAL timezone ='Japan';
SELECT TO_TIMESTAMP('2022年2月4日、8時58分59秒、タイムゾーン：+0900', '%Y年%m月%d日、%H時%M分%S秒、タイムゾーン：%z');

---
2022-02-04 08:58:59.000000

SET GLOBAL timezone ='America/Toronto';
SELECT TO_TIMESTAMP('2022年2月4日、8時58分59秒、タイムゾーン：+0900', '%Y年%m月%d日、%H時%M分%S秒、タイムゾーン：%z');

---
2022-02-03 18:58:59.000000
```