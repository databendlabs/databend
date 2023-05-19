---
title: TO_DATE
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.39"/>

TO_DATE converts an expression to a date format. 

The function can accept one or two arguments. If given one argument, the function extracts a date from the string. If the argument is an integer, the function interprets the integer as the number of days before (for a negative number) or after (for a positive number) the Unix epoch (midnight on January 1, 1970). 

If given two arguments, the function converts the first string to a date based on the format specified in the second string. To customize the format of date and time in Databend, you can utilize specifiers. These specifiers allow you to define the desired format for date and time values. For a comprehensive list of supported specifiers, see [Formatting Date and Time](../../13-sql-reference/10-data-types/20-data-type-time-date-types.md#formatting-date-and-time).

See also: [TO_TIMESTAMP](totimestamp.md)

## Syntax

```sql
-- Convert a string or integer to a date
TO_DATE(<expr>)

-- Convert a string to a date using the given pattern
TO_DATE(<expr, expr>)
```

## Return Type

Returns a date in the format "YYYY-MM-DD".

## Examples

### Given a String Argument

```sql
SELECT TO_DATE('2022-01-02T01:12:00+07:00');

---
2022-01-02

SELECT TO_DATE('2022-01-02 03:25:02.868894');

---
2022-01-02

SELECT TO_DATE('2022-01-02 02:00:11');

---
2022-01-02

SELECT TO_DATE('2022-01-02T02:00:22');

---
2022-01-02

SELECT TO_DATE('2022-01-02');

---
2022-01-02
```

### Given an Integer Argument

```sql
SELECT TO_DATE(1);

---
1970-01-02

SELECT TO_DATE(-1);

---
1969-12-31
```

:::tip

Please note that a Date value ranges from 1000-01-01 to 9999-12-31. Databend would return an error if you run the following statement:

```sql
SELECT TO_DATE(9999999999999999999);
```
:::

### Given Two Arguments

```sql
SELECT TO_DATE('Month/Day/Year: 12/25/2022','Month/Day/Year: %m/%d/%Y');

---
2022-12-25
```