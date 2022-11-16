---
title: TO_DATE
---

Converts the input expression to a date.

## Syntax

```sql
to_date( <expr> )
```

## Arguments

| Arguments   | Description         |
| ----------- | ------------------- |
| `<expr>`    | A string or integer |

The function extracts a date from the provided string. If the argument is an integer, the function interprets the integer as the number of days before (for a negative number) or after (for a positive number) the Unix epoch (midnight on January 1, 1970). See [Examples](#examples) for more details.

## Return Type

Returns a of Date type in the format “YYYY-MM-DD”.

## Examples

### Using a String as Argument

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

### Using an Integer as Argument

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