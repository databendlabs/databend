---
title: TO_DATETIME
---

Converts the input expression to a date with time.

## Syntax

```sql
to_datetime( <expr> )
```

## Arguments

| Arguments   | Description         |
| ----------- | ------------------- |
| `<expr>`    | A string or integer |

The function extracts a date and time from the provided string. If the argument is an integer, the function interprets the integer as the number of seconds before (for a negative number) or after (for a positive number) the Unix epoch (midnight on January 1, 1970). See [Examples](#examples) for more details.

## Return Type

Returns a value of Timestamp type in the format “YYYY-MM-DD hh:mm:ss.ffffff”.

If the expr matches this format but does not have a time part, it is automatically extended to this pattern. The padding value is 0.

## Examples

### Using a String as Argument

```sql
SELECT TO_DATETIME('2022-01-02T03:25:02.868894-07:00');

---
2022-01-02 10:25:02.868894

SELECT TO_DATETIME('2022-01-02 02:00:11');

---
2022-01-02 02:00:11.000000

SELECT TO_DATETIME('2022-01-02T02:00:22');

---
2022-01-02 02:00:22.000000

SELECT TO_DATETIME('2022-01-02T01:12:00-07:00');

---
2022-01-02 08:12:00.000000

SELECT TO_DATETIME('2022-01-02T01');

---
2022-01-02 01:00:00.000000
```

### Using an Integer as Argument

```sql
SELECT TO_DATETIME(1);

---
1970-01-01 00:00:01.000000

SELECT TO_DATETIME(-1);

---
1969-12-31 23:59:59.000000
```

:::tip

Please note that a Timestamp value ranges from 1000-01-01 00:00:00.000000 to 9999-12-31 23:59:59.999999. Databend would return an error if you run the following statement:

```sql
SELECT TO_DATETIME(9999999999999999999);
```
:::