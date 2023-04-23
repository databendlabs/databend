---
title: STRING_AGG
---

Aggregate function.

The STRING_AGG() function converts all the non-NULL values of a column to String, separated by the delimiter.

## Syntax

```sql
STRING_AGG(<expr>)
STRING_AGG(<expr> [, delimiter])
```

:::info
If `<expr>` is not a String expression, should use `::VARCHAR` to convert.

For example:
```sql
SELECT string_agg(number::VARCHAR, '|') AS s FROM numbers(5);
+-----------+
| s         |
+-----------+
| 0|1|2|3|4 |
+-----------+
```
:::

## Arguments

| Arguments   | Description                                                         |
|-------------|---------------------------------------------------------------------|
| `<expr>`    | Any string expression (if not a string, use `::VARCHAR` to convert) |
| `delimiter` | Optional constant String, if not specified, use empty String        |

## Return Type

the String type

## Example

**Create a Table and Insert Sample Data**

```sql
CREATE TABLE programming_languages (
  id INT,
  language_name VARCHAR
);

INSERT INTO programming_languages (id, language_name)
VALUES (1, 'Python'),
       (2, 'JavaScript'),
       (3, 'Java'),
       (4, 'C#'),
       (5, 'Ruby');
```

**Query Demo: Concatenate Programming Language Names with a Delimiter**
```sql
SELECT STRING_AGG(language_name, ', ') AS concatenated_languages
FROM programming_languages;
```

**Result**
```sql
|          concatenated_languages         |
|------------------------------------------|
| Python, JavaScript, Java, C#, Ruby      |
```

