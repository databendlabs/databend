---
title: STRING_AGG
---

Aggregate function.

The STRING_AGG() function converts all the non-NULL values of a column to String, separated by the delimiter.

## Syntax

```sql
STRING_AGG(expression)
STRING_AGG(expression, delimiter)
```

## Arguments

| Arguments  | Description                                                  |
|------------|--------------------------------------------------------------|
| expression | Any String expression                                        |
| delimiter  | Optional constant String, if not specified, use empty String |

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

