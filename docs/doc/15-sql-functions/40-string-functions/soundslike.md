---
title: SOUNDS LIKE
---

Compares the pronunciation of two strings by their Soundex codes. Soundex is a phonetic algorithm that produces a code representing the pronunciation of a string, allowing for approximate matching of strings based on their pronunciation rather than their spelling. Databend offers the [SOUNDEX](soundex.md) function that allows you to get the Soundex code from a string.

SOUNDS LIKE is frequently employed in the WHERE clause of SQL queries to narrow down rows using fuzzy string matching, such as for names and addresses, see [Filtering Rows](#filtering-rows) in [Examples](#examples).

:::note
While the function can be useful for approximate string matching, it is important to note that it is not always accurate. The Soundex algorithm is based on English pronunciation rules and may not work well for strings from other languages or dialects. 
:::

## Syntax

```sql
<str1> SOUNDS LIKE <str2>
```

## Arguments

| Arguments | Description |
|-----------|-------------|
| str1, 2   | The strings you compare. |

## Return Type

Return a Boolean value of 1 if the Soundex codes for the two strings are the same (which means they sound alike) and 0 otherwise.

## Examples

### Comparing Strings

```sql
SELECT 'two' SOUNDS LIKE 'too'
----
1

SELECT CONCAT('A', 'B') SOUNDS LIKE 'AB';
----
1

SELECT 'Monday' SOUNDS LIKE 'Sunday';
----
0
```

### Filtering Rows

```sql
SELECT * FROM  employees;

id|first_name|last_name|age|
--+----------+---------+---+
 0|John      |Smith    | 35|
 0|Mark      |Smythe   | 28|
 0|Johann    |Schmidt  | 51|
 0|Eric      |Doe      | 30|
 0|Sue       |Johnson  | 45|


SELECT * FROM  employees
WHERE  first_name SOUNDS LIKE 'John';

id|first_name|last_name|age|
--+----------+---------+---+
 0|John      |Smith    | 35|
 0|Johann    |Schmidt  | 51|
```