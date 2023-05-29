---
title: SELECT
---

Retrieves data from a table.

## Syntax

```sql
[WITH]
SELECT
    [ALL | DISTINCT]
    <select_expr> [[AS] alias], ...
    [EXCLUDE (<col_name1> [, <col_name2>, <col_name3>, ...] ) ]
    [FROM table_references
    [AT ...]
    [WHERE <expr>]
    [GROUP BY {{<col_name> | <expr> | <col_alias> | <col_position>}, 
         ... | <extended_grouping_expr>}]
    [HAVING <expr>]
    [ORDER BY {<col_name> | <expr> | <col_alias> | <col_position>} [ASC | DESC],
         [ NULLS { FIRST | LAST }]
    [LIMIT <row_count>]
    [OFFSET <row_count>]
    [IGNORE_RESULT]
```

:::tip
numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

## SELECT Clause

```sql
SELECT number FROM numbers(3);
+--------+
| number |
+--------+
|      0 |
|      1 |
|      2 |
+--------+
```

You can also alias a column to make the column name more readable and understandable in the result:

- Databend suggests avoiding special characters as much as possible when creating column aliases. However, if special characters are necessary in some cases, the alias should be enclosed in backticks, like this: SELECT price AS \`$CA\` FROM ...

- Databend will automatically convert aliases into lowercase. For example, if you alias a column as *Total*, it will appear as *total* in the result. If the capitalization matters to you, enclose the alias in backticks: \`Total\`.

```sql
SELECT number AS Total FROM numbers(3);
+--------+
| total  |
+--------+
|      0 |
|      1 |
|      2 |
+--------+

SELECT number AS `Total` FROM numbers(3);
+--------+
| Total  |
+--------+
|      0 |
|      1 |
|      2 |
+--------+
```

## EXCLUDE Parameter

Excludes one or more columns by their names from the result. The parameter is usually used in conjunction with `SELECT * ...` to exclude a few columns from the result instead of retrieving them all.

```sql
SELECT * FROM allemployees ORDER BY id;

---
| id | firstname | lastname | gender |
|----|-----------|----------|--------|
| 1  | Ryan      | Tory     | M      |
| 2  | Oliver    | Green    | M      |
| 3  | Noah      | Shuster  | M      |
| 4  | Lily      | McMeant   | F      |
| 5  | Macy      | Lee      | F      |

-- Exclude the column "id" from the result
SELECT * EXCLUDE id FROM allemployees;

---
| firstname | lastname | gender |
|-----------|----------|--------|
| Noah      | Shuster  | M      |
| Ryan      | Tory     | M      |
| Oliver    | Green    | M      |
| Lily      | McMeant   | F      |
| Macy      | Lee      | F      |

-- Exclude the columns "id" and "lastname" from the result
SELECT * EXCLUDE (id,lastname) FROM allemployees;

---
| firstname | gender |
|-----------|--------|
| Oliver    | M      |
| Ryan      | M      |
| Lily      | F      |
| Noah      | M      |
| Macy      | F      |
```

## FROM Clause

```sql
SELECT number FROM numbers(3);

---
+--------+
| number |
+--------+
|      0 |
|      1 |
|      2 |
+--------+
```

Databend supports direct querying of data from various locations without the need to load it into a table. These locations include user stages, internal stages, external stages, object storage buckets/containers (e.g., Amazon S3, Google Cloud Storage, Microsoft Azure), and remote servers accessible via HTTPS and IPFS. You can leverage this feature within the FROM clause to efficiently query data directly from these sources. See [Querying Staged Files](../../12-load-data/00-transform/05-querying-stage.md) for details.

## AT Clause

The AT clause enables you to query previous versions of your data. For more information, see [AT](./03-query-at.md).

## WHERE Clause

```sql
SELECT number FROM numbers(3) WHERE number > 1;
+--------+
| number |
+--------+
|      2 |
+--------+
```

If you alias a column in the SELECT clause, you can use the alias in the WHERE clause:

```sql
SELECT number * 2 AS a FROM numbers(3) WHERE (a + 1) % 3 = 0
+--------+
|    a   |
+--------+
|      2 |
+--------+
```

If the alias and the column name are the same, the WHERE clause will recognize the alias as the column name:

```sql
SELECT number * 2 AS number FROM numbers(3) WHERE (number + 1) % 3 = 0
+--------+
| number |
+--------+
|      4 |
+--------+
```

## GROUP BY Clause

```sql
--Group the rows of the result set by column alias
SELECT number%2 as c1, number%3 as c2, MAX(number) FROM numbers(10000) GROUP BY c1, c2;
+------+------+-------------+
| c1   | c2   | MAX(number) |
+------+------+-------------+
|    1 |    2 |        9995 |
|    1 |    1 |        9997 |
|    0 |    2 |        9998 |
|    0 |    1 |        9994 |
|    0 |    0 |        9996 |
|    1 |    0 |        9999 |
+------+------+-------------+

--Group the rows of the result set by column position in the SELECT list
SELECT number%2 as c1, number%3 as c2, MAX(number) FROM numbers(10000) GROUP BY 1, 2;
+------+------+-------------+
| c1   | c2   | MAX(number) |
+------+------+-------------+
|    1 |    2 |        9995 |
|    1 |    1 |        9997 |
|    0 |    2 |        9998 |
|    0 |    1 |        9994 |
|    0 |    0 |        9996 |
|    1 |    0 |        9999 |
+------+------+-------------+

```


`GROUP BY` can be extended with [GROUPING SETS](./07-query-group-by-grouping-sets.md) to do more complex grouping operations.

## HAVING Clause

```sql
SELECT number%2 as c1, number%3 as c2, MAX(number) as max FROM numbers(10000) GROUP BY c1, c2 HAVING max>9996;
+------+------+------+
| c1   | c2   | max  |
+------+------+------+
|    1 |    0 | 9999 |
|    1 |    1 | 9997 |
|    0 |    2 | 9998 |
+------+------+------+
```

## ORDER BY Clause

```sql
--Sort by column name in ascending order.
SELECT number FROM numbers(5) ORDER BY number ASC;
+--------+
| number |
+--------+
|      0 |
|      1 |
|      2 |
|      3 |
|      4 |
+--------+

--Sort by column name in descending order.
SELECT number FROM numbers(5) ORDER BY number DESC;
+--------+
| number |
+--------+
|      4 |
|      3 |
|      2 |
|      1 |
|      0 |
+--------+

--Sort by column alias.
SELECT number%2 AS c1, number%3 AS c2  FROM numbers(5) ORDER BY c1 ASC, c2 DESC;
+------+------+
| c1   | c2   |
+------+------+
|    0 |    2 |
|    0 |    1 |
|    0 |    0 |
|    1 |    1 |
|    1 |    0 |
+------+------+

--Sort by column position in the SELECT list
SELECT * FROM t1 ORDER BY 2 DESC;
+------+------+
| a    | b    |
+------+------+
|    2 |    3 |
|    1 |    2 |
+------+------+

SELECT a FROM t1 ORDER BY 1 DESC;
+------+
| a    |
+------+
|    2 |
|    1 |
+------+

--Sort with the NULLS FIRST or LAST option.

CREATE TABLE t_null (
  number INTEGER
);

INSERT INTO t_null VALUES (1);
INSERT INTO t_null VALUES (2);
INSERT INTO t_null VALUES (3);
INSERT INTO t_null VALUES (NULL);
INSERT INTO t_null VALUES (NULL);

--Databend considers NULL values larger than any non-NULL values.
--The NULL values appear last in the following example that sorts the results in ascending order:

SELECT number FROM t_null order by number ASC;
+--------+
| number |
+--------+
|      1 |
|      2 |
|      3 |
|   NULL |
|   NULL |
+--------+

-- To make the NULL values appear first in the preceding example, use the NULLS FIRST option:

SELECT number FROM t_null order by number ASC nulls first;
+--------+
| number |
+--------+
|   NULL |
|   NULL |
|      1 |
|      2 |
|      3 |
+--------+

-- Use the NULLS LAST option to make the NULL values appear last in descending order:

SELECT number FROM t_null order by number DESC nulls last;
+--------+
| number |
+--------+
|      3 |
|      2 |
|      1 |
|   NULL |
|   NULL |
+--------+
```

## LIMIT Clause

```sql
SELECT number FROM numbers(1000000000) LIMIT 1;
+--------+
| number |
+--------+
|      0 |
+--------+

SELECT number FROM numbers(100000) ORDER BY number LIMIT 2 OFFSET 10;
+--------+
| number |
+--------+
|     10 |
|     11 |
+--------+
```

## OFFSET Clause

```sql
SELECT number FROM numbers(5) ORDER BY number OFFSET 2;
+--------+
| number |
+--------+
|      2 |
|      3 |
|      4 |
+--------+
```

## IGNORE_RESULT

Do not output the result set.

```sql
SELECT number FROM numbers(2);
+--------+
| number |
+--------+
|      0 |
|      1 |
+--------+

mysql> SELECT number FROM numbers(2) IGNORE_RESULT;
-- Empty set
```

## Nested Sub-Selects

SELECT statements can be nested in queries.

```
SELECT ... [SELECT ...[SELECT [...]]]
```

```sql
SELECT MIN(number) FROM (SELECT number%3 AS number FROM numbers(10)) GROUP BY number%2;
+-------------+
| min(number) |
+-------------+
|           1 |
|           0 |
+-------------+
```
