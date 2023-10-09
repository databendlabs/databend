---
title: SHOW TABLE STATUS
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.131"/>

Shows the status of the tables in a database. The status information includes various physical sizes and timestamps about a table, see [Examples](#examples) for details.

## Syntax

```sql
SHOW TABLE STATUS
    [{FROM | IN} <database_name>]
    [LIKE 'pattern' | WHERE expr]
```

| Parameter | Description                                                                                                                 |
|-----------|-----------------------------------------------------------------------------------------------------------------------------|
| FROM / IN | Specifies a database. If omitted, the command returns the results from the current database.                                |
| LIKE      | Filters the results by the table names using case-sensitive pattern matching.                                                              |
| WHERE     | Filters the results using an expression in the WHERE clause.                                                                |

## Examples

The following example displays the status of tables in the current database, providing details such as name, engine, rows, and other relevant information:

```sql
SHOW TABLE STATUS;

name   |engine|version|row_format|rows|avg_row_length|data_length|max_data_length|index_length|data_free|auto_increment|create_time                  |update_time|check_time|collation|checksum|comment|cluster_by|
-------+------+-------+----------+----+--------------+-----------+---------------+------------+---------+--------------+-----------------------------+-----------+----------+---------+--------+-------+----------+
books  |FUSE  |      0|          |   2|              |        160|               |         713|         |              |2023-09-25 06:40:47.237 +0000|           |          |         |        |       |          |
mytable|FUSE  |      0|          |   5|              |         40|               |        1665|         |              |2023-08-28 07:53:05.455 +0000|           |          |         |        |       |((a + 1)) |
ontime |FUSE  |      0|          | 199|              |     147981|               |       22961|         |              |2023-09-19 07:04:06.414 +0000|           |          |         |        |       |          |
```

The following example displays the status of tables in the current database where the names start with 'my':

```sql
SHOW TABLE STATUS LIKE 'my%';

name   |engine|version|row_format|rows|avg_row_length|data_length|max_data_length|index_length|data_free|auto_increment|create_time                  |update_time|check_time|collation|checksum|comment|cluster_by|
-------+------+-------+----------+----+--------------+-----------+---------------+------------+---------+--------------+-----------------------------+-----------+----------+---------+--------+-------+----------+
mytable|FUSE  |      0|          |   5|              |         40|               |        1665|         |              |2023-08-28 07:53:05.455 +0000|           |          |         |        |       |((a + 1)) |
```

The following example displays the status of tables in the current database where the number of rows is greater than 100:

:::note
When using the SHOW TABLE STATUS query, be aware that some column names, such as "rows," may be interpreted as SQL keywords, potentially leading to errors. To avoid this issue, always enclose column names with backticks, as shown in this example. This ensures that column names are treated as identifiers rather than keywords in the SQL query.
:::

```sql
SHOW TABLE STATUS WHERE `rows` > 100;

name  |engine|version|row_format|rows|avg_row_length|data_length|max_data_length|index_length|data_free|auto_increment|create_time                  |update_time|check_time|collation|checksum|comment|cluster_by|
------+------+-------+----------+----+--------------+-----------+---------------+------------+---------+--------------+-----------------------------+-----------+----------+---------+--------+-------+----------+
ontime|FUSE  |      0|          | 199|              |     147981|               |       22961|         |              |2023-09-19 07:04:06.414 +0000|           |          |         |        |       |          |
```