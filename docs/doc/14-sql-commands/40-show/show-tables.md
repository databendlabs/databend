---
title: SHOW TABLES
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.131"/>

Lists the tables in the current or a specified database.

## Syntax

```sql
SHOW [FULL] TABLES 
[{FROM | IN} <database_name>] 
[HISTORY] 
[LIKE '<pattern>' | WHERE <expr>]
```

| Parameter | Description                                                                                                                 |
|-----------|-----------------------------------------------------------------------------------------------------------------------------|
| FULL      | Lists the tables with their property information. See [Examples](#examples) for more details.                               |
| FROM / IN | Specifies a database. If omitted, the command returns the results from the current database.                                |
| HISTORY   | If present, the results will include the dropped tables that are still within their retention period (24 hours by default). |
| LIKE      | Filters the results by the table names using case-sensitive pattern matching.                                                              |
| WHERE     | Filters the results using an expression in the WHERE clause.                                                                |

## Examples

The following example lists the names of all tables in the current database (default):

```sql
SHOW TABLES;

---
Tables_in_default|
-----------------+
books            |
mytable          |
ontime           |
products         |
```

The following example lists all the tables with their properties information:

```sql
SHOW FULL TABLES;

---
tables  |table_type|database|catalog|engine|cluster_by|create_time                  |num_rows|data_size|data_compressed_size|index_size|
--------+----------+--------+-------+------+----------+-----------------------------+--------+---------+--------------------+----------+
books   |BASE TABLE|default |default|FUSE  |          |2023-09-25 06:40:47.237 +0000|       2|      160|                 579|       713|
mytable |BASE TABLE|default |default|FUSE  |((a + 1)) |2023-08-28 07:53:05.455 +0000|       5|       40|                 958|      1665|
ontime  |BASE TABLE|default |default|FUSE  |          |2023-09-19 07:04:06.414 +0000|     199|   147981|               26802|     22961|
products|BASE TABLE|default |default|FUSE  |          |2023-09-06 07:09:00.619 +0000|       3|       99|                 387|       340|
```

The following example demonstrates that the results will include dropped tables when the optional parameter HISTORY is present:

```sql
DROP TABLE products;

SHOW TABLES;

---
Tables_in_default|
-----------------+
books            |
mytable          |
ontime           |

SHOW TABLES HISTORY;

---
Tables_in_default       |drop_time                    |
------------------------+-----------------------------+
books                   |NULL                         |
mytable                 |NULL                         |
ontime                  |NULL                         |
products                |2023-09-27 01:14:21.421 +0000|
```

The following example lists the tables containing the string "time" at the end of their name:

```sql
SHOW TABLES LIKE '%time';

---
Tables_in_default|
-----------------+
ontime           |

-- CASE-SENSITIVE pattern matching. 
-- No results will be returned if you code the previous statement like this: 
SHOW TABLES LIKE '%TIME';
```

The following example lists tables where the data size is greater than 1,000 bytes:

```sql
SHOW TABLES WHERE data_size > 1000 ;

---
Tables_in_default|
-----------------+
ontime           |
```