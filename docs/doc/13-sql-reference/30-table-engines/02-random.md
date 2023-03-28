---
title: Random Engine
---

## Syntax

```sql
CREATE TABLE table_name (
  column_name1 column_type1,
  column_name2 column_type2,
  ...
) ENGINE = Random;
```

## Use cases

Random engine is used to generate random data for testing purposes. If limit is not specified in query, it will use `max_block_size` as limit value.

Example:

```sql
> create table r (a int, b timestamp, c String, d Variant) Engine = Random;

> select * from r limit 3;

+-------------+----------------------------+-------+-----------------------------------------------------------------------------------------+
| a           | b                          | c     | d                                                                                       |
+-------------+----------------------------+-------+-----------------------------------------------------------------------------------------+
|  1515222693 | 5644-08-08 08:18:02.153307 | Cd4EL | false                                                                                   |
| -1721583430 | 2801-10-02 21:10:08.239880 | bdREv | {"2CbjY":5566179425126492090,"FtfVz":9111809212512345709,"VQuVv":-9022907531714095185}  |
| -1865622072 | 6158-08-16 19:03:47.563392 | wKcpg | {"7dZAZ":-6513853114735328596,"nkZOZ":-400420074545948308,"px9BO":-8839157034206468334} |
+-------------+----------------------------+-------+-----------------------------------------------------------------------------------------+
```
