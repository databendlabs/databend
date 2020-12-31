---
id: set-statement
title: SET
---

    SET param = value

Assigns value to the param setting for the current session. 

## SETTINGS

    mysql> SELECT * FROM system.settings;
    
    +----------------+---------+
    | name           | value   |
    +----------------+---------+
    | max_threads    | 8       |
    | max_block_size | 10000   |
    | default_db     | default |
    +----------------+---------+
    3 rows in set (0.00 sec)



## SET

    mysql> SET max_threads=4;
    Query OK, 0 rows affected (0.00 sec)

    mysql> SELECT * FROM system.settings WHERE name='max_threads';
    +-------------+-------+
    | name        | value |
    +-------------+-------+
    | max_threads | 4     |
    +-------------+-------+
    1 row in set (0.00 sec)


