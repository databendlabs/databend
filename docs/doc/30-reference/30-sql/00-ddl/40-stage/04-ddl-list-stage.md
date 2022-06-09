---
title: LIST { internalStage | externalStage }
sidebar_label: LIST STAGE FILES 
---

Returns a list of files that have been staged (i.e. uploaded from a local file system).

## Syntax

```sql
LIST { internalStage | externalStage } [ PATTERN = '<regex_pattern>' ]
```

## Examples

```sql
LIST @my_int_stage;
+-----------+------+------+-------------------------------+--------------------+
| name      | size | md5  | last_modified                 | creator            |
+-----------+------+------+-------------------------------+--------------------+
| books.csv |   91 | NULL | 2022-06-10 12:01:40.000 +0000 | 'root'@'127.0.0.1' |
+-----------+------+------+-------------------------------+--------------------+
```
