---
title: List files in a stage
---


## Syntax

```
list @<stage_name> [pattern = '<regexp_pattern>']
```

## Examples

```sql
MySQL [(none)]> list @named_external_stage PATTERN = 'ontime.*parquet';
+-----------------------+
| file_name             |
+-----------------------+
| ontime_200.parquet    |
| ontime_200_v1.parquet |
+-----------------------+
2 rows in set (2.150 sec)
```
