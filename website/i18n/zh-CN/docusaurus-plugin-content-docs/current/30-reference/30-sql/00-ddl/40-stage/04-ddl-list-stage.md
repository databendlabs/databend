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
+-----------+
| file_name |
+-----------+
| books.csv |
+-----------+
```
