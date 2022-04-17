---
title: SHOW TABLE STATUS
---

Shows the list of table status in the currently selected database.

## Syntax

```
SHOW TABLE STATUS
    [{FROM | IN} db_name]
    [LIKE 'pattern' | WHERE expr]
```

## Examples

```sql
mysql> CREATE TABLE t(id int);
mysql> SHOW TABLE STATUS\G
*************************** 1. row ***************************
           Name: t
         Engine: FUSE
        Version: 0
     Row_format: NULL
           Rows: NULL
 Avg_row_length: NULL
    Data_length: NULL
Max_data_length: NULL
   Index_length: NULL
      Data_free: NULL
 Auto_increment: NULL
    Create_time: 2022-04-08 04:13:48.988 +0000
    Update_time: NULL
     Check_time: NULL
      Collation: NULL
       Checksum: NULL
        Comment:
```

Showing the tables with table name `"t"`:
```sql
mysql> SHOW TABLE STATUS LIKE 't'\G
*************************** 1. row ***************************
           Name: t
         Engine: FUSE
        Version: 0
     Row_format: NULL
           Rows: NULL
 Avg_row_length: NULL
    Data_length: NULL
Max_data_length: NULL
   Index_length: NULL
      Data_free: NULL
 Auto_increment: NULL
    Create_time: 2022-04-08 04:13:48.988 +0000
    Update_time: NULL
     Check_time: NULL
      Collation: NULL
       Checksum: NULL
        Comment:
```

Showing the tables begin with `"t"`:
```sql
mysql> SHOW TABLE STATUS LIKE 't%'\G
*************************** 1. row ***************************
           Name: t
         Engine: FUSE
        Version: 0
     Row_format: NULL
           Rows: NULL
 Avg_row_length: NULL
    Data_length: NULL
Max_data_length: NULL
   Index_length: NULL
      Data_free: NULL
 Auto_increment: NULL
    Create_time: 2022-04-08 04:13:48.988 +0000
    Update_time: NULL
     Check_time: NULL
      Collation: NULL
       Checksum: NULL
        Comment: 
```

Showing the tables begin with `"t"` with `WHERE`:
```sql
mysql> SHOW TABLE STATUS WHERE table_name LIKE 't%'\G
*************************** 1. row ***************************
           Name: t
         Engine: FUSE
        Version: 0
     Row_format: NULL
           Rows: NULL
 Avg_row_length: NULL
    Data_length: NULL
Max_data_length: NULL
   Index_length: NULL
      Data_free: NULL
 Auto_increment: NULL
    Create_time: 2022-04-08 04:13:48.988 +0000
    Update_time: NULL
     Check_time: NULL
      Collation: NULL
       Checksum: NULL
        Comment:
```

Showing the tables are inside `"default"`:
```sql
mysql> SHOW TABLE STATUS FROM 'default'\G
*************************** 1. row ***************************
           Name: t
         Engine: FUSE
        Version: 0
     Row_format: NULL
           Rows: NULL
 Avg_row_length: NULL
    Data_length: NULL
Max_data_length: NULL
   Index_length: NULL
      Data_free: NULL
 Auto_increment: NULL
    Create_time: 2022-04-08 04:13:48.988 +0000
    Update_time: NULL
     Check_time: NULL
      Collation: NULL
       Checksum: NULL
        Comment:
```
