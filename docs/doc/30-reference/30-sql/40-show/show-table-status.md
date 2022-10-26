---
title: SHOW TABLE STATUS
---

Shows the status of the tables in a database. The status information includes various physical sizes and timestamps about a table, see [Examples](#examples) for details. 

The command returns status information of all the tables in the current database by default. Use a FROM | IN clause to specify another database rather than the current one. You can also filter tables with a LIKE or WHERE clause.

## Syntax

```sql
SHOW TABLE STATUS
    [{FROM | IN} db_name]
    [LIKE 'pattern' | WHERE expr]
```

## Examples

```sql
CREATE TABLE t(id INT);

-- Show status of all tables in the current database
SHOW TABLE STATUS;
*************************** 1. row ***************************
           Name: t
         Engine: FUSE
        Version: 0
     Row_format: NULL
           Rows: 0
 Avg_row_length: NULL
    Data_length: 0
Max_data_length: NULL
   Index_length: 0
      Data_free: NULL
 Auto_increment: NULL
    Create_time: 2022-04-08 04:13:48.988 +0000
    Update_time: NULL
     Check_time: NULL
      Collation: NULL
       Checksum: NULL
        Comment:
```

The following returns status information of the table named "t":

```sql
SHOW TABLE STATUS LIKE 't';
*************************** 1. row ***************************
           Name: t
         Engine: FUSE
        Version: 0
     Row_format: NULL
           Rows: 0
 Avg_row_length: NULL
    Data_length: 0
Max_data_length: NULL
   Index_length: 0
      Data_free: NULL
 Auto_increment: NULL
    Create_time: 2022-04-08 04:13:48.988 +0000
    Update_time: NULL
     Check_time: NULL
      Collation: NULL
       Checksum: NULL
        Comment:
```

The following uses a LIKE clause to return status information of the tables with a name starting with "t":

```sql
SHOW TABLE STATUS LIKE 't%';
*************************** 1. row ***************************
           Name: t
         Engine: FUSE
        Version: 0
     Row_format: NULL
           Rows: 0
 Avg_row_length: NULL
    Data_length: 0
Max_data_length: NULL
   Index_length: 0
      Data_free: NULL
 Auto_increment: NULL
    Create_time: 2022-04-08 04:13:48.988 +0000
    Update_time: NULL
     Check_time: NULL
      Collation: NULL
       Checksum: NULL
        Comment:
```

The following uses a WHERE clause to return status information of the tables with a name starting with "t":

```sql
SHOW TABLE STATUS WHERE name LIKE 't%';
*************************** 1. row ***************************
           Name: t
         Engine: FUSE
        Version: 0
     Row_format: NULL
           Rows: 0
 Avg_row_length: NULL
    Data_length: 0
Max_data_length: NULL
   Index_length: 0
      Data_free: NULL
 Auto_increment: NULL
    Create_time: 2022-04-08 04:13:48.988 +0000
    Update_time: NULL
     Check_time: NULL
      Collation: NULL
       Checksum: NULL
        Comment:
```

The following returns status information of all tables in the database `default`:

```sql
SHOW TABLE STATUS FROM 'default';
*************************** 1. row ***************************
           Name: t
         Engine: FUSE
        Version: 0
     Row_format: NULL
           Rows: 0
 Avg_row_length: NULL
    Data_length: 0
Max_data_length: NULL
   Index_length: 0
      Data_free: NULL
 Auto_increment: NULL
    Create_time: 2022-04-08 04:13:48.988 +0000
    Update_time: NULL
     Check_time: NULL
      Collation: NULL
       Checksum: NULL
        Comment:
```