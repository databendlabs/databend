---
title: SHOW TABLES
---

Lists the tables in the current or a specified database.

## Syntax

```sql
SHOW [FULL] TABLES 
[{FROM | IN} <database_name>] 
[HISTORY] 
[LIKE '<pattern>' | WHERE <expr>]
```
Where:

`[FULL]`: Lists the tables with their properties information. See [Examples](#examples) for more details.

`[{FROM | IN} <database_name>]`: Specifies a database. If omitted, the command returns the results from the current database.

`[HISTORY]`: If present, the results will include the dropped tables that are still within their retention period (24 hours by default).

`[LIKE '<pattern>']`: Filters the results by the table names using pattern matching.

`[WHERE <expr>]`: Filters the results using an expression in the WHERE clause.

## Examples

The following example lists all the tables in the current database:

```sql
SHOW TABLES;

---
| tables_in_default |
|-------------------|
| hr_team           |
| members           |
| members_previous  |
| members_view      |
| support_team      |
| t1                |
```
The following example lists all the tables with their properties information:

```sql
SHOW FULL TABLES;

---
| tables_in_default | table_type | table_catalog | engine | create_time                   | num_rows | data_size | data_compressed_size | index_size |
|-------------------|------------|---------------|--------|-------------------------------|----------|-----------|----------------------|------------|
| hr_team           | BASE TABLE | default       | FUSE   | 2022-08-29 12:58:09.992 +0000 | 4        | 80        | 674                  | 774        |
| members           | BASE TABLE | default       | FUSE   | 2022-08-29 17:53:34.282 +0000 | 2        | 38        | 392                  | 460        |
| members_previous  | BASE TABLE | default       | FUSE   | 2022-08-29 18:20:57.599 +0000 | 0        | 0         | 0                    | 0          |
| members_view      | BASE TABLE | default       | VIEW   | 2022-09-02 18:24:04.658 +0000 | \N       | \N        | \N                   | \N         |
| support_team      | BASE TABLE | default       | FUSE   | 2022-08-29 12:57:45.469 +0000 | 3        | 57        | 350                  | 387        |
| t1                | BASE TABLE | default       | FUSE   | 2022-08-23 18:08:40.158 +0000 | 0        | 0         | 0                    | 0          |
```

The following example demonstrates that the results will include dropped tables when the option HISTORY is present:

```sql
DROP TABLE t1;

SHOW TABLES;

---
| tables_in_default |
|-------------------|
| hr_team           |
| members           |
| members_previous  |
| members_view      |
| support_team      |

SHOW TABLES HISTORY;

---
| tables_in_default | drop_time                     |
|-------------------|-------------------------------|
| hr_team           | NULL                          |
| members           | NULL                          |
| members_previous  | NULL                          |
| members_view      | NULL                          |
| support_team      | NULL                          |
| t1                | 2022-09-02 18:29:27.918 +0000 |
```

The following example lists the tables containing the string "team" at the end of their name:

```sql
SHOW TABLES LIKE '%team';

---
| tables_in_default |
|-------------------|
| hr_team           |
| support_team      |
```
:::tip
Please note that the pattern matching is CASE-SENSITIVE. No results will be returned if you code the previous statement like this: 

```sql
SHOW TABLES LIKE '%TEAM';
```
:::

The following example lists the views in the current database using a WHERE clause:

```sql
SHOW TABLES WHERE engine = 'VIEW';

---
| tables_in_default |
|-------------------|
| members_view      |
```