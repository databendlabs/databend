---
title: AT
---

The SELECT statement can include an AT clause to query previous versions of your data with a specific timestamp.

This is part of the Databend's Time Travel feature that allows you to query, back up, and restore from a previous version of your data within the retention period.

:::tip

Before including an AT clause in the SELECT statement, you must enable the new Databend planner. To do so, perform the following command in the SQL client:

```sql
> set enable_planner_v2=1;
```
:::

## Syntax

```sql    
SELECT ... FROM ... { AT TIMESTAMP => <timestamp>}
```

## Examples

```sql
-- Create a table
create table demo(c varchar);

-- Insert two rows
insert into demo values('batch1.1'),('batch1.2');

-- Insert another row
insert into demo values('batch2.1');

-- Show snapshot timestamps
select timestamp from fuse_snapshot('default', 'demo'); 
+----------------------------+
| timestamp                  |
+----------------------------+
| 2022-06-22 08:58:54.509008 |
| 2022-06-22 08:58:36.254458 |
+----------------------------+

-- Enable the new Databend planner
set enable_planner_v2 = 1;  

-- Travel to the time when the last row was inserted
select * from demo at (TIMESTAMP => '2022-06-22 08:58:54.509008'::TIMESTAMP); 
+----------+
| c        |
+----------+
| batch1.1 |
| batch1.2 |
| batch2.1 |
+----------+

-- Travel to the time when the first two rows were inserted
select * from demo at (TIMESTAMP => '2022-06-22 08:58:36.254458'::TIMESTAMP); 
+----------+
| c        |
+----------+
| batch1.1 |
| batch1.2 |
+----------+

```
