---
title: AT
---

The SELECT statement can include an AT clause to query previous versions of your data at a specific time in the past.

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
select * from t12_0004 at ( TIMESTAMP => cast('2022-06-16 23:07:01.580417', timestamp) );
+------+
| c    |
+------+
|    3 |
|    1 |
|    2 |
+------+

select * from t12_0004 at ( TIMESTAMP => now() );
+------+
| c    |
+------+
|    1 |
|    2 |
|    3 |
|    3 |
|    3 |
+------+

```
