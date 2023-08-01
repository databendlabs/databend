---
title: Order By With Fill Gap
---


Some query results will have gaps because no information was saved for specific ranges. We can use `range`(`generate_series`) table function and left join operator to fill the gaps.

## Examples

Let's create a sample table named `t_metrics` using bendsql:

```sql
CREATE TABLE t_metrics (
  date Date,
  value INT
);

INSERT INTO t_metrics VALUES
  ('2020-01-01', 200),
  ('2020-01-01', 300),
  ('2020-01-04', 300),
  ('2020-01-04', 300),
  ('2020-01-05', 400),
  ('2020-01-10', 700);
```

```sql
select date, sum(value), count() from t_metrics group by date order by date;
```

The result will be:

```sql
┌────────────────────────────────────────┐
│    date    │    sum(value)   │ count() │
│    Date    │ Nullable(Int64) │  UInt64 │
├────────────┼─────────────────┼─────────┤
│ 2020-01-01 │             500 │       2 │
│ 2020-01-04 │             600 │       2 │
│ 2020-01-05 │             400 │       1 │
│ 2020-01-10 │             700 │       1 │
└────────────────────────────────────────┘
```

In this example, if we want to fill the gaps between `2020-01-01` and `2020-01-10`, we can query like:

```sql
SELECT t.date, COALESCE(SUM(t_metrics.value), 0), COUNT(t_metrics.value)
FROM generate_series(
  '2020-01-01'::Date,
  '2020-01-10'::Date
) as t(date)
LEFT JOIN t_metrics ON t_metrics.date = t.date
GROUP BY t.date order by t.date;
```

The result will be:

```sql
┌─────────────────────────────────────────────────────────────────────────┐
│    date    │ coalesce(sum(t_metrics.value), 0) │ count(t_metrics.value) │
│    Date    │          Nullable(Int64)          │         UInt64         │
├────────────┼───────────────────────────────────┼────────────────────────┤
│ 2020-01-01 │                               500 │                      2 │
│ 2020-01-02 │                                 0 │                      0 │
│ 2020-01-03 │                                 0 │                      0 │
│ 2020-01-04 │                               600 │                      2 │
│ 2020-01-05 │                               400 │                      1 │
│ 2020-01-06 │                                 0 │                      0 │
│ 2020-01-07 │                                 0 │                      0 │
│ 2020-01-08 │                                 0 │                      0 │
│ 2020-01-09 │                                 0 │                      0 │
│ 2020-01-10 │                               700 │                      1 │
└─────────────────────────────────────────────────────────────────────────┘
```