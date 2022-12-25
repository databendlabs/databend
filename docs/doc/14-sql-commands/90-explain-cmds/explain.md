---
title: EXPLAIN
---

Shows the execution plan of a SQL statement. An execution plan is shown as a tree consisting of different operators where you can see how Databend will execute the SQL statement. An operator usually includes one or more fields describing the actions Databend will perform or the objects related to the query.

For example, the following execution plan returned by the EXPLAIN command includes an operator named *TableScan* with several fields. For a list of common operators and fields, see [Common Operators and Fields](#common-operators-and-fields).

```sql
EXPLAIN SELECT * FROM allemployees;

---
TableScan
├── table: default.default.allemployees
├── read rows: 5
├── read bytes: 592
├── partitions total: 5
├── partitions scanned: 5
└── push downs: [filters: [], limit: NONE]
```

## Syntax

```sql
EXPLAIN <statement>
```

## Common Operators and Fields

Explanation plans may include a variety of operators, depending on the SQL statement you want Databend to EXPLAIN. The following is a list of common operators and their fields:

* **TableScan**: Reads data from the table.
    - table: The full name of the table. For example, `catalog1.database1.table1`.
    - read rows: The number of rows to read.
    - read bytes: The number of bytes of data to read.
    - partition total: The total number of partitions of the table.
    - partition scanned: The number of partitions to read.
    - push downs: The filters and limits to be pushed down to the storage layer for processing.
* **Filter**: Filters the read data.
    - filters: The predicate expression used to filter the data. Data that returns false for the expression evaluation will be filtered out.
* **EvalScalar**: Evaluates scalar expressions. For example, `a+1` in `SELECT a+1 AS b FROM t`.
    - expressions: The scalar expressions to evaluate.
* **AggregatePartial** & **AggregateFinal**: Aggregates by keys and returns the result of the aggregation functions.
    - group by: The keys used for aggregation.
    - aggregate functions: The functions used for aggregation.
* **Sort**: Sorts data by keys.
    - sort keys: The expressions used for sorting.
* **Limit**: Limits the number of rows returned.
    - limit: The number of rows to return.
    - offset: The number of rows to skip before returning any rows.
* **HashJoin**: Uses the Hash Join algorithm to perform Join operations for two tables. The Hash Join algorithm will select one of the two tables as the build side to build the Hash table. It will then use the other table as the probe side to read the matching data from the Hash table to form the result.
    - join type: The JOIN type (INNER, LEFT OUTER, RIGHT OUTER, FULL OUTER, CROSS, SINGLE, or MARK).
    - build keys: The expressions used by the build side to build the Hash table.
    - probe keys: The expressions used by the probe side to read data from the Hash table.
    - filters: The non-equivalence JOIN conditions, such as `t.a > t1.a`.
* **Exchange**: Exchanges data between Databend query nodes for distributed parallel computing.
    - exchange type: Data repartition type (Hash, Broadcast, or Merge).

## Examples

```sql
EXPLAIN select t.number from numbers(1) as t, numbers(1) as t1 where t.number = t1.number;
----
Project
├── columns: [number (#0)]
└── HashJoin
    ├── join type: INNER
    ├── build keys: [numbers.number (#1)]
    ├── probe keys: [numbers.number (#0)]
    ├── filters: []
    ├── TableScan(Build)
    │   ├── table: default.system.numbers
    │   ├── read rows: 1
    │   ├── read bytes: 8
    │   ├── partitions total: 1
    │   ├── partitions scanned: 1
    │   └── push downs: [filters: [], limit: NONE]
    └── TableScan(Probe)
        ├── table: default.system.numbers
        ├── read rows: 1
        ├── read bytes: 8
        ├── partitions total: 1
        ├── partitions scanned: 1
        └── push downs: [filters: [], limit: NONE]
```