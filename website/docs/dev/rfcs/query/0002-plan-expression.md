# Expression and plan builder

## Summary

Logic plan and expression play a big role throughout the life cycle of SQL query.
This doc is intended to explain the new design of expressions and plan builder.

## Expression

### Alias Expression

Aliasing is useful in SQL, we can alias a complex expression as a short alias name. Such as:
`select a + 3 as b`.

In the standard SQL protocol, aliasing can work in:

- Group By, eg: ```select a + 3 as b, count(1) from table group by b```
- Having, eg: ```select a + 3 as b, count(1) as c from table group by b having c > 0```
- Order By: eg: ```select a + 3 as b from table order by b```


:::note Notes
ClickHouse has extended the usage of expression alias, it can be work in:

- recursive alias expression: eg: `select a + 1 as b, b + 1 as c`

- filter: eg: `select a + 1 as b, b + 1 as c  from table where c > 0`

Note Currently we do not support clickhouse style alias expression. It can be implemented later.
:::

For expression alias, we only handle it at last, in projection stage. But We have to replace the alias of the expression as early as possible to prevent ambiguity later.

Eg:

`select number + 1 as c, sum(number) from numbers(10) group by c having c > 3 order by c limit 10`

- Firstly, we can scan all the alias expressions from projection ASTs. `c ---> (number + 1)`
- Then we replaced the alias into the corresponding expression in *having*, *order by*, *group by* clause. So the query will be: `select number + 1 as c, sum(number) from numbers(10) group by (number + 1) having (number + 1) > 3 order by (number + 1) limit 10`
- At last, when the query is finished, we apply the projection to rename the column `(number+1)` to `c`

Let's take a look at the explain result of this query:

```
| Limit: 10
  Projection: (number + 1) as c:UInt64, sum(number):UInt64
    Sort: (number + 1):UInt64
      Having: ((number + 1) > 3)
        AggregatorFinal: groupBy=[[(number + 1)]], aggr=[[sum(number)]]
          RedistributeStage[state: AggregatorMerge, id: 0]
            AggregatorPartial: groupBy=[[(number + 1)]], aggr=[[sum(number)]]
              Expression: (number + 1):UInt64, number:UInt64 (Before GroupBy)
                ReadDataSource: scan partitions: [4], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80]
```

We can see we do not need to care about aliasing until the projection, so it will be very convenient to apply other expressions.

### Materialized Expression

Materialized expression processing is that we can rebase the expression as a *ExpressionColumn* if the same expression is already processed upstream.

Eg:

`select number + 1 as c, sum(number) as d group by c having number + 1 > 3 order by  d desc`

After aliases replacement, we will know that order by is `sum(number)`, but `sum(number)` is already processed during the aggregating stage, so we can rebase the order by expression `SortExpression { ... }` to `Column("sum(number)")`, this could remove useless calculation of same expressions.

So `number + 1` in having can also apply to rebase the expression.


### Expression Functions

There are many kinds of expression functions.

- ScalarFunctions, One-to-one calculation process, the result rows is same as the input rows. eg: `select database()`
- AggregateFunctions, Many-to-one calculation process, eg: `select sum(number)`
- BinaryFunctions, a special kind of ·ScalarFunctions· eg: `select 1 + 2 `
- ...

For ScalarFunctions, we really don't care about the whole block, we just care about the columns involved by the arguments. `sum(number)` just care about the Column which named *number* .  And the result is also a column, so we have the virtual method in `IFunction` is:


```rust
fn eval(&self, columns: &[DataColumn], _input_rows: usize) -> Result<DataColumn>;
```

For AggregateFunctions, we should keep the state in the corresponding function instance to apply the two-level merge, we have the following virtual method in `IAggregateFunction`:

```rust
fn accumulate(&mut self, columns: &[DataColumn], _input_rows: usize) -> Result<()>;
fn accumulate_result(&self) -> Result<Vec<DataValue>>;
fn merge(&mut self, _states: &[DataValue]) -> Result<()>;
fn merge_result(&self) -> Result<DataValue>;
```

The process is `accumulate`(apply data to the function) --> `accumulate_result`(to get the current state) --> `merge` (merge current state from other state) ---> `merge_result (to get the final result value)`


ps: We don't store the arguments types and arguments names in functions, we can store them later if we need.

### Column

*Block* is the unit of data passed between streams for pipeline processing, while *Column* is the unit of data passed between expressions.
So in the view of expression(functions, literal, ...), everything is *Column*, we have *DataColumn* to represent a column.
```rust
#[derive(Clone, Debug)]
pub enum DataColumn {
    // Array of values.
    Array(DataArrayRef),
    // A Single value.
    Constant(DataValue, usize)
}
```

*DataColumn::Constant* is like  *ConstantColumn* in *ClickHouse*.

Note: We don't have *ScalarValue* , because it can be known as `Constant(DataValue, 1)`, and there is *DataValue* struct.


### Expression chain and expression executor

Currently, we can collect the inner expression from expressions to build ExpressionChain. This could be done by Depth-first-search visiting.  ExpressionFunction: `number + (number + 1)` will be :  `[ ExpressionColumn(number),  ExpressionColumn(number), ExpressionLiteral(1),  ExpressionBinary('+', 'number', '1'), ExpressionBinary('+', 'number',  '(number + 1)')  ]`.

We have the *ExpressionExecutor* the execute the expression chain, during the execution, we don't need to care about the kind of the arguments. We just consider them as *ColumnExpression* from upstream, so we just fetch the column *number* and the column *(number + 1)* from the block.


## Plan Builder

### None aggregation query

This is for queries without *group by* and *aggregate functions*.

Eg: `explain select number + 1 as b from numbers(10) where number + 1 > 3  order by number + 3 `


```
| explain                                                                                                                                                                                                                                                                                             |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Projection: (number + 1) as b:UInt64
  Sort: (number + 3):UInt64
    Expression: (number + 1):UInt64, (number + 3):UInt64 (Before OrderBy)
      Filter: ((number + 1) > 3)
        ReadDataSource: scan partitions: [4], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80] |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.02 sec)
```

The build process is

- SourcePlan : schema --> [number]
- FilterPlan:  filter expression is  `(number + 1) > 3`, the schema keeps the same,  schema --> [number]
- Expression:  we will collect expressions from `order by` and `having ` clauses to apply the expression, schema --> `[number, number + 1, number + 3]`
- Sort: since we already have the `number + 1` in the input plan, so the sorting will consider `number + 1` as *ColumnExpression*, schema --> `[number, number + 1, number + 3]`
- Projection: applying the aliases and projection the columns,  schema --> `[b]`


### Aggregation query

To build `Aggregation` query, there will be more complex than the previous one.

Eg:  `explain select number + 1 as b, sum(number + 2 ) + 4 as c from numbers(10) where number + 3 > 0  group by number + 1 having c > 3 and sum(number + 4) + 1 > 4  order by sum(number + 5) + 1;`

```
| Projection: (number + 1) as b:UInt64, (sum((number + 2)) + 4) as c:UInt64
  Sort: sum((number + 5)):UInt64
    Having: (((sum((number + 2)) + 4) > 3) AND (sum((number + 4)) > 0))
      Expression: (number + 1):UInt64, (sum((number + 2)) + 4):UInt64, sum((number + 5)):UInt64 (Before OrderBy)
        AggregatorFinal: groupBy=[[(number + 1)]], aggr=[[sum((number + 2)), sum((number + 5)), sum((number + 4))]]
          RedistributeStage[state: AggregatorMerge, id: 0]
            AggregatorPartial: groupBy=[[(number + 1)]], aggr=[[sum((number + 2)), sum((number + 5)), sum((number + 4))]]
              Expression: (number + 1):UInt64, (number + 2):UInt64, (number + 5):UInt64, (number + 4):UInt64 (Before GroupBy)
                Filter: ((number + 3) > 0)
                  ReadDataSource: scan partitions: [4], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80]
```

The build process is

- SourcePlan : schema --> [number]
- FilterPlan:  filter expression is  `(number + 3) > 0`, the schema keeps the same,  schema --> [number]
- Expression: Before group by  `(number + 1):UInt64, (number + 2):UInt64, (number + 5):UInt64, (number + 4):UInt64 (Before GroupBy)`
Before GroupBy, We must visit all the expression in `projections`, `having`, `group by` to collect the expressions and aggregate functions,  schema --> `[number, number + 1, number + 2, number + 4, number + 5]`
- AggregatorPartial: `groupBy=[[(number + 1)]], aggr=[[sum((number + 2)), sum((number + 5)), sum((number + 4))]]`, note that: the expressions are already materialized in upstream, so we just conside all the arguments as columns.
- AggregatorFinal,  schema --> `[number + 1, sum((number + 2)), sum((number + 5)), sum((number + 4))]`
- Expression:  schema --> `[number + 1, sum((number + 2)), sum((number + 5)), sum((number + 4)),  sum((number + 2)) + 4, sum((number + 5)) + 1]`
- Sort: the schema keeps the same
- Projection: schema --> `b, c`
