# Proposal: Join framework

## Background

Join is one of the major features in SQL. Meanwhile, it's the most complicated part either.

Thus in this section, we will make a brief introduction to types of join semantics and join algorithms.

Generally, join can be categorized as following types by semantic:

- `INNER JOIN`: return all tuples satisfy the join condition
- `LEFT OUTER JOIN`: return all tuples satisfy the join condition and the rows from left table for which no row from right table satisfies the join condition
- `RIGHT OUTER JOIN`: return all tuples satisfy the join condition and the rows from right table for which no row from left table satisfies the join condition
- `FULL OUTER JOIN`: return all tuples satisfy the join condition and the rows from a table for which no row from other table satisfies the join condition
- `CROSS JOIN`: cartesian product of joined tables

Besides, `IN`, `EXISTS`, `NOT IN`, `NOT EXISTS` expressions can be implemented by **semi-join** and **anti-join**(known as subquery).

There are three kinds of common join algorithms:

- Nested-loop join
- Hash join
- Sort-merge join

Nested-loop join is the basic join algorithm, it can be described as following pseudo code:

```
// R⋈S
var innerTable = R
var outerTable = S
var result
for s <- outerTable:
    for r <- innerTable:
        if condition(r, s) == true:
            insert(result, combine(r, s))
```

Before introducing hash join, we introduce the definition of **equi join** here. A **equi join** is join whose join condition is an equation(e.g. `r.a == s.a`). For the joins whose join condition is not an equation, we call them **non-equi join**

Hash join can only work with equi join. It can be described as two phase: **build phase** and **probe phase**. 

As inner table and outer table of nested-loop join, hash join will choose a table as **build side** and another table as **probe side**.

The pseudo code of hash join:
```
// R⋈S
var build = R
var probe = S
var hashTable
var result
// Build phase
for r <- build:
    var key = hash(r, condition)
    insert(hashTable, key, r)

// Probe phase
for s <- probe:
    var key = hash(s, condition)
    if exists(hashTable, key):
        var r = get(hashTable, key)
        insert(result, combine(r, s))
```

Sort-merge join will sort the joined tables if they are not sorted by join key, and then merge them like merge sort.

Generally, a sort-merge join can only work with equi-join either, but it exists a band join optimization that can make sort-merge join work with some specific non-equi join. We won't talk about this here since it's a little bit out of scope.

## Join framework

To implement join, we have several parts of work to be done:

- Support parse join statement into logical plan
- Support bind column reference for joined tables
- Support some basic heuristic optimization(e.g. outer join elimination, subquery elimination) and join reorder with choosing implementation
- Support some join algorithms(local execution for now but design for distributed execution)

### Parser & Planner

According to ANSI-SQL specification, joins are defined in `FROM` clause. Besides, subquery in other clauses can be translated to join(correlated subquery will be translated to semi join or anti join) in some cases.

After parsing SQL string into AST, we will build logical plan from AST with `PlanParser`.

Following bnf definition is a simplified ANSI-SQL specification of `FROM` clause:
```bnf
<from clause> ::= FROM <table reference list>

<table reference list> ::= <table reference> [ { <comma> <table reference> }... ]

<table reference> ::= <table primary or joined table>

<table primary or joined table> ::= <table primary> | <joined table>

<table primary> ::=
		<table or query name> [ [ AS ] <correlation name> [ <left paren> <derived column list> <right paren> ] ]
	|	<derived table> [ AS ] <correlation name> [ <left paren> <derived column list> <right paren> ]
	|	<left paren> <joined table> <right paren>

<joined table> ::=
		<cross join>
	|	<qualified join>
	|	<natural join>

<cross join> ::= <table reference> CROSS JOIN <table primary>

<qualified join> ::= <table reference> [ <join type> ] JOIN <table reference> <join specification>

<natural join> ::= <table reference> NATURAL [ <join type> ] JOIN <table primary>

<join specification> ::= <join condition> | <named columns join>

<join condition> ::= ON <search condition>

<named columns join> ::= USING <left paren> <join column list> <right paren>

<join type> ::= INNER | <outer join type> [ OUTER ]

<outer join type> ::= LEFT | RIGHT | FULL

<join column list> ::= <column name list>
```

`<table reference>` concated with `<comma>` are cross joined. And it's possible to find some conjunctions in `WHERE` clause as their join conditions, that is rewriting cross join into inner join.

There are many queries organized in this way that doesn't explicitly specify join condition, for example TPCH query set.

`sqlparser` library can parse a SQL string into AST. Joins are organized as a tree structure.

There are following kinds of join trees:

- Left deep tree
- Right deep tree
- Bushy tree

In left deep tree, every join node's right child is a table, for example:
```sql
SELECT *
FROM a, b, c, d;
/*    
      join
     /    \
    join   d
   /    \
  join   c
 /    \
a      b
*/
```

In right deep tree, every join node's left child is a table, for example:
```sql
SELECT *
FROM a, b, c, d;
/*
  join    
 /    \
a   join
   /    \
  b   join
     /    \
    c      d
*/
```

In bushy tree, all children of every join node can be either result of join or table, for example:
```sql
SELECT *
FROM a, b, c, d;
/*
    join
   /    \
  join  join    
  /  \  /  \
  a  b  c  d
*/
```

Most of join s can be represented as left deep tree, which is easier to optimize. We can rewrite some joins to left deep tree during parsing phase.

Here's an example of `sqlparser` AST, the comment part is simplified AST debug string:
```sql
SELECT *
FROM a, b NATURAL JOIN c, d;
/*
Query { 
    with: None, 
    body: Select(
        Select { 
            projection: [Wildcard], 
            from: [
                TableWithJoins { 
                    relation: Table { 
                        name: "a",
                    }, 
                    joins: [] 
                }, 
                TableWithJoins { 
                    relation: Table { 
                        name: "b",
                    }, 
                    joins: [
                        Join { 
                            relation: Table { 
                                name: "c", 
                            }, 
                            join_operator: Inner(Natural)
                        }
                    ] 
                }, 
                TableWithJoins { 
                    relation: Table { 
                        name: "d", 
                    }, 
                    joins: [] 
                }
            ], 
        }
    ), 
}
*/
```

The AST above can be directly represented as a bushy tree:
``` 
    join
   /    \
  join   d
 /    \
a     join
     /    \
    b      c
```

This bushy tree is equivalent to the following left deep tree so we can rewrite it in parsing phase:
```
      join
     /    \
    join   d
   /    \
  join   c
 /    \
a      b
```

After rewriting AST to left deep tree, we will bind the AST to concrete tables and columns with catalog. During binding, semantic checking is necessary(e.g. check whether column name is ambiguous).

To implement semantic checking and simplify the binding process, we introduce `Scope` to represent context of each query block. It will record information about available columns in current context and which table they belong to.

Columns from a parent `Scope` is visible to all of its children `Scope`.

```rust
struct Scope {
    pub parent: Arc<Scope>,
    pub columns: Vec<ColumnRef>
}
```

Here's an example to explain how `Scope` works:
```sql
CREATE TABLE t0 (a INT);
CREATE TABLE t1 (b INT);
CREATE TABLE t2 (c INT);

SELECT *
FROM t0, (
    SELECT b, c, c+1 AS d FROM t1, t2
) t;

/*
Scope root: [t0.a, t.b, t.c, t.d]
|  \
|   Scope t0: [a]
|
Scope t: [t1.b, t2.c, d]
|  \
|   Scope t1: [b]
|
Scope t2: [c]
*/
```

Since it may exist different column with same name after join, we should identify `ColumnRef` with a unique `ColumnID`. Meanwhile, correlation names are ensured to be unique, it's fine to identify them with name strings.

```rust
struct ColumnRef {
    pub id: ColumnID,
    pub column_name: String,
    pub table_name: String
}
```

With unique `ColumnID`, we can check whether a query is ambiguous or not and keep their original name at the same time.

For planner, we will add a variant `Join` for `PlanNode` to represent join operator:

```rust
enum PlanNode {
    ...
    Join(JoinPlan)
}

enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross
}

struct JoinPlan {
    pub join_type: JoinType,
    pub join_conditions: Vec<ExpressionPlan>, // Conjunctions of join condition
    pub left_child: Arc<PlanNode>,
    pub right_child: Arc<PlanNode>
}
```

Here's a problem that databend-query uses `arrow::datatypes::Schema` to represent data schema, while `arrow::datatypes::Schema` doesn't support identify columns with `ColumnID` natively.

I suggest to introduce an internal `DataSchema` struct to represent data schema in databend-query, which can store more information and can be converted to `arrow::datatypes::Schema` naturally.

```rust
struct DataSchema {
    pub columns: Vec<Arc<Column>>
}

struct Column {
    pub column_id: ColumnID,
    pub column_name: String,
    pub data_type: DataType,
    pub is_nullable: bool
}
```

### Optimizer

There are two kinds of optimization to be done:

- Heuristic optimization
- Cost-based optimization

The heuristic optimization(**RBO**, aka rule-based optimization), is the optimization which can always reduce cost of a query. Since there are too many heuristic rules, we won't discuss this here.

The cost-based optimization uses statistic information to calculate the cost of a query. With exploring framework(e.g. Volcano optimizer, Cascades optimizer), it can choose the best execution plan.

Optimizer is the most complicated part in a SQL engine, we'd better only support limited heuristic optimization at the beginning.

> TODO: list common heuristic rules

### Execution

As we discussed in section [Background](#Background), join algorithms can be categorized into three kinds:

- Nested-loop join
- Hash join
- Sort-merge join

Besides, there are two kinds of distributed join algorithms:

- Broadcast join
- Repartition join(aka shuffle join)

We won't talk about detail of distributed join algorithms here, but we still need to consider about them.

Different join algorithms have advantage on different scenarios.

Nested-loop join is effective if the amount of data is relatively small. With vectorized execution model, it's natural to implement block nested-loop join, which is a refined nested-loop join algorithm. Another advantage of nested-loop join is it can work with non-equi join condition.

Hash join is effective if one of the joined table is small and the other one is large. Since distributed join algorithm will always produce small tables(by partition), it fits hash join a lot. Meanwhile, vectorized hash join algorithm has been introduced by **Marcin Zucowski**(Co-founder of Snowflake, Phd of CWI). The disadvantage of hash join is that hash join will consume more memory than other join algorithms, and it only supports equi join.

Sort-merge join is effective if inputs are sorted, while this is rarely happened.

The comparison above is much biased, in fact it can hardly say that which algorithm is better. IMO, we can implement hash join and nested-loop join first since they are more common.

Since we don't have infrastructure(planner, optimizer) for choosing join algorithm for now, I suggest to only implement block nested-loop join at present so we can build a complete prototype.

We'are going to introduce a vectorized block nested-loop join algorithm.

Pseudo code of naive nested-loop join has been introduced in [Background](#Background) section. As we know, nested-loop join will fetch only one row from outer table in each loop, which doen't have good locality. Block nested-loop join is a nested-loop join that will fetch a block of data in each loop. Here we introduce the naive block nested-loop join.

```
// R⋈S
var innerTable = R
var outerTable = S
var result

for s <- outerTable.fetchBlock():
    for r <- innerTable.fetchBlock():
        buffer = conditionEvalBlock(s, r)
        for row <- buffer:
            insert(result, row)
```

In vetorized execution, we can use a bit map to indicate whether a row should be return to result set or not. Then we can materialize the result later.

For example, assume we have following SQL query:

```SQL
CREATE TABLE t(a int, b int);
CREATE TABLE t1(b int, c int);
-- insert some rows
SELECT a, b, c FROM t INNER JOIN t1 ON t.b = t1.b;
```

The execution plan of this query should look like:

```
Join (t.b = t1.b)
    -> TableScan t
    -> TableScan t1
```

If we use the vectorized block nested-loop join algorithm introduced above, the pseudo code should look like:

```
var leftChild: BlockStream = scan(t)
var rightChild: BlockStream = scan(t1)
var condition: Expression = equal(column(t.b), column(t1.b))
var result

for l <- leftChild:
    for r <- rightChild:
        buffer = mergeBlock(l, r)
        var bitMap: Array[boolean] = condition.eval(buffer)
        buffer.insertColumn(bitMap)
        result.insertBlock(buffer)

materialize(result)
```

In databend-query, we can add a `NestedLoopJoinTransform` to implement vectorized block nested-loop join.