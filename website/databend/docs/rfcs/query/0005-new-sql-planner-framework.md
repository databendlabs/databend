- Start date: 2021/09/13
- Tracking issues: https://github.com/datafuselabs/datafuse/issues/1217

# Sumamry

In order to support more complicated SQL queries, for example the queries contain `JOIN` and correlated subquery, we need to redesign the SQL planner component.

The main problems of current implementation we are going to discussed in this RFC are as follows:

- Doesn't support `JOIN` and correlated subquery
- Doesn't have ability to do strict semantic checking, e.g. type check and name resolution, which brings unnecessary complexity of correctness ensurance during SQL optimization and execution
- Doesn't have a universal SQL optimization framework

Let's start with a simple example.

In SQL, it allows duplicated names of fields in a tuple. In PostgreSQL, a result set can contain different columns with same name:

```
postgres=# create table t(a int);
CREATE TABLE
postgres=# insert into t values(1),(2);
INSERT 0 2
postgres=# select * from t t1 cross join t t2;
 a | a
---+---
 1 | 1
 1 | 2
 2 | 1
 2 | 2
(4 rows)
```

We can see that there are two fields named with `a`, one of them comes from derived table `t1` and the other one comes from derived table `t2`.

If you try to reference the column with duplicated name `a`, it will return an error:

```
postgres=# select a from t t1, t t2;
ERROR:  column reference "a" is ambiguous
LINE 1: select a from t t1, t t2;
```

While you can reference the column with a canonical name like `t.a` since the table name is required to be unique in a query context.

Currently, `databend` uses `DataSchema` to represent input and output relation schema, which can not provide enough information to handle the case shown above. In a `DataSchema`, each column is represented with `DataField`, which has following definition:

```rust
pub struct DataField {
    name: String,
    data_type: DataType,
    nullable: bool,
}
```

Each `DataField` inside a `DataSchema` is identified with a unique `name` string. For now, the `name` just represent column name, thus it's difficult to implement `JOIN` with this abstraction. We will talk about the detailed solution of this in later sections.

The second problem is about semantic check.

Take type check as an example, each variable(e.g. column reference, constant value) inside an expression has it's own data type. And each scalar expresiion has requirement of data type for its arguments, for instance, a `+` expression requires its arguments to be numeric.

To make sure that the query is valid and correct, we need to do type checking before executing the query.

Since both optimizer and executor has requirement on type checking, it's better to resolve this with a single component, which can make it more maintainable.

The last problem is about the query optimization framework.

Many of modern optimizers are implemented in Volcano/Cascades style, which is a highly modular approach.

A typical Cascades optimizer consists of independent modules:

- Transformation rules
- Implementation rules
- Exploration engine
- Cost model

What's insteresting is that, the rule system(transformation and implementation) is decoupled with exploration engine and cost model, which means it's easy to build a heuristic optimizer without CBO(cost based optimization). And as soon as we're going to implement CBO, the rule system can be reused.

Actually, this is the practical way. In some industrial Cascades implementation(e.g. SQL Server and CockroachDB), there is always a heuristic optimization phase, for example `pre-exploration` in SQL Server and `normalization` in CockroachDB, which generally shares a same rule system with exploration engine.

In summary, this RFC will:

- Introduce a new framework to support planning `JOIN` and correlated subquery
- Introduce a rule system that allows developer to write transformation rules easily

# Design Details

## Architecture

In current implementation, a SQL query will be processed as follows:

1. `PlanParser` will parse the SQL text into AST(Abstrct Syntax Tree)
2. `PlanParser` will also build a canonical plan tree represented with `PlanNode` from the AST
3. After building plan tree, `Optimizer` will do some canonical optimization to the plan, and produce the final `PlanNode`
4. `Intepreter` will take the `PlanNode` as input and interpret it as an executable `Pipeline` consists of `Processor`s
5. Executor will execute the `Pipeline` with specific runtime

In our new framework, `PlanParser` will be refactored into two components:

- `Parser`: parsing SQL text into uniform AST representation, which has been introduced in [this PR](https://github.com/datafuselabs/databend/pull/1478)
- `Binder`: bind variables appeared in the AST with objects(e.g. tables, columns, etc) in database and perform semantic check(name resolution, type checking). Will produce logical representation of plan tree

Besides, a `rule system` will be introduced and replace current optimizer.

## Binder

Since there maybe many syntactic contexts in a SQL query, we need a way to track the dependency relationship between them and the visibility of `name`s in different contexts.

Here we propose the abstraction `Metadata`, which stores all the metadata we need for query optimization, including base tables from catalog, derived tables(subquery and join) and columns. Each table and column will be assigned with a unique identifier.

```rust
pub struct Metadata {
    pub tables: Vec<TableEntry>,
}

pub struct TableEntry {
    pub index: IndexType, // Index of the table in `Metadata`
    pub columns: Vec<ColumnEntry>,
    // And metadata about the table, e.g. name, database name and etc.
}

pub struct ColumnEntry {
    pub index: ColumnIndex,
    // And metadata about the column, e.g. name, data type and etc.
}

pub type IndexType = usize;

pub struct ColumnIndex {
    pub table_index: IndexType, // Index of the table this column belongs to
    pub column_index: IndexType, // Index of the column inside its `TableEntry`
}
```

Therefore, after name resolution each variable will be bound with a unique `ColumnIndex` but not a string, so we don't need to worry about the issues like duplicated name.

During the binding procedure, we need to track the state of binding. The state may contain the following information:

- Visible columns in current context, used when processing wildcard result set(`SELECT * FROM t`)
- If a column in a context is group by key or not
- If a column is derived column(i.e. projection like `a+1 AS a`) or not
- If a variable is from current subquery or not, to identify correlated column reference

To maintain the state, we propose a data structure `BindContext`(this name is inspired by `BinderContext` from CMU Peloton, which is a very appropriate name in my mind).

`BindContext` is a stack-like structure, each `BindContext` node in the stack records state of corresponding syntactic context. SQL binding is a bottom-up procedure, which means it will process AST recursively, add columns produced by data source(e.g. table scan, join, subquery) into 

Briefly, `BindContext` is a set of column references. To be clear, we will use diagram to explain hwo this mechanism works.

Take this example:

```sql
create table t (a int);

select * from t -- table: context 1 [t.a]
cross join t t1 -- table: context 2 [t1.a]
-- join: context 3 [t.a, t1.a]
where t.a = 1
```

According to semantic of SQL, we can describe the binding procedure as follows:
<!-- 
```rust
let mut context_1 = BindContext::new();
binder.bind_table("t", &mut context_1);

let mut context_2 = BindContext::new();
binder.bind_table("t", &mut context_2);

let mut context_3 = BindContext::new();
binder.bind_join(context_1, context_2, &mut context_3);

binder.bind_expression(expr, &context_3);
``` -->

1. Create an empty `BindContext` context 1 for table `t`, and fill it with columns from `t`
2. Create an empty `BindContext` context 2 for table `t`, fill it with columns from `t`, and rename the table to `t1`
3. Create an empty `BindContext` context 3 for `t cross join t1`, and fill it with columns from `t` and `t1`
4. Perform name resolution for predicate `t.a = 1`
5. Lookup context 3, and find the corresponding `ColumnEntry` for variable `t.a`

Let's take a look at how `BindContext` handles correlated subquery.

A correlated subquery indicates that the subquery depends on a column from outer context.

There is a canonical `Apply` operator to execute correlated subquery, which will evaluate the subquery expression for each tuple(like a cross join). While most of the correlated subqueries can be decorrelated into join(e.g. semi-join, anti-semi-join and etc.).

Take this query as example:

```sql
create table t (a int);

select * from t -- table: context 1 [t.a]
where exists (
    select * from t t1 -- table: context 2 with parent 1 [t1.a]
    where t1.a = t.a -- t.a is a correlated column since it comes from t that appears in outer query
);
```

Before binding the `exists` subquery, we will create a new `BindContext` for it, and pass the outer `BindContext` as its parent context.

When we bind the correlated column reference `t.a` inside the subquery, we will first lookup current `BindContext` to see if it exists an appropriate column, if not, then we will keep trying to do the lookup in parent context until we find the corresponding column or we exhaust all the parents.

If we find the column in parent context, then we can confirm that this subquery is a correlated subquery, and the column reference bound with parent context is the correlated column.

The procedure can be summarized as follows:

1. Create an empty `BindContext` context 1 for table `t`, and fill it with columns from `t`
2. Create an empty `BindContext` context 2 for table `t` with context 1 as its parent cotext, fill it with columns from `t`, and rename the table to `t1`
3. Perform name resolution for predicate `t1.a = t.a`
4. Lookup context 2, and find the corresponding `ColumnEntry` for variable `t1.a`, but can not find `t.a`. So we will keep going through step 5
5. Lookup parent of context 2(context 1), and find the corresponding `ColumnEntry` for variable `t.a`. Since the variable is found in outer context, it will be marked as correlated column reference, and the subquery will be marked as correlated

## Optimizer & Rule system

SQL optimization is based on the equivalence of relational algebra. There are a bunch of different thereoms and lemmas can help us identify if two relational algebra trees are logically equivalent.

In Cascades/Volcano style query optimizer, transformation rules are used to perform the algebra transformation on a plan tree.

Each transformation rule has description about the relational operator it can be applied to, which we call it a `rule pattern`. The optimizer will provide a scheme to walk through the plan tree, check if any rule can be applied to the plan tree, and then apply transformation for the matched rules.

The `rule pattern` is also a tree structure, which will look like:

```rust
pub trait RulePattern {
    fn get_children_pattern(&self) -> Vec<Box<dyn RulePattern>>;

    fn match(&self, plan: &PlanNode) -> bool;
}
```

The transformation rule should implement the following trait:

```rust
pub trait TransformationRule {
    fn get_rule_pattern(&self) -> &dyn RulePattern;

    fn apply(&self, plan: &PlanNode) -> Result<PlanNode>;
}
```

With these rule abstraction, we can write transformation rules easily. For example, we want to implement `ConstantFolding` transformation, the scheme can be described as:

1. Find all operators with scalar expressions
2. Fold the constant values found in scalar expressions
3. Replace original operators with rewritten operators

The pseudo code:

```rust
pub struct ConstantFoldingPattern;

impl RulePattern for ConstantFoldingPattern {
    fn get_children_pattern(&self) -> Vec<Box<dyn RulePattern>> {
        vec![]
    }

    fn match(&self, plan: &PlanNode) -> bool {
        !plan.expr.empty()
    }
}

pub struct ConstantFoldingRule {
    pattern: ConstantFoldingPattern,
}

impl TransformationRule for ConstantFoldingRule {
    fn get_rule_pattern(&self) -> &dyn RulePattern {
        &self.pattern
    }

    fn apply(&self, plan: &PlanNode) -> Result<PlanNode> {
        let new_expr = plan.get_expr().constant_folding()?;
        let mut new_plan = plan.clone();
        new_plan.set_expr(new_expr);
        Ok(new_plan)
    }
}

pub struct Optimizer {
    rules: Vec<Box<dyn TransformationRule>>
}

impl ReplaceVisitor for Optimizer {
    fn visit(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut new_plan = plan.clone();
        for rule in self.rules.iter() {
            if rule.get_rule_pattern().match(plan) {
                new_plan = rule.apply(&new_plan)?;
            }
        }
        Ok(new_plan)
    }
}

pub fn optimize(plan: &PlanNode) -> Result<PlanNode> {
    let opt = Optimizer { rules: vec![ConstantFoldingRule::new()], };
    opt.visit(plan)
}
```

# Milestone

After this refactoring, we want:

- Provide naive implementation(hash join for ) for `JOIN`, including planning and execution
- Support running most of the queries from TPCH benchmark(contains different types of joins and correlated subquery) with `databend`
- Implement several simple optimization rules, e.g. outer join elimination, decorrelation, predicate pushdown and etc.
- Migrate to the new planner framework

And at the same time, we won't:

- Take performance serious, related work should be done in next stage
- Implement cost based optimization, this work depends on design of statistics system
