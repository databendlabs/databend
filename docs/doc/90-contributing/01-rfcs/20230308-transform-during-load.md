---
title: Transform During Load 
---

## Summary

Support transform when copy from stage into table.

## Motivation

Currently, we support stage table function, so we can transform when **insert** from stage into table. 

```sql
insert into table1 from (select c1， c2 from @stage1/path/to/dir);
```

but this still missing one feature of `COPY INTO`: incremental load (remember files already loaded).

`COPY INTO <table>` only support `FROM <STAGE>` now. 

## Guide-level explanation

support copy into like this:

```sql
COPY into table1 from (select c1， c2 from @stage1/path/to/dir where c3 > 1);
```

### limitation to the subquery part:

1. contain only one **base table** and must be a [stage table function](../../15-sql-functions/112-table-functions/stage_table_function.md)
2. aggregation is not allowed, because each record inserted should belong to some file. 
3. due to the prev 2 limits, it is reasonable to state that nested subquery is not need, we may choose to forbidden it.

### auto cast

the output expr of subquery should be cast to schema of table if it satisfies the auto_cast_rule.

some cast can be optimized out. e.g.: 

for ` COPY into table1 from (select c1::string from @stage1/path/to/dir); ` where c1 is int,
we can parse c1 as string directly. 
if dest column for c1 is int too, '::string' can be dropped.


### optimize

projection and filter: 

1. for file formats like parquet, column pruning and filter pushdown can work well.
2. for other formats like CSV/TSV/NDJSON, there are chance to quickly parse though the unused columns, for example for TSV, 
we can jump to next `\n` directly after the last field in projection is parsed.


## Reference-level explanation

```sql
COPY into <table> from (select <expr> from @<stage>/<path>(<options>) <alias> where <expr>);
```

## Rationale and alternatives

some other features can be used for "Transform During Load", but only in limited situations:

### auto cast 

if the source schema can auto cast to dest schema, no explicit transform in SQL should be required.
now, some 'cast' is implemented by the parser, as long as use provide the dest type like in [RFC: Stage With Schema](./20230308-transform-during-load.md) 
e.g. Timestamp Type decoder may accept both number and string. 

we should improve this later. before that, user can use the way in this RFC to complete the same function.

### FileFormatOptions

for example, some Database has an option `TRIM` for CSV file format. but user may need diff variant of it 

1. trim heading or tailing space？ or both?
2. inside quote or outside quote？
3. apply to some column? whitespaces may be meaningful for some column.
