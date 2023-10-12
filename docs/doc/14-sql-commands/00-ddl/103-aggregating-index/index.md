---
title: AGGREGATING INDEX
---
import IndexOverviewList from '@site/src/components/IndexOverviewList';
import EEFeature from '@site/src/components/EEFeature';

<EEFeature featureName='AGGREGATING INDEX'/>

### Why Aggregating Index?

The primary purpose of the aggregating index is to enhance query performance, especially in scenarios involving aggregation queries such as MIN, MAX, and SUM. It achieves this by precomputing and storing query results separately in blocks, eliminating the need to scan the entire table and thereby speeding up data retrieval.

The feature also incorporates a refresh mechanism that enables you to update and save the latest query results as needed, ensuring that the query responses consistently reflect the most current data. This manual control allows you to maintain data accuracy and reliability by refreshing the results when deemed necessary.

Please note the following when creating aggregating indexes:

- When creating aggregating indexes, limit their usage to standard aggregate functions (e.g., AVG, SUM, MIN, MAX, COUNT), while keeping in mind that GROUPING SETS, window functions, LIMIT, and ORDER BY are not accepted.

- The query filter scope defined when creating aggregating indexes should either match or encompass the scope of your actual queries.

- To confirm if an aggregating index works for a query, use the [EXPLAIN](../../90-explain-cmds/explain.md) command to analyze the query.

Databend recommends refreshing an aggregating index before executing a query that relies on it to retrieve the most up-to-date data (while Databend Cloud automatically refreshes aggregating indexes for you). If you no longer need an aggregating index, consider deleting it. Please note that deleting an aggregating index does NOT remove the associated storage blocks. To delete the blocks as well, use the [VACUUM TABLE](../20-table/91-vacuum-table.md) command. To disable the aggregating indexing feature, set 'enable_aggregating_index_scan' to 0.

### Implementing Aggregating Index

Databend provides the following commands to manage aggregating indexes:

<IndexOverviewList />

### Usage Example

This example demonstrates the utilization of aggregating indexes and illustrates their impact on the query execution plan.

```sql
-- Prepare data
CREATE TABLE agg(a int, b int, c int);
INSERT INTO agg VALUES (1,1,4), (1,2,1), (1,2,4), (2,2,5);

-- Create an aggregating index
CREATE AGGREGATING INDEX my_agg_index AS SELECT MIN(a), MAX(c) FROM agg;

-- Refresh the aggregating index
REFRESH AGGREGATING INDEX my_agg_index;

-- Verify if the aggregating index works
EXPLAIN SELECT MIN(a), MAX(c) FROM agg;

explain                                                                                                               |
----------------------------------------------------------------------------------------------------------------------+
AggregateFinal                                                                                                        |
├── output columns: [MIN(a) (#8), MAX(c) (#9)]                                                                        |
├── group by: []                                                                                                      |
├── aggregate functions: [min(a), max(c)]                                                                             |
├── estimated rows: 1.00                                                                                              |
└── AggregatePartial                                                                                                  |
    ├── output columns: [MIN(a) (#8), MAX(c) (#9)]                                                                    |
    ├── group by: []                                                                                                  |
    ├── aggregate functions: [min(a), max(c)]                                                                         |
    ├── estimated rows: 1.00                                                                                          |
    └── TableScan                                                                                                     |
        ├── table: default.default.agg                                                                                |
        ├── output columns: [a (#5), c (#7)]                                                                          |
        ├── read rows: 4                                                                                              |
        ├── read bytes: 61                                                                                            |
        ├── partitions total: 1                                                                                       |
        ├── partitions scanned: 1                                                                                     |
        ├── pruning stats: [segments: <range pruning: 1 to 1>, blocks: <range pruning: 1 to 1, bloom pruning: 0 to 0>]|
        ├── push downs: [filters: [], limit: NONE]                                                                    |
        ├── aggregating index: [SELECT MIN(a), MAX(c) FROM default.agg]                                               |
        ├── rewritten query: [selection: [index_col_0 (#0), index_col_1 (#1)]]                                        |
        └── estimated rows: 4.00                                                                                      |

-- Delete the aggregating index
DROP AGGREGATING INDEX my_agg_index;

EXPLAIN SELECT MIN(a), MAX(c) FROM agg;

explain                                                                                                               |
----------------------------------------------------------------------------------------------------------------------+
AggregateFinal                                                                                                        |
├── output columns: [MIN(a) (#3), MAX(c) (#4)]                                                                        |
├── group by: []                                                                                                      |
├── aggregate functions: [min(a), max(c)]                                                                             |
├── estimated rows: 1.00                                                                                              |
└── AggregatePartial                                                                                                  |
    ├── output columns: [MIN(a) (#3), MAX(c) (#4)]                                                                    |
    ├── group by: []                                                                                                  |
    ├── aggregate functions: [min(a), max(c)]                                                                         |
    ├── estimated rows: 1.00                                                                                          |
    └── TableScan                                                                                                     |
        ├── table: default.default.agg                                                                                |
        ├── output columns: [a (#0), c (#2)]                                                                          |
        ├── read rows: 4                                                                                              |
        ├── read bytes: 61                                                                                            |
        ├── partitions total: 1                                                                                       |
        ├── partitions scanned: 1                                                                                     |
        ├── pruning stats: [segments: <range pruning: 1 to 1>, blocks: <range pruning: 1 to 1, bloom pruning: 0 to 0>]|
        ├── push downs: [filters: [], limit: NONE]                                                                    |
        └── estimated rows: 4.00                                                                                      |
```