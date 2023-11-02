---
title: CREATE AGGREGATING INDEX
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.151"/>

import EEFeature from '@site/src/components/EEFeature';

<EEFeature featureName='AGGREGATING INDEX'/>

Creates a new aggregating index in Databend.

## Syntax

```sql
CREATE AGGREGATING INDEX <index_name> AS SELECT ...
```

- When creating aggregating indexes, limit their usage to standard aggregate functions (e.g., AVG, SUM, MIN, MAX, COUNT), while keeping in mind that GROUPING SETS, window functions, LIMIT, and ORDER BY are not accepted.

- The query filter scope defined when creating aggregating indexes should either match or encompass the scope of your actual queries.

- To confirm if an aggregating index works for a query, use the [EXPLAIN](../../90-explain-cmds/explain.md) command to analyze the query.

## Examples

This example creates an aggregating index named *my_agg_index* for the query "SELECT MIN(a), MAX(c) FROM agg":

```sql
-- Prepare data
CREATE TABLE agg(a int, b int, c int);
INSERT INTO agg VALUES (1,1,4), (1,2,1), (1,2,4), (2,2,5);

-- Create an aggregating index
CREATE AGGREGATING INDEX my_agg_index AS SELECT MIN(a), MAX(c) FROM agg;
```