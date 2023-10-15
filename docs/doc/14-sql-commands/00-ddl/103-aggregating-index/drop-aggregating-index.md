---
title: DROP AGGREGATING INDEX
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.151"/>

import EEFeature from '@site/src/components/EEFeature';

<EEFeature featureName='AGGREGATING INDEX'/>

Deletes an existing aggregating index. Please note that deleting an aggregating index does NOT remove the associated storage blocks. To delete the blocks as well, use the [VACUUM TABLE](../20-table/91-vacuum-table.md) command. To disable the aggregating indexing feature, set 'enable_aggregating_index_scan' to 0.

## Syntax

```sql
DROP AGGREGATING INDEX <index_name>
```

## Examples

This example deleted an aggregating index named *my_agg_index*:

```sql
DROP AGGREGATING INDEX my_agg_index;
```