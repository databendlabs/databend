---
title: REFRESH AGGREGATING INDEX
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.151"/>

import EEFeature from '@site/src/components/EEFeature';

<EEFeature featureName='AGGREGATING INDEX'/>

Refreshes an aggregating index to update its stored results. Databend recommends refreshing an aggregating index before executing a query that relies on it to retrieve the most up-to-date data.

## Syntax

```sql
REFRESH AGGREGATING INDEX <index_name>
```

## Examples

This example refreshes an aggregating index named *my_agg_index*:

```sql
REFRESH AGGREGATING INDEX my_agg_index;
```