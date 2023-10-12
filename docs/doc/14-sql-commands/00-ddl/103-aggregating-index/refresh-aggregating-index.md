---
title: REFRESH AGGREGATING INDEX
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.151"/>

import EEFeature from '@site/src/components/EEFeature';

<EEFeature featureName='AGGREGATING INDEX'/>

Refreshes an aggregating index to update its stored results. Databend recommends refreshing an aggregating index before executing a query that relies on it to retrieve the most up-to-date data.

:::note
When using Databend Cloud, manual execution of this refresh command is unnecessary, as the system automatically handles index updates for you.
:::

## Syntax

```sql
REFRESH AGGREGATING INDEX <index_name> [LIMIT <limit>]
```

The "LIMIT" parameter allows you to control the maximum number of blocks that can be updated with each refresh action. It is strongly recommended to use this parameter with a defined limit to optimize memory usage. Please also note that setting a limit may result in partial data updates. For example, if you have 100 blocks but set a limit of 10, a single refresh might not update the most recent data, potentially leaving some blocks unrefreshed. You may need to execute multiple refresh actions to ensure a complete update.

## Examples

This example refreshes an aggregating index named *my_agg_index*:

```sql
REFRESH AGGREGATING INDEX my_agg_index;
```