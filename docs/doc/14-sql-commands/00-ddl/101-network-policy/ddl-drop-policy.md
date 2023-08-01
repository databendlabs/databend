---
title: DROP NETWORK POLICY
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.26"/>

Deletes an existing network policy from Databend. When you drop a network policy, it is removed from Databend, and its associated rules for allowed and blocked IP address lists are no longer in effect. Please note that, before dropping a network policy, ensure that this policy is not associated with any users.

## Syntax

```sql
DROP NETWORK POLICY [IF EXISTS] policy_name
```

## Examples

```sql
DROP NETWORK POLICY test_policy
```