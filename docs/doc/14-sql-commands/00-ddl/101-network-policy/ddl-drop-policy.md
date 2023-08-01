---
title: DROP NETWORK POLICY
---

Deletes an existing network policy from Databend. When you drop a network policy, it is removed from Databend, and its associated rules for allowed and blocked IP address lists are no longer in effect.

## Syntax

```sql
DROP NETWORK POLICY [IF EXISTS] policy_name
```

## Examples

```sql
DROP NETWORK POLICY test_policy
```