---
title: SHOW NETWORK POLICIES
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.26"/>

Displays a list of all existing network policies in Databend. It provides information about the available network policies, including their names and whether they have any allowed or blocked IP address lists configured.

## Syntax

```sql
SHOW NETWORK POLICIES
```

## Examples

```sql
SHOW NETWORK POLICIES;

Name        |Allowed Ip List |Blocked Ip List|Comment     |
------------+----------------+---------------+------------+
test_policy |192.168.1.0/24  |192.168.1.99   |test comment|
test_policy1|192.168.100.0/24|               |            |
```