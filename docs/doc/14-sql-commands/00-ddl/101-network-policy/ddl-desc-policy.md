---
title: DESC NETWORK POLICY
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.26"/>

Displays detailed information about a specific network policy in Databend. It provides information about the allowed and blocked IP address lists associated with the policy and the comment, if any, that describes the purpose or function of the policy.

## Syntax

```sql
DESC NETWORK POLICY policy_name
```

## Examples

```sql
DESC NETWORK POLICY test_policy;

Name       |Allowed Ip List          |Blocked Ip List|Comment    |
-----------+-------------------------+---------------+-----------+
test_policy|192.168.10.0,192.168.20.0|               |new comment|
```