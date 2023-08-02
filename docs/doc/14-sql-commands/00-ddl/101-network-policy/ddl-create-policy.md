---
title: CREATE NETWORK POLICY
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.26"/>

Creates a new network policy in Databend.

## Syntax

```sql
CREATE NETWORK POLICY [IF NOT EXISTS] policy_name
    ALLOWED_IP_LIST=('allowed_ip1', 'allowed_ip2', ...)
    [BLOCKED_IP_LIST=('blocked_ip1', 'blocked_ip2', ...)]
    [COMMENT='comment']
```

| Parameter       	| Description                                                                                                                                                                                      	|
|-----------------	|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
| policy_name     	| Specifies the name of the network policy to be created.                                                                                                                                          	|
| ALLOWED_IP_LIST 	| Specifies a comma-separated list of allowed IP address ranges for the policy. Users associated with this policy can access the network using the specified IP ranges.                    	|
| BLOCKED_IP_LIST 	| Specifies a comma-separated list of blocked IP address ranges for the policy. Users associated with this policy can still access the network from ALLOWED_IP_LIST, except for the IPs specified in BLOCKED_IP_LIST, which will be restricted from access. 	|
| COMMENT         	| An optional parameter used to add a description or comment for the network policy.                                                                                                               	|

## Examples

```sql
-- Create a network policy named test_policy with allowed IP address 192.168.1.0/24 and blocked IP address 192.168.1.99 along with a comment
CREATE NETWORK POLICY test_policy ALLOWED_IP_LIST=('192.168.1.0/24') BLOCKED_IP_LIST=('192.168.1.99') COMMENT='test comment'

-- Create a network policy named test_policy1 with allowed IP address 192.168.100.0/24 and no blocked IP address specified:
CREATE NETWORK POLICY test_policy1 ALLOWED_IP_LIST=('192.168.100.0/24')

SHOW NETWORK POLICIES;

Name        |Allowed Ip List |Blocked Ip List|Comment     |
------------+----------------+---------------+------------+
test_policy |192.168.1.0/24  |192.168.1.99   |test comment|
test_policy1|192.168.100.0/24|               |            |
```