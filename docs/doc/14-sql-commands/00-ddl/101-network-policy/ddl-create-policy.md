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

This example demonstrates creating a network policy with specified allowed and blocked IP addresses, and then associating this policy with a user to control network access. The network policy allows all IP addresses ranging from 192.168.1.0 to 192.168.1.255, except for the specific IP address 192.168.1.99.

```sql
-- Create a network policy
CREATE NETWORK POLICY sample_policy
    ALLOWED_IP_LIST=('192.168.1.0/24')
    BLOCKED_IP_LIST=('192.168.1.99')
    COMMENT='Sample';

SHOW NETWORK POLICIES;

Name         |Allowed Ip List          |Blocked Ip List|Comment    |
-------------+-------------------------+---------------+-----------+
sample_policy|192.168.1.0/24           |192.168.1.99   |Sample     |

-- Create a user
CREATE USER sample_user IDENTIFIED BY 'databend';

-- Associate the network policy with the user
ALTER USER sample_user WITH SET NETWORK POLICY='sample_policy';
```