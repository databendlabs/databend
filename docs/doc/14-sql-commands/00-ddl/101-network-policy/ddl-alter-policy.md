---
title: ALTER NETWORK POLICY
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.26"/>

Modifies an existing network policy in Databend. 

## Syntax

```sql
ALTER NETWORK POLICY [IF EXISTS] policy_name
    SET [ALLOWED_IP_LIST=('allowed_ip1', 'allowed_ip2', ...)]
    [BLOCKED_IP_LIST=('blocked_ip1', 'blocked_ip2', ...)]
    [COMMENT='comment']
```

| Parameter       	| Description                                                                                                                                                                                                                                                           	|
|-----------------	|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
| policy_name     	| Specifies the name of the network policy to be modified.                                                                                                                                                                                                              	|
| ALLOWED_IP_LIST 	| Specifies a comma-separated list of allowed IP address ranges to update for the policy. This overwrites the existing allowed IP address list with the new one provided.                                                                                               	|
| BLOCKED_IP_LIST 	| Specifies a comma-separated list of blocked IP address ranges to update for the policy. This overwrites the existing blocked IP address list with the new one provided. If this parameter is set to an empty list (), it removes all blocked IP address restrictions. 	|
| COMMENT         	| An optional parameter used to update the description or comment associated with the network policy.                                                                                                                                                                   	|

:::note
This command provides the flexibility to update either the allowed IP list or the blocked IP list, while leaving the other list unchanged. Both ALLOWED_IP_LIST and BLOCKED_IP_LIST are optional parameters.
:::

## Examples

```sql
-- Modify the network policy test_policy to change the blocked IP address list from ('192.168.1.99') to ('192.168.1.10'):
ALTER NETWORK POLICY test_policy SET BLOCKED_IP_LIST=('192.168.1.10')

-- Update the network policy test_policy to allow IP address ranges ('192.168.10.0', '192.168.20.0') and remove any blocked IP address restrictions. Also, change the comment to 'new comment':

ALTER NETWORK POLICY test_policy SET ALLOWED_IP_LIST=('192.168.10.0', '192.168.20.0') BLOCKED_IP_LIST=() COMMENT='new comment'
```