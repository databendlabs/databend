---
title: NETWORK POLICY
---

### What is Networking Policy?

Network policy in Databend is a configuration mechanism designed to manage and enforce network access control for users within the system. It allows you to define sets of rules governing the allowed and blocked IP address ranges for specific users, effectively controlling their network-level access.

For example, imagine a situation where you wish to define a distinct range of IP addresses that a specific user can use to access Databend. In this scenario, if the user tries to establish a connection with Databend from an IP address beyond the predefined range, even with accurate login credentials, Databend will decline the connection. This mechanism guarantees that access remains confined within the designated IP range, thereby boosting security and enforcing control at the network level.

### Implementing Networking Policy

To implement networking policies in Databend, you need to create a network policy wherein you specify the IP addresses you want to allow or restrict. Afterward, you associate this network policy with a specific user using the [ALTER USER](../30-user/03-user-alter-user.md) command. It's important to note that a single networking policy can be associated with multiple users, as long as they align with the same policy criteria. To manage networking policies in Databend, use the following commands:

- [ALTER NETWORK POLICY](ddl-alter-policy.md)
- [CREATE NETWORK POLICY](ddl-create-policy.md)
- [DESC NETWORK POLICY](ddl-desc-policy.md)
- [DROP NETWORK POLICY](ddl-drop-policy.md)
- [SHOW NETWORK POLICIES](ddl-show-policy.md)

### Usage Example

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