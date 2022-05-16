---
title: How to Analyze User Retention With Databend
sidebar_label: User Retention Analysis
description: How to do user retention analysis with simplicity and high performance.
---

User retention helps you analyze how many users return to your product or service. Let's go through an example and see how to analyze it in Databend.

It's **easy** and **performance** to use [Databend Retention Function](../30-reference/20-functions/10-aggregate-functions/aggregate-retention.md) to do the user retention analysis.

## Step 1. Databend

### 1.1 Deploy Databend

Make sure you have installed Databend, if not please see:

* [How to Deploy Databend](../00-guides/index.md#deployment)

### 1.2 Create a Databend User

Connect to Databend server with MySQL client:
```shell
mysql -h127.0.0.1 -uroot -P3307 
```

Create a user:
```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
```

Grant privileges for the user:
```sql
GRANT ALL ON *.* TO user1;
```

See also [How To Create User](../30-reference/30-sql/00-ddl/30-user/01-user-create-user.md).

### 1.3 Create a Table

Connect to Databend server with MySQL client:
```shell
mysql -h127.0.0.1 -uuser1 -pabc123 -P3307 
```

```sql
CREATE TABLE events(`user_id` INT, `login_date` DATE);
```
We have a table with the following fields:
* user_id - a unique identifier for user
* login_date - user login date

Prepare data:
```sql
INSERT INTO events SELECT number AS user_id, '2022-05-15' FROM numbers(1000000);
INSERT INTO events SELECT number AS user_id, '2022-05-16' FROM numbers(900000);
INSERT INTO events SELECT number As user_id, '2022-05-17' FROM numbers(100000);
```

## Step 2. User Retention Analysis

```sql
SELECT
    sum(r[0]) AS r1,
    sum(r[1]) AS r2,
    sum(r[2]) AS r3
FROM
(
    SELECT
        user_id,
        retention(login_date = '2022-05-15', login_date = '2022-05-16', login_date = '2022-05-17') AS r
    FROM events
    GROUP BY user_id
);
```

The retention result is:
```sql
+---------+--------+--------+
| r1      | r2     | r3     |
+---------+--------+--------+
| 1000000 | 900000 | 100000 |
+---------+--------+--------+
```

**Enjoy your journey.** 
