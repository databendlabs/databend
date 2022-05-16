---
title: How to Do Funnel Analysis With Databend
sidebar_label: Funnel Analysis
---

Funnel analysis measures the number of unique users who has performed a set of actions, and we use it to see drop-off and conversion in multi-step processes.

In Databend, it's **easy** and **performance** to do it using [WINDOW_FUNNEL FUNCTION](../30-reference/20-functions/10-aggregate-functions/aggregate-windowfunnel.md)

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
CREATE TABLE events(user_id BIGINT, event_name VARCHAR, event_timestamp TIMESTAMP);
```

Prepare data:
```sql
INSERT INTO events VALUES(100123, 'login', '2022-05-14 10:01:00');
INSERT INTO events VALUES(100123, 'visit', '2022-05-14 10:02:00');
INSERT INTO events VALUES(100123, 'cart', '2022-05-14 10:04:00');
INSERT INTO events VALUES(100123, 'purchase', '2022-05-14 10:10:00');

INSERT INTO events VALUES(100125, 'login', '2022-05-15 11:00:00');
INSERT INTO events VALUES(100125, 'visit', '2022-05-15 11:01:00');
INSERT INTO events VALUES(100125, 'cart', '2022-05-15 11:02:00');

INSERT INTO events VALUES(100126, 'login', '2022-05-15 12:00:00');
INSERT INTO events VALUES(100126, 'visit', '2022-05-15 12:01:00');
```

Input table:

``` sql
+---------+------------+----------------------------+
| user_id | event_name | event_timestamp            |
+---------+------------+----------------------------+
|  100123 | login      | 2022-05-14 10:01:00.000000 |
|  100123 | visit      | 2022-05-14 10:02:00.000000 |
|  100123 | cart       | 2022-05-14 10:04:00.000000 |
|  100123 | purchase   | 2022-05-14 10:10:00.000000 |
|  100125 | login      | 2022-05-15 11:00:00.000000 |
|  100125 | visit      | 2022-05-15 11:01:00.000000 |
|  100125 | cart       | 2022-05-15 11:02:00.000000 |
|  100126 | login      | 2022-05-15 12:00:00.000000 |
|  100126 | visit      | 2022-05-15 12:01:00.000000 |
+---------+------------+----------------------------+
```

## Step 2. Funnel Analysis

Find out how far the user `user_id` could get through the chain in an hour window slides.

``` sql
SELECT
    level,
    count() AS count
FROM
(
    SELECT
        user_id,
        window_funnel(3600000000)(event_timestamp, event_name = 'login', event_name = 'visit', event_name = 'cart', event_name = 'purchase') AS level
    FROM events
    GROUP BY user_id
)
GROUP BY level ORDER BY level ASC;
```
:::tip

The `event_timestamp` type is timestamp, `3600000000` is a hour time window.

:::

Result:

``` sql
+-------+-------+
| level | count |
+-------+-------+
|     2 |     1 |
|     3 |     1 |
|     4 |     1 |
+-------+-------+
```

* User `100126` level is 2 (`login -> visit`) .
* user `100125` level is 3 (`login -> visit -> cart`).
* User `100123` level is 4 (`login -> visit -> cart -> purchase`).

**Enjoy your journey.** 
