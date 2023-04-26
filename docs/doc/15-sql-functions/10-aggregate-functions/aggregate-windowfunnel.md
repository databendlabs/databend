---
title: WINDOW_FUNNEL
description: Funnel Analysis
---

<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/learn/databend-funnel.png" width="550"/>
</p>

## WINDOW_FUNNEL

Similar to `windowFunnel` in ClickHouse (they were created by the same author), it searches for event chains in a sliding time window and calculates the maximum number of events from the chain.

The function works according to the algorithm:

-   The function searches for data that triggers the first condition in the chain and sets the event counter to 1. This is the moment when the sliding window starts.

-   If events from the chain occur sequentially within the window, the counter is incremented. If the sequence of events is disrupted, the counter isn’t incremented.

-   If the data has multiple event chains at varying completion points, the function will only output the size of the longest chain.


```sql
WINDOW_FUNNEL( <window> )( <timestamp>, <cond1>, <cond2>, ..., <condN> )
```

**Arguments**

-   `<timestamp>` — Name of the column containing the timestamp. Data types supported: integer types and datetime types.
-   `<cond>` — Conditions or data describing the chain of events. Must be `Boolean` datatype.

**Parameters**

-   `<window>` — Length of the sliding window, it is the time interval between the first and the last condition. The unit of `window` depends on the `timestamp` itself and varies. Determined using the expression `timestamp of cond1 <= timestamp of cond2 <= ... <= timestamp of condN <= timestamp of cond1 + window`.

**Returned value**

The maximum number of consecutive triggered conditions from the chain within the sliding time window.
All the chains in the selection are analyzed.

Type: `UInt8`.


**Example**

Determine if a set period of time is enough for the user to SELECT a phone and purchase it twice in the online store.

Set the following chain of events:

1. The user logged into their account on the store (`event_name = 'login'`).
2. The user land the page (`event_name = 'visit'`).
3. The user adds to the shopping cart(`event_name = 'cart'`).
4. The user complete the purchase (`event_name = 'purchase'`).


```sql
CREATE TABLE events(user_id BIGINT, event_name VARCHAR, event_timestamp TIMESTAMP);

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

```sql
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

Find out how far the user `user_id` could get through the chain in an hour window slides.

Query:

```sql
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

```sql
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

