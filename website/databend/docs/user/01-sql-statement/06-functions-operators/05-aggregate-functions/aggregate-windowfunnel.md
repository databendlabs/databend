---
title: windowFunnel
---


## windowFunnel

Similar to `windowFunnel` in ClickHouse (they were created by the same author), it searches for event chains in a sliding time window and calculates the maximum number of events that occurred from the chain.

The function works according to the algorithm:

-   The function searches for data that triggers the first condition in the chain and sets the event counter to 1. This is the moment when the sliding window starts.

-   If events from the chain occur sequentially within the window, the counter is incremented. If the sequence of events is disrupted, the counter isn’t incremented.

-   If the data has multiple event chains at varying points of completion, the function will only output the size of the longest chain.


``` sql
windowFunnel(window)(timestamp, cond1, cond2, ..., condN)
```

**Arguments**

-   `timestamp` — Name of the column containing the timestamp. Data types supported: unsigned integer types.
-   `cond` — Conditions or data describing the chain of events. Must be `Boolean` datatype.

**Parameters**

-   `window` — Length of the sliding window, it is the time interval between the first and the last condition. The unit of `window` depends on the `timestamp` itself and varies. Determined using the expression `timestamp of cond1 <= timestamp of cond2 <= ... <= timestamp of condN <= timestamp of cond1 + window`.

**Returned value**

The maximum number of consecutive triggered conditions from the chain within the sliding time window.
All the chains in the selection are analyzed.

Type: `UInt8`.


**Example**

Determine if a set period of time is enough for the user to select a phone and purchase it twice in the online store.

Set the following chain of events:

1.  The user logged in to their account on the store (`eventID = 1003`).
2.  The user searches for a phone (`eventID = 1007, product = 'phone'`).
3.  The user placed an order (`eventID = 1009`).
4.  The user made the order again (`eventID = 1010`).

Input table:

``` sql
┌─user_id─┬─timestamp─┬─eventID─┬─product─┐
│       1 │        20 │    1003 │ phone   │
│       1 │       200 │    1007 │ phone   │
│       1 │        50 │    1009 │ phone   │
│       1 │       400 │    1010 │ phone   │
└─────────┴───────────┴─────────┴─────────┘
```

Find out how far the user `user_id` could get through the chain.

Query:

``` sql
SELECT
    level,
    count() AS c
FROM
(
    SELECT
        user_id,
        windowFunnel(500)(timestamp, eventID = 1003, eventID = 1009, eventID = 1007, eventID = 1010) AS level
    FROM test
    GROUP BY user_id
)
GROUP BY level ORDER BY level ASC;
```

Result:

``` text
┌─level─┬─c─┐
│     4 │ 1 │
└───────┴───┘
```
