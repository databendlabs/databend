---
title: SET_VAR
---

SET_VAR is used to specify optimizer hints within a single SQL statement, allowing for finer control over the execution plan of that specific statement. This includes:

- Configure settings temporarily, affecting only the duration of the SQL statement execution. It's important to note that the settings specified with SET_VAR will solely impact the result of the current statement being executed and will not have any lasting effects on the overall database configuration. For a list of available settings that can be configured using SET_VAR, see [SHOW SETTINGS](../40-show/show-settings.md). To understand how it works, see these examples:

    - [Example 1. Temporarily Set Timezone](#example-1-temporarily-set-timezone)
    - [Example 2: Control Parallel Processing for COPY INTO](#example-2-control-parallel-processing-for-copy-into)

- Control the deduplication behavior on [INSERT](../10-dml/dml-insert.md), [UPDATE](../10-dml/dml-update.md), or [REPLACE](../10-dml/dml-replace.md) operations with the label *deduplicate_label*. For those operations with a deduplicate_label in the SQL statements, Databend executes only the first statement, and subsequent statements with the same deduplicate_label value are ignored, regardless of their intended data modifications. Please note that once you set a deduplicate_label, it will remain in effect for a period of 24 hours. To understand how the deduplicate_label assists in deduplication, see [Example 3: Set Deduplicate Label](#example-3-set-deduplicate-label).

See also: [SET](01-set-global.md)

## Syntax

```sql
/*+ SET_VAR(key=value) SET_VAR(key=value) ... */
```

- The hint must immediately follow an [SELECT](../20-query-syntax/01-query-select.md), [INSERT](../10-dml/dml-insert.md), [UPDATE](../10-dml/dml-update.md), [REPLACE](../10-dml/dml-replace.md), [DELETE](../10-dml/dml-delete-from.md), or [COPY](../10-dml/dml-copy-into-table.md) (INTO) keyword that begins the SQL statement.
- A SET_VAR can include only one Key=Value pair, which means you can configure only one setting with one SET_VAR. However, you can use multiple SET_VAR hints to configure multiple settings.
    - If multiple SET_VAR hints containing a same key, the first Key=Value pair will be applied.
    - If a key fails to parse or bind, all hints will be ignored.

## Examples

### Example 1: Temporarily Set Timezone

```sql
root@localhost> SELECT TIMEZONE();

SELECT
  TIMEZONE();

┌────────────┐
│ timezone() │
│   String   │
├────────────┤
│ UTC        │
└────────────┘

1 row in 0.011 sec. Processed 1 rows, 1B (91.23 rows/s, 91B/s)

root@localhost> SELECT /*+SET_VAR(timezone='America/Toronto') */ TIMEZONE();

SELECT
  /*+SET_VAR(timezone='America/Toronto') */
  TIMEZONE();

┌─────────────────┐
│    timezone()   │
│      String     │
├─────────────────┤
│ America/Toronto │
└─────────────────┘

1 row in 0.023 sec. Processed 1 rows, 1B (43.99 rows/s, 43B/s)

root@localhost> SELECT TIMEZONE();

SELECT
  TIMEZONE();

┌────────────┐
│ timezone() │
│   String   │
├────────────┤
│ UTC        │
└────────────┘

1 row in 0.010 sec. Processed 1 rows, 1B (104.34 rows/s, 104B/s)
```
### Example 2: Control Parallel Processing for COPY INTO

In Databend, the *max_threads* setting specifies the maximum number of threads that can be utilized to execute a request. By default, this value is typically set to match the number of CPU cores available on the machine.

When loading data into Databend with COPY INTO, you can control the parallel processing capabilities by injecting hints into the COPY INTO command and setting the *max_threads* parameter. For example:

```sql
COPY /*+ set_var(max_threads=6) */ INTO mytable FROM @mystage/ pattern='.*[.]parq' FILE_FORMAT=(TYPE=parquet);
```

### Example 3: Set Deduplicate Label

```sql
CREATE TABLE t1(a Int, b bool);
INSERT /*+ SET_VAR(deduplicate_label='databend') */ INTO t1 (a, b) VALUES(1, false);
SELECT * FROM t1;

a|b|
-+-+
1|0|

UPDATE /*+ SET_VAR(deduplicate_label='databend') */ t1 SET a = 20 WHERE b = false;
SELECT * FROM t1;

a|b|
-+-+
1|0|

REPLACE /*+ SET_VAR(deduplicate_label='databend') */ INTO t1 on(a,b) VALUES(40, false);
SELECT * FROM t1;

a|b|
-+-+
1|0|
```