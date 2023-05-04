---
title: SET_VAR
---

The SET_VAR is an Optimizer Hint that allows you to set a global-level variable for the duration of the query, which can be used in other parts of the query to control behavior. In addition to setting variables that affect the optimizer's execution plan, you can also use SET_VAR to set variables that affect other aspects of query processing, such as locking, sorting, or parallelism.

See also: [SET](01-set-global.md)

## Syntax

```sql
/*+ SET_VAR(key=value) SET_VAR(key=value) ... */
```



## Examples

The following example sets the `max_memory_usage` setting to `4 GB`:

```sql
SET max_memory_usage = 1024*1024*1024*4;
```

The following example sets the `max_threads` setting to `4`:

```sql
SET max_threads = 4;
```

The following example sets the `max_threads` setting to `4` and changes it to be a global-level setting:

```sql
SET GLOBAL max_threads = 4;
```