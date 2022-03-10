---
title: system.settings
---


Contains information about session settings for current user.


```sql

mysql> SELECT * FROM system.settings;
+------------------------------------+-----------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------+
| name                               | value     | default_value | description                                                                                                                                |
+------------------------------------+-----------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------+
| storage_read_buffer_size           | 1048576   | 1048576       | The size of buffer in bytes for buffered reader of dal. By default, it is 1MB.                                                             |
| min_distributed_rows               | 100000000 | 100000000     | Minimum distributed read rows. In cluster mode, when read rows exceeds this value, the local table converted to distributed query.         |
| max_threads                        | 16        | 16            | The maximum number of threads to execute the request. By default, it is determined automatically.                                          |
| flight_client_timeout              | 60        | 60            | Max duration the flight client request is allowed to take in seconds. By default, it is 60 seconds                                         |
| parallel_read_threads              | 1         | 1             | The maximum number of parallelism for reading data. By default, it is 1.                                                                   |
| storage_occ_backoff_init_delay_ms  | 5         | 5             | The initial retry delay in millisecond. By default,  it is 5 ms.                                                                           |
| max_block_size                     | 10000     | 10000         | Maximum block size for reading                                                                                                             |
| min_distributed_bytes              | 524288000 | 524288000     | Minimum distributed read bytes. In cluster mode, when read bytes exceeds this value, the local table converted to distributed query.       |
| storage_occ_backoff_max_delay_ms   | 20000     | 20000         | The maximum  back off delay in millisecond, once the retry interval reaches this value, it stops increasing. By default, it is 20 seconds. |
| storage_occ_backoff_max_elapsed_ms | 120000    | 120000        | The maximum elapsed time after the occ starts, beyond which there will be no more retries. By default, it is 2 minutes                     |
+------------------------------------+-----------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------+
```

Examples：

E1： Change number of parallelism for reading data， This is good for performance.
```
set  parallel_read_threads=8;
```

E2: Limit the CPU usage of a query

```
set  max_threads = N;
```

E3： Change Read buffer size 2M

```
storage_read_buffer_size=2097152;
```
