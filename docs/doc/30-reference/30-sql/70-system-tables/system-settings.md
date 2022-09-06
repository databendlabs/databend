---
title: system.settings
---

Contains information about databend system settings.

Query all settings can use [show settings](../40-show/show-settings.md)

## compression

Format compression. Commonly used in stream load scenarios, default values is None. Now support:

```
None
Auto
Gzip
Bz2
Brotli
Zstd
Deflate
RawDeflate
Lzo
Snappy
Xz
```

Examples:

```sql
set compression = 'Gzip';
```

## empty_as_default

Format empty_as_default. Commonly used in stream load scenarios. It indicates whether the imported text is allowed to have empty values, default value is 1.

Examples：

```sql
set empty_as_default = 1;
```

## enable_new_processor_framework

Enable new processor framework if value != 0. Default use new processor framework.

## enable_planner_v2

Enable planner v2 by setting this variable to 1. Default use planner v2.

## field_delimiter

Format field delimiter. Commonly used in stream load scenarios, default value:, .

Examples：

```sql
set field_delimiter = ',';
set field_delimiter = '|';
```

## flight_client_timeout

Max duration the flight client request is allowed to take in seconds. By default, it is 60 seconds.

Examples：

```sql
set flight_client_timeout = 30;
```

## group_by_two_level_threshold

The threshold of keys to open two-level aggregation, default value: 10000.

Examples：

```sql
set group_by_two_level_threshold = 10000;
```

## max_block_size

Maximum block size for reading, default value: 10000.

Examples:

```sql
set max_block_size = 10000;
```

## max_threads

The maximum number of threads to execute the request. By default, it is determined automatically. The value usually the same as the number of logical cpus.

Examples:

```sql
set max_threads = 8;
```

## quoted_ident_case_sensitive

Case sensitivity of quoted identifiers. This is related to [sql_dialect](#sql_dialect) default value: 1 (aka case-sensitive).

Examples:

```sql
-- set to 0

databend :) set quoted_ident_case_sensitive = 0;

databend :) create table "T"("A" int);

databend :) insert into "t"("a") values(1);

databend :) select t.a from "T";
+------+
| a    |
+------+
|    1 |
+------+

databend :) select t."a" from "T";
+------+
| a    |
+------+
|    1 |
+------+

databend :) select t.A from "T";
+------+
| A    |
+------+
|    1 |
+------+

-- set to 1

databend :) set quoted_ident_case_sensitive = 1; 
Query OK, 0 rows affected (0.03 sec)

databend :) set sql_dialect = 'MySQL';

databend :) create table t(a string);

databend :) insert into t values("a");

databend :) set sql_dialect = 'postgresql';

databend :) insert into t values("a");
ERROR 1105 (HY000): Code: 1065, displayText = error: 
  --> SQL:1:2
  |
1 | ("a")
  |  ^^^ column doesn't exist

```

## record_delimiter

Format record_delimiter. Commonly used in stream load scenarios, default value: "\n"

Examples:

```sql
set record_delimiter='\n';
```

## skip_header

Whether to skip the input header. Commonly used in stream load scenarios, default value: 0

If value greater than 0, that means the number of lines at the start of the file to skip.

Examples:

```sql
set skip_header=3;
```

## sql_dialect

SQL dialect, support "PostgreSQL" and "MySQL", default value: "PostgreSQL".

```sql
set sql_dialect='PostgreSQL';
```

## storage_read_buffer_size

The size of buffer in bytes for buffered reader of dal. By default, it is 1MB.

Examples:

```sql
set storage_read_buffer_size = 1048576;
```

## timezone

The server timezone, default value: "UTC".

Examples:

```sql
set timezone = 'Asia/Shanghai';
```

## unquoted_ident_case_sensitive

Case sensitivity of unquoted identifiers, default value: 0 (aka case-insensitive).

Examples:

```sql
databend :) create table t(a string);

databend :) set unquoted_ident_case_sensitive=0;

databend :) create table T(a string);
ERROR 1105 (HY000): Code: 2302, displayText = Table 't' already exists.

databend :) set unquoted_ident_case_sensitive=1;

databend :) create table T(a string);

databend :) insert into t values(1);

databend :) insert into T values(2);

databend :) select * from t;
+------+
| a    |
+------+
| 1    |
+------+

databend :) select * from T;
+------+
| a    |
+------+
| 2    |
+------+

```

## enable_async_insert

Whether the client open async insert mode, default value: 0.

## wait_for_async_insert          

Whether the client wait for the reply of async insert, default value: 1.

## wait_for_async_insert_timeout

The timeout in seconds for waiting for processing of async insert, default value: 100.
