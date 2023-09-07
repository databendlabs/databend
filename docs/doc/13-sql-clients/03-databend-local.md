---
title: databend-local
sidebar_label: databend-local
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.97"/>

databend-local is a simplified version of Databend for easy SQL interaction and testing directly from the command line, without the need for a full Databend deployment. It's perfect for developers and testers looking for a lightweight, hassle-free way to explore Databend features.

:::note non-production use only
databend-local runs a temporary databend-query process. Data storage is in a temporary directory, and all resources, including data, are deleted when the process ends. Be cautious to prevent unintended data loss.
:::

### Setting up databend-local

1. Download the installation package suitable for your platform from the [Download](https://databend.rs/download) page, and extract the **databend-query** binary located in the **bin** folder from the installation package.

2. Add the path to the **databend-query** binary to your PATH environment variable. For example, if your **databend-query** binary is located in the folder */Users/eric/Downloads/data*, set the PATH environment variable as follows:

```bash
macdeMacBook-Pro:rsdoc eric$ export PATH=/Users/eric/Downloads/data:$PATH
```

3. Create an alias called "databend-local" for the "databend-query local" command:

```bash
macdeMacBook-Pro:rsdoc eric$ alias databend-local="databend-query local"
```

4. Run databend-local:

```bash
macdeMacBook-Pro:rsdoc eric$ databend-local
Welcome to Databend, version v1.2.100-nightly-29d6bf3217(rust-1.72.0-nightly-2023-09-05T16:14:14.152454000Z).

databend-local:) 
```

To exit databend-local, simple type "exit":

```bash
databend-local:) exit
Bye~
macdeMacBook-Pro:rsdoc eric$
```

To view available arguments for databend-local:

```bash
macdeMacBook-Pro:rsdoc eric$ databend-local --help
Usage: databend-query local [OPTIONS]

Options:
  -q, --query <QUERY>                  [default: ]
      --output-format <OUTPUT_FORMAT>  [default: ]
  -h, --help                           Print help
```

| Argument            | Description                                          |
|---------------------|------------------------------------------------------|
| -q, --query         | Specifies the query to be executed.                 |
| --output-format     | Determines the file format for saving query results. |
| -h, --help          | Displays usage instructions.                          |

### Usage Examples

The following examples shed light on how to use databend-local.

#### Example 1: Querying from Command-Line

```bash
macdeMacBook-Pro:rsdoc eric$ databend-local
Welcome to Databend, version v1.2.100-nightly-29d6bf3217(rust-1.72.0-nightly-2023-09-05T16:14:14.152454000Z).

databend-local:) select max(a) from range(1,1000) t(a);
┌────────────┐
│   max(a)   │
│ Int64 NULL │
├────────────┤
│ 999        │
└────────────┘
1 row result in 0.013 sec. Processed 999 rows, 999 B (76.89 thousand rows/s, 600.67 KiB/s)
```

#### Example 2: Saving Results to a File

This example demonstrates how to create a Parquet file in a single command.

```bash
databend-local --query "select number, number + 1 as b from numbers(10)" --output-format parquet > /tmp/a.parquet
```

#### Example 3: Analyzing Data using Shell Pipe Mode

This example demonstrates the use of shell pipe mode to analyze data. The $STDIN macro interprets stdin as a temporary stage table.

```bash
echo '3,4' | databend-local -q "select \$1 a, \$2 b  from \$STDIN  (file_format => 'csv') " --output-format table

SELECT $1 AS a, $2 AS b FROM 'fs:///dev/fd/0' (FILE_FORMAT => 'csv')

┌─────────────────┐
│    a   │    b   │
│ String │ String │
├────────┼────────┤
│ '3'    │ '4'    │
└─────────────────┘
```

#### Example 4: Reading Staged Files

This example demonstrates how to read data from staged files.

```bash
databend-local --query "select count() from 'fs:///tmp/a.parquet'  (file_format => 'parquet')"

10
```

#### Example 5: Analyzing System Processes

This example is about analyzing system processes to find memory usage per user.

```bash
ps aux | tail -n +2 | awk '{ printf("%s\t%s\n", $1, $4) }' | databend-local -q "select  \$1 as user,  sum(\$2::double) as memory  from \$STDIN  (file_format => 'tsv')  group by user  "

_fpsd   0.0
_hidd   0.0
_nearbyd        0.1
_timed  0.0
_netbios        0.0
_trustd 0.1
root    5.899999999999998
_biome  0.1
...
```

#### Example 6: Data Transformation

This example demonstrates data transformation from one format to another, supporting formats CSV, TSV, Parquet, and NDJSON.

```bash
databend-local -q 'select rand() as a, rand() as b from numbers(100)' > /tmp/a.tsv

cat /tmp/a.tsv | databend-local -q "select \$1 a, \$2 b  from \$STDIN  (file_format => 'tsv') " --output-format parquet > /tmp/a.parquet
```