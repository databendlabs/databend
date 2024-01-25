### Overview
This is Databend's [sqllogictest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) implementation. It uses [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) to parse test files and run test cases.

### Basic usage
Before running the following commands, you should generate **databend-sqllogictests** binary file.

---
Run all tests under the three handlers(mysql, http) in turn.
```shell
databend-sqllogictests
```
---
Run all tests with specific handler.
```shell
databend-sqllogictests --handlers <handler_name>
```
---
Run tests under specific directory.

```shell
databend-sqllogictests --run_dir <dir_name>
```
---
Run tests under specific file. This is the most commonly used command because users do not need to run all tests at a time and only need to run their newly added test files or test files with changes
```shell
databend-sqllogictests --run_file <file_name>
```
---
Auto complete test file which is very convenient. What you need to do is just a final check to see if the generated results meet expectations.
```
databend-sqllogictests --run_file <file_name> --complete
```
---
By default, sqllogictest will fail fast when a failed test is encountered. If you want to run the full test, even with a failed test, you can run the following command:
```
databend-sqllogictests --no-fail-fast
```
---
For more information, run help command:
```shell
databend-sqllogictests --help
```

### Parallel
If you want to run test files in parallel, please add the following args:
```shell
databend-sqllogictest --enable_sandbox --parallel <number>
```

When start databend query, please add `--internal-enable-sandbox-tenant` config.

### Sqllogictest
Most records are either a statement or a query. A statement is an SQL command that is to be evaluated but from which we do not expect to get results (other than success or failure). A statement might be a CREATE TABLE or an INSERT or an UPDATE or a DROP INDEX. A query is an SQL command from which we expect to receive results. The result set might be empty.

A statement record begins with one of the following two lines:
```
statement ok
statement error <error info>
```
The SQL command to be evaluated is found on the second and all subsequent liens of the record. Only a single SQL command is allowed per statement. The SQL should not have a semicolon or other terminator at the end.

A query record begins with a line of the following form:
```
# comments
query <type_string> <sort_mode> <label>
<sql_query>
----
<expected_result>
```
The SQL for the query is found on second an subsequent lines of the record up to first line of the form "----" or until the end of the record. Lines following the "----" are expected results of the query, one value per line. If the "----" and/or the results are omitted, then the query is expected to return an empty set.

For more information about arguments, such as <type_string>, <sort_mode>, <label> please refer to [sqllogictest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki).

### Aditional features

- sql with regexp pattern `\$RAND_(\d+)_(\d+)` will be replaced by a random number from the range.