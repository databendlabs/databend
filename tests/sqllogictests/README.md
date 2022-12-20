### Overview
This is Databend's [sqllogictest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) implementation. It uses [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) to parse test files and run test cases.

### Usage
You can directly run the following commands under databend directory

---
Run all tests under the three handlers(mysql, http, clickhouse) in turn.
```shell
cargo run -p sqllogictests
```
---
Run all tests with specific handler.
```shell
cargo run -p sqllogictests -- --handlers <handler_name>
```
---
Run tests under specific directory.

```shell
cargo run -p sqllogictests -- --run_dir <dir_name>
```
---
Run tests under specific file. This is the most commonly used command because users do not need to run all tests at a time and only need to run their newly added test files or test files with changes
```shell
cargo run -p sqllogictests -- --run_file <file_name>
```
---
For more information, run help command:
```shell
cargo run -p sqllogictests -- --help
```

### sqllogictest
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

