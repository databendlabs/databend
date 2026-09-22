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
Run tests by glob pattern.

```shell
databend-sqllogictests --run 'tests/sqllogictests/suites/base/**/*.test'
```
or
```shell
databend-sqllogictests --run '**/suites/base/**/*'
```


---
Run a small set of files or directories with multiple glob patterns.
```shell
databend-sqllogictests --run 'tests/sqllogictests/suites/base/00_dummy/*.test,tests/sqllogictests/suites/base/03_common/*.test'
```
---
Skip part of the matched files.
```shell
databend-sqllogictests --run 'tests/sqllogictests/suites/base/**/*.test' --skip 'tests/sqllogictests/suites/base/01_system'
```
---
Run one or more complete suites by paths relative to `--suites`.
```shell
databend-sqllogictests --run-suite query,dictionaries
```
 It cannot be
combined with `--run`, `--skip`, `--run_dir`, `--run_file`, `--skip_dir`, or
`--skip_file`. Use `--run_dir` for nested directory-name selection.
---
Run tests under a specific file name found under `--suites`.
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

### Suite hooks

A suite is a direct child directory of `--suites`. A suite can contain one
optional top-level `hook.toml`; hook files are not discovered recursively.
`--run` and `--run-suite` only filter test files and do not change suite
ownership. Files selected outside `--suites` do not run a hook.

```toml
name = "TPC-H"
prepare = ["nox", "-f", "tests/nox/noxfile.py", "-s", "sqllogic_hook", "--", "prepare", "tpch"]
```

`name` is only a display label. `prepare` is required and `cleanup` is optional.
The runner executes only declared phases: `prepare` before all selected handlers
and `cleanup`, when present, after them in reverse suite order. It does not
assume that the commands use nox; any executable argv is valid. If preparation
fails, cleanup is attempted for hooks that declare it, including the failing
hook, which may have partially prepared resources. Cleanup is also attempted
when SQL tests fail.

Business-specific preparation for the built-in TPCH, TPCDS, Stage, native UDF,
and Dictionaries suites is implemented under `tests/nox/sqllogic/` and requires
`nox==2025.5.1`. SQL hooks use the pinned Databend Python driver rather
than the `bendsql` CLI; TPCH and TPCDS data generation uses the pinned DuckDB
Python package. Generated CSV data is cached under `tests/nox/cache/` and reused
when every expected table CSV exists and is non-empty; otherwise it is rebuilt.
The Dictionaries suite is a top-level suite and can be selected with
`--run-suite dictionaries`.

### Parallel
If you want to run test files in parallel, please add the following args:
```shell
databend-sqllogictests --enable_sandbox --parallel <number>
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

### Run a file again with non-default settings

Many bugs only show up on a setting-gated execution path (or on its legacy fallback) and
surface the day the default is flipped. To give such a path the same coverage as the default
path without duplicating the file, add a directive comment anywhere in the test file:

```
# run-with-settings: enable_fixed_rows_sort=0
# run-with-settings: enable_experimental_new_join=0, join_spilling_memory_ratio=0
```

Every directive line adds one extra pass over the whole file. The runner always executes the
file once with default settings, then once more per directive, applying `SET key = value` to
every new connection of that pass (after the sandbox is initialised, before the first record).
A `SET` inside the file still wins over the directive; an `UNSET` inside the file falls back to
the server default, not to the directive value.

Only use it for settings that switch the implementation path without changing the observable
result, otherwise the expected output can not be shared between passes. A malformed directive
fails the run instead of silently dropping the pass. Failures are reported with the pass name,
e.g. `foo.test [settings: enable_fixed_rows_sort=0]`.

Files still run in parallel, but the passes of one file run sequentially, each in its own
sandbox tenant. Sequential execution matters because not all state is tenant-scoped (e.g. the
storage path of an internal stage is `stage/internal/<name>`), so concurrent passes of the same
file could otherwise pollute each other. For the same reason a file that writes to an internal
stage should drop it at the end, so the next pass starts clean.
