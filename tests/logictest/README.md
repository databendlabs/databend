
# Sqllogic test

The database return right with different handlers, for example MySQL and http

# Usage

## Prepare
Change to the scripts dir:
```shell
cd tests/logictest/
```

Make sure python3 is installed.

You can use [Poetry](https://github.com/python-poetry/poetry) to install dependency, dependency see tests/pyproject.toml

If you are familiar with `pip`, you can install dependency with:
```shell
pip install -r requirements.txt
```

## Need to know
1. Cases from **tests/suites/0_stateless/**  to  **tests/logictest/suites/gen/**
2. If a case file already exists in gen/, gen_suites will ignore it.
3. Regenerate：delete case file in gen/ and run gen_suites.py

## Generate sqllogic test cases from Stateless Test
1. python3 gen_suites.py

## Usage
You can simply run all tests with:
```shell
python main.py
```

Get help with:
```shell
python main.py -h
```

Useful arguments:
1. --run-dir ydb  will only run the suites in dir ./suites/ydb/
2. --skip-dir ydb  will skip the suites in dir ./suites/ydb
3. --suites other_dir  wiil use suites file in dir ./other_dir
4. Run files by pattern string like: python main.py "03_0001"

## Docker

### Build image

docker build -t sqllogic/test:latest .

### Run with docker

1. Image release: datafuselabs/sqllogictest:latest
2. Set envs
- SKIP_TEST_FILES (skip test case, set file name here split by `,` )
- QUERY_MYSQL_HANDLER_HOST
- QUERY_MYSQL_HANDLER_PORT
- QUERY_HTTP_HANDLER_HOST
- QUERY_HTTP_HANDLER_PORT
- MYSQL_DATABASE
- MYSQL_USER
- ADDITIONAL_HEADERS (for security scenario)
3. docker run --name logictest --rm --network host datafuselabs/sqllogictest:latest

## How to write logic test

Fast start, you can follow this demo: https://github.com/datafuselabs/databend/blob/main/tests/logictest/suites/select_0

Runner supported: mysql handler, http handler, clickhouse handler.

- ok
  - Returns no error, don't care about the result
- error
  - Returns with error and expected error message, usually with an error code, but also with a message string; the way to determine whether the specified string is in the returned message
- query
  - Return result and check the result with expected, follow by query_type and query_label
  - query_type is a char represent a column in result, multi char means multi column
    - B Boolean
    - T text
    - F floating point
    - I integer
    - R regex
  - query_label If different runner return inconsistency, you can write like this(suppose that mysql handler is get different result)

This is a query demo(query_label is optional):

```
statement query III label(mysql)
select number, number + 1, number + 999 from numbers(10);

----
     0     1   999
     1     2  1000
     2     3  1001
     3     4  1002
     4     5  1003
     5     6  1004
     6     7  1005
     7     8  1006
     8     9  1007
     9    10  1008.0

----  mysql
     0     1   999
     1     2  1000
     2     3  1001
     3     4  1002
     4     5  1003
     5     6  1004
     6     7  1005
     7     8  1006
     8     9  1007
     9    10  1008
```

## How to use regex in logic test

1. Regular expressions are implemented in regex_type.py. Additions can be made as needed but modifications require particular care.
```
regex_type_map = {
    "ANYTHING": ".*",
    "DATE": "\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d[.]\d\d\d [+-]\d\d\d\d",
}
```

2. A demo of regex likes:
```
statement query TTTRRTIIII
SELECT * FROM system.tables WHERE database='db1';

----
db1 t1 FUSE $ANYTHING $DATE NULL	0	0	0	0
```

### limitation

The space is used to split results into columns, space in regex expression should specify the number, do not use `( )+` or `( )*`. Do not contain space in the expected result of regex expression: ANYTHING, it should be written as a single column.

## Write logic test tips

1. skipif  help you skip test of given handler
```
skipif clickhouse
statement query I
select 1;

----
1
```

2. onlyif help you run test only by given handler
```
onlyif mysql
statement query I
select 1;

----
1
```

3. if some test has a flaky failure, and you want to ignore it, simply add skipped before statement query. (Remove it after the problem is solved)
```
statement query skipped I
select 1;

----
1
```

### Acknowledgement
- *tips* If you do not care about results, use statement ok instead of statement query
- *tips* Add ORDER BY to ensure that the order of returned results is always consistent
- *warning* A statement query need results, and even if you want to skip a case, you still need to keep the results in the test content

## Tools

complete.py can auto-complete test file for you, It does as follow steps:

1. Get SQLs from source-file, whether an SQL file or logictest file.
2. Execute SQL one by one, if SQL fetches results, add statement query; if SQL fetches nothing, add statement ok; if SQL gets an error, add statement error.

### Usage

- Pre-run, you need to start databend server or MySQL server
- Use `./complete.py --source-file="xxx.sql" --dest-file="my-gen"`  for SQL files(suffix name must be like *.sql)
- Use `./complete.py --source-file="xxx.test" --fest-file="my-gen"` for logictest files(suffix name not like *.sql, maybe like *.test)
- Use `--enable-auto-cleanup` to add `drop table if exists xxx` or `drop database if exists xxx` at the beginning of the test file
- If you want to see what SQLs get from source-file, add `--show-sql`
- Use the command line to specify host, user, port, password and database. Details in `./complete.py -h`

### Acknowledgement

- *tips* You can use MYSQL syntax to auto-complete the test suite, but make sure you know all grammar differences.
- *tips* MYSQL return bool as 1 and 0, this tool makes it as `int(I)` in query type option.
- *warning* No multi handlers use in the auto-complete tool(MYSQL only), if handlers return a difference, manual fix it, please.

# Learn More

RFC: https://github.com/datafuselabs/databend/blob/main/docs/doc/60-contributing/03-rfcs/20220425-new_sql_logic_test_framework.md
Migration discussion: https://github.com/datafuselabs/databend/discussions/5838