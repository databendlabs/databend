---
title: New SQL Logic Test Framework
description:
  New sql logic test framework design RFC.
---


## Background

Basically all robust database system needs to be tested in the following scope.

1. Reasonable high level of unit test coverage.
2. A large set of query logic tests.(**mainly discussed**)
3. Distributed system related behavior tests.
4. Performance tests (https://perf.databend.rs/)

Currently, our test framework is based on the following design.

We mainly adopted mysql binary client to test the query logic, and cover some basic test on driver compatibility part.

However, it has some shortages in current logic test which should be improved.

1. Comparing output from binary to a result file cannot be extended to other protocols. for example `http_handler` have json output format and we should have specified result to cover the case.
2. Currently, we test sql file cover multiple statements in the same time, and result file cannot show the result for each statement
3. Currently, we do not provide error handling for sql logic tests
4. We could not extend sql logic statement with sorting, retry and other logics.

## Detailed design

The test input is an extended version of sql logic test(https://www.sqlite.org/sqllogictest/)

The file is expressed in a domain specific language called test script. and it supports sql statements generate no output or statements intentionally has error

The statement spec could be categorized to following fields

`statement ok`: the sql statement is correct and the output is expected success.
  
for example: the following sql would be ok with no output
  
```text
statement ok
create database if not exists db1;
```

`statement error <error regex>`: the sql statement output is expected error.
  
for example: the following sql would be error with error message `table db1.tbl1 does not exist`
 
```text
statement error table db1.tbl1 does not exist
create table db1.tbl1 (id int);
```

`statement query <desired_query_schema_type> <options> <labels>`: the sql statement output is expected success with desired result.

`desired_query_schema_type` represent the schema type of the query result. which is documented in https://github.com/gregrahn/sqllogictest/blob/master/about.wiki#test-script-format

- `I` for integer
- `F` for floating point
- `R` for decimal
- `T` for text or variants (json, timestamps, etc.).
- `B` for boolean

`options` is a list of options for query, such as sort, retry etc.
`label` could allow query to match given suite label result at first which resolved the result compatibility issue

for example: the following sql would be ok with output table
  
```text
statement query III
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
     9    10  1008  
```

The follow sql configured match mysql label first then default label

```text
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