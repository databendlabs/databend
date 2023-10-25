---
title: "Leveraging Randomized Testing with SQLsmith to Uncover Bugs"
date: 2023-10-24
slug: 2023-10-24-sqlsmith-hero
cover_url: 'sqlsmith-hero.png'
image: 'sqlsmith-hero.png'
tags: ["fuzz testing", "SQLsmith", "test"]
description: "Discover bugs before your users do! Learn how we implement our SQLsmith, a randomized SQL test generator, to find bugs in Databend."
authors:
  - name: b41sh
    url: https://github.com/b41sh
    image_url: https://github.com/b41sh.png
---

# Leveraging Randomized Testing with SQLsmith to Uncover Bugs

Testing plays a crucial role in the development and maintenance of database systems. It helps verify functionality correctness and proactively identify potential issues to maintain performance and stability. Databend’s CI already supports these test types:

- **Unit Test**: A unit test validates the functionality of minimum testable code units like functions and modules, and ensure they execute successfully and return the expected results.
- **SQL Logic Test**: A SQL logic test verifies SQL syntax and logic correctness using test cases, covering various scenarios.
- **Performance Test**: A performance test validates performance impact of new features and optimizations. This helps prevent performance regressions.

While these tests ensure functional correctness and stability during rapid development, they have limitations. Handwritten SQL queries are often simplistic and lack coverage of complex real-world scenarios, edge cases, and exceptions.

## Introducing SQLsmith

[SQLsmith](https://github.com/anse1/sqlsmith) is a randomized SQL query generator that produces a high volume of diverse test cases to simulate real-world variability. Compared to other testing approaches, SQLsmith improves test coverage to uncover more potential issues and bugs.

## Implementing SQLsmith for Databend

The original SQLsmith for PostgreSQL fuzz testing was inspired by [Csmith](https://github.com/csmith-project/csmith). Several well-known open-source databases have adapted SQLsmith for their own use, such as [CockroachDB](https://github.com/cockroachdb/cockroach/blob/master/pkg/workload/sqlsmith/sqlsmith.go), [TiDB](https://github.com/PingCAP-QE/go-sqlsmith), and [RisingWave](https://github.com/risingwavelabs/risingwave/tree/main/src/tests/sqlsmith). 

These open source SQLsmiths use diverse languages (C++, Go, Rust) with different syntax support suitable for their domains. We couldn't directly use them and had to build our own SQLsmith in Rust to fully support Databend's syntax and features.

SQLsmith has three main components:

- **SQL Generator**: Generates abstract syntax trees (ASTs) of various types
- **SQL Reducer**: Simplifies complex SQLs to pinpoint bugs
- **Runner**: Executes SQLs and records errors 

## SQL Generator

The SQL Generator randomly generates ASTs including:

- **Data Types**: Basic types, Nested types like `Array`, `Map`, `Tuple` etc.
- **DDLs**: `CREATE`, `ALTER`, `DROP TABLE` etc. to create and modify test tables.
- **DMLs**: `INSERT`, `UPDATE`, `DELETE`, `MERGE` etc. to populate test data.
- **Queries**: `WITH`, `SELECT`, `JOIN`, `SubQuery`, `ORDER BY` etc. `WITH` and `SubQuery` can generate complex queries.
- **Expressions**: `Column`, `Literal`, `Scalar Function` and `Aggregate Function` etc. Expressions can be nested.

By randomly generating AST components, it covers all possible SQL syntax. Controlled recursion also creates complex nested SQLs to find obscure bugs. But depth is capped to avoid unexecutable SQLs.

## SQL Reducer

Since generated SQLs can be very complex with the bug trigger buried, simplifying the original SQL makes bug isolation easier.

The SQL Reducer iteratively removes AST components like `WITH`, `SubQuery`, `Expression` etc. If the simplified SQL continues to trigger the bug, it is adopted; otherwise, the original SQL is preserved. In the end, it generates the most concise SQL to reproduce the bug by systematically examining all AST components.

## Runner 

The SQL Runner executes test SQL queries using Databend, and in case of failure, it proceeds with the following error-handling steps:

- Check for expected errors like syntax, semantics etc.
- Verify if it's a known issue or unimplemented feature.
- Call the SQL Reducer to generate the smallest reproducible SQL.
- Log errors and simplified SQLs.

The Runner is now integrated into Databend's CI, running with each release to log errors for further analysis.

## Impact

After a month of running, SQLsmith has uncovered [50+ bugs](https://github.com/datafuselabs/databend/issues?q=is%3Aissue+label%3Afound-by-sqlsmith) in Databend, including:
  
- Internal logic errors (17 bugs)
- Invalid function/expression checks (12 bugs)  
- Missing semantic checks (9 bugs)
- Improper `unwrap`/`unreachable` handling (7 bugs) 
- Parser failures (3 bugs)
- Failed casts across types (4 bugs)
- Parquet I/O errors (1 bug)

## Key Lessons Learned

Analyzing the uncovered bugs led to some key learnings to avoid common pitfalls:

1. Thoroughly validate function parameters and edge cases. Main cases include:
   - For `String` parameters, check for empty strings.
   - For `Int` parameters, check for large number values.
   - When supporting any parameter type, consider unusual types like `Null`, `EmptyArray`, `Bitmap` etc.
   - When only supporting specific types, see if other types can auto-convert or check and return errors early.
2. Use unwrap judiciously. Explicitly handle `Result` and `Option` instead of assuming success.
3. Understand SQL semantic rules during development e.g. constraints around `GROUP BY`, `ORDER BY` etc. Perform semantic checks in the Binder phase to prevent runtime errors.
4. Add more unit test cases for critical modules to prevent bugs from internal logic errors.

## What's Next

SQLsmith helps enhance Databend's stability and reliability by uncovering hidden issues. Upcoming improvements include:

- Add support for more SQL features like `UNION`, **Computed Column** etc.
- Introduce more configurations like expression nesting depths, query complexity.
- Improve SQL Reducer.
- Optimize query executions and result analysis.
