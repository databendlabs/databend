// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Write;

use common_ast::parser::error::Backtrace;
use common_ast::parser::error::DisplayError as _;
use common_ast::parser::expr::*;
use common_ast::parser::parse_sql;
use common_ast::parser::query::*;
use common_ast::parser::token::*;
use common_ast::parser::tokenize_sql;
use common_ast::parser::util::Input;
use common_ast::rule;
use common_exception::Result;
use goldenfile::Mint;
use nom::Parser;

macro_rules! test_parse {
    ($file:expr, $parser:expr, $source:expr $(,)*) => {
        let tokens = Tokenizer::new($source).collect::<Result<Vec<_>>>().unwrap();
        let backtrace = Backtrace::new();
        let parser = $parser;
        let mut parser = rule! { #parser ~ &EOI };
        match parser.parse(Input(&tokens, &backtrace)) {
            Ok((i, (output, _))) => {
                assert_eq!(i[0].kind, TokenKind::EOI);
                writeln!($file, "---------- Input ----------").unwrap();
                writeln!($file, "{}", $source).unwrap();
                writeln!($file, "---------- Output ---------").unwrap();
                writeln!($file, "{}", output).unwrap();
                writeln!($file, "---------- AST ------------").unwrap();
                writeln!($file, "{:#?}", output).unwrap();
                writeln!($file, "\n").unwrap();
            }
            Err(nom::Err::Error(err) | nom::Err::Failure(err)) => {
                let report = err.display_error(()).trim_end().to_string();
                writeln!($file, "---------- Input ----------").unwrap();
                writeln!($file, "{}", $source).unwrap();
                writeln!($file, "---------- Output ---------").unwrap();
                writeln!($file, "{}", report).unwrap();
                writeln!($file, "\n").unwrap();
            }
            Err(nom::Err::Incomplete(_)) => unreachable!(),
        }
    };
}

#[test]
fn test_statement() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("statement.txt").unwrap();
    let cases = &[
        r#"show tables"#,
        r#"show processlist;"#,
        r#"show create table a.b;"#,
        r#"explain pipeline select a from b;"#,
        r#"describe a;"#,
        r#"describe a; describe b"#,
        r#"create table if not exists a.b (c integer not null default 1, b varchar);"#,
        r#"create table if not exists a.b (c integer default 1 not null, b varchar) as select * from t;"#,
        r#"create table a.b like c.d;"#,
        r#"create table t like t2 engine = memory;"#,
        r#"truncate table a;"#,
        r#"truncate table "a".b;"#,
        r#"drop table a;"#,
        r#"drop table if exists a."b";"#,
        r#"use "a";"#,
        r#"create database if not exists a;"#,
        r#"create table c(a DateTime null, b DateTime(3));"#,
        r#"truncate table test;"#,
        r#"truncate table test_db.test;"#,
        r#"DROP table table1;"#,
        r#"DROP table IF EXISTS table1;"#,
        r#"CREATE TABLE t(c1 int null, c2 bigint null, c3 varchar null);"#,
        r#"CREATE TABLE t(c1 int not null, c2 bigint not null, c3 varchar not null);"#,
        r#"CREATE TABLE t(c1 int default 1);"#,
        r#"DROP database if exists db1;"#,
        r#"select distinct a, count(*) from t where a = 1 and b - 1 < a group by a having a = 1;"#,
        r#"select * from t4;"#,
        r#"select * from aa.bb;"#,
        r#"select * from a, b, c;"#,
        r#"select * from a join b on a.a = b.a;"#,
        r#"select * from a left outer join b on a.a = b.a;"#,
        r#"select * from a right outer join b on a.a = b.a;"#,
        r#"select * from a full outer join b on a.a = b.a;"#,
        r#"select * from a inner join b on a.a = b.a;"#,
        r#"select * from a left outer join b using(a);"#,
        r#"select * from a right outer join b using(a);"#,
        r#"select * from a full outer join b using(a);"#,
        r#"select * from a inner join b using(a);"#,
        r#"insert into t (c1, c2) values (1, 2), (3, 4);"#,
        r#"insert into table t format json;"#,
        r#"insert into table t select * from t2;"#,
    ];

    for case in cases {
        let tokens = tokenize_sql(case).unwrap();
        let backtrace = Backtrace::new();
        let stmts = parse_sql(&tokens, &backtrace).unwrap();
        writeln!(file, "---------- Input ----------").unwrap();
        writeln!(file, "{}", case).unwrap();
        for stmt in stmts {
            writeln!(file, "---------- Output ---------").unwrap();
            writeln!(file, "{}", stmt).unwrap();
            writeln!(file, "---------- AST ------------").unwrap();
            writeln!(file, "{:#?}", stmt).unwrap();
            writeln!(file, "\n").unwrap();
        }
    }
}

// TODO(andylokandy): remove this test once the new optimizer has been being tested on suites
#[test]
fn test_statements_in_legacy_suites() {
    for entry in glob::glob("../../tests/suites/**/*.sql").unwrap() {
        let file_content = std::fs::read(entry.unwrap()).unwrap();
        let file_str = String::from_utf8_lossy(&file_content).into_owned();

        // Remove error cases
        let file_str = regex::Regex::new(".+ErrorCode.+\n")
            .unwrap()
            .replace_all(&file_str, "")
            .into_owned();

        // TODO(andylokandy): support all cases eventually
        // Remove currently unimplemented cases
        let file_str = regex::Regex::new(
            "(?i).*(SLAVE|MASTER|COMMIT|START|ROLLBACK|FIELDS|GRANT|COPY|ROLE|STAGE|ENGINES).*\n",
        )
        .unwrap()
        .replace_all(&file_str, "")
        .into_owned();

        let tokens = tokenize_sql(&file_str).unwrap();
        let backtrace = Backtrace::new();
        parse_sql(&tokens, &backtrace).expect(
            "Parser error should not exist in integration suites. \
            Please add parser error cases to `common/ast/tests/it/parser.rs`",
        );
    }
}

#[test]
fn test_statement_error() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("statement-error.txt").unwrap();

    let cases = &[
        r#"create table a.b (c integer not null 1, b float(10))"#,
        r#"create table a (c float(10))"#,
        r#"create table a (c varch)"#,
        r#"drop table if a.b"#,
        r#"truncate table a.b.c.d"#,
        r#"truncate a"#,
        r#"drop a"#,
        r#"insert into t format"#,
        r#"alter database system x rename to db"#,
    ];

    for case in cases {
        let tokens = tokenize_sql(case).unwrap();
        let backtrace = Backtrace::new();
        let err = parse_sql(&tokens, &backtrace).unwrap_err();
        writeln!(file, "---------- Input ----------").unwrap();
        writeln!(file, "{}", case).unwrap();
        writeln!(file, "---------- Output ---------").unwrap();
        writeln!(file, "{}", err.message()).unwrap();
    }
}

#[test]
fn test_query() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("query.txt").unwrap();
    let cases = &[
        r#"select * from customer inner join orders on a = b limit 1"#,
        r#"select * from customer inner join orders on a = b limit 2 offset 3"#,
        r#"select * from customer natural full join orders"#,
        r#"select * from customer natural join orders left outer join detail using (id)"#,
        r#"select c_count, count(*) as custdist, sum(c_acctbal) as totacctbal
            from customer, orders ODS,
                (
                    select
                        c_custkey,
                        count(o_orderkey)
                    from
                        customer left outer join orders on
                            c_custkey = o_custkey
                            and o_comment not like '%:1%:2%'
                    group by
                        c_custkey
                ) as c_orders
            group by c_count
            order by custdist desc, c_count asc, totacctbal
            limit 10, totacctbal"#,
    ];

    for case in cases {
        test_parse!(file, query, case);
    }
}

#[test]
fn test_query_error() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("query-error.txt").unwrap();
    let cases = &[
        r#"select * from customer join where a = b"#,
        r#"select * from join customer"#,
        r#"select * from t inner join t1"#,
        r#"select * from customer natural inner join orders on a = b"#,
        r#"select * order a"#,
        r#"select * order"#,
        r#"select number + 5 as a, cast(number as float(255))"#,
    ];

    for case in cases {
        test_parse!(file, query, case);
    }
}

#[test]
fn test_expr() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("expr.txt").unwrap();

    let cases = &[
        r#"a"#,
        r#"-1"#,
        r#"(1,)"#,
        r#"(1,2)"#,
        r#"(1,2,)"#,
        r#"typeof(1 + 2)"#,
        r#"- - + + - 1 + + - 2"#,
        r#"1 + a * c.d"#,
        r#"number % 2"#,
        r#"col1 not between 1 and 2"#,
        r#"sum(col1)"#,
        r#""random"()"#,
        r#"random(distinct)"#,
        r#"covar_samp(number, number)"#,
        r#"CAST(col1 AS BIGINT UNSIGNED)"#,
        r#"TRY_CAST(col1 AS BIGINT UNSIGNED)"#,
        r#"trim(leading 'abc' from 'def')"#,
        r#"extract(year from d)"#,
        r#"position('a' in str)"#,
        r#"substring(a from b for c)"#,
        r#"substring(a, b, c)"#,
        r#"col1::UInt8"#,
        r#"(arr[0]:a).b"#,
        r#"arr[4]["k"]"#,
        r#"a rlike '^11'"#,
        r#"G.E.B IS NOT NULL AND col1 not between col2 and (1 + col3) DIV sum(col4)"#,
        r#"sum(CASE WHEN n2.n_name = 'GERMANY' THEN ol_amount ELSE 0 END) / CASE WHEN sum(ol_amount) = 0 THEN 1 ELSE sum(ol_amount) END"#,
        r#"p_partkey = l_partkey
            AND p_brand = 'Brand#12'
            AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            AND l_quantity >= CAST (1 AS smallint) AND l_quantity <= CAST (1 + 10 AS smallint)
            AND p_size BETWEEN CAST (1 AS smallint) AND CAST (5 AS smallint)
            AND l_shipmode IN ('AIR', 'AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON'"#,
    ];

    for case in cases {
        test_parse!(file, expr, case);
    }
}

#[test]
fn test_expr_error() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("expr-error.txt").unwrap();

    let cases = &[
        r#"5 * (a and ) 1"#,
        r#"a + +"#,
        r#"CAST(col1 AS foo)"#,
        r#"CAST(col1)"#,
        r#"G.E.B IS NOT NULL AND
            col1 NOT BETWEEN col2 AND
                AND 1 + col3 DIV sum(col4)"#,
    ];

    for case in cases {
        test_parse!(file, expr, case);
    }
}
