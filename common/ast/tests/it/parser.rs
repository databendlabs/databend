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

use common_ast::parser::error::pretty_print_error;
use common_ast::parser::expr::*;
use common_ast::parser::parse_sql;
use common_ast::parser::query::*;
use common_ast::parser::statement::*;
use common_ast::parser::token::*;
use common_ast::parser::tokenize_sql;
use common_exception::Result;
use goldenfile::Mint;
use nom::Parser;

macro_rules! test_parse {
    ($file:expr, $parser:expr, $source:expr $(,)*) => {
        let tokens = Tokenizer::new($source).collect::<Result<Vec<_>>>().unwrap();
        match $parser.parse(&(tokens)) {
            Ok((i, output)) if i[0].kind == TokenKind::EOI => {
                writeln!($file, "---------- Input ----------").unwrap();
                writeln!($file, "{}", $source).unwrap();
                writeln!($file, "---------- Output ---------").unwrap();
                writeln!($file, "{}", output).unwrap();
                writeln!($file, "---------- AST ------------").unwrap();
                writeln!($file, "{:#?}", output).unwrap();
                writeln!($file, "\n").unwrap();
            }
            Ok((i, output)) => {
                writeln!($file, "---------- Input ----------").unwrap();
                writeln!($file, "{}", $source).unwrap();
                writeln!($file, "---------- Output ---------").unwrap();
                writeln!($file, "{}", output).unwrap();
                writeln!($file, "---------- AST ------------").unwrap();
                writeln!($file, "{:#?}", output).unwrap();
                writeln!($file, "---------- REST -----------").unwrap();
                writeln!($file, "{}", &$source[i[0].span.start..]).unwrap();
                writeln!($file, "\n").unwrap();
            }
            Err(nom::Err::Error(err) | nom::Err::Failure(err)) => {
                let report = pretty_print_error($source, err.to_labels())
                    .trim_end()
                    .to_string();
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
        r#"show tables;"#,
        r#"show processlist;"#,
        r#"show create table a.b;"#,
        r#"explain analyze select a from b;"#,
        r#"describe a;"#,
        r#"create table if not exists a.b (c integer not null default 1, b varchar);"#,
        r#"create table if not exists a.b (c integer default 1 not null, b varchar);"#,
        r#"create table a.b like c.d;"#,
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
        r#"select distinct a, count(*) from t where a = 1 and b - 1 < a group by a having a = 1;"#,
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
        let stmts = parse_sql(&tokens).unwrap();
        assert_eq!(stmts.len(), 1);
        writeln!(file, "---------- Input ----------").unwrap();
        writeln!(file, "{}", case).unwrap();
        writeln!(file, "---------- Output ---------").unwrap();
        writeln!(file, "{}", stmts[0]).unwrap();
        writeln!(file, "---------- AST ------------").unwrap();
        writeln!(file, "{:#?}", stmts[0]).unwrap();
        writeln!(file, "\n").unwrap();
    }
}

#[test]
fn test_statement_error() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("statement-error.txt").unwrap();

    let cases = &[
        r#"create table a.b (c integer not null 1, b varchar(10))"#,
        r#"create table a (c varchar(10))"#,
        r#"create table a (c varch)"#,
        r#"drop table if a.b"#,
        r#"truncate table a.b.c.d"#,
        r#"truncate a"#,
        r#"drop a"#,
        r#"insert into t format"#,
    ];

    for case in cases {
        test_parse!(file, statement, case);
    }
}

#[test]
fn test_query() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("query.txt").unwrap();
    let cases = &[
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
        r#"select * from customer inner join orders on a = b limit 1"#,
        r#"select * from customer natural full join orders"#,
        r#"select * from customer natural join orders left outer join detail using (id)"#,
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
        "select * from customer join where a = b",
        "select * from join customer",
        "select * from customer natural inner join orders on a = b",
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
        r#"1 + a * c.d"#,
        r#"col1 not between 1 and 2"#,
        r#"sum(col1)"#,
        r#"rand()"#,
        r#"rand(distinct)"#,
        r#"CAST(col1 AS BIGINT UNSIGNED)"#,
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
        r#"G.E.B IS NOT NULL AND
            col1 NOT BETWEEN col2 AND
                AND 1 + col3 DIV sum(col4)"#,
    ];

    for case in cases {
        test_parse!(file, expr, case);
    }
}
