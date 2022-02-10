// Copyright 2021 Datafuse Labs.
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

use common_ast::parser::ast::*;
use common_ast::parser::*;
use pretty_assertions::assert_eq;

#[test]
fn test_parse_with_sqlparser() {
    let parser = Parser {};
    let sqls = vec![
        "select distinct a, count(*) from t where a = 1 and b - 1 < a group by a having a = 1;",
        "select * from a, b, c;",
        "select * from a join b on a.a = b.a;",
        "select * from a left outer join b on a.a = b.a;",
        "select * from a right outer join b on a.a = b.a;",
        "select * from a full outer join b on a.a = b.a;",
        "select * from a inner join b on a.a = b.a;",
        "select * from a left outer join b using(a);",
        "select * from a right outer join b using(a);",
        "select * from a full outer join b using(a);",
        "select * from a inner join b using(a);",
    ];
    let stmts: Vec<Statement> = sqls
        .into_iter()
        .flat_map(|sql| {
            parser
                .parse_with_sqlparser(sql)
                .map_err(|e| e.add_message(format!("SQL: {}", sql.to_owned())))
                .unwrap()
        })
        .collect();
    let expected = vec![
        r#"SELECT DISTINCT a, COUNT(*) FROM t WHERE a = 1 AND b - 1 < a GROUP BY a HAVING a = 1"#,
        r#"SELECT * FROM a CROSS JOIN b CROSS JOIN c"#,
        r#"SELECT * FROM a INNER JOIN b ON a.a = b.a"#,
        r#"SELECT * FROM a LEFT OUTER JOIN b ON a.a = b.a"#,
        r#"SELECT * FROM a RIGHT OUTER JOIN b ON a.a = b.a"#,
        r#"SELECT * FROM a FULL OUTER JOIN b ON a.a = b.a"#,
        r#"SELECT * FROM a INNER JOIN b ON a.a = b.a"#,
        r#"SELECT * FROM a LEFT OUTER JOIN b USING(a)"#,
        r#"SELECT * FROM a RIGHT OUTER JOIN b USING(a)"#,
        r#"SELECT * FROM a FULL OUTER JOIN b USING(a)"#,
        r#"SELECT * FROM a INNER JOIN b USING(a)"#,
    ];
    for (stmt, expect) in stmts.iter().zip(expected) {
        assert_eq!(format!("{}", stmt), expect);
    }
}

#[test]
fn test_parse_with_ddl() {
    let parser = Parser {};
    let sqls = vec![
        "truncate table test",
        "truncate table test_db.test",
        "DROP table table1",
        "DROP table IF EXISTS table1",
        "CREATE TABLE t(c1 int null, c2 bigint null, c3 varchar(255) null)",
        "CREATE TABLE t(c1 int not null, c2 bigint not null, c3 varchar(255) not null)",
        "CREATE TABLE t(c1 int default 1)",
    ];
    let stmts: Vec<Statement> = sqls
        .into_iter()
        .flat_map(|sql| {
            parser
                .parse_with_sqlparser(sql)
                .map_err(|e| e.add_message(format!("SQL: {}", sql.to_owned())))
                .unwrap()
        })
        .collect();
    let expected = vec![
        r#"TRUNCATE TABLE test"#,
        r#"TRUNCATE TABLE test_db.test"#,
        r#"DROP TABLE table1"#,
        r#"DROP TABLE IF EXISTS table1"#,
        r#"CREATE TABLE t (c1 INTEGER NULL, c2 BIGINT NULL, c3 VARCHAR(255) NULL)"#,
        r#"CREATE TABLE t (c1 INTEGER NOT NULL, c2 BIGINT NOT NULL, c3 VARCHAR(255) NOT NULL)"#,
        r#"CREATE TABLE t (c1 INTEGER NULL DEFAULT 1)"#,
    ];
    for (stmt, expect) in stmts.iter().zip(expected) {
        assert_eq!(format!("{}", stmt), expect);
    }
}
