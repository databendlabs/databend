// Copyright 2020 Datafuse Labs.
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

#[cfg(test)]
mod test {
    use crate::sql::parser::*;

    #[test]
    fn test_parse_with_sqlparser() {
        use crate::sql::parser::ast::*;
        let parser = Parser {};
        let sqls = vec![
            "select distinct a, count(*) from t where a = 1 and b - 1 < a group by a having a = 1;",
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
            .map(|sql| {
                parser
                    .parse_with_sqlparser(sql)
                    .map_err(|e| e.add_message(format!("SQL: {}", sql.to_owned())))
                    .unwrap()
            })
            .flatten()
            .collect();
        let expected = vec![
            r#"SELECT DISTINCT a, count(*) FROM t WHERE a = 1 AND b - 1 < a GROUP BY a HAVING a = 1"#,
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
}
