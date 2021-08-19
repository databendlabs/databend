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
mod tests {
    use common_exception::Result;
    use common_planners::DatabaseEngineType;
    use common_planners::TableEngineType;
    use sqlparser::ast::*;

    use crate::sql::sql_statement::DfDropDatabase;
    use crate::sql::sql_statement::DfUseDatabase;
    use crate::sql::*;

    fn expect_parse_ok(sql: &str, expected: DfStatement) -> Result<()> {
        let (statements, _) = DfParser::parse_sql(sql)?;
        assert_eq!(
            statements.len(),
            1,
            "Expected to parse exactly one statement"
        );
        assert_eq!(statements[0], expected);
        Ok(())
    }

    /// Parses sql and asserts that the expected error message was found
    fn expect_parse_error(sql: &str, expected_error: &str) -> Result<()> {
        match DfParser::parse_sql(sql) {
            Ok(statements) => {
                panic!(
                    "Expected parse error for '{}', but was successful: {:?}",
                    sql, statements
                );
            }
            Err(e) => {
                let error_message = e.to_string();
                assert!(
                    error_message.contains(expected_error),
                    "Expected error '{}' not found in actual error '{}'",
                    expected_error,
                    error_message
                );
            }
        }
        Ok(())
    }

    fn make_column_def(name: impl Into<String>, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name: Ident {
                value: name.into(),
                quote_style: None,
            },
            data_type,
            collation: None,
            options: vec![],
        }
    }

    #[test]
    fn create_database() -> Result<()> {
        {
            let sql = "CREATE DATABASE db1";
            let expected = DfStatement::CreateDatabase(DfCreateDatabase {
                if_not_exists: false,
                name: ObjectName(vec![Ident::new("db1")]),
                engine: DatabaseEngineType::Remote,
                options: vec![],
            });
            expect_parse_ok(sql, expected)?;
        }

        {
            let sql = "CREATE DATABASE IF NOT EXISTS db1";
            let expected = DfStatement::CreateDatabase(DfCreateDatabase {
                if_not_exists: true,
                name: ObjectName(vec![Ident::new("db1")]),
                engine: DatabaseEngineType::Remote,
                options: vec![],
            });
            expect_parse_ok(sql, expected)?;
        }

        {
            let sql = "CREATE DATABASE db1 ENGINE=Local";
            let expected = DfStatement::CreateDatabase(DfCreateDatabase {
                if_not_exists: false,
                name: ObjectName(vec![Ident::new("db1")]),
                engine: DatabaseEngineType::Local,
                options: vec![],
            });
            expect_parse_ok(sql, expected)?;
        }

        // Error cases: Invalid type
        {
            let sql = "CREATE DATABASE db1 ENGINE=XX";
            expect_parse_error(sql, "Expected Engine must one of Local, Remote, found: XX")?;
        }

        Ok(())
    }

    #[test]
    fn drop_database() -> Result<()> {
        {
            let sql = "DROP DATABASE db1";
            let expected = DfStatement::DropDatabase(DfDropDatabase {
                if_exists: false,
                name: ObjectName(vec![Ident::new("db1")]),
            });
            expect_parse_ok(sql, expected)?;
        }
        {
            let sql = "DROP DATABASE IF EXISTS db1";
            let expected = DfStatement::DropDatabase(DfDropDatabase {
                if_exists: true,
                name: ObjectName(vec![Ident::new("db1")]),
            });
            expect_parse_ok(sql, expected)?;
        }

        Ok(())
    }

    #[test]
    fn create_table() -> Result<()> {
        // positive case
        let sql = "CREATE TABLE t(c1 int) ENGINE = CSV location = '/data/33.csv' ";
        let expected = DfStatement::CreateTable(DfCreateTable {
            if_not_exists: false,
            name: ObjectName(vec![Ident::new("t")]),
            columns: vec![make_column_def("c1", DataType::Int)],
            engine: TableEngineType::Csv,
            options: vec![SqlOption {
                name: Ident::new("LOCATION".to_string()),
                value: Value::SingleQuotedString("/data/33.csv".into()),
            }],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: it is ok for parquet files not to have columns specified
        let sql = "CREATE TABLE t(c1 int, c2 bigint, c3 varchar(255) ) ENGINE = Parquet location = 'foo.parquet' ";
        let expected = DfStatement::CreateTable(DfCreateTable {
            if_not_exists: false,
            name: ObjectName(vec![Ident::new("t")]),
            columns: vec![
                make_column_def("c1", DataType::Int),
                make_column_def("c2", DataType::BigInt),
                make_column_def("c3", DataType::Varchar(Some(255))),
            ],
            engine: TableEngineType::Parquet,
            options: vec![SqlOption {
                name: Ident::new("LOCATION".to_string()),
                value: Value::SingleQuotedString("foo.parquet".into()),
            }],
        });
        expect_parse_ok(sql, expected)?;

        // Error cases: Invalid type
        let sql = "CREATE TABLE t(c1 int) ENGINE = XX location = 'foo.parquet' ";
        expect_parse_error(
            sql,
            "Expected Engine must be one of Parquet, JSONEachRow, Null, Memory or CSV, found: XX",
        )?;

        Ok(())
    }

    #[test]
    fn drop_table() -> Result<()> {
        {
            let sql = "DROP TABLE t1";
            let expected = DfStatement::DropTable(DfDropTable {
                if_exists: false,
                name: ObjectName(vec![Ident::new("t1")]),
            });
            expect_parse_ok(sql, expected)?;
        }
        {
            let sql = "DROP TABLE IF EXISTS t1";
            let expected = DfStatement::DropTable(DfDropTable {
                if_exists: true,
                name: ObjectName(vec![Ident::new("t1")]),
            });
            expect_parse_ok(sql, expected)?;
        }

        Ok(())
    }

    #[test]
    fn describe_table() -> Result<()> {
        {
            let sql = "DESCRIBE t1";
            let expected = DfStatement::DescribeTable(DfDescribeTable {
                name: ObjectName(vec![Ident::new("t1")]),
            });
            expect_parse_ok(sql, expected)?;
        }
        {
            let sql = "DESC t1";
            let expected = DfStatement::DescribeTable(DfDescribeTable {
                name: ObjectName(vec![Ident::new("t1")]),
            });
            expect_parse_ok(sql, expected)?;
        }

        Ok(())
    }

    #[test]
    fn show_queries() -> Result<()> {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        use sqlparser::tokenizer::Tokenizer;

        // positive case
        expect_parse_ok("SHOW TABLES", DfStatement::ShowTables(DfShowTables::All))?;
        expect_parse_ok("SHOW SETTINGS", DfStatement::ShowSettings(DfShowSettings))?;
        expect_parse_ok(
            "SHOW TABLES LIKE 'aaa'",
            DfStatement::ShowTables(DfShowTables::Like(Ident::with_quote('\'', "aaa"))),
        )?;

        expect_parse_ok(
            "SHOW TABLES --comments should not in sql case1",
            DfStatement::ShowTables(DfShowTables::All),
        )?;

        expect_parse_ok(
            "SHOW TABLES LIKE 'aaa' --comments should not in sql case2",
            DfStatement::ShowTables(DfShowTables::Like(Ident::with_quote('\'', "aaa"))),
        )?;

        let parse_sql_to_expr = |query_expr: &str| -> Expr {
            let dialect = GenericDialect {};
            let mut tokenizer = Tokenizer::new(&dialect, &query_expr);
            let tokens = tokenizer.tokenize().unwrap();
            let mut parser = Parser::new(tokens, &dialect);
            return parser.parse_expr().unwrap();
        };

        expect_parse_ok(
            "SHOW TABLES WHERE t LIKE 'aaa'",
            DfStatement::ShowTables(DfShowTables::Where(parse_sql_to_expr("t LIKE 'aaa'"))),
        )?;

        expect_parse_ok(
            "SHOW TABLES WHERE t LIKE 'aaa' AND t LIKE 'a%'",
            DfStatement::ShowTables(DfShowTables::Where(parse_sql_to_expr(
                "t LIKE 'aaa' AND t LIKE 'a%'",
            ))),
        )?;

        Ok(())
    }

    #[test]
    fn use_database_test() -> Result<()> {
        expect_parse_ok(
            "USe db1",
            DfStatement::UseDatabase(DfUseDatabase {
                name: ObjectName(vec![Ident::new("db1")]),
            }),
        )?;
        expect_parse_ok(
            "use db1",
            DfStatement::UseDatabase(DfUseDatabase {
                name: ObjectName(vec![Ident::new("db1")]),
            }),
        )?;

        Ok(())
    }

    #[test]
    fn truncate_table() -> Result<()> {
        {
            let sql = "TRUNCATE TABLE t1";
            let expected = DfStatement::TruncateTable(DfTruncateTable {
                name: ObjectName(vec![Ident::new("t1")]),
            });
            expect_parse_ok(sql, expected)?;
        }

        Ok(())
    }

    #[test]
    fn hint_test() -> Result<()> {
        {
            let comment = " { ErrorCode  1002 }";
            let expected = DfHint::create_from_comment(comment, "--");
            assert_eq!(expected.error_code, Some(1002));
        }

        {
            let comment = " { ErrorCode1002 }";
            let expected = DfHint::create_from_comment(comment, "--");
            assert_eq!(expected.error_code, None);
        }

        {
            let comment = " { ErrorCode 22}";
            let expected = DfHint::create_from_comment(comment, "--");
            assert_eq!(expected.error_code, Some(22));
        }

        {
            let comment = " { ErrorCode: 22}";
            let expected = DfHint::create_from_comment(comment, "--");
            assert_eq!(expected.error_code, None);
        }

        {
            let comment = " { Errorcode 22}";
            let expected = DfHint::create_from_comment(comment, "--");
            assert_eq!(expected.error_code, None);
        }

        Ok(())
    }
}
