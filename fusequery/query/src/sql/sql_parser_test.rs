// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod tests {
    use common_planners::TableEngineType;
    use sqlparser::ast::*;
    use sqlparser::parser::*;

    use crate::sql::*;

    fn expect_parse_ok(sql: &str, expected: DfStatement) -> Result<(), ParserError> {
        let statements = DfParser::parse_sql(sql)?;
        assert_eq!(
            statements.len(),
            1,
            "Expected to parse exactly one statement"
        );
        assert_eq!(statements[0], expected);
        Ok(())
    }

    /// Parses sql and asserts that the expected error message was found
    fn expect_parse_error(sql: &str, expected_error: &str) -> Result<(), ParserError> {
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
    fn create_table() -> Result<(), ParserError> {
        // positive case
        let sql = "CREATE TABLE t(c1 int) ENGINE = CSV location = '/data/33.csv' ";
        let expected = DfStatement::CreateTable(DfCreateTable {
            if_not_exists: false,
            name: ObjectName(vec![Ident::new("t")]),
            columns: vec![make_column_def("c1", DataType::Int)],
            engine: TableEngineType::Csv,
            table_properties: vec![SqlOption {
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
            table_properties: vec![SqlOption {
                name: Ident::new("LOCATION".to_string()),
                value: Value::SingleQuotedString("foo.parquet".into()),
            }],
        });
        expect_parse_ok(sql, expected)?;

        // Error cases: Invalid type
        let sql = "CREATE TABLE t(c1 int) ENGINE = XX location = 'foo.parquet' ";
        expect_parse_error(
            sql,
            "Expected one of Parquet, JSONEachRaw, Null or CSV, found: XX",
        )?;

        Ok(())
    }

    #[test]
    fn show_queries() -> Result<(), ParserError> {
        // positive case
        expect_parse_ok("SHOW TABLES", DfStatement::ShowTables(DfShowTables))?;
        expect_parse_ok("SHOW SETTINGS", DfStatement::ShowSettings(DfShowSettings))?;

        Ok(())
    }
}
