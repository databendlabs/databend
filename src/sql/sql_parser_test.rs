// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod tests {
    use sqlparser::ast::{ColumnDef, DataType, Ident, ObjectName, SqlOption, Value};
    use sqlparser::parser::ParserError;

    use crate::sql::sql_parser::FuseCreateTable;
    use crate::sql::{DFParser, DFStatement, EngineType};

    fn expect_parse_ok(sql: &str, expected: DFStatement) -> Result<(), ParserError> {
        let statements = DFParser::parse_sql(sql)?;
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
        match DFParser::parse_sql(sql) {
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
        let expected = DFStatement::Create(FuseCreateTable {
            if_not_exists: false,
            name: ObjectName(vec![Ident::new("t")]),
            columns: vec![make_column_def("c1", DataType::Int)],
            engine: EngineType::Csv,
            table_properties: vec![SqlOption {
                name: Ident::new("LOCATION".to_string()),
                value: Value::SingleQuotedString("/data/33.csv".into()),
            }],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: it is ok for parquet files not to have columns specified
        let sql = "CREATE TABLE t(c1 int, c2 bigint, c3 varchar(255) ) ENGINE = Parquet location = 'foo.parquet' ";
        let expected = DFStatement::Create(FuseCreateTable {
            if_not_exists: false,
            name: ObjectName(vec![Ident::new("t")]),
            columns: vec![
                make_column_def("c1", DataType::Int),
                make_column_def("c2", DataType::BigInt),
                make_column_def("c3", DataType::Varchar(Some(255))),
            ],
            engine: EngineType::Parquet,
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
}
