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

use std::collections::HashMap;

use common_exception::Result;
use common_meta_types::AuthInfo;
use common_meta_types::AuthInfoArgs;
use common_meta_types::AuthType;
use common_meta_types::Compression;
use common_meta_types::Credentials;
use common_meta_types::FileFormat;
use common_meta_types::Format;
use common_meta_types::PasswordType;
use common_meta_types::StageParams;
use common_meta_types::UserIdentity;
use common_meta_types::UserPrivilegeSet;
use common_meta_types::UserPrivilegeType;
use common_planners::Optimization;
use databend_query::sql::statements::DfAlterUDF;
use databend_query::sql::statements::DfAlterUser;
use databend_query::sql::statements::DfCopy;
use databend_query::sql::statements::DfCreateDatabase;
use databend_query::sql::statements::DfCreateStage;
use databend_query::sql::statements::DfCreateTable;
use databend_query::sql::statements::DfCreateUDF;
use databend_query::sql::statements::DfCreateUser;
use databend_query::sql::statements::DfDescribeTable;
use databend_query::sql::statements::DfDropDatabase;
use databend_query::sql::statements::DfDropStage;
use databend_query::sql::statements::DfDropTable;
use databend_query::sql::statements::DfDropUDF;
use databend_query::sql::statements::DfDropUser;
use databend_query::sql::statements::DfGrantObject;
use databend_query::sql::statements::DfGrantStatement;
use databend_query::sql::statements::DfOptimizeTable;
use databend_query::sql::statements::DfQueryStatement;
use databend_query::sql::statements::DfRevokeStatement;
use databend_query::sql::statements::DfShowCreateDatabase;
use databend_query::sql::statements::DfShowCreateTable;
use databend_query::sql::statements::DfShowDatabases;
use databend_query::sql::statements::DfShowGrants;
use databend_query::sql::statements::DfShowTables;
use databend_query::sql::statements::DfShowUDF;
use databend_query::sql::statements::DfTruncateTable;
use databend_query::sql::statements::DfUseDatabase;
use databend_query::sql::statements::DfUseTenant;
use databend_query::sql::*;
use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Tokenizer;

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

fn expect_parse_err(sql: &str, expected: String) -> Result<()> {
    let err = DfParser::parse_sql(sql).unwrap_err();
    assert_eq!(err.message(), expected);
    Ok(())
}

fn expect_parse_err_contains(sql: &str, expected: String) -> Result<()> {
    let err = DfParser::parse_sql(sql).unwrap_err();
    assert!(err.message().contains(&expected));
    Ok(())
}

fn verified_query(sql: &str) -> Result<Box<DfQueryStatement>> {
    let mut parser = DfParser::new_with_dialect(sql, &GenericDialect {})?;
    let stmt = parser.parse_statement()?;
    if let DfStatement::Query(query) = stmt {
        return Ok(query);
    }
    Err(ParserError::ParserError("Expect query statement".to_string()).into())
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

fn parse_sql_to_expr(query_expr: &str) -> Expr {
    let dialect = GenericDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, query_expr);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(tokens, &dialect);
    parser.parse_expr().unwrap()
}

#[test]
fn create_database() -> Result<()> {
    {
        let sql = "CREATE DATABASE db1";
        let expected = DfStatement::CreateDatabase(DfCreateDatabase {
            if_not_exists: false,
            name: ObjectName(vec![Ident::new("db1")]),
            engine: "".to_string(),
            engine_options: HashMap::new(),
            options: HashMap::new(),
        });
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "CREATE DATABASE db1 engine = github";
        let expected = DfStatement::CreateDatabase(DfCreateDatabase {
            if_not_exists: false,
            name: ObjectName(vec![Ident::new("db1")]),
            engine: "github".to_string(),
            engine_options: HashMap::new(),
            options: HashMap::new(),
        });
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "CREATE DATABASE IF NOT EXISTS db1";
        let expected = DfStatement::CreateDatabase(DfCreateDatabase {
            if_not_exists: true,
            name: ObjectName(vec![Ident::new("db1")]),
            engine: "".to_string(),
            engine_options: HashMap::new(),
            options: HashMap::new(),
        });
        expect_parse_ok(sql, expected)?;
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
    let sql = "CREATE TABLE t(c1 int) ENGINE = Fuse location = '/data/33.csv' ";
    let expected = DfStatement::CreateTable(DfCreateTable {
        if_not_exists: false,
        name: ObjectName(vec![Ident::new("t")]),
        columns: vec![make_column_def("c1", DataType::Int(None))],
        engine: "Fuse".to_string(),
        options: maplit::hashmap! {"location".into() => "/data/33.csv".into()},
        like: None,
        query: None,
    });
    expect_parse_ok(sql, expected)?;

    // positive case: it is ok for parquet files not to have columns specified
    let sql = "CREATE TABLE t(c1 int, c2 bigint, c3 varchar(255) ) ENGINE = Fuse location = 'foo.parquet' comment = 'foo'";
    let expected = DfStatement::CreateTable(DfCreateTable {
        if_not_exists: false,
        name: ObjectName(vec![Ident::new("t")]),
        columns: vec![
            make_column_def("c1", DataType::Int(None)),
            make_column_def("c2", DataType::BigInt(None)),
            make_column_def("c3", DataType::Varchar(Some(255))),
        ],
        engine: "Fuse".to_string(),

        options: maplit::hashmap! {
            "location".into() => "foo.parquet".into(),
            "comment".into() => "foo".into(),
        },
        like: None,
        query: None,
    });
    expect_parse_ok(sql, expected)?;

    // create table like statement
    let sql = "CREATE TABLE db1.test1 LIKE db2.test2 ENGINE = Parquet location = 'batcave'";
    let expected = DfStatement::CreateTable(DfCreateTable {
        if_not_exists: false,
        name: ObjectName(vec![Ident::new("db1"), Ident::new("test1")]),
        columns: vec![],
        engine: "Parquet".to_string(),

        options: maplit::hashmap! {"location".into() => "batcave".into()},
        like: Some(ObjectName(vec![Ident::new("db2"), Ident::new("test2")])),
        query: None,
    });
    expect_parse_ok(sql, expected)?;

    // create table as select statement
    let sql = "CREATE TABLE db1.test1(c1 int, c2 varchar(255)) ENGINE = Parquet location = 'batcave' AS SELECT * FROM t2";
    let expected = DfStatement::CreateTable(DfCreateTable {
        if_not_exists: false,
        name: ObjectName(vec![Ident::new("db1"), Ident::new("test1")]),
        columns: vec![
            make_column_def("c1", DataType::Int(None)),
            make_column_def("c2", DataType::Varchar(Some(255))),
        ],
        engine: "Parquet".to_string(),

        options: maplit::hashmap! {"location".into() => "batcave".into()},
        like: None,
        query: Some(Box::new(DfQueryStatement {
            from: vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec![Ident::new("t2")]),
                    alias: None,
                    args: vec![],
                    with_hints: vec![],
                },
                joins: vec![],
            }],
            projection: vec![SelectItem::Wildcard],
            selection: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        })),
    });
    expect_parse_ok(sql, expected)?;

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
    use databend_query::sql::statements::DfShowSettings;
    use databend_query::sql::statements::DfShowTables;

    // positive case
    expect_parse_ok("SHOW TABLES", DfStatement::ShowTables(DfShowTables::All))?;
    expect_parse_ok("SHOW TABLES;", DfStatement::ShowTables(DfShowTables::All))?;
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

    expect_parse_ok(
        "SHOW TABLES WHERE t LIKE 'aaa'",
        DfStatement::ShowTables(DfShowTables::Where(parse_sql_to_expr("t LIKE 'aaa'"))),
    )?;

    expect_parse_ok(
        "SHOW TABLES LIKE 'aaa' --comments should not in sql case2",
        DfStatement::ShowTables(DfShowTables::Like(Ident::with_quote('\'', "aaa"))),
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
fn show_tables_test() -> Result<()> {
    let mut ident = Ident::new("ss");
    ident.quote_style = Some('`');
    let v = vec![ident];
    let name = ObjectName(v);
    let name_two = name.clone();

    expect_parse_ok(
        "SHOW TABLES FROM `ss`",
        DfStatement::ShowTables(DfShowTables::FromOrIn(name)),
    )?;
    expect_parse_ok(
        "SHOW TABLES IN `ss`",
        DfStatement::ShowTables(DfShowTables::FromOrIn(name_two)),
    )?;
    Ok(())
}

#[test]
fn show_grants_test() -> Result<()> {
    expect_parse_ok(
        "SHOW GRANTS",
        DfStatement::ShowGrants(DfShowGrants {
            user_identity: None,
        }),
    )?;

    expect_parse_ok(
        "SHOW GRANTS FOR 'u1'@'%'",
        DfStatement::ShowGrants(DfShowGrants {
            user_identity: Some(UserIdentity {
                username: "u1".into(),
                hostname: "%".into(),
            }),
        }),
    )?;

    Ok(())
}

#[test]
fn show_functions_tests() -> Result<()> {
    use databend_query::sql::statements::DfShowFunctions;

    // positive case
    expect_parse_ok(
        "SHOW FUNCTIONS",
        DfStatement::ShowFunctions(DfShowFunctions::All),
    )?;
    expect_parse_ok(
        "SHOW FUNCTIONS;",
        DfStatement::ShowFunctions(DfShowFunctions::All),
    )?;
    expect_parse_ok(
        "SHOW FUNCTIONS --comments should not in sql case1",
        DfStatement::ShowFunctions(DfShowFunctions::All),
    )?;

    expect_parse_ok(
        "SHOW FUNCTIONS LIKE 'aaa'",
        DfStatement::ShowFunctions(DfShowFunctions::Like(Ident::with_quote('\'', "aaa"))),
    )?;
    expect_parse_ok(
        "SHOW FUNCTIONS LIKE 'aaa';",
        DfStatement::ShowFunctions(DfShowFunctions::Like(Ident::with_quote('\'', "aaa"))),
    )?;
    expect_parse_ok(
        "SHOW FUNCTIONS LIKE 'aaa' --comments should not in sql case2",
        DfStatement::ShowFunctions(DfShowFunctions::Like(Ident::with_quote('\'', "aaa"))),
    )?;

    expect_parse_ok(
        "SHOW FUNCTIONS WHERE t LIKE 'aaa'",
        DfStatement::ShowFunctions(DfShowFunctions::Where(parse_sql_to_expr("t LIKE 'aaa'"))),
    )?;
    expect_parse_ok(
        "SHOW FUNCTIONS LIKE 'aaa' --comments should not in sql case2",
        DfStatement::ShowFunctions(DfShowFunctions::Like(Ident::with_quote('\'', "aaa"))),
    )?;
    expect_parse_ok(
        "SHOW FUNCTIONS WHERE t LIKE 'aaa' AND t LIKE 'a%'",
        DfStatement::ShowFunctions(DfShowFunctions::Where(parse_sql_to_expr(
            "t LIKE 'aaa' AND t LIKE 'a%'",
        ))),
    )?;

    Ok(())
}

#[test]
fn use_test() -> Result<()> {
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
fn sudo_command_test() -> Result<()> {
    expect_parse_ok(
        "use `tenant`",
        DfStatement::UseDatabase(DfUseDatabase {
            name: ObjectName(vec![Ident::with_quote('`', "tenant")]),
        }),
    )?;

    expect_parse_ok(
        "sudo use tenant 'xx'",
        DfStatement::UseTenant(DfUseTenant {
            name: ObjectName(vec![Ident::with_quote('\'', "xx")]),
        }),
    )?;

    expect_parse_err(
        "sudo yy",
        String::from("sql parser error: Expected Unsupported sudo command, found: yy"),
    )?;

    expect_parse_err(
        "sudo use xx",
        String::from("sql parser error: Expected Unsupported sudo command, found: xx"),
    )?;

    Ok(())
}

#[test]
fn truncate_table() -> Result<()> {
    {
        let sql = "TRUNCATE TABLE t1";
        let expected = DfStatement::TruncateTable(DfTruncateTable {
            name: ObjectName(vec![Ident::new("t1")]),
            purge: false,
        });
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "TRUNCATE TABLE t1 purge";
        let expected = DfStatement::TruncateTable(DfTruncateTable {
            name: ObjectName(vec![Ident::new("t1")]),
            purge: true,
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

#[test]
fn copy_test() -> Result<()> {
    let ident = Ident::new("test_csv");
    let v = vec![ident];
    let name = ObjectName(v);

    expect_parse_ok(
        "copy into test_csv from '@my_ext_stage/tutorials/sample.csv' format csv csv_header = 1 field_delimitor = ',';",
        DfStatement::Copy(DfCopy {
            name,
            columns: vec![],
            location: "@my_ext_stage/tutorials/sample.csv".to_string(),
            format: "csv".to_string(),
            options: maplit::hashmap! {
                "csv_header".into() => "1".into(),
                "field_delimitor".into() => ",".into(),
         }
        }
        ),



    )?;

    Ok(())
}

#[test]
fn show_databases_test() -> Result<()> {
    expect_parse_ok(
        "SHOW DATABASES",
        DfStatement::ShowDatabases(DfShowDatabases { where_opt: None }),
    )?;

    expect_parse_ok(
        "SHOW DATABASES;",
        DfStatement::ShowDatabases(DfShowDatabases { where_opt: None }),
    )?;

    expect_parse_ok(
        "SHOW DATABASES WHERE Database = 'ss'",
        DfStatement::ShowDatabases(DfShowDatabases {
            where_opt: Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("name"))),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::SingleQuotedString("ss".to_string()))),
            }),
        }),
    )?;

    expect_parse_ok(
        "SHOW DATABASES WHERE Database Like 'ss%'",
        DfStatement::ShowDatabases(DfShowDatabases {
            where_opt: Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("name"))),
                op: BinaryOperator::Like,
                right: Box::new(Expr::Value(Value::SingleQuotedString("ss%".to_string()))),
            }),
        }),
    )?;

    expect_parse_ok(
        "SHOW DATABASES LIKE 'ss%'",
        DfStatement::ShowDatabases(DfShowDatabases {
            where_opt: Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("name"))),
                op: BinaryOperator::Like,
                right: Box::new(Expr::Value(Value::SingleQuotedString("ss%".to_string()))),
            }),
        }),
    )?;

    expect_parse_ok(
        "SHOW DATABASES LIKE SUBSTRING('ss%' FROM 1 FOR 3)",
        DfStatement::ShowDatabases(DfShowDatabases {
            where_opt: Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("name"))),
                op: BinaryOperator::Like,
                right: Box::new(Expr::Substring {
                    expr: Box::new(Expr::Value(Value::SingleQuotedString("ss%".to_string()))),
                    substring_from: Some(Box::new(Expr::Value(Value::Number(
                        "1".to_string(),
                        false,
                    )))),
                    substring_for: Some(Box::new(Expr::Value(Value::Number(
                        "3".to_string(),
                        false,
                    )))),
                }),
            }),
        }),
    )?;

    expect_parse_ok(
        "SHOW DATABASES LIKE POSITION('012345' IN 'abcdef')",
        DfStatement::ShowDatabases(DfShowDatabases {
            where_opt: Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("name"))),
                op: BinaryOperator::Like,
                right: Box::new(Expr::Position {
                    substr_expr: Box::new(Expr::Value(Value::SingleQuotedString(
                        "012345".to_string(),
                    ))),
                    str_expr: Box::new(Expr::Value(Value::SingleQuotedString(
                        "abcdef".to_string(),
                    ))),
                }),
            }),
        }),
    )?;

    Ok(())
}

#[test]
fn show_create_test() -> Result<()> {
    expect_parse_ok(
        "SHOW CREATE TABLE test",
        DfStatement::ShowCreateTable(DfShowCreateTable {
            name: ObjectName(vec![Ident::new("test")]),
        }),
    )?;

    expect_parse_ok(
        "SHOW CREATE DATABASE test",
        DfStatement::ShowCreateDatabase(DfShowCreateDatabase {
            name: ObjectName(vec![Ident::new("test")]),
        }),
    )?;

    Ok(())
}

fn test_diff_password_type(password_type_str: &str, password_type: PasswordType) -> Result<()> {
    expect_parse_ok(
        &format!(
            "CREATE USER 'test'@'localhost' IDENTIFIED WITH {} BY 'password'",
            password_type_str
        ),
        DfStatement::CreateUser(DfCreateUser {
            if_not_exists: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_info: AuthInfo::Password {
                password: Vec::from("password"),
                password_type,
            },
        }),
    )
}

#[test]
fn create_user_test() -> Result<()> {
    // normal
    test_diff_password_type("plaintext_password", PasswordType::PlainText)?;
    test_diff_password_type("sha256_password", PasswordType::Sha256)?;
    test_diff_password_type("double_sha1_password", PasswordType::DoubleSha1)?;

    // no with
    expect_parse_ok(
        "CREATE USER 'test'@'localhost' IDENTIFIED BY 'password'",
        DfStatement::CreateUser(DfCreateUser {
            if_not_exists: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_info: AuthInfo::Password {
                password: Vec::from("password"),
                password_type: PasswordType::Sha256,
            },
        }),
    )?;

    // no password, 3 ways
    expect_parse_ok(
        "CREATE USER 'test'@'localhost' IDENTIFIED WITH no_password",
        DfStatement::CreateUser(DfCreateUser {
            if_not_exists: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_info: AuthInfo::None,
        }),
    )?;

    expect_parse_ok(
        "CREATE USER 'test'@'localhost' NOT IDENTIFIED",
        DfStatement::CreateUser(DfCreateUser {
            if_not_exists: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_info: AuthInfo::None,
        }),
    )?;

    expect_parse_ok(
        "CREATE USER 'test'@'localhost'",
        DfStatement::CreateUser(DfCreateUser {
            if_not_exists: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_info: AuthInfo::None,
        }),
    )?;

    // username contains '@'
    expect_parse_ok(
        "CREATE USER 'test@localhost'",
        DfStatement::CreateUser(DfCreateUser {
            if_not_exists: false,
            name: String::from("test@localhost"),
            hostname: String::from("%"),
            auth_info: AuthInfo::None,
        }),
    )?;

    // errors
    expect_parse_err(
        "CREATE USER 'test'@'localhost' IDENTIFIED WITH no_password BY 'password'",
        String::from("sql parser error: Expected end of statement, found: BY"),
    )?;

    expect_parse_err(
        "CREATE USER 'test'@'localhost' IDENTIFIED WITH sha256_password",
        String::from("sql parser error: Expected keyword BY"),
    )?;

    expect_parse_err(
        "CREATE USER 'test'@'localhost' IDENTIFIED WITH sha256_password BY",
        String::from("sql parser error: Expected literal string, found: EOF"),
    )?;

    expect_parse_err(
        "CREATE USER 'test'@'localhost' IDENTIFIED WITH sha256_password BY ''",
        String::from("sql parser error: Missing password"),
    )?;
    Ok(())
}

fn test_diff_auth_type(with_clause: &str, auth_type: Option<AuthType>) -> Result<()> {
    expect_parse_ok(
        &format!(
            "ALTER USER 'test'@'localhost' IDENTIFIED {} BY 'password'",
            with_clause
        ),
        DfStatement::AlterUser(DfAlterUser {
            if_current_user: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_info_args: Some(AuthInfoArgs {
                arg_with: auth_type,
                arg_by: Some("password".to_string()),
            }),
        }),
    )
}
#[test]
fn alter_user_test() -> Result<()> {
    expect_parse_ok(
        "ALTER USER 'test'@'localhost' IDENTIFIED BY 'password'",
        DfStatement::AlterUser(DfAlterUser {
            if_current_user: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_info_args: Some(AuthInfoArgs {
                arg_with: None,
                arg_by: Some("password".to_string()),
            }),
        }),
    )?;

    expect_parse_ok(
        "ALTER USER USER() IDENTIFIED BY 'password'",
        DfStatement::AlterUser(DfAlterUser {
            if_current_user: true,
            name: String::from(""),
            hostname: String::from(""),
            auth_info_args: Some(AuthInfoArgs {
                arg_with: None,
                arg_by: Some("password".to_string()),
            }),
        }),
    )?;

    test_diff_auth_type("WITH plaintext_password", Some(AuthType::PlaintextPassword))?;
    test_diff_auth_type("WITH sha256_password", Some(AuthType::Sha256Password))?;
    test_diff_auth_type(
        "WITH double_sha1_password",
        Some(AuthType::DoubleShaPassword),
    )?;

    expect_parse_ok(
        "ALTER USER 'test'@'localhost' IDENTIFIED WITH no_password",
        DfStatement::AlterUser(DfAlterUser {
            if_current_user: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_info_args: Some(AuthInfoArgs {
                arg_with: Some(AuthType::NoPassword),
                arg_by: None,
            }),
        }),
    )?;

    expect_parse_ok(
        "ALTER USER 'test@localhost' IDENTIFIED WITH sha256_password BY 'password'",
        DfStatement::AlterUser(DfAlterUser {
            if_current_user: false,
            name: String::from("test@localhost"),
            hostname: String::from("%"),
            auth_info_args: Some(AuthInfoArgs {
                arg_with: Some(AuthType::Sha256Password),
                arg_by: Some("password".to_string()),
            }),
        }),
    )?;

    expect_parse_ok(
        "ALTER USER 'test'@'localhost' NOT IDENTIFIED",
        DfStatement::AlterUser(DfAlterUser {
            if_current_user: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_info_args: Some(AuthInfoArgs {
                arg_with: Some(AuthType::NoPassword),
                arg_by: None,
            }),
        }),
    )?;

    expect_parse_ok(
        "ALTER USER 'test'@'localhost'",
        DfStatement::AlterUser(DfAlterUser {
            if_current_user: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_info_args: None,
        }),
    )?;

    expect_parse_err(
        "ALTER USER 'test'@'localhost' IDENTIFIED WITH no_password BY 'password'",
        String::from("sql parser error: Expected end of statement, found: BY"),
    )?;

    expect_parse_err(
        "ALTER USER 'test'@'localhost' IDENTIFIED WITH sha256_password",
        String::from("sql parser error: Expected keyword BY"),
    )?;

    expect_parse_err(
        "ALTER USER 'test'@'localhost' IDENTIFIED WITH sha256_password BY",
        String::from("sql parser error: Expected literal string, found: EOF"),
    )?;

    expect_parse_err(
        "ALTER USER 'test'@'localhost' IDENTIFIED WITH sha256_password BY ''",
        String::from("sql parser error: Missing password"),
    )?;
    Ok(())
}

#[test]
fn drop_user_test() -> Result<()> {
    expect_parse_ok(
        "DROP USER 'test'@'localhost'",
        DfStatement::DropUser(DfDropUser {
            if_exists: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
        }),
    )?;

    expect_parse_ok(
        "DROP USER 'test'@'127.0.0.1'",
        DfStatement::DropUser(DfDropUser {
            if_exists: false,
            name: String::from("test"),
            hostname: String::from("127.0.0.1"),
        }),
    )?;

    expect_parse_ok(
        "DROP USER 'test'",
        DfStatement::DropUser(DfDropUser {
            if_exists: false,
            name: String::from("test"),
            hostname: String::from("%"),
        }),
    )?;

    expect_parse_ok(
        "DROP USER IF EXISTS 'test'@'localhost'",
        DfStatement::DropUser(DfDropUser {
            if_exists: true,
            name: String::from("test"),
            hostname: String::from("localhost"),
        }),
    )?;

    expect_parse_ok(
        "DROP USER IF EXISTS 'test'@'127.0.0.1'",
        DfStatement::DropUser(DfDropUser {
            if_exists: true,
            name: String::from("test"),
            hostname: String::from("127.0.0.1"),
        }),
    )?;

    expect_parse_ok(
        "DROP USER IF EXISTS 'test'",
        DfStatement::DropUser(DfDropUser {
            if_exists: true,
            name: String::from("test"),
            hostname: String::from("%"),
        }),
    )?;
    Ok(())
}

#[test]
fn grant_privilege_test() -> Result<()> {
    expect_parse_ok(
        "GRANT ALL ON * TO 'test'@'localhost'",
        DfStatement::GrantPrivilege(DfGrantStatement {
            name: String::from("test"),
            hostname: String::from("localhost"),
            on: DfGrantObject::Database(None),
            priv_types: UserPrivilegeSet::all_privileges(),
        }),
    )?;

    expect_parse_ok(
        "GRANT ALL PRIVILEGES ON * TO 'test'@'localhost'",
        DfStatement::GrantPrivilege(DfGrantStatement {
            name: String::from("test"),
            hostname: String::from("localhost"),
            on: DfGrantObject::Database(None),
            priv_types: UserPrivilegeSet::all_privileges(),
        }),
    )?;

    expect_parse_ok(
        "GRANT INSERT ON `db1`.`tb1` TO 'test'@'localhost'",
        DfStatement::GrantPrivilege(DfGrantStatement {
            name: String::from("test"),
            hostname: String::from("localhost"),
            on: DfGrantObject::Table(Some("db1".into()), "tb1".into()),
            priv_types: {
                let mut privileges = UserPrivilegeSet::empty();
                privileges.set_privilege(UserPrivilegeType::Insert);
                privileges
            },
        }),
    )?;

    expect_parse_ok(
        "GRANT INSERT ON `tb1` TO 'test'@'localhost'",
        DfStatement::GrantPrivilege(DfGrantStatement {
            name: String::from("test"),
            hostname: String::from("localhost"),
            on: DfGrantObject::Table(None, "tb1".into()),
            priv_types: {
                let mut privileges = UserPrivilegeSet::empty();
                privileges.set_privilege(UserPrivilegeType::Insert);
                privileges
            },
        }),
    )?;

    expect_parse_ok(
        "GRANT INSERT ON `db1`.'*' TO 'test'@'localhost'",
        DfStatement::GrantPrivilege(DfGrantStatement {
            name: String::from("test"),
            hostname: String::from("localhost"),
            on: DfGrantObject::Database(Some("db1".into())),
            priv_types: {
                let mut privileges = UserPrivilegeSet::empty();
                privileges.set_privilege(UserPrivilegeType::Insert);
                privileges
            },
        }),
    )?;

    expect_parse_ok(
        "GRANT CREATE, SELECT ON * TO 'test'@'localhost'",
        DfStatement::GrantPrivilege(DfGrantStatement {
            name: String::from("test"),
            hostname: String::from("localhost"),
            on: DfGrantObject::Database(None),
            priv_types: {
                let mut privileges = UserPrivilegeSet::empty();
                privileges.set_privilege(UserPrivilegeType::Select);
                privileges.set_privilege(UserPrivilegeType::Create);
                privileges
            },
        }),
    )?;

    expect_parse_ok(
        "GRANT CREATE USER, CREATE ROLE, CREATE, SELECT ON * TO 'test'@'localhost'",
        DfStatement::GrantPrivilege(DfGrantStatement {
            name: String::from("test"),
            hostname: String::from("localhost"),
            on: DfGrantObject::Database(None),
            priv_types: {
                let mut privileges = UserPrivilegeSet::empty();
                privileges.set_privilege(UserPrivilegeType::Create);
                privileges.set_privilege(UserPrivilegeType::CreateUser);
                privileges.set_privilege(UserPrivilegeType::CreateRole);
                privileges.set_privilege(UserPrivilegeType::Select);
                privileges
            },
        }),
    )?;

    expect_parse_err(
        "GRANT TEST, ON * TO 'test'@'localhost'",
        String::from("sql parser error: Expected privilege type, found: TEST"),
    )?;

    expect_parse_err(
        "GRANT SELECT, ON * TO 'test'@'localhost'",
        String::from("sql parser error: Expected privilege type, found: ON"),
    )?;

    expect_parse_err(
        "GRANT SELECT IN * TO 'test'@'localhost'",
        String::from("sql parser error: Expected keyword ON, found: IN"),
    )?;

    expect_parse_err(
        "GRANT SELECT ON * 'test'@'localhost'",
        String::from("sql parser error: Expected keyword TO, found: 'test'"),
    )?;

    expect_parse_err(
        "GRANT INSERT ON *.`tb1` TO 'test'@'localhost'",
        String::from("sql parser error: Expected whitespace, found: ."),
    )?;

    Ok(())
}

#[test]
fn revoke_privilege_test() -> Result<()> {
    expect_parse_ok(
        "REVOKE ALL ON * FROM 'test'@'localhost'",
        DfStatement::RevokePrivilege(DfRevokeStatement {
            username: String::from("test"),
            hostname: String::from("localhost"),
            on: DfGrantObject::Database(None),
            priv_types: UserPrivilegeSet::all_privileges(),
        }),
    )?;

    expect_parse_err(
        "REVOKE SELECT ON * 'test'@'localhost'",
        String::from("sql parser error: Expected keyword FROM, found: 'test'"),
    )?;

    Ok(())
}

#[test]
fn create_stage_test() -> Result<()> {
    expect_parse_ok(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z')",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: false,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format: FileFormat::default(),
            comments: "".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE STAGE IF NOT EXISTS test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z')",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: true,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format: FileFormat::default(),
            comments: "".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE STAGE IF NOT EXISTS test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=CSV compression=GZIP record_delimiter=',')",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: true,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format:  FileFormat { compression: Compression::Gzip, record_delimiter: ",".to_string(),..Default::default()},
            comments: "".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE STAGE IF NOT EXISTS test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=CSV compression=GZIP record_delimiter=',') comments='test'",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: true,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format:  FileFormat { compression: Compression::Gzip, record_delimiter: ",".to_string(),..Default::default()},
            comments: "test".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=Parquet compression=AUTO) comments='test'",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: false,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format:  FileFormat { format: Format::Parquet, compression: Compression::Auto ,..Default::default()},
            comments: "test".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=csv compression=AUTO) comments='test'",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: false,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format:  FileFormat { format: Format::Csv, compression: Compression::Auto,..Default::default()},
            comments: "test".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=json) comments='test'",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: false,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format:  FileFormat { format: Format::Json,..Default::default()},
            comments: "test".to_string(),
        }),
    )?;

    expect_parse_err(
        "CREATE STAGE test_stage credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=csv compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("sql parser error: Missing URL"),
    )?;

    expect_parse_err(
        "CREATE STAGE test_stage url='s3://load/files/' password=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=csv compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("sql parser error: Expected end of statement, found: password"),
    )?;

    expect_parse_err(
        "CREATE STAGE test_stage url='s4://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=csv compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("sql parser error: Not supported storage"),
    )?;

    expect_parse_err(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=csv compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("sql parser error: Invalid credentials options: unknown field `access_key`, expected `access_key_id` or `secret_access_key`"),
    )?;

    expect_parse_err(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' aecret_access_key='4x5y6z') file_format=(FORMAT=csv compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("sql parser error: Invalid credentials options: unknown field `aecret_access_key`, expected `access_key_id` or `secret_access_key`"),
    )?;

    expect_parse_err_contains(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(type=csv compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("unknown field `type`"),
    )?;

    expect_parse_err_contains(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(format=csv compression=AUTO1 record_delimiter=NONE) comments='test'",
        String::from("unknown variant `auto1`"),
    )?;

    expect_parse_err_contains(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(format=csv1 compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("unknown variant `csv1`"),
    )?;

    Ok(())
}

#[test]
fn create_table_select() -> Result<()> {
    expect_parse_ok(
        "CREATE TABLE foo AS SELECT a, b FROM bar",
        DfStatement::CreateTable(DfCreateTable {
            if_not_exists: false,
            name: ObjectName(vec![Ident::new("foo")]),
            columns: vec![],
            engine: "FUSE".to_string(),
            options: maplit::hashmap! {},
            like: None,
            query: Some(verified_query("SELECT a, b FROM bar")?),
        }),
    )?;

    expect_parse_ok(
        "CREATE TABLE foo (a INT) SELECT a, b FROM bar",
        DfStatement::CreateTable(DfCreateTable {
            if_not_exists: false,
            name: ObjectName(vec![Ident::new("foo")]),
            columns: vec![make_column_def("a", DataType::Int(None))],
            engine: "FUSE".to_string(),
            options: maplit::hashmap! {},
            like: None,
            query: Some(verified_query("SELECT a, b FROM bar")?),
        }),
    )?;

    Ok(())
}

#[test]
fn optimize_table() -> Result<()> {
    {
        let sql = "optimize TABLE t1";
        let expected = DfStatement::OptimizeTable(DfOptimizeTable {
            name: ObjectName(vec![Ident::new("t1")]),
            operation: Optimization::PURGE,
        });
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "OPTIMIZE tABLE t1";
        let expected = DfStatement::OptimizeTable(DfOptimizeTable {
            name: ObjectName(vec![Ident::new("t1")]),
            operation: Optimization::PURGE,
        });
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "optimize TABLE t1 purge";
        let expected = DfStatement::OptimizeTable(DfOptimizeTable {
            name: ObjectName(vec![Ident::new("t1")]),
            operation: Optimization::PURGE,
        });
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "optimize TABLE t1 compact";
        let expected = DfStatement::OptimizeTable(DfOptimizeTable {
            name: ObjectName(vec![Ident::new("t1")]),
            operation: Optimization::COMPACT,
        });
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "optimize TABLE t1 all";
        let expected = DfStatement::OptimizeTable(DfOptimizeTable {
            name: ObjectName(vec![Ident::new("t1")]),
            operation: Optimization::ALL,
        });
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "optimize TABLE t1 unacceptable";
        expect_parse_err(
            sql,
            "sql parser error: Expected one of PURGE, COMPACT, ALL, found: unacceptable"
                .to_string(),
        )?;
    }

    {
        let sql = "optimize TABLE t1 (";
        expect_parse_err(
            sql,
            "sql parser error: Expected Nothing, or one of PURGE, COMPACT, ALL, found: ("
                .to_string(),
        )?;
    }

    Ok(())
}

#[test]
fn drop_stage_test() -> Result<()> {
    expect_parse_ok(
        "DROP STAGE test_stage",
        DfStatement::DropStage(DfDropStage {
            if_exists: false,
            stage_name: "test_stage".to_string(),
        }),
    )?;

    expect_parse_ok(
        "DROP STAGE IF EXISTS test_stage",
        DfStatement::DropStage(DfDropStage {
            if_exists: true,
            stage_name: "test_stage".to_string(),
        }),
    )?;

    Ok(())
}

#[test]
fn test_create_udf() -> Result<()> {
    expect_parse_err_contains(
        "CREATE FUNCTION test_udf AS p -> not(isnotnull(p))",
        "Expected (, found: p".to_string(),
    )?;

    expect_parse_err_contains(
        "CREATE FUNCTION test_udf AS (as) -> not(isnotnull(as))",
        "Keyword can not be parameter, got: as".to_string(),
    )?;

    expect_parse_err_contains(
        "CREATE FUNCTION test_udf AS (\"p\") -> not(isnotnull(p))",
        "Quote is not allowed in parameters, remove: \"".to_string(),
    )?;

    expect_parse_err_contains(
        "CREATE FUNCTION test_udf AS (p, p) -> not(isnotnull(p))",
        "Duplicate parameter is not allowed, keep only one: p".to_string(),
    )?;

    expect_parse_err_contains(
        "CREATE FUNCTION test_udf AS (p:) -> not(isnotnull(p))",
        "Expect words or comma, but got: :".to_string(),
    )?;

    expect_parse_err_contains(
        "CREATE FUNCTION test_udf AS (p,) -> not(isnotnull(p))",
        "Found a redundant `,` in the parameters".to_string(),
    )?;

    expect_parse_err_contains(
        "CREATE FUNCTION test_udf AS (p;) -> not(isnotnull(p))",
        "Can not find complete parameters, `)` is missing".to_string(),
    )?;

    expect_parse_ok(
        "CREATE FUNCTION test_udf AS (p) -> not(isnotnull(p))",
        DfStatement::CreateUDF(DfCreateUDF {
            if_not_exists: false,
            udf_name: "test_udf".to_string(),
            parameters: vec!["p".to_string()],
            definition: "not(isnotnull(p))".to_string(),
            description: "".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE FUNCTION test_udf AS (p, d) -> not(isnotnull(p, d))",
        DfStatement::CreateUDF(DfCreateUDF {
            if_not_exists: false,
            udf_name: "test_udf".to_string(),
            parameters: vec!["p".to_string(), "d".to_string()],
            definition: "not(isnotnull(p,d))".to_string(),
            description: "".to_string(),
        }),
    )?;

    expect_parse_err_contains(
        "CREATE FUNCTION test_udf AS (p) -> not(isnotnull(p)) DESC",
        "Expected =, found: ".to_string(),
    )?;

    expect_parse_err_contains(
        "CREATE FUNCTION test_udf AS (p) -> not(isnotnull(p)) DESC =",
        "Expected literal string, found: EOF".to_string(),
    )?;

    expect_parse_ok(
        "CREATE FUNCTION test_udf AS (p, d) -> not(isnotnull(p, d)) DESC = 'this is a description'",
        DfStatement::CreateUDF(DfCreateUDF {
            if_not_exists: false,
            udf_name: "test_udf".to_string(),
            parameters: vec!["p".to_string(), "d".to_string()],
            definition: "not(isnotnull(p,d))".to_string(),
            description: "this is a description".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE FUNCTION test_udf as (p, d) -> not(isnotnull(p, d)) DESC = 'this is a description'",
        DfStatement::CreateUDF(DfCreateUDF {
            if_not_exists: false,
            udf_name: "test_udf".to_string(),
            parameters: vec!["p".to_string(), "d".to_string()],
            definition: "not(isnotnull(p,d))".to_string(),
            description: "this is a description".to_string(),
        }),
    )?;

    Ok(())
}

#[test]
fn test_drop_udf() -> Result<()> {
    expect_parse_ok(
        "DROP FUNCTION test_udf",
        DfStatement::DropUDF(DfDropUDF {
            if_exists: false,
            udf_name: "test_udf".to_string(),
        }),
    )?;

    expect_parse_ok(
        "DROP FUNCTION IF EXISTS test_udf",
        DfStatement::DropUDF(DfDropUDF {
            if_exists: true,
            udf_name: "test_udf".to_string(),
        }),
    )?;

    Ok(())
}

#[test]
fn test_show_udf() -> Result<()> {
    expect_parse_ok(
        "SHOW FUNCTION test_udf",
        DfStatement::ShowUDF(DfShowUDF {
            udf_name: "test_udf".to_string(),
        }),
    )?;

    Ok(())
}

#[test]
fn test_alter_udf() -> Result<()> {
    expect_parse_err_contains(
        "ALTER FUNCTION test_udf AS p -> not(isnotnull(p))",
        "Expected (, found: p".to_string(),
    )?;

    expect_parse_err_contains(
        "ALTER FUNCTION test_udf AS (as) -> not(isnotnull(as))",
        "Keyword can not be parameter, got: as".to_string(),
    )?;

    expect_parse_err_contains(
        "ALTER FUNCTION test_udf AS (\"p\") -> not(isnotnull(p))",
        "Quote is not allowed in parameters, remove: \"".to_string(),
    )?;

    expect_parse_err_contains(
        "ALTER FUNCTION test_udf AS (p, p) -> not(isnotnull(p))",
        "Duplicate parameter is not allowed, keep only one: p".to_string(),
    )?;

    expect_parse_err_contains(
        "ALTER FUNCTION test_udf AS (p:) -> not(isnotnull(p))",
        "Expect words or comma, but got: :".to_string(),
    )?;

    expect_parse_err_contains(
        "ALTER FUNCTION test_udf AS (p,) -> not(isnotnull(p))",
        "Found a redundant `,` in the parameters".to_string(),
    )?;

    expect_parse_err_contains(
        "ALTER FUNCTION test_udf AS (p;) -> not(isnotnull(p))",
        "Can not find complete parameters, `)` is missing".to_string(),
    )?;

    expect_parse_ok(
        "ALTER FUNCTION test_udf AS (p) -> not(isnotnull(p))",
        DfStatement::AlterUDF(DfAlterUDF {
            udf_name: "test_udf".to_string(),
            parameters: vec!["p".to_string()],
            definition: "not(isnotnull(p))".to_string(),
            description: "".to_string(),
        }),
    )?;

    expect_parse_ok(
        "ALTER FUNCTION test_udf AS (p, d) -> not(isnotnull(p, d))",
        DfStatement::AlterUDF(DfAlterUDF {
            udf_name: "test_udf".to_string(),
            parameters: vec!["p".to_string(), "d".to_string()],
            definition: "not(isnotnull(p,d))".to_string(),
            description: "".to_string(),
        }),
    )?;

    expect_parse_err_contains(
        "ALTER FUNCTION test_udf AS (p) -> not(isnotnull(p)) DESC",
        "Expected =, found: ".to_string(),
    )?;

    expect_parse_err_contains(
        "ALTER FUNCTION test_udf AS (p) -> not(isnotnull(p)) DESC =",
        "Expected literal string, found: EOF".to_string(),
    )?;

    expect_parse_ok(
        "ALTER FUNCTION test_udf AS (p, d) -> not(isnotnull(p, d)) DESC = 'this is a description'",
        DfStatement::AlterUDF(DfAlterUDF {
            udf_name: "test_udf".to_string(),
            parameters: vec!["p".to_string(), "d".to_string()],
            definition: "not(isnotnull(p,d))".to_string(),
            description: "this is a description".to_string(),
        }),
    )?;

    Ok(())
}
