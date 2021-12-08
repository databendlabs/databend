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

use common_exception::Result;
use common_meta_types::AuthType;
use common_meta_types::Compression;
use common_meta_types::Credentials;
use common_meta_types::FileFormat;
use common_meta_types::StageParams;
use common_meta_types::UserPrivilege;
use common_meta_types::UserPrivilegeType;
use sqlparser::ast::*;

use crate::sql::statements::DfAlterUser;
use crate::sql::statements::DfCopy;
use crate::sql::statements::DfCreateDatabase;
use crate::sql::statements::DfCreateStage;
use crate::sql::statements::DfCreateTable;
use crate::sql::statements::DfCreateUser;
use crate::sql::statements::DfDescribeTable;
use crate::sql::statements::DfDropDatabase;
use crate::sql::statements::DfDropTable;
use crate::sql::statements::DfDropUser;
use crate::sql::statements::DfGrantObject;
use crate::sql::statements::DfGrantStatement;
use crate::sql::statements::DfRevokeStatement;
use crate::sql::statements::DfShowDatabases;
use crate::sql::statements::DfShowTables;
use crate::sql::statements::DfTruncateTable;
use crate::sql::statements::DfUseDatabase;
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

fn expect_parse_err(sql: &str, expected: String) -> Result<()> {
    let err = DfParser::parse_sql(sql).unwrap_err();
    assert_eq!(err.message(), expected);
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
            engine: "".to_string(),
            options: vec![],
        });
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "CREATE DATABASE db1 engine = github";
        let expected = DfStatement::CreateDatabase(DfCreateDatabase {
            if_not_exists: false,
            name: ObjectName(vec![Ident::new("db1")]),
            engine: "github".to_string(),
            options: vec![],
        });
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "CREATE DATABASE IF NOT EXISTS db1";
        let expected = DfStatement::CreateDatabase(DfCreateDatabase {
            if_not_exists: true,
            name: ObjectName(vec![Ident::new("db1")]),
            engine: "".to_string(),
            options: vec![],
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
    let sql = "CREATE TABLE t(c1 int) ENGINE = CSV location = '/data/33.csv' ";
    let expected = DfStatement::CreateTable(DfCreateTable {
        if_not_exists: false,
        name: ObjectName(vec![Ident::new("t")]),
        columns: vec![make_column_def("c1", DataType::Int(None))],
        engine: "CSV".to_string(),
        options: vec![SqlOption {
            name: Ident::new("location".to_string()),
            value: Value::SingleQuotedString("/data/33.csv".into()),
        }],
        like: None,
    });
    expect_parse_ok(sql, expected)?;

    // positive case: it is ok for parquet files not to have columns specified
    let sql = "CREATE TABLE t(c1 int, c2 bigint, c3 varchar(255) ) ENGINE = Parquet location = 'foo.parquet' comment = 'foo'";
    let expected = DfStatement::CreateTable(DfCreateTable {
        if_not_exists: false,
        name: ObjectName(vec![Ident::new("t")]),
        columns: vec![
            make_column_def("c1", DataType::Int(None)),
            make_column_def("c2", DataType::BigInt(None)),
            make_column_def("c3", DataType::Varchar(Some(255))),
        ],
        engine: "Parquet".to_string(),
        options: vec![
            SqlOption {
                name: Ident::new("location".to_string()),
                value: Value::SingleQuotedString("foo.parquet".into()),
            },
            SqlOption {
                name: Ident::new("comment".to_string()),
                value: Value::SingleQuotedString("foo".into()),
            },
        ],
        like: None,
    });
    expect_parse_ok(sql, expected)?;

    // create table like statement
    let sql = "CREATE TABLE db1.test1 LIKE db2.test2 ENGINE = Parquet location = 'batcave'";
    let expected = DfStatement::CreateTable(DfCreateTable {
        if_not_exists: false,
        name: ObjectName(vec![Ident::new("db1"), Ident::new("test1")]),
        columns: vec![],
        engine: "Parquet".to_string(),
        options: vec![SqlOption {
            name: Ident::new("location".to_string()),
            value: Value::SingleQuotedString("batcave".into()),
        }],
        like: Some(ObjectName(vec![Ident::new("db2"), Ident::new("test2")])),
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
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use sqlparser::tokenizer::Tokenizer;

    use crate::sql::statements::DfShowSettings;
    use crate::sql::statements::DfShowTables;

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

    let parse_sql_to_expr = |query_expr: &str| -> Expr {
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, query_expr);
        let tokens = tokenizer.tokenize().unwrap();
        let mut parser = Parser::new(tokens, &dialect);
        parser.parse_expr().unwrap()
    };

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
            options:  vec![SqlOption {
                name: Ident::new("csv_header".to_string()),
                value: Value::Number("1".to_owned(), false),
            },
            SqlOption {
                name: Ident::new("field_delimitor".to_string()),
                value: Value::SingleQuotedString(",".into()),
            }],
        }),



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
fn create_user_test() -> Result<()> {
    expect_parse_ok(
        "CREATE USER 'test'@'localhost' IDENTIFIED BY 'password'",
        DfStatement::CreateUser(DfCreateUser {
            if_not_exists: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_type: AuthType::Sha256,
            password: String::from("password"),
        }),
    )?;

    expect_parse_ok(
        "CREATE USER 'test'@'localhost' IDENTIFIED WITH plaintext_password BY 'password'",
        DfStatement::CreateUser(DfCreateUser {
            if_not_exists: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_type: AuthType::PlainText,
            password: String::from("password"),
        }),
    )?;

    expect_parse_ok(
        "CREATE USER 'test'@'localhost' IDENTIFIED WITH sha256_password BY 'password'",
        DfStatement::CreateUser(DfCreateUser {
            if_not_exists: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_type: AuthType::Sha256,
            password: String::from("password"),
        }),
    )?;

    expect_parse_ok(
        "CREATE USER 'test'@'localhost' IDENTIFIED WITH double_sha1_password BY 'password'",
        DfStatement::CreateUser(DfCreateUser {
            if_not_exists: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_type: AuthType::DoubleSha1,
            password: String::from("password"),
        }),
    )?;

    expect_parse_ok(
        "CREATE USER 'test'@'localhost' IDENTIFIED WITH no_password",
        DfStatement::CreateUser(DfCreateUser {
            if_not_exists: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_type: AuthType::None,
            password: String::from(""),
        }),
    )?;

    expect_parse_ok(
        "CREATE USER IF NOT EXISTS 'test'@'localhost' IDENTIFIED WITH sha256_password BY 'password'",
        DfStatement::CreateUser(DfCreateUser {
            if_not_exists: true,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_type: AuthType::Sha256,
            password: String::from("password"),
        }),
    )?;

    expect_parse_ok(
        "CREATE USER 'test@localhost' IDENTIFIED WITH sha256_password BY 'password'",
        DfStatement::CreateUser(DfCreateUser {
            if_not_exists: false,
            name: String::from("test@localhost"),
            hostname: String::from("%"),
            auth_type: AuthType::Sha256,
            password: String::from("password"),
        }),
    )?;

    expect_parse_ok(
        "CREATE USER 'test'@'localhost' NOT IDENTIFIED",
        DfStatement::CreateUser(DfCreateUser {
            if_not_exists: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_type: AuthType::None,
            password: String::from(""),
        }),
    )?;

    expect_parse_ok(
        "CREATE USER 'test'@'localhost'",
        DfStatement::CreateUser(DfCreateUser {
            if_not_exists: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_type: AuthType::None,
            password: String::from(""),
        }),
    )?;

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

#[test]
fn alter_user_test() -> Result<()> {
    expect_parse_ok(
        "ALTER USER 'test'@'localhost' IDENTIFIED BY 'password'",
        DfStatement::AlterUser(DfAlterUser {
            if_current_user: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            new_auth_type: AuthType::Sha256,
            new_password: String::from("password"),
        }),
    )?;

    expect_parse_ok(
        "ALTER USER USER() IDENTIFIED BY 'password'",
        DfStatement::AlterUser(DfAlterUser {
            if_current_user: true,
            name: String::from(""),
            hostname: String::from(""),
            new_auth_type: AuthType::Sha256,
            new_password: String::from("password"),
        }),
    )?;

    expect_parse_ok(
        "ALTER USER 'test'@'localhost' IDENTIFIED WITH plaintext_password BY 'password'",
        DfStatement::AlterUser(DfAlterUser {
            if_current_user: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            new_auth_type: AuthType::PlainText,
            new_password: String::from("password"),
        }),
    )?;

    expect_parse_ok(
        "ALTER USER 'test'@'localhost' IDENTIFIED WITH sha256_password BY 'password'",
        DfStatement::AlterUser(DfAlterUser {
            if_current_user: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            new_auth_type: AuthType::Sha256,
            new_password: String::from("password"),
        }),
    )?;

    expect_parse_ok(
        "ALTER USER 'test'@'localhost' IDENTIFIED WITH double_sha1_password BY 'password'",
        DfStatement::AlterUser(DfAlterUser {
            if_current_user: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            new_auth_type: AuthType::DoubleSha1,
            new_password: String::from("password"),
        }),
    )?;

    expect_parse_ok(
        "ALTER USER 'test'@'localhost' IDENTIFIED WITH no_password",
        DfStatement::AlterUser(DfAlterUser {
            if_current_user: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            new_auth_type: AuthType::None,
            new_password: String::from(""),
        }),
    )?;

    expect_parse_ok(
        "ALTER USER 'test@localhost' IDENTIFIED WITH sha256_password BY 'password'",
        DfStatement::AlterUser(DfAlterUser {
            if_current_user: false,
            name: String::from("test@localhost"),
            hostname: String::from("%"),
            new_auth_type: AuthType::Sha256,
            new_password: String::from("password"),
        }),
    )?;

    expect_parse_ok(
        "ALTER USER 'test'@'localhost' NOT IDENTIFIED",
        DfStatement::AlterUser(DfAlterUser {
            if_current_user: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            new_auth_type: AuthType::None,
            new_password: String::from(""),
        }),
    )?;

    expect_parse_ok(
        "ALTER USER 'test'@'localhost'",
        DfStatement::AlterUser(DfAlterUser {
            if_current_user: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            new_auth_type: AuthType::None,
            new_password: String::from(""),
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
            priv_types: UserPrivilege::all_privileges(),
        }),
    )?;

    expect_parse_ok(
        "GRANT ALL PRIVILEGES ON * TO 'test'@'localhost'",
        DfStatement::GrantPrivilege(DfGrantStatement {
            name: String::from("test"),
            hostname: String::from("localhost"),
            on: DfGrantObject::Database(None),
            priv_types: UserPrivilege::all_privileges(),
        }),
    )?;

    expect_parse_ok(
        "GRANT INSERT ON `db1`.`tb1` TO 'test'@'localhost'",
        DfStatement::GrantPrivilege(DfGrantStatement {
            name: String::from("test"),
            hostname: String::from("localhost"),
            on: DfGrantObject::Table(Some("db1".into()), "tb1".into()),
            priv_types: {
                let mut privileges = UserPrivilege::empty();
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
                let mut privileges = UserPrivilege::empty();
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
                let mut privileges = UserPrivilege::empty();
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
                let mut privileges = UserPrivilege::empty();
                privileges.set_privilege(UserPrivilegeType::Select);
                privileges.set_privilege(UserPrivilegeType::Create);
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
            priv_types: UserPrivilege::all_privileges(),
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
            stage_params: StageParams::new("s3://load/files/", Credentials::S3 { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format: None,
            comments: "".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE STAGE IF NOT EXISTS test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z')",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: true,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials::S3 { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format: None,
            comments: "".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE STAGE IF NOT EXISTS test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=CSV compression=GZIP record_delimiter=',')",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: true,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials::S3 { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format: Some(FileFormat::Csv { compression: Compression::Gzip, record_delimiter: ",".to_string() }),
            comments: "".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE STAGE IF NOT EXISTS test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=CSV compression=GZIP record_delimiter=',') comments='test'",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: true,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials::S3 { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format: Some(FileFormat::Csv { compression: Compression::Gzip, record_delimiter: ",".to_string() }),
            comments: "test".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=Parquet compression=AUTO) comments='test'",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: false,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials::S3 { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format: Some(FileFormat::Parquet { compression: Compression::Auto}),
            comments: "test".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=csv compression=AUTO record_delimiter=NONE) comments='test'",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: false,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials::S3 { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format: Some(FileFormat::Csv { compression: Compression::Auto, record_delimiter: "".to_string() }),
            comments: "test".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=json) comments='test'",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: false,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials::S3 { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format: Some(FileFormat::Json ),
            comments: "test".to_string(),
        }),
    )?;

    expect_parse_err(
        "CREATE STAGE test_stage credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=csv compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("sql parser error: Missing URL"),
    )?;

    expect_parse_err(
        "CREATE STAGE test_stage url='s3://load/files/' password=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=csv compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("sql parser error: Missing CREDENTIALS"),
    )?;

    expect_parse_err(
        "CREATE STAGE test_stage url='s4://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=csv compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("sql parser error: Not supported storage"),
    )?;

    expect_parse_err(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=csv compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("sql parser error: Missing S3 ACCESS_KEY_ID"),
    )?;

    expect_parse_err(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' aecret_access_key='4x5y6z') file_format=(FORMAT=csv compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("sql parser error: Missing S3 SECRET_ACCESS_KEY"),
    )?;

    expect_parse_err(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(type=csv compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("sql parser error: Missing FORMAT"),
    )?;

    expect_parse_err(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(format=csv compression=AUTO1 record_delimiter=NONE) comments='test'",
        String::from("sql parser error: no match for compression"),
    )?;

    expect_parse_err(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(format=csv compression=AUTO record_delimiter=NONE1) comments='test'",
        String::from("sql parser error: Expected record delimiter NONE, found: NONE1"),
    )?;

    expect_parse_err(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(format=csv1 compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("sql parser error: Expected format type CSV|PARQUET|JSON, found: CSV1"),
    )?;

    Ok(())
}
