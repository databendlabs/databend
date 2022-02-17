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

use common_exception::Result;
use common_meta_types::UserIdentity;
use common_meta_types::UserPrivilegeSet;
use common_meta_types::UserPrivilegeType;
use databend_query::sql::statements::DfAlterUser;
use databend_query::sql::statements::DfAuthOption;
use databend_query::sql::statements::DfCreateUser;
use databend_query::sql::statements::DfDropUser;
use databend_query::sql::statements::DfGrantObject;
use databend_query::sql::statements::DfGrantStatement;
use databend_query::sql::statements::DfRevokeStatement;
use databend_query::sql::statements::DfShowGrants;
use databend_query::sql::*;

use crate::sql::sql_parser::*;

fn create_user_auth_test(
    auth_clause: &str,
    auth_type: Option<String>,
    auth_string: Option<String>,
) -> Result<()> {
    expect_parse_ok(
        &format!("CREATE USER 'test'@'localhost' {}", auth_clause),
        DfStatement::CreateUser(DfCreateUser {
            if_not_exists: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_options: DfAuthOption {
                auth_type,
                by_value: auth_string,
            },
        }),
    )
}

fn create_user_auth_test_normal(plugin_name: &str) -> Result<()> {
    let password = "password";
    let sql = format!("IDENTIFIED with {} BY '{}'", plugin_name, password);
    create_user_auth_test(
        &sql,
        Some(plugin_name.to_string()),
        Some(password.to_string()),
    )
}

fn alter_user_auth_test(
    auth_clause: &str,
    auth_type: Option<String>,
    auth_string: Option<String>,
) -> Result<()> {
    expect_parse_ok(
        &format!("ALTER USER 'test'@'localhost' {}", auth_clause),
        DfStatement::AlterUser(DfAlterUser {
            if_current_user: false,
            name: String::from("test"),
            hostname: String::from("localhost"),
            auth_option: DfAuthOption {
                auth_type,
                by_value: auth_string,
            },
        }),
    )
}

fn alter_user_auth_test_normal(plugin_name: &str) -> Result<()> {
    let password = "password";
    let sql = format!("IDENTIFIED with {} BY '{}'", plugin_name, password);
    alter_user_auth_test(
        &sql,
        Some(plugin_name.to_string()),
        Some(password.to_string()),
    )
}

#[test]
fn create_user_test() -> Result<()> {
    // normal
    create_user_auth_test_normal("plaintext_password")?;
    create_user_auth_test_normal("sha256_password")?;
    create_user_auth_test_normal("double_sha1_password")?;

    create_user_auth_test(
        "IDENTIFIED BY 'password'",
        None,
        Some("password".to_string()),
    )?;
    create_user_auth_test(
        "IDENTIFIED WITH no_password",
        Some("no_password".to_string()),
        None,
    )?;
    create_user_auth_test("NOT IDENTIFIED", Some("no_password".to_string()), None)?;
    create_user_auth_test("", None, None)?;

    // username contains '@'
    expect_parse_ok(
        "CREATE USER 'test@localhost'",
        DfStatement::CreateUser(DfCreateUser {
            if_not_exists: false,
            name: String::from("test@localhost"),
            hostname: String::from("%"),
            auth_options: DfAuthOption::default(),
        }),
    )?;

    // errors
    expect_parse_err(
        "CREATE USER 'test'@'localhost' IDENTIFIED WITH no_password BY 'password'",
        String::from("sql parser error: Expected end of statement, found: BY"),
    )?;

    expect_parse_err(
        "CREATE USER 'test'@'localhost' IDENTIFIED WITH sha256_password BY",
        String::from("sql parser error: Expected literal string, found: EOF"),
    )?;

    Ok(())
}

#[test]
fn alter_user_test() -> Result<()> {
    let password = "password".to_string();

    alter_user_auth_test_normal("plaintext_password")?;
    alter_user_auth_test_normal("sha256_password")?;
    alter_user_auth_test_normal("double_sha1_password")?;

    alter_user_auth_test(
        "IDENTIFIED WITH no_password",
        Some("no_password".to_string()),
        None,
    )?;

    alter_user_auth_test("IDENTIFIED BY 'password'", None, Some(password.clone()))?;

    alter_user_auth_test("NOT IDENTIFIED", Some("no_password".to_string()), None)?;

    alter_user_auth_test("", None, None)?;

    expect_parse_ok(
        "ALTER USER USER() IDENTIFIED BY 'password'",
        DfStatement::AlterUser(DfAlterUser {
            if_current_user: true,
            name: String::from(""),
            hostname: String::from(""),
            auth_option: DfAuthOption {
                auth_type: None,
                by_value: Some(password),
            },
        }),
    )?;

    expect_parse_ok(
        "ALTER USER 'test@localhost' IDENTIFIED WITH sha256_password BY 'password'",
        DfStatement::AlterUser(DfAlterUser {
            if_current_user: false,
            name: String::from("test@localhost"),
            hostname: String::from("%"),
            auth_option: DfAuthOption {
                auth_type: Some("sha256_password".to_string()),
                by_value: Some("password".to_string()),
            },
        }),
    )?;

    expect_parse_err(
        "ALTER USER 'test'@'localhost' IDENTIFIED WITH no_password BY 'password'",
        String::from("sql parser error: Expected end of statement, found: BY"),
    )?;

    expect_parse_err(
        "ALTER USER 'test'@'localhost' IDENTIFIED WITH sha256_password BY",
        String::from("sql parser error: Expected literal string, found: EOF"),
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
