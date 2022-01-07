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

use common_exception::exception::Result;
use common_meta_types::GrantEntry;
use common_meta_types::GrantObject;
use common_meta_types::UserGrantSet;
use common_meta_types::UserPrivilegeType;
use enumflags2::make_bitflags;

#[test]
fn test_user_grant_entry() -> Result<()> {
    let grant = GrantEntry::new(
        "u1".into(),
        "h1".into(),
        GrantObject::Global,
        make_bitflags!(UserPrivilegeType::{Create}),
    );
    assert!(grant.verify_database_privilege("u1", "h1", "db1", UserPrivilegeType::Create));
    assert!(!grant.verify_database_privilege("u1", "h1", "db1", UserPrivilegeType::Insert));
    assert!(!grant.verify_database_privilege("u1", "h2", "db1", UserPrivilegeType::Create));

    let grant = GrantEntry::new(
        "u1".into(),
        "%".into(),
        GrantObject::Database("db1".into()),
        make_bitflags!(UserPrivilegeType::{Create}),
    );
    assert!(grant.verify_table_privilege("u1", "h1", "db1", "table1", UserPrivilegeType::Create));
    assert!(!grant.verify_table_privilege("u1", "h1", "db2", "table1", UserPrivilegeType::Create));
    assert!(!grant.verify_table_privilege("u1", "h1", "db1", "table1", UserPrivilegeType::Insert));
    assert!(grant.verify_table_privilege("u1", "h233", "db1", "table1", UserPrivilegeType::Create));
    assert!(grant.verify_database_privilege("u1", "h233", "db1", UserPrivilegeType::Create));
    assert!(!grant.verify_database_privilege("u1", "h233", "db2", UserPrivilegeType::Create));

    let grant = GrantEntry::new(
        "u1".into(),
        "%".into(),
        GrantObject::Database("db1".into()),
        make_bitflags!(UserPrivilegeType::{Create}),
    );
    assert!(grant.verify_table_privilege("u1", "h1", "db1", "table1", UserPrivilegeType::Create));
    assert!(!grant.verify_table_privilege("u1", "h1", "db2", "table1", UserPrivilegeType::Create));
    assert!(!grant.verify_table_privilege("u1", "h1", "db1", "table1", UserPrivilegeType::Insert));
    assert!(grant.verify_table_privilege("u1", "h233", "db1", "table1", UserPrivilegeType::Create));

    Ok(())
}

#[test]
fn test_user_grant_set() -> Result<()> {
    let mut grants = UserGrantSet::empty();

    grants.grant_privileges(
        "u1",
        "h1",
        &GrantObject::Global,
        make_bitflags!(UserPrivilegeType::{Create}).into(),
    );
    grants.grant_privileges(
        "u1",
        "h1",
        &GrantObject::Global,
        make_bitflags!(UserPrivilegeType::{Insert}).into(),
    );
    grants.grant_privileges(
        "u1",
        "%",
        &GrantObject::Global,
        make_bitflags!(UserPrivilegeType::{Insert}).into(),
    );
    grants.grant_privileges(
        "u1",
        "%",
        &GrantObject::Table("db1".into(), "table1".into()),
        make_bitflags!(UserPrivilegeType::{Select | Create}).into(),
    );
    assert_eq!(3, grants.entries().len());

    grants.revoke_privileges(
        "u1",
        "%",
        &GrantObject::Global,
        make_bitflags!(UserPrivilegeType::{Insert}).into(),
    );
    assert_eq!(2, grants.entries().len());
    assert!(grants.verify_privilege(
        "u1",
        "h1",
        &GrantObject::Database("db1".into()),
        UserPrivilegeType::Create
    ));
    assert!(!grants.verify_privilege(
        "u1",
        "h1",
        &GrantObject::Database("db1".into()),
        UserPrivilegeType::Select
    ));
    assert!(grants.verify_privilege(
        "u1",
        "h1",
        &GrantObject::Table("db1".into(), "table1".into()),
        UserPrivilegeType::Create
    ));
    assert!(!grants.verify_privilege(
        "u1",
        "h2",
        &GrantObject::Table("db1".into(), "table1".into()),
        UserPrivilegeType::Insert
    ));
    assert!(grants.verify_privilege(
        "u1",
        "h3",
        &GrantObject::Table("db1".into(), "table1".into()),
        UserPrivilegeType::Select
    ));
    Ok(())
}
