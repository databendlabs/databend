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
fn test_grant_object_contains() -> Result<()> {
    struct Test {
        lhs: GrantObject,
        rhs: GrantObject,
        expect: bool,
    }
    let tests: Vec<Test> = vec![
        Test {
            lhs: GrantObject::Global,
            rhs: GrantObject::Table("a".into(), "b".into()),
            expect: true,
        },
        Test {
            lhs: GrantObject::Global,
            rhs: GrantObject::Global,
            expect: true,
        },
        Test {
            lhs: GrantObject::Global,
            rhs: GrantObject::Database("a".into()),
            expect: true,
        },
        Test {
            lhs: GrantObject::Database("a".into()),
            rhs: GrantObject::Global,
            expect: false,
        },
        Test {
            lhs: GrantObject::Database("a".into()),
            rhs: GrantObject::Database("b".into()),
            expect: false,
        },
        Test {
            lhs: GrantObject::Database("a".into()),
            rhs: GrantObject::Table("b".into(), "c".into()),
            expect: false,
        },
        Test {
            lhs: GrantObject::Database("db1".into()),
            rhs: GrantObject::Table("db1".into(), "c".into()),
            expect: true,
        },
        Test {
            lhs: GrantObject::Table("db1".into(), "c".into()),
            rhs: GrantObject::Table("db1".into(), "c".into()),
            expect: true,
        },
        Test {
            lhs: GrantObject::Table("db1".into(), "c".into()),
            rhs: GrantObject::Global,
            expect: false,
        },
        Test {
            lhs: GrantObject::Table("db1".into(), "c".into()),
            rhs: GrantObject::Database("db1".into()),
            expect: false,
        },
    ];
    for t in tests {
        assert_eq!(
            t.lhs.contains(&t.rhs),
            t.expect,
            "{} contains {} expect {}",
            &t.lhs,
            &t.rhs,
            &t.expect,
        );
    }
    Ok(())
}

#[test]
fn test_user_grant_entry() -> Result<()> {
    let grant = GrantEntry::new(
        "u1".into(),
        "h1".into(),
        GrantObject::Global,
        make_bitflags!(UserPrivilegeType::{Create}),
    );
    assert!(grant.verify_privilege(
        &GrantObject::Database("db1".into()),
        UserPrivilegeType::Create
    ));
    assert!(!grant.verify_privilege(
        &GrantObject::Database("db1".into()),
        UserPrivilegeType::Insert
    ));
    assert!(grant.verify_privilege(
        &GrantObject::Database("db2".into()),
        UserPrivilegeType::Create
    ));

    let grant = GrantEntry::new(
        "u1".into(),
        "%".into(),
        GrantObject::Database("db1".into()),
        make_bitflags!(UserPrivilegeType::{Create}),
    );
    assert!(grant.verify_privilege(
        &GrantObject::Table("db1".into(), "table1".into()),
        UserPrivilegeType::Create
    ));
    assert!(!grant.verify_privilege(
        &GrantObject::Table("db2".into(), "table1".into()),
        UserPrivilegeType::Create
    ));
    assert!(grant.verify_privilege(
        &GrantObject::Database("db1".into()),
        UserPrivilegeType::Create
    ));

    let grant = GrantEntry::new(
        "u1".into(),
        "%".into(),
        GrantObject::Database("db1".into()),
        make_bitflags!(UserPrivilegeType::{Create}),
    );
    assert!(grant.verify_privilege(
        &GrantObject::Table("db1".into(), "table1".into()),
        UserPrivilegeType::Create
    ));
    assert!(!grant.verify_privilege(
        &GrantObject::Table("db2".into(), "table1".into()),
        UserPrivilegeType::Create
    ));
    assert!(!grant.verify_privilege(
        &GrantObject::Table("db1".into(), "table1".into()),
        UserPrivilegeType::Insert
    ));
    assert!(grant.verify_privilege(
        &GrantObject::Table("db1".into(), "table1".into()),
        UserPrivilegeType::Create
    ));

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
    assert_eq!(2, grants.entries().len());

    grants.revoke_privileges(
        &GrantObject::Global,
        make_bitflags!(UserPrivilegeType::{Insert}).into(),
    );
    assert_eq!(2, grants.entries().len());
    assert!(grants.verify_privilege(
        &GrantObject::Database("db1".into()),
        UserPrivilegeType::Create
    ));
    assert!(!grants.verify_privilege(
        &GrantObject::Database("db1".into()),
        UserPrivilegeType::Select
    ));
    assert!(grants.verify_privilege(
        &GrantObject::Table("db1".into(), "table1".into()),
        UserPrivilegeType::Create
    ));
    assert!(!grants.verify_privilege(
        &GrantObject::Table("db1".into(), "table1".into()),
        UserPrivilegeType::Insert
    ));
    assert!(grants.verify_privilege(
        &GrantObject::Table("db1".into(), "table1".into()),
        UserPrivilegeType::Select
    ));
    Ok(())
}
