// Copyright 2021 Datafuse Labs
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

use databend_common_meta_app::principal::GrantEntry;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserGrantSet;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_app::principal::UserPrivilegeType;
use enumflags2::make_bitflags;

#[test]
fn test_attach_table_privilege_scope() {
    let attach = UserPrivilegeType::AttachTable;
    assert_eq!(attach as u64, 1u64 << 32);
    assert_eq!(attach.to_string(), "ATTACH TABLE");
    assert_eq!(
        UserPrivilegeType::from(databend_common_ast::ast::UserPrivilegeType::AttachTable),
        attach
    );
    assert!(UserPrivilegeSet::available_privileges_on_global().has_privilege(attach));
    for ownership in [false, true] {
        assert!(
            UserPrivilegeSet::available_privileges_on_database(ownership).has_privilege(attach)
        );
        assert!(!UserPrivilegeSet::available_privileges_on_table(ownership).has_privilege(attach));
        assert!(!UserPrivilegeSet::available_privileges_on_stage(ownership).has_privilege(attach));
        assert!(
            !UserPrivilegeSet::available_privileges_on_connection(ownership).has_privilege(attach)
        );
    }
}

#[test]
fn test_attach_table_privilege_json() -> anyhow::Result<()> {
    let flags = enumflags2::BitFlags::<UserPrivilegeType>::from(UserPrivilegeType::AttachTable);
    let json = serde_json::to_string(&flags)?;
    assert_eq!(json, "4294967296");
    assert_eq!(
        serde_json::from_str::<enumflags2::BitFlags<UserPrivilegeType>>(&json)?,
        flags
    );
    Ok(())
}

#[test]
fn test_attach_table_grant_revoke() {
    let object = GrantObject::DatabaseById("default".to_string(), 42);
    let mut grants = UserGrantSet::default();
    grants.grant_privileges(&object, UserPrivilegeType::Create.into());
    assert!(!grants.verify_privilege(&object, UserPrivilegeType::AttachTable));
    grants.grant_privileges(&object, UserPrivilegeType::AttachTable.into());
    assert!(grants.verify_privilege(&object, UserPrivilegeType::AttachTable));
    grants.revoke_privileges(&object, UserPrivilegeType::AttachTable.into());
    assert!(!grants.verify_privilege(&object, UserPrivilegeType::AttachTable));
    assert!(grants.verify_privilege(&object, UserPrivilegeType::Create));
}

#[test]
fn test_grant_object_contains() -> anyhow::Result<()> {
    struct Test {
        lhs: GrantObject,
        rhs: GrantObject,
        expect: bool,
    }
    let tests: Vec<Test> = vec![
        Test {
            lhs: GrantObject::Global,
            rhs: GrantObject::Table("default".into(), "a".into(), "b".into()),
            expect: true,
        },
        Test {
            lhs: GrantObject::Global,
            rhs: GrantObject::Global,
            expect: true,
        },
        Test {
            lhs: GrantObject::Global,
            rhs: GrantObject::Database("default".into(), "a".into()),
            expect: true,
        },
        Test {
            lhs: GrantObject::Database("default".into(), "a".into()),
            rhs: GrantObject::Global,
            expect: false,
        },
        Test {
            lhs: GrantObject::Database("default".into(), "a".into()),
            rhs: GrantObject::Database("default".into(), "b".into()),
            expect: false,
        },
        Test {
            lhs: GrantObject::Database("default".into(), "a".into()),
            rhs: GrantObject::Table("default".into(), "b".into(), "c".into()),
            expect: false,
        },
        Test {
            lhs: GrantObject::Database("default".into(), "db1".into()),
            rhs: GrantObject::Table("default".into(), "db1".into(), "c".into()),
            expect: true,
        },
        Test {
            lhs: GrantObject::Table("default".into(), "db1".into(), "c".into()),
            rhs: GrantObject::Table("default".into(), "db1".into(), "c".into()),
            expect: true,
        },
        Test {
            lhs: GrantObject::Table("default".into(), "db1".into(), "c".into()),
            rhs: GrantObject::Global,
            expect: false,
        },
        Test {
            lhs: GrantObject::Table("default".into(), "db1".into(), "c".into()),
            rhs: GrantObject::Database("default".into(), "db1".into()),
            expect: false,
        },
        Test {
            lhs: GrantObject::Stage("c".into()),
            rhs: GrantObject::Stage("c".into()),
            expect: true,
        },
        Test {
            lhs: GrantObject::Stage("c".into()),
            rhs: GrantObject::Stage("c".into()),
            expect: true,
        },
        Test {
            lhs: GrantObject::UDF("c".into()),
            rhs: GrantObject::UDF("c".into()),
            expect: true,
        },
        Test {
            lhs: GrantObject::UDF("a".into()),
            rhs: GrantObject::UDF("c".into()),
            expect: false,
        },
        Test {
            lhs: GrantObject::Stage("a".into()),
            rhs: GrantObject::UDF("a".into()),
            expect: false,
        },
        Test {
            lhs: GrantObject::Stage("a".into()),
            rhs: GrantObject::Table("default".into(), "db1".into(), "c".into()),
            expect: false,
        },
    ];
    for t in tests {
        assert_eq!(
            t.lhs.contains(&t.rhs),
            t.expect,
            "{} contains {} expect {}",
            t.lhs,
            t.rhs,
            t.expect,
        );
    }
    Ok(())
}

#[test]
fn test_user_grant_entry() -> anyhow::Result<()> {
    let grant = GrantEntry::new(
        GrantObject::Global,
        make_bitflags!(UserPrivilegeType::{Create}),
    );
    assert!(grant.verify_privilege(
        &GrantObject::Database("default".into(), "db1".into()),
        UserPrivilegeType::Create
    ));
    assert!(!grant.verify_privilege(
        &GrantObject::Database("default".into(), "db1".into()),
        UserPrivilegeType::Insert
    ));
    assert!(grant.verify_privilege(
        &GrantObject::Database("default".into(), "db2".into()),
        UserPrivilegeType::Create
    ));

    let grant = GrantEntry::new(
        GrantObject::Database("default".into(), "db1".into()),
        make_bitflags!(UserPrivilegeType::{Create}),
    );
    assert!(grant.verify_privilege(
        &GrantObject::Table("default".into(), "db1".into(), "table1".into()),
        UserPrivilegeType::Create
    ));
    assert!(!grant.verify_privilege(
        &GrantObject::Table("default".into(), "db2".into(), "table1".into()),
        UserPrivilegeType::Create
    ));
    assert!(grant.verify_privilege(
        &GrantObject::Database("default".into(), "db1".into()),
        UserPrivilegeType::Create
    ));

    let grant = GrantEntry::new(
        GrantObject::Database("default".into(), "db1".into()),
        make_bitflags!(UserPrivilegeType::{Create}),
    );
    assert!(grant.verify_privilege(
        &GrantObject::Table("default".into(), "db1".into(), "table1".into()),
        UserPrivilegeType::Create
    ));
    assert!(!grant.verify_privilege(
        &GrantObject::Table("default".into(), "db2".into(), "table1".into()),
        UserPrivilegeType::Create
    ));
    assert!(!grant.verify_privilege(
        &GrantObject::Table("default".into(), "db1".into(), "table1".into()),
        UserPrivilegeType::Insert
    ));
    assert!(grant.verify_privilege(
        &GrantObject::Table("default".into(), "db1".into(), "table1".into()),
        UserPrivilegeType::Create
    ));

    Ok(())
}

#[test]
fn test_user_grant_set() -> anyhow::Result<()> {
    let mut grants = UserGrantSet::empty();

    grants.grant_privileges(
        &GrantObject::Global,
        make_bitflags!(UserPrivilegeType::{Create}).into(),
    );
    grants.grant_privileges(
        &GrantObject::Global,
        make_bitflags!(UserPrivilegeType::{Insert}).into(),
    );
    grants.grant_privileges(
        &GrantObject::Global,
        make_bitflags!(UserPrivilegeType::{Insert}).into(),
    );
    grants.grant_privileges(
        &GrantObject::Table("default".into(), "db1".into(), "table1".into()),
        make_bitflags!(UserPrivilegeType::{Select | Create}).into(),
    );
    assert_eq!(2, grants.entries().len());

    grants.revoke_privileges(
        &GrantObject::Global,
        make_bitflags!(UserPrivilegeType::{Insert}).into(),
    );
    grants.revoke_privileges(
        &GrantObject::Global,
        make_bitflags!(UserPrivilegeType::{Insert}).into(),
    );

    assert_eq!(2, grants.entries().len());

    assert!(grants.verify_privilege(
        &GrantObject::Database("default".into(), "db1".into()),
        UserPrivilegeType::Create
    ));
    assert!(!grants.verify_privilege(
        &GrantObject::Database("default".into(), "db1".into()),
        UserPrivilegeType::Select
    ));
    assert!(grants.verify_privilege(
        &GrantObject::Table("default".into(), "db1".into(), "table1".into()),
        UserPrivilegeType::Create
    ));
    assert!(!grants.verify_privilege(
        &GrantObject::Table("default".into(), "db1".into(), "table1".into()),
        UserPrivilegeType::Insert
    ));
    assert!(grants.verify_privilege(
        &GrantObject::Table("default".into(), "db1".into(), "table1".into()),
        UserPrivilegeType::Select
    ));
    Ok(())
}
