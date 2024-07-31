// Copyright 2023 Datafuse Labs.
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

use std::collections::BTreeSet;

use chrono::TimeZone;
use chrono::Utc;
use databend_common_meta_app::share;
use enumflags2::BitFlags;
use minitrace::func_name;

use crate::common;

// These bytes are built when a new version in introduced,
// and are kept for backward compatibility test.
//
// *************************************************************
// * These messages should never be updated,                   *
// * only be added when a new version is added,                *
// * or be removed when an old version is no longer supported. *
// *************************************************************
//
// The message bytes are built from the output of `test_pb_from_to()`
#[test]
fn test_v103_share_meta_v2() -> anyhow::Result<()> {
    let bytes: Vec<u8> = vec![
        10, 1, 97, 10, 1, 98, 18, 7, 99, 111, 109, 109, 101, 110, 116, 26, 23, 50, 48, 49, 52, 45,
        49, 49, 45, 50, 56, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 34, 23, 50, 48, 49,
        52, 45, 49, 49, 45, 50, 57, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 42, 39, 8,
        1, 18, 2, 100, 98, 24, 4, 34, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49, 50, 58,
        48, 48, 58, 48, 57, 32, 85, 84, 67, 160, 6, 103, 168, 6, 24, 50, 40, 8, 2, 18, 3, 100, 98,
        49, 24, 5, 34, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49, 50, 58, 48, 48, 58, 48,
        57, 32, 85, 84, 67, 160, 6, 103, 168, 6, 24, 58, 53, 8, 4, 18, 5, 116, 97, 98, 108, 101,
        24, 4, 32, 41, 42, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49, 50, 58, 48, 48, 58,
        48, 57, 32, 85, 84, 67, 50, 4, 86, 73, 69, 87, 58, 1, 42, 160, 6, 103, 168, 6, 24, 66, 53,
        8, 2, 18, 5, 116, 97, 98, 108, 101, 24, 5, 32, 42, 42, 23, 50, 48, 49, 52, 45, 49, 49, 45,
        50, 56, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 50, 4, 70, 85, 83, 69, 58, 1,
        41, 160, 6, 103, 168, 6, 24, 160, 6, 103, 168, 6, 24,
    ];

    let want = || {
        let now = Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap();

        let use_database = Some(share::ShareDatabase {
            privileges: BitFlags::from(share::ShareGrantObjectPrivilege::Usage),
            name: "db".to_string(),
            db_id: 4,
            grant_on: now,
        });

        let reference_database = vec![share::ShareDatabase {
            privileges: BitFlags::from(share::ShareGrantObjectPrivilege::ReferenceUsage),
            name: "db1".to_string(),
            db_id: 5,
            grant_on: now,
        }];

        let mut reference_table = BTreeSet::new();
        reference_table.insert(42);
        let table = vec![share::ShareTable {
            privileges: BitFlags::from(share::ShareGrantObjectPrivilege::Select),
            name: "table".to_string(),
            db_id: 4,
            table_id: 41,
            grant_on: now,
            engine: "VIEW".to_string(),
            reference_table,
        }];

        let mut reference_by = BTreeSet::new();
        reference_by.insert(41);
        let reference_table = vec![share::ShareReferenceTable {
            privileges: BitFlags::from(share::ShareGrantObjectPrivilege::ReferenceUsage),
            name: "table".to_string(),
            db_id: 5,
            table_id: 42,
            grant_on: now,
            engine: "FUSE".to_string(),
            reference_by,
        }];

        share::ShareMeta {
            accounts: BTreeSet::from_iter(vec![s("a"), s("b")]),
            comment: Some(s("comment")),
            create_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
            update_on: Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 9).unwrap(),

            use_database,
            reference_database,
            table,
            reference_table,
        }
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 103, want())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
