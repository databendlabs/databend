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
use databend_common_meta_app::schema as mt;
use databend_common_meta_app::schema::ShareDbId;
use databend_common_meta_app::share::share_name_ident::ShareNameIdentRaw;
use maplit::btreemap;
use minitrace::func_name;

use crate::common;

#[test]
fn v099_database_meta() -> anyhow::Result<()> {
    let bytes: Vec<u8> = vec![
        34, 10, 10, 3, 120, 121, 122, 18, 3, 102, 111, 111, 42, 2, 52, 52, 50, 10, 10, 3, 97, 98,
        99, 18, 3, 100, 101, 102, 162, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49, 50,
        58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 170, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 57,
        32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 178, 1, 7, 102, 111, 111, 32, 98, 97,
        114, 202, 1, 21, 10, 6, 116, 101, 110, 97, 110, 116, 18, 5, 115, 104, 97, 114, 101, 160, 6,
        99, 168, 6, 24, 218, 1, 8, 101, 110, 100, 112, 111, 105, 110, 116, 226, 1, 11, 10, 9, 8,
        128, 8, 160, 6, 99, 168, 6, 24, 160, 6, 99, 168, 6, 24,
    ];

    let want = || mt::DatabaseMeta {
        engine: "44".to_string(),
        engine_options: btreemap! {s("abc") => s("def")},
        options: btreemap! {s("xyz") => s("foo")},
        created_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
        updated_on: Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 9).unwrap(),
        comment: "foo bar".to_string(),
        drop_on: None,
        shared_by: BTreeSet::new(),
        from_share: Some(ShareNameIdentRaw::new("tenant", "share")),
        using_share_endpoint: Some("endpoint".to_string()),
        from_share_db_id: Some(ShareDbId::Usage(1024)),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 99, want())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
