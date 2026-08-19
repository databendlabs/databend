// Copyright 2026 Datafuse Labs.
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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use chrono::TimeZone;
use chrono::Utc;
use databend_common_meta_app::data_share::DataShareDatabaseGrant;
use databend_common_meta_app::data_share::DataShareMeta;
use databend_common_meta_app::data_share::DataShareTableGrant;
use fastrace::func_name;

use crate::common;

fn want() -> DataShareMeta {
    DataShareMeta {
        provider: "provider".to_string(),
        name: "sales".to_string(),
        created_on: Utc.timestamp_opt(1_754_956_800, 0).unwrap(),
        comment: Some("shared sales data".to_string()),
        accounts: BTreeSet::from(["consumer_a".to_string(), "consumer_b".to_string()]),
        database: Some(DataShareDatabaseGrant {
            database: "analytics".to_string(),
            database_id: 11,
            shared_on: Utc.timestamp_opt(1_754_956_801, 0).unwrap(),
        }),
        tables: BTreeMap::from([("orders".to_string(), DataShareTableGrant {
            table_id: 101,
            shared_on: Utc.timestamp_opt(1_754_956_802, 0).unwrap(),
        })]),
        connection: Some("share_conn".to_string()),
    }
}

// These bytes are built when version 183 is introduced and must not be changed.
#[test]
fn test_decode_v183_data_share() -> anyhow::Result<()> {
    let data_share_meta_v183 = vec![
        10, 8, 112, 114, 111, 118, 105, 100, 101, 114, 18, 5, 115, 97, 108, 101, 115, 26, 23, 50,
        48, 50, 53, 45, 48, 56, 45, 49, 50, 32, 48, 48, 58, 48, 48, 58, 48, 48, 32, 85, 84, 67, 34,
        17, 115, 104, 97, 114, 101, 100, 32, 115, 97, 108, 101, 115, 32, 100, 97, 116, 97, 42, 10,
        99, 111, 110, 115, 117, 109, 101, 114, 95, 97, 42, 10, 99, 111, 110, 115, 117, 109, 101,
        114, 95, 98, 50, 38, 10, 9, 97, 110, 97, 108, 121, 116, 105, 99, 115, 16, 11, 26, 23, 50,
        48, 50, 53, 45, 48, 56, 45, 49, 50, 32, 48, 48, 58, 48, 48, 58, 48, 49, 32, 85, 84, 67, 58,
        37, 10, 6, 111, 114, 100, 101, 114, 115, 18, 27, 8, 101, 18, 23, 50, 48, 50, 53, 45, 48,
        56, 45, 49, 50, 32, 48, 48, 58, 48, 48, 58, 48, 50, 32, 85, 84, 67, 66, 10, 115, 104, 97,
        114, 101, 95, 99, 111, 110, 110, 160, 6, 183, 1, 168, 6, 24,
    ];

    common::test_load_old(func_name!(), data_share_meta_v183.as_slice(), 183, want())?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}
