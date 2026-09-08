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
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::storage::StorageS3Config;
use fastrace::func_name;

use crate::common;

fn want() -> DataShareMeta {
    DataShareMeta {
        provider: "provider".to_string(),
        name: "sales".to_string(),
        created_on: Utc.timestamp_opt(1_757_894_400, 0).unwrap(),
        comment: Some("shared sales data".to_string()),
        accounts: BTreeSet::from(["consumer".to_string()]),
        database: Some(DataShareDatabaseGrant {
            database_id: 11,
            shared_on: Utc.timestamp_opt(1_757_894_401, 0).unwrap(),
        }),
        tables: BTreeMap::from([(101, DataShareTableGrant {
            shared_on: Utc.timestamp_opt(1_757_894_402, 0).unwrap(),
            storage_params: Some(StorageParams::S3(StorageS3Config {
                endpoint_url: "http://192.168.1.100:9001".to_string(),
                region: "us-east-1".to_string(),
                bucket: "v-wubx".to_string(),
                root: "t0807".to_string(),
                enable_virtual_host_style: false,
                ..Default::default()
            })),
        })]),
        connection: Some("share_conn".to_string()),
    }
}

// These bytes are built when version 184 is introduced and must not be changed.
#[test]
fn test_v184_data_share_storage_round_trip() -> anyhow::Result<()> {
    let data_share_meta_v184 = vec![
        10, 8, 112, 114, 111, 118, 105, 100, 101, 114, 18, 5, 115, 97, 108, 101, 115, 26, 23, 50,
        48, 50, 53, 45, 48, 57, 45, 49, 53, 32, 48, 48, 58, 48, 48, 58, 48, 48, 32, 85, 84, 67, 34,
        17, 115, 104, 97, 114, 101, 100, 32, 115, 97, 108, 101, 115, 32, 100, 97, 116, 97, 42, 8,
        99, 111, 110, 115, 117, 109, 101, 114, 50, 27, 8, 11, 18, 23, 50, 48, 50, 53, 45, 48, 57,
        45, 49, 53, 32, 48, 48, 58, 48, 48, 58, 48, 49, 32, 85, 84, 67, 58, 93, 8, 101, 18, 89, 10,
        23, 50, 48, 50, 53, 45, 48, 57, 45, 49, 53, 32, 48, 48, 58, 48, 48, 58, 48, 50, 32, 85, 84,
        67, 18, 62, 10, 60, 10, 9, 117, 115, 45, 101, 97, 115, 116, 45, 49, 18, 25, 104, 116, 116,
        112, 58, 47, 47, 49, 57, 50, 46, 49, 54, 56, 46, 49, 46, 49, 48, 48, 58, 57, 48, 48, 49,
        42, 6, 118, 45, 119, 117, 98, 120, 50, 5, 116, 48, 56, 48, 55, 160, 6, 184, 1, 168, 6, 24,
        66, 10, 115, 104, 97, 114, 101, 95, 99, 111, 110, 110, 160, 6, 184, 1, 168, 6, 24,
    ];

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), &data_share_meta_v184, 184, want())?;
    Ok(())
}
