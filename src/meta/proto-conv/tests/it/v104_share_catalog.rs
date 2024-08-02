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

use chrono::TimeZone;
use chrono::Utc;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::ShareCatalogOption;
use fastrace::func_name;

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
fn test_v104_share_catalog() -> anyhow::Result<()> {
    let bytes: Vec<u8> = vec![
        18, 40, 34, 38, 10, 8, 112, 114, 111, 118, 105, 100, 101, 114, 18, 4, 116, 101, 115, 116,
        26, 14, 115, 104, 97, 114, 101, 95, 101, 110, 100, 112, 111, 105, 110, 116, 160, 6, 104,
        168, 6, 24, 162, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49, 50, 58, 48, 48, 58,
        48, 57, 32, 85, 84, 67, 160, 6, 104, 168, 6, 24,
    ];

    let want = || databend_common_meta_app::schema::CatalogMeta {
        catalog_option: CatalogOption::Share(ShareCatalogOption {
            provider: "provider".to_string(),
            share_name: "test".to_string(),
            share_endpoint: "share_endpoint".to_string(),
        }),
        created_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 104, want())
}
