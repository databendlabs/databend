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

use chrono::TimeZone;
use chrono::Utc;
use databend_common_meta_app::schema::CatalogMeta;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::PaimonCatalogOption;
use fastrace::func_name;
use maplit::hashmap;

use crate::common;

// These bytes are built when a new version is introduced,
// and are kept for backward compatibility test.
//
// *************************************************************
// * These messages should never be updated,                   *
// * only be added when a new version is added,                *
// * or be removed when an old version is no longer supported. *
// *************************************************************
//
// Encoded data of version 180 of CatalogMeta (Paimon catalog option).
// It is generated with common::test_pb_from_to().
#[test]
fn test_decode_v180_paimon_catalog_option() -> anyhow::Result<()> {
    let catalog_meta_v180 = vec![
        18, 70, 42, 68, 10, 23, 10, 9, 109, 101, 116, 97, 115, 116, 111, 114, 101, 18, 10, 102,
        105, 108, 101, 115, 121, 115, 116, 101, 109, 10, 34, 10, 9, 119, 97, 114, 101, 104, 111,
        117, 115, 101, 18, 21, 115, 51, 58, 47, 47, 98, 117, 99, 107, 101, 116, 47, 119, 97, 114,
        101, 104, 111, 117, 115, 101, 160, 6, 180, 1, 168, 6, 24, 162, 1, 23, 50, 48, 49, 52, 45,
        49, 49, 45, 50, 56, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 160, 6, 180, 1,
        168, 6, 24,
    ];

    let want = || CatalogMeta {
        catalog_option: CatalogOption::Paimon(PaimonCatalogOption {
            options: hashmap! {
                "metastore".to_string() => "filesystem".to_string(),
                "warehouse".to_string() => "s3://bucket/warehouse".to_string(),
            },
        }),
        created_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), catalog_meta_v180.as_slice(), 180, want())?;

    Ok(())
}
