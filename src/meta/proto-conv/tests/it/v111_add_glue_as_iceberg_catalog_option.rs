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
use databend_common_meta_app::schema::IcebergCatalogOption;
use databend_common_meta_app::schema::IcebergGlueCatalogOption;
use fastrace::func_name;
use maplit::hashmap;

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
fn test_v111_add_glue_as_iceberg_catalog_option() -> anyhow::Result<()> {
    let catalog_meta_v111 = vec![
        18, 169, 1, 26, 166, 1, 34, 157, 1, 10, 14, 115, 51, 58, 47, 47, 109, 121, 95, 98, 117, 99,
        107, 101, 116, 18, 37, 10, 10, 65, 87, 83, 95, 75, 69, 89, 95, 73, 68, 18, 23, 115, 117,
        112, 101, 114, 32, 115, 101, 99, 117, 114, 101, 32, 97, 99, 99, 101, 115, 115, 32, 107,
        101, 121, 18, 45, 10, 14, 65, 87, 83, 95, 83, 69, 67, 82, 69, 84, 95, 75, 69, 89, 18, 27,
        101, 118, 101, 110, 32, 109, 111, 114, 101, 32, 115, 101, 99, 117, 114, 101, 32, 115, 101,
        99, 114, 101, 116, 32, 107, 101, 121, 18, 47, 10, 6, 82, 69, 71, 73, 79, 78, 18, 37, 117,
        115, 45, 101, 97, 115, 116, 45, 49, 32, 97, 107, 97, 32, 97, 110, 116, 105, 45, 109, 117,
        108, 116, 105, 45, 97, 118, 97, 105, 108, 97, 98, 105, 108, 105, 116, 121, 160, 6, 111,
        168, 6, 24, 160, 6, 111, 168, 6, 24, 162, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56,
        32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 160, 6, 111, 168, 6, 24,
    ];

    let want = || databend_common_meta_app::schema::CatalogMeta {
        catalog_option: CatalogOption::Iceberg(IcebergCatalogOption::Glue(
            IcebergGlueCatalogOption {
                warehouse: "s3://my_bucket".to_string(),
                props: hashmap! {
                    s("AWS_KEY_ID") => s("super secure access key"),
                    s("AWS_SECRET_KEY") => s("even more secure secret key"),
                    s("REGION") => s("us-east-1 aka anti-multi-availability"),
                },
            },
        )),
        created_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), catalog_meta_v111.as_slice(), 111, want())?;

    Ok(())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
