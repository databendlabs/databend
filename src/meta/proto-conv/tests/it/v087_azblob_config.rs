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

use databend_common_meta_app::storage::StorageAzblobConfig;
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
#[test]
fn test_decode_v87_azblob_config() -> anyhow::Result<()> {
    // Encoded data of version 87 of databend_common_meta_app::storage::storage_params::StorageAzblobConfig:
    // It is generated with common::test_pb_from_to().
    let storage_azblob_config_v87 = vec![
        10, 19, 104, 116, 116, 112, 115, 58, 47, 47, 104, 101, 108, 108, 111, 46, 119, 111, 114,
        108, 100, 18, 4, 116, 101, 115, 116, 26, 15, 109, 121, 95, 97, 99, 99, 111, 117, 110, 116,
        95, 110, 97, 109, 101, 34, 14, 109, 121, 95, 97, 99, 99, 111, 117, 110, 116, 95, 107, 101,
        121, 42, 1, 47, 160, 6, 87, 168, 6, 24,
    ];

    let want = || StorageAzblobConfig {
        endpoint_url: "https://hello.world".to_string(),
        container: "test".to_string(),
        account_name: "my_account_name".to_string(),
        account_key: "my_account_key".to_string(),

        root: "/".to_string(),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(
        func_name!(),
        storage_azblob_config_v87.as_slice(),
        87,
        want(),
    )?;
    Ok(())
}
