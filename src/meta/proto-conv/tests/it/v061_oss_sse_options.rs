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

use databend_common_meta_app::storage::StorageOssConfig;
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
#[test]
fn test_decode_v61_oss_sse_options() -> anyhow::Result<()> {
    let bytes = vec![
        10, 23, 104, 116, 116, 112, 115, 58, 47, 47, 111, 98, 115, 46, 101, 120, 97, 109, 112, 108,
        101, 46, 99, 111, 109, 18, 6, 98, 117, 99, 107, 101, 116, 26, 20, 47, 112, 97, 116, 104,
        47, 116, 111, 47, 115, 116, 97, 103, 101, 47, 102, 105, 108, 101, 115, 34, 13, 97, 99, 99,
        101, 115, 115, 95, 107, 101, 121, 95, 105, 100, 42, 17, 97, 99, 99, 101, 115, 115, 95, 107,
        101, 121, 95, 115, 101, 99, 114, 101, 116, 66, 6, 65, 69, 83, 50, 53, 54, 74, 3, 49, 50,
        51, 160, 6, 61, 168, 6, 24,
    ];

    let want = || StorageOssConfig {
        endpoint_url: "https://obs.example.com".to_string(),
        root: "/path/to/stage/files".to_string(),
        server_side_encryption: "AES256".to_string(),
        access_key_id: "access_key_id".to_string(),
        bucket: "bucket".to_string(),
        presign_endpoint_url: "".to_string(),
        access_key_secret: "access_key_secret".to_string(),
        server_side_encryption_key_id: "123".to_string(),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 61, want())?;
    Ok(())
}
