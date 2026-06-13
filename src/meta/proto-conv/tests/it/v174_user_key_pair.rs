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

use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::PublicKeyEntry;
use fastrace::func_name;

use crate::common;

#[test]
fn test_decode_v174_auth_info_key_pair() -> anyhow::Result<()> {
    let auth_info_v174 = vec![
        34, 55, 10, 53, 10, 32, 77, 73, 73, 66, 73, 106, 65, 78, 66, 103, 107, 113, 104, 107, 105,
        71, 57, 119, 48, 66, 65, 81, 69, 70, 65, 65, 79, 67, 65, 81, 56, 65, 18, 11, 99, 105, 45,
        112, 105, 112, 101, 108, 105, 110, 101, 24, 192, 219, 189, 192, 6, 160, 6, 174, 1, 168, 6,
        24,
    ];

    let want = || AuthInfo::KeyPair {
        public_keys: vec![PublicKeyEntry {
            key: "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A".to_string(),
            label: "ci-pipeline".to_string(),
            created_at: 1745841600,
        }],
    };

    common::test_load_old(func_name!(), auth_info_v174.as_slice(), 174, want())?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}
