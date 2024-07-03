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

use std::collections::BTreeMap;

use chrono::TimeZone;
use chrono::Utc;
use databend_common_meta_app::share::ShareCredential;
use databend_common_meta_app::share::ShareCredentialHmac;
use databend_common_meta_app::share::ShareEndpointMeta;
use minitrace::func_name;

use crate::common;

#[test]
fn test_decode_v95_share_endpoint_meta() -> anyhow::Result<()> {
    let bytes = vec![
        10, 21, 104, 116, 116, 112, 58, 47, 47, 49, 50, 55, 46, 48, 46, 48, 46, 49, 58, 50, 50, 50,
        50, 26, 12, 10, 3, 107, 101, 121, 18, 5, 118, 97, 108, 117, 101, 34, 7, 99, 111, 109, 109,
        101, 110, 116, 42, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49, 50, 58, 48, 48, 58,
        48, 57, 32, 85, 84, 67, 50, 15, 10, 13, 10, 5, 104, 101, 108, 108, 111, 160, 6, 94, 168, 6,
        24, 160, 6, 95, 168, 6, 24,
    ];

    let create_on = Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap();
    let mut args: BTreeMap<String, String> = BTreeMap::new();
    args.insert("key".to_string(), "value".to_string());

    let want = || ShareEndpointMeta {
        url: "http://127.0.0.1:2222".to_string(),
        tenant: "".to_string(),
        args: args.clone(),
        comment: Some("comment".to_string()),
        create_on,
        credential: Some(ShareCredential::HMAC(ShareCredentialHmac {
            key: "hello".to_string(),
        })),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 95, want())?;

    Ok(())
}
